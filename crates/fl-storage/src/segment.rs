//! One segment file of the log.
//!
//! A segment starts with a 16-byte header and is followed by frames:
//!
//! ```text
//! u32  magic "FLSG"
//! u32  version
//! u64  base index, the log index of the first entry in the file
//! ```

use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use fl_types::{LogIndex, Term};

use crate::codec;
use crate::error::{Error, Result};
use crate::fsutil;
use crate::index::{self, TermRun};

/// Bytes in the segment header.
pub(crate) const HEADER_LEN: u64 = 16;

const MAGIC: &[u8; 4] = b"FLSG";
const VERSION: u32 = 1;

/// Path of the segment file whose first entry is `base`.
pub(crate) fn log_path(dir: &Path, base: LogIndex) -> PathBuf {
    dir.join(format!("{:020}.log", base.get()))
}

/// Path of the index file for the segment whose first entry is `base`.
pub(crate) fn idx_path(dir: &Path, base: LogIndex) -> PathBuf {
    dir.join(format!("{:020}.idx", base.get()))
}

/// Recovers a segment's base index from its file name.
pub(crate) fn parse_base(file_name: &str) -> Option<LogIndex> {
    let stem = file_name.strip_suffix(".log")?;
    if stem.len() != 20 || !stem.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    stem.parse().ok().map(LogIndex::new)
}

/// An open segment file plus the position of every frame in it.
pub(crate) struct Segment {
    /// Log index of the first entry.
    pub base: LogIndex,
    /// Path of the segment file.
    pub path: PathBuf,
    file: File,
    /// File position where each entry's frame starts.
    positions: Vec<u32>,
    /// One past the last valid byte.
    end: u64,
}

/// The result of opening an existing segment.
pub(crate) struct Opened {
    pub segment: Segment,
    /// Term runs found in this segment alone.
    pub runs: Vec<TermRun>,
    /// Bytes cut from the end of the file during recovery.
    pub truncated_bytes: u64,
    /// True if the index file was missing or bad and had to be rebuilt.
    pub rebuilt_index: bool,
}

impl Segment {
    /// Creates a new, empty segment and syncs it. The caller must sync the
    /// directory afterwards.
    pub(crate) fn create(dir: &Path, base: LogIndex) -> Result<Segment> {
        let path = log_path(dir, base);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .map_err(Error::io(&path))?;
        write_header(&file, &path, base)?;
        Ok(Segment {
            base,
            path,
            file,
            positions: Vec::new(),
            end: HEADER_LEN,
        })
    }

    /// Opens a segment that is not the last one in the log.
    ///
    /// Uses the index file if it is valid. Otherwise scans the segment,
    /// which must be clean from start to end, and rewrites the index.
    pub(crate) fn open_sealed(dir: &Path, base: LogIndex) -> Result<Opened> {
        let path = log_path(dir, base);
        let file = open_rw(&path)?;
        let file_len = file.metadata().map_err(Error::io(&path))?.len();

        if let Some(loaded) = index::load(&idx_path(dir, base), base, file_len) {
            return Ok(Opened {
                segment: Segment {
                    base,
                    path,
                    file,
                    positions: loaded.positions,
                    end: file_len,
                },
                runs: loaded.runs,
                truncated_bytes: 0,
                rebuilt_index: false,
            });
        }

        let scan = scan(&path, base)?;
        match scan.header {
            Header::Valid => {}
            Header::Short => {
                return Err(Error::corrupt(&path, 0, "segment header is incomplete"));
            }
            Header::Invalid(reason) => return Err(Error::corrupt(&path, 0, reason)),
        }
        if let Some(reason) = scan.stopped {
            return Err(Error::corrupt(&path, scan.valid_end, reason));
        }

        let segment = Segment {
            base,
            path,
            file,
            positions: scan.positions,
            end: scan.valid_end,
        };
        segment.write_index(dir, &scan.runs)?;
        Ok(Opened {
            segment,
            runs: scan.runs,
            truncated_bytes: 0,
            rebuilt_index: true,
        })
    }

    /// Opens the last segment in the log and repairs a torn tail.
    ///
    /// The file is always scanned. Anything after the last good frame is cut
    /// off, because the only writes that can be incomplete are the ones that
    /// were in flight at the end of this file.
    pub(crate) fn open_active(dir: &Path, base: LogIndex) -> Result<Opened> {
        let path = log_path(dir, base);
        let file = open_rw(&path)?;

        // The active segment has no index. A leftover one is stale.
        fsutil::remove_if_exists(&idx_path(dir, base))?;

        let scan = scan(&path, base)?;
        let truncated_bytes = match scan.header {
            Header::Invalid(reason) => return Err(Error::corrupt(&path, 0, reason)),
            Header::Short => {
                // The process died while creating the file.
                file.set_len(0).map_err(Error::io(&path))?;
                write_header(&file, &path, base)?;
                scan.file_len
            }
            Header::Valid => {
                let extra = scan.file_len - scan.valid_end;
                if extra > 0 {
                    file.set_len(scan.valid_end).map_err(Error::io(&path))?;
                    file.sync_all().map_err(Error::io(&path))?;
                }
                extra
            }
        };

        Ok(Opened {
            segment: Segment {
                base,
                path,
                file,
                positions: scan.positions,
                end: scan.valid_end,
            },
            runs: scan.runs,
            truncated_bytes,
            rebuilt_index: false,
        })
    }

    /// Number of entries in the segment.
    pub(crate) fn count(&self) -> usize {
        self.positions.len()
    }

    /// The index the next appended entry will have.
    pub(crate) fn next_index(&self) -> LogIndex {
        LogIndex::new(self.base.get() + self.positions.len() as u64)
    }

    /// Bytes of valid data in the file, header included.
    pub(crate) fn end(&self) -> u64 {
        self.end
    }

    /// File position where the entry in `slot` starts.
    pub(crate) fn position(&self, slot: usize) -> u64 {
        u64::from(self.positions[slot])
    }

    /// Length in bytes of the frame in `slot`.
    pub(crate) fn frame_len(&self, slot: usize) -> usize {
        let start = self.position(slot);
        let end = match self.positions.get(slot + 1) {
            Some(&next) => u64::from(next),
            None => self.end,
        };
        (end - start) as usize
    }

    /// Reads the raw frames for slots `from..to` in one call.
    pub(crate) fn read_frames(&self, from: usize, to: usize) -> Result<Vec<u8>> {
        debug_assert!(from < to && to <= self.count());
        let start = self.position(from);
        let end = if to < self.count() {
            self.position(to)
        } else {
            self.end
        };
        let mut buf = vec![0u8; (end - start) as usize];
        self.file
            .read_exact_at(&mut buf, start)
            .map_err(Error::io(&self.path))?;
        Ok(buf)
    }

    /// Writes already-encoded frames at the end of the file. `frame_lens`
    /// gives the length of each frame in `bytes`, in order.
    ///
    /// If the write fails, the in-memory state is unchanged. Any partial
    /// bytes on disk sit past `end` and are overwritten by the next append
    /// or removed by recovery.
    pub(crate) fn append(&mut self, bytes: &[u8], frame_lens: &[usize]) -> Result<()> {
        debug_assert_eq!(bytes.len(), frame_lens.iter().sum::<usize>());
        self.file
            .write_all_at(bytes, self.end)
            .map_err(Error::io(&self.path))?;
        let mut position = self.end;
        for len in frame_lens {
            self.positions.push(position as u32);
            position += *len as u64;
        }
        self.end = position;
        Ok(())
    }

    /// Flushes the file's data to disk.
    pub(crate) fn sync(&self) -> Result<()> {
        self.file.sync_data().map_err(Error::io(&self.path))
    }

    /// Drops every entry from `slot` onward and syncs the shorter file.
    pub(crate) fn truncate(&mut self, slot: usize) -> Result<()> {
        if slot >= self.count() {
            return Ok(());
        }
        let new_end = self.position(slot);
        self.file.set_len(new_end).map_err(Error::io(&self.path))?;
        self.file.sync_all().map_err(Error::io(&self.path))?;
        self.positions.truncate(slot);
        self.end = new_end;
        Ok(())
    }

    /// Writes the index file for this segment. `runs` must be the term runs
    /// of this segment only.
    pub(crate) fn write_index(&self, dir: &Path, runs: &[TermRun]) -> Result<()> {
        let bytes = index::encode(self.base, self.end, &self.positions, runs);
        fsutil::write_atomic(&idx_path(dir, self.base), &bytes)
    }
}

fn open_rw(path: &Path) -> Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(Error::io(path))
}

fn write_header(file: &File, path: &Path, base: LogIndex) -> Result<()> {
    let mut header = [0u8; HEADER_LEN as usize];
    header[0..4].copy_from_slice(MAGIC);
    header[4..8].copy_from_slice(&VERSION.to_le_bytes());
    header[8..16].copy_from_slice(&base.get().to_le_bytes());
    file.write_all_at(&header, 0).map_err(Error::io(path))?;
    file.sync_all().map_err(Error::io(path))
}

enum Header {
    Valid,
    /// The file is shorter than a header.
    Short,
    /// A full header is present but wrong.
    Invalid(&'static str),
}

struct Scan {
    header: Header,
    positions: Vec<u32>,
    runs: Vec<TermRun>,
    /// One past the last good frame.
    valid_end: u64,
    file_len: u64,
    /// Why the scan stopped before the end of the file, if it did.
    stopped: Option<&'static str>,
}

/// Reads a whole segment and checks every frame in order.
fn scan(path: &Path, base: LogIndex) -> Result<Scan> {
    let bytes = fs::read(path).map_err(Error::io(path))?;
    let mut scan = Scan {
        header: Header::Valid,
        positions: Vec::new(),
        runs: Vec::new(),
        valid_end: HEADER_LEN,
        file_len: bytes.len() as u64,
        stopped: None,
    };

    if scan.file_len < HEADER_LEN {
        scan.header = Header::Short;
        return Ok(scan);
    }
    if &bytes[0..4] != MAGIC {
        scan.header = Header::Invalid("bad segment magic");
        return Ok(scan);
    }
    if bytes[4..8] != VERSION.to_le_bytes() {
        scan.header = Header::Invalid("unsupported segment version");
        return Ok(scan);
    }
    if bytes[8..16] != base.get().to_le_bytes() {
        scan.header = Header::Invalid("segment header does not match its file name");
        return Ok(scan);
    }

    let mut position = HEADER_LEN as usize;
    let mut expected = base;
    let mut last_term = Term::ZERO;
    while position < bytes.len() {
        let (entry, frame_len) = match codec::decode_frame(&bytes[position..]) {
            Ok(decoded) => decoded,
            Err(e) => {
                scan.stopped = Some(e.0);
                break;
            }
        };
        if entry.index() != expected {
            scan.stopped = Some("entry index out of sequence");
            break;
        }
        if entry.term() < last_term {
            scan.stopped = Some("entry term went backwards");
            break;
        }
        if scan.runs.is_empty() || entry.term() != last_term {
            scan.runs.push(TermRun {
                term: entry.term(),
                first: entry.index(),
            });
        }
        last_term = entry.term();
        scan.positions.push(position as u32);
        position += frame_len;
        expected = expected.next();
    }
    scan.valid_end = position as u64;
    Ok(scan)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_names_round_trip() {
        let dir = Path::new("/data");
        let base = LogIndex::new(48_213);
        let path = log_path(dir, base);
        assert_eq!(path, Path::new("/data/00000000000000048213.log"));
        assert_eq!(
            idx_path(dir, base),
            Path::new("/data/00000000000000048213.idx")
        );
        let name = path.file_name().unwrap().to_str().unwrap();
        assert_eq!(parse_base(name), Some(base));
    }

    #[test]
    fn unrelated_file_names_are_ignored() {
        for name in [
            "hardstate",
            "00000000000000000001.idx",
            "1.log",
            "0000000000000000000x.log",
            "00000000000000000001.log.tmp",
        ] {
            assert_eq!(parse_base(name), None, "{name}");
        }
    }
}
