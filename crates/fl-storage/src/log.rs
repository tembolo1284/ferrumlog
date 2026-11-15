//! The segmented, append-only log.

use std::fs;
use std::path::{Path, PathBuf};

use fl_types::{LogEntry, LogId, LogIndex, Term};

use crate::codec;
use crate::error::{Error, Result};
use crate::fsutil;
use crate::index::TermRun;
use crate::segment::{self, Segment, HEADER_LEN};

/// Largest allowed segment size. Frame positions are stored as `u32`.
const MAX_SEGMENT_BYTES: u64 = 1 << 31;

/// When appended entries are flushed to disk.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum SyncPolicy {
    /// [`Log::append`] syncs before it returns. One append of many entries
    /// costs one sync, so batching appends gives group commit.
    #[default]
    Always,
    /// [`Log::append`] only writes. The caller must call [`Log::sync`]
    /// before treating the entries as durable.
    Manual,
}

/// Settings for a [`Log`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct LogConfig {
    /// Size at which the active segment is sealed and a new one started.
    /// A single entry larger than this still gets written, alone in its
    /// own segment.
    pub segment_bytes: u64,
    /// When appends are flushed to disk.
    pub sync: SyncPolicy,
}

impl Default for LogConfig {
    fn default() -> Self {
        LogConfig {
            segment_bytes: 64 << 20,
            sync: SyncPolicy::Always,
        }
    }
}

impl LogConfig {
    fn validate(&self) -> Result<()> {
        if self.segment_bytes <= HEADER_LEN {
            return Err(Error::InvalidConfig(
                "segment_bytes must be larger than the 16-byte segment header",
            ));
        }
        if self.segment_bytes > MAX_SEGMENT_BYTES {
            return Err(Error::InvalidConfig("segment_bytes must be at most 2 GiB"));
        }
        Ok(())
    }
}

/// What [`Log::open`] found and repaired.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct RecoveryReport {
    /// Number of segment files.
    pub segments: usize,
    /// Number of entries recovered.
    pub entries: u64,
    /// Bytes of torn tail cut from the active segment.
    pub truncated_bytes: u64,
    /// Sealed segments whose index file had to be rebuilt by scanning.
    pub rebuilt_indexes: usize,
}

/// A durable, append-only sequence of [`LogEntry`] values.
///
/// Entries are addressed by [`LogIndex`] and are always contiguous. The log
/// supports the three operations Raft needs: append at the end, read a
/// range, and cut off a suffix that conflicts with the leader.
///
/// After any method returns an I/O error, drop the `Log` and open it again.
/// Reopening runs recovery and restores a consistent view.
pub struct Log {
    dir: PathBuf,
    config: LogConfig,
    /// Ordered by base index. Never empty. The last one is the active
    /// segment.
    segments: Vec<Segment>,
    /// Term boundaries for the whole log.
    runs: Vec<TermRun>,
    /// True if the active segment has writes that are not yet synced.
    dirty: bool,
    recovery: RecoveryReport,
}

impl Log {
    /// Opens the log in `dir`, creating it if it does not exist, and runs
    /// crash recovery.
    pub fn open(dir: impl AsRef<Path>, config: LogConfig) -> Result<Log> {
        config.validate()?;
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).map_err(Error::io(&dir))?;
        fsutil::remove_stale_tmp(&dir)?;

        let bases = list_bases(&dir)?;
        let mut segments: Vec<Segment> = Vec::with_capacity(bases.len().max(1));
        let mut runs: Vec<TermRun> = Vec::new();
        let mut recovery = RecoveryReport::default();

        if bases.is_empty() {
            segments.push(Segment::create(&dir, LogIndex::FIRST)?);
            fsutil::sync_dir(&dir)?;
        }

        for (k, &base) in bases.iter().enumerate() {
            let opened = if k + 1 == bases.len() {
                Segment::open_active(&dir, base)?
            } else {
                Segment::open_sealed(&dir, base)?
            };
            let path = &opened.segment.path;

            if let Some(previous) = segments.last() {
                if previous.next_index() != base {
                    return Err(Error::corrupt(
                        path,
                        0,
                        format!(
                            "segment starts at {base} but the previous one ends before {}",
                            previous.next_index()
                        ),
                    ));
                }
            }
            for run in &opened.runs {
                match runs.last() {
                    Some(last) if run.term == last.term => {}
                    Some(last) if run.term < last.term => {
                        return Err(Error::corrupt(
                            path,
                            0,
                            "term goes backwards across segments",
                        ));
                    }
                    _ => runs.push(*run),
                }
            }

            recovery.entries += opened.segment.count() as u64;
            recovery.truncated_bytes += opened.truncated_bytes;
            recovery.rebuilt_indexes += usize::from(opened.rebuilt_index);
            segments.push(opened.segment);
        }
        recovery.segments = segments.len();

        Ok(Log {
            dir,
            config,
            segments,
            runs,
            dirty: false,
            recovery,
        })
    }

    /// What the most recent [`Log::open`] found and repaired.
    pub fn recovery(&self) -> RecoveryReport {
        self.recovery
    }

    /// Index of the first entry the log holds. For a log that has never
    /// been compacted this is 1, even when the log is empty.
    pub fn first_index(&self) -> LogIndex {
        self.segments[0].base
    }

    /// Index of the last entry, or `first_index() - 1` if the log is empty.
    pub fn last_index(&self) -> LogIndex {
        self.active().next_index().prev()
    }

    /// Term and index of the last entry, or [`LogId::ZERO`] for an empty
    /// log.
    pub fn last_id(&self) -> LogId {
        match self.runs.last() {
            Some(run) => LogId::new(run.term, self.last_index()),
            None => LogId::ZERO,
        }
    }

    /// Number of entries.
    pub fn len(&self) -> u64 {
        self.active().next_index().entries_since(self.first_index())
    }

    /// True if the log holds no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of segment files.
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Total size of all segment files in bytes.
    pub fn size_bytes(&self) -> u64 {
        self.segments.iter().map(Segment::end).sum()
    }

    /// The term of the entry at `index`.
    ///
    /// Index 0 has term 0 by Raft convention. Any other index outside the
    /// log returns `None`. This never touches the disk.
    pub fn term(&self, index: LogIndex) -> Option<Term> {
        if index.is_zero() {
            return (self.first_index() == LogIndex::FIRST).then_some(Term::ZERO);
        }
        if index < self.first_index() || index > self.last_index() {
            return None;
        }
        let after = self.runs.partition_point(|run| run.first <= index);
        Some(self.runs[after - 1].term)
    }

    /// Appends entries at the end of the log.
    ///
    /// The first entry must directly follow the current last entry, the
    /// entries must be consecutive, and terms must not decrease. The whole
    /// batch is checked and encoded before anything is written, so a
    /// rejected batch leaves the log untouched.
    ///
    /// With [`SyncPolicy::Always`] the entries are on disk when this
    /// returns.
    pub fn append(&mut self, entries: &[LogEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut expected = self.last_index().next();
        let mut last_term = self.last_id().term;
        for entry in entries {
            if entry.index() != expected {
                return Err(Error::NonContiguous {
                    expected,
                    got: entry.index(),
                });
            }
            if entry.term() < last_term {
                return Err(Error::TermRegression {
                    last: last_term,
                    got: entry.term(),
                });
            }
            expected = expected.next();
            last_term = entry.term();
        }

        let mut bytes = Vec::new();
        let mut lens = Vec::with_capacity(entries.len());
        for entry in entries {
            lens.push(codec::encode_frame(entry, &mut bytes)?);
        }

        // Write the frames in chunks, one chunk per segment.
        let mut next = 0;
        let mut offset = 0;
        while next < entries.len() {
            let active = self.active();
            let room_from = active.end();
            let has_entries = active.count() > 0;

            let mut take = next;
            let mut chunk = 0usize;
            while take < entries.len() {
                let end = room_from + (chunk + lens[take]) as u64;
                let fits = end <= self.config.segment_bytes;
                // An entry that can never fit is allowed into an empty
                // segment, otherwise it could not be stored at all.
                let alone = !has_entries && take == next;
                if !fits && !alone {
                    break;
                }
                chunk += lens[take];
                take += 1;
            }

            if take == next {
                self.roll()?;
                continue;
            }

            self.active_mut()
                .append(&bytes[offset..offset + chunk], &lens[next..take])?;
            for entry in &entries[next..take] {
                if self.runs.last().map(|run| run.term) != Some(entry.term()) {
                    self.runs.push(TermRun {
                        term: entry.term(),
                        first: entry.index(),
                    });
                }
            }
            self.dirty = true;
            offset += chunk;
            next = take;
        }

        if self.config.sync == SyncPolicy::Always {
            self.sync()?;
        }
        Ok(())
    }

    /// Flushes appended entries to disk. Does nothing if there is nothing
    /// to flush.
    pub fn sync(&mut self) -> Result<()> {
        if self.dirty {
            self.active().sync()?;
            self.dirty = false;
        }
        Ok(())
    }

    /// Reads the entry at `index`, or `None` if the log does not hold it.
    pub fn entry(&self, index: LogIndex) -> Result<Option<LogEntry>> {
        if index < self.first_index() || index > self.last_index() {
            return Ok(None);
        }
        Ok(self.read(index, 1, usize::MAX)?.pop())
    }

    /// Reads consecutive entries starting at `from`.
    ///
    /// Stops after `max_entries` entries or once the frames read would
    /// exceed `max_bytes`, whichever comes first. If any entry exists at
    /// `from`, at least one is returned even if it alone exceeds
    /// `max_bytes`. Returns an empty vector if `from` is past the end, and
    /// [`Error::OutOfRange`] if `from` is before the first entry.
    ///
    /// Every frame's checksum is verified on the way out.
    pub fn read(
        &self,
        from: LogIndex,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Vec<LogEntry>> {
        let (first, last) = (self.first_index(), self.last_index());
        if from < first {
            return Err(Error::OutOfRange {
                index: from,
                first,
                last,
            });
        }
        let mut out = Vec::new();
        if from > last || max_entries == 0 {
            return Ok(out);
        }

        let mut budget = max_bytes;
        let mut index = from;
        for segment in &self.segments[self.segment_slot(from)..] {
            let start = index.entries_since(segment.base) as usize;
            let mut end = start;
            let mut chunk = 0usize;
            while end < segment.count() && out.len() + (end - start) < max_entries {
                let len = segment.frame_len(end);
                let first_overall = out.is_empty() && end == start;
                if chunk + len > budget && !first_overall {
                    break;
                }
                chunk += len;
                end += 1;
            }
            if end == start {
                break;
            }

            let raw = segment.read_frames(start, end)?;
            let mut at = 0;
            for slot in start..end {
                let position = segment.position(slot);
                let (entry, used) = codec::decode_frame(&raw[at..])
                    .map_err(|e| Error::corrupt(&segment.path, position, e.0))?;
                if used != segment.frame_len(slot) || entry.index() != index {
                    return Err(Error::corrupt(
                        &segment.path,
                        position,
                        "frame does not match the segment index",
                    ));
                }
                out.push(entry);
                at += used;
                index = index.next();
            }

            budget = budget.saturating_sub(chunk);
            if end < segment.count() {
                break;
            }
        }
        Ok(out)
    }

    /// Removes the entry at `from` and every entry after it.
    ///
    /// A follower calls this when the leader's log disagrees with its own.
    /// Does nothing if `from` is past the end. The removal is on disk when
    /// this returns.
    pub fn truncate_suffix(&mut self, from: LogIndex) -> Result<()> {
        let (first, last) = (self.first_index(), self.last_index());
        if from > last {
            return Ok(());
        }
        if from < first {
            return Err(Error::OutOfRange {
                index: from,
                first,
                last,
            });
        }

        // Delete whole segments after the one holding `from`, newest first.
        // A crash partway through leaves a shorter but still contiguous log.
        let keep = self.segment_slot(from);
        let reactivated = keep + 1 < self.segments.len();
        while self.segments.len() > keep + 1 {
            if let Some(segment) = self.segments.pop() {
                fsutil::remove_if_exists(&segment::idx_path(&self.dir, segment.base))?;
                fsutil::remove_if_exists(&segment.path)?;
            }
        }
        if reactivated {
            // The kept segment was sealed. It is the active one again, so
            // its index no longer applies.
            let base = self.active().base;
            fsutil::remove_if_exists(&segment::idx_path(&self.dir, base))?;
            fsutil::sync_dir(&self.dir)?;
        }

        let active = self.active_mut();
        let slot = from.entries_since(active.base) as usize;
        active.truncate(slot)?;

        while self.runs.last().is_some_and(|run| run.first >= from) {
            self.runs.pop();
        }
        self.dirty = false;
        Ok(())
    }

    fn active(&self) -> &Segment {
        self.segments.last().expect("a log always has a segment")
    }

    fn active_mut(&mut self) -> &mut Segment {
        self.segments
            .last_mut()
            .expect("a log always has a segment")
    }

    /// Position in `self.segments` of the segment that holds `index`.
    /// `index` must not be before the first segment.
    fn segment_slot(&self, index: LogIndex) -> usize {
        self.segments
            .partition_point(|segment| segment.base <= index)
            .saturating_sub(1)
    }

    /// Seals the active segment and starts a new one.
    ///
    /// The order matters for crash safety. The old segment is synced first,
    /// then its index is written, and only then does the new file appear.
    /// So any segment that is not the last one is always fully on disk.
    fn roll(&mut self) -> Result<()> {
        let (base, next) = {
            let active = self.active();
            (active.base, active.next_index())
        };
        let runs = self.runs_between(base, next);

        let active = self.active();
        active.sync()?;
        active.write_index(&self.dir, &runs)?;

        let segment = Segment::create(&self.dir, next)?;
        fsutil::sync_dir(&self.dir)?;
        self.segments.push(segment);
        self.dirty = false;
        Ok(())
    }

    /// The term runs restricted to the entries in `base..next`.
    fn runs_between(&self, base: LogIndex, next: LogIndex) -> Vec<TermRun> {
        let mut out = Vec::new();
        for (k, run) in self.runs.iter().enumerate() {
            let run_end = self
                .runs
                .get(k + 1)
                .map_or(next, |following| following.first);
            let start = run.first.max(base);
            let end = run_end.min(next);
            if start < end {
                out.push(TermRun {
                    term: run.term,
                    first: start,
                });
            }
        }
        out
    }
}

/// Base indexes of every segment file in `dir`, in ascending order.
fn list_bases(dir: &Path) -> Result<Vec<LogIndex>> {
    let mut bases = Vec::new();
    for item in fs::read_dir(dir).map_err(Error::io(dir))? {
        let name = item.map_err(Error::io(dir))?.file_name();
        if let Some(base) = name.to_str().and_then(segment::parse_base) {
            bases.push(base);
        }
    }
    bases.sort_unstable();
    Ok(bases)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::{idx_path, log_path};
    use crate::testutil::{entries, entry, TempDir};
    use fl_types::Command;
    use std::fs::OpenOptions;
    use std::io::Write;

    /// With a 54-byte value each frame is exactly 100 bytes, so a 512-byte
    /// segment holds a 16-byte header and four entries.
    const VALUE: usize = 54;
    const FRAME: u64 = 100;

    fn small() -> LogConfig {
        LogConfig {
            segment_bytes: 512,
            sync: SyncPolicy::Always,
        }
    }

    fn idx(n: u64) -> LogIndex {
        LogIndex::new(n)
    }

    fn read_all(log: &Log) -> Vec<LogEntry> {
        log.read(log.first_index(), usize::MAX, usize::MAX).unwrap()
    }

    fn flip_byte(path: &Path, at: u64) {
        let mut bytes = fs::read(path).unwrap();
        bytes[at as usize] ^= 0xFF;
        fs::write(path, bytes).unwrap();
    }

    #[test]
    fn fresh_log_is_empty() {
        let dir = TempDir::new("log-fresh");
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert!(log.is_empty());
        assert_eq!(log.len(), 0);
        assert_eq!(log.first_index(), LogIndex::FIRST);
        assert_eq!(log.last_index(), LogIndex::ZERO);
        assert_eq!(log.last_id(), LogId::ZERO);
        assert_eq!(log.term(LogIndex::ZERO), Some(Term::ZERO));
        assert_eq!(log.term(idx(1)), None);
        assert_eq!(log.entry(idx(1)).unwrap(), None);
        assert!(read_all(&log).is_empty());
        assert_eq!(log.segment_count(), 1);
        assert_eq!(log.size_bytes(), HEADER_LEN);
    }

    #[test]
    fn frame_size_assumption_holds() {
        let mut buf = Vec::new();
        let len = codec::encode_frame(&entry(1, 1, VALUE), &mut buf).unwrap();
        assert_eq!(len as u64, FRAME);
    }

    #[test]
    fn appended_entries_survive_reopen() {
        let dir = TempDir::new("log-reopen");
        let want = entries(1, 1, 3, VALUE);
        {
            let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
            log.append(&want[..1]).unwrap();
            log.append(&want[1..]).unwrap();
            assert_eq!(read_all(&log), want);
        }
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert_eq!(read_all(&log), want);
        assert_eq!(log.entry(idx(2)).unwrap().as_ref(), Some(&want[1]));
        assert_eq!(log.last_id(), LogId::new(Term::new(1), idx(3)));
        assert_eq!(
            log.recovery(),
            RecoveryReport {
                segments: 1,
                entries: 3,
                truncated_bytes: 0,
                rebuilt_indexes: 0
            }
        );
    }

    #[test]
    fn every_command_kind_round_trips_through_disk() {
        use fl_types::{Membership, NodeId, TopicName};
        let dir = TempDir::new("log-kinds");
        let want = vec![
            LogEntry::new(Term::new(1), idx(1), Command::Noop),
            LogEntry::new(
                Term::new(1),
                idx(2),
                Command::CreateTopic {
                    name: TopicName::new("quotes").unwrap(),
                },
            ),
            entry(1, 3, 10),
            LogEntry::new(
                Term::new(2),
                idx(4),
                Command::Reconfigure(Membership::new([1, 2, 3].map(NodeId::new)).unwrap()),
            ),
        ];
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        log.append(&want).unwrap();
        drop(log);
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert_eq!(read_all(&log), want);
    }

    #[test]
    fn rejected_appends_change_nothing() {
        let dir = TempDir::new("log-reject");
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        log.append(&entries(2, 1, 2, VALUE)).unwrap();
        let size = log.size_bytes();

        // starts one too far ahead
        let err = log.append(&[entry(2, 4, VALUE)]).unwrap_err();
        assert!(matches!(
            err,
            Error::NonContiguous { expected, got } if expected == idx(3) && got == idx(4)
        ));
        // overlaps an existing entry
        assert!(matches!(
            log.append(&[entry(2, 2, VALUE)]),
            Err(Error::NonContiguous { .. })
        ));
        // gap inside the batch
        assert!(matches!(
            log.append(&[entry(2, 3, VALUE), entry(2, 5, VALUE)]),
            Err(Error::NonContiguous { .. })
        ));
        // term lower than the last entry
        assert!(matches!(
            log.append(&[entry(1, 3, VALUE)]),
            Err(Error::TermRegression { .. })
        ));
        // term drops inside the batch
        assert!(matches!(
            log.append(&[entry(3, 3, VALUE), entry(2, 4, VALUE)]),
            Err(Error::TermRegression { .. })
        ));

        assert_eq!(log.last_index(), idx(2));
        assert_eq!(log.size_bytes(), size);
        log.append(&[]).unwrap();
        log.append(&[entry(2, 3, VALUE)]).unwrap();
        assert_eq!(log.last_index(), idx(3));
    }

    #[test]
    fn log_rolls_into_new_segments() {
        let dir = TempDir::new("log-roll");
        let want = entries(1, 1, 10, VALUE);
        let mut log = Log::open(dir.path(), small()).unwrap();
        log.append(&want).unwrap();

        // 4 + 4 + 2 entries
        assert_eq!(log.segment_count(), 3);
        assert_eq!(log.len(), 10);
        assert_eq!(read_all(&log), want);
        for base in [1, 5, 9] {
            assert!(log_path(dir.path(), idx(base)).exists(), "segment {base}");
        }
        // sealed segments have an index, the active one does not
        assert!(idx_path(dir.path(), idx(1)).exists());
        assert!(idx_path(dir.path(), idx(5)).exists());
        assert!(!idx_path(dir.path(), idx(9)).exists());
        drop(log);

        let log = Log::open(dir.path(), small()).unwrap();
        assert_eq!(read_all(&log), want);
        assert_eq!(
            log.recovery(),
            RecoveryReport {
                segments: 3,
                entries: 10,
                truncated_bytes: 0,
                rebuilt_indexes: 0
            }
        );
    }

    #[test]
    fn rolling_one_entry_at_a_time_matches_one_big_append() {
        let dir = TempDir::new("log-roll-single");
        let want = entries(1, 1, 10, VALUE);
        let mut log = Log::open(dir.path(), small()).unwrap();
        for e in &want {
            log.append(std::slice::from_ref(e)).unwrap();
        }
        assert_eq!(log.segment_count(), 3);
        assert_eq!(read_all(&log), want);
    }

    #[test]
    fn oversized_entry_gets_its_own_segment() {
        let dir = TempDir::new("log-oversized");
        let mut log = Log::open(dir.path(), small()).unwrap();
        let want = vec![
            entry(1, 1, VALUE),
            entry(1, 2, 2_000),
            entry(1, 3, VALUE),
            entry(1, 4, 3_000),
        ];
        log.append(&want).unwrap();
        assert_eq!(log.segment_count(), 4);
        assert_eq!(read_all(&log), want);
        drop(log);
        let log = Log::open(dir.path(), small()).unwrap();
        assert_eq!(read_all(&log), want);
    }

    #[test]
    fn read_honours_limits() {
        let dir = TempDir::new("log-read");
        let want = entries(1, 1, 10, VALUE);
        let mut log = Log::open(dir.path(), small()).unwrap();
        log.append(&want).unwrap();
        let frame = FRAME as usize;

        // entry limit, within one segment and across segments
        assert_eq!(log.read(idx(1), 3, usize::MAX).unwrap(), want[0..3]);
        assert_eq!(log.read(idx(3), 4, usize::MAX).unwrap(), want[2..6]);
        assert_eq!(log.read(idx(1), 0, usize::MAX).unwrap(), vec![]);

        // byte limit
        assert_eq!(
            log.read(idx(1), usize::MAX, 2 * frame + 50).unwrap(),
            want[0..2]
        );
        assert_eq!(log.read(idx(1), usize::MAX, 6 * frame).unwrap(), want[0..6]);
        // always at least one entry
        assert_eq!(log.read(idx(4), usize::MAX, 1).unwrap(), want[3..4]);
        assert_eq!(log.read(idx(4), usize::MAX, 0).unwrap(), want[3..4]);

        // to the end, and past the end
        assert_eq!(log.read(idx(8), usize::MAX, usize::MAX).unwrap(), want[7..]);
        assert_eq!(
            log.read(idx(10), usize::MAX, usize::MAX).unwrap(),
            want[9..]
        );
        assert_eq!(log.read(idx(11), usize::MAX, usize::MAX).unwrap(), vec![]);

        // before the start
        assert!(matches!(
            log.read(LogIndex::ZERO, 1, usize::MAX),
            Err(Error::OutOfRange { .. })
        ));
        assert_eq!(log.entry(LogIndex::ZERO).unwrap(), None);
        assert_eq!(log.entry(idx(11)).unwrap(), None);
    }

    #[test]
    fn term_lookup_follows_runs_across_segments() {
        let dir = TempDir::new("log-terms");
        // terms: 1 1 3 3 3 3 7 7 7, segments of four entries
        let mut want = entries(1, 1, 2, VALUE);
        want.extend(entries(3, 3, 6, VALUE));
        want.extend(entries(7, 7, 9, VALUE));

        let check = |log: &Log| {
            let terms: Vec<u64> = (0..=9).map(|i| log.term(idx(i)).unwrap().get()).collect();
            assert_eq!(terms, [0, 1, 1, 3, 3, 3, 3, 7, 7, 7]);
            assert_eq!(log.term(idx(10)), None);
            assert_eq!(log.last_id(), LogId::new(Term::new(7), idx(9)));
        };

        let mut log = Log::open(dir.path(), small()).unwrap();
        log.append(&want).unwrap();
        check(&log);
        drop(log);

        // reopened from index files
        let log = Log::open(dir.path(), small()).unwrap();
        assert_eq!(log.recovery().rebuilt_indexes, 0);
        check(&log);
        drop(log);

        // reopened by scanning
        fs::remove_file(idx_path(dir.path(), idx(1))).unwrap();
        fs::remove_file(idx_path(dir.path(), idx(5))).unwrap();
        let log = Log::open(dir.path(), small()).unwrap();
        assert_eq!(log.recovery().rebuilt_indexes, 2);
        check(&log);
    }

    #[test]
    fn torn_tail_is_cut_off() {
        let dir = TempDir::new("log-torn");
        let want = entries(1, 1, 3, VALUE);
        {
            let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
            log.append(&want).unwrap();
        }
        // The last write only partly reached the disk.
        let path = log_path(dir.path(), idx(1));
        let len = fs::metadata(&path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(len - 5).unwrap();
        drop(file);

        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert_eq!(log.recovery().truncated_bytes, FRAME - 5);
        assert_eq!(log.recovery().entries, 2);
        assert_eq!(read_all(&log), want[..2]);

        // The log accepts the lost entry again and is clean afterwards.
        log.append(&want[2..]).unwrap();
        drop(log);
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert_eq!(log.recovery().truncated_bytes, 0);
        assert_eq!(read_all(&log), want);
    }

    #[test]
    fn garbage_after_the_last_frame_is_cut_off() {
        let dir = TempDir::new("log-garbage");
        let want = entries(1, 1, 2, VALUE);
        {
            let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
            log.append(&want).unwrap();
        }
        let path = log_path(dir.path(), idx(1));
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[0u8; 300]).unwrap();
        drop(file);

        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert_eq!(log.recovery().truncated_bytes, 300);
        assert_eq!(read_all(&log), want);
        assert_eq!(fs::metadata(&path).unwrap().len(), HEADER_LEN + 2 * FRAME);
    }

    #[test]
    fn damage_in_the_active_segment_cuts_from_that_frame() {
        let dir = TempDir::new("log-active-damage");
        let want = entries(1, 1, 3, VALUE);
        {
            let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
            log.append(&want).unwrap();
        }
        // Inside the second frame's payload.
        flip_byte(&log_path(dir.path(), idx(1)), HEADER_LEN + FRAME + 40);

        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert_eq!(log.recovery().truncated_bytes, 2 * FRAME);
        assert_eq!(read_all(&log), want[..1]);
    }

    #[test]
    fn damage_in_a_sealed_segment_is_reported_not_repaired() {
        let dir = TempDir::new("log-sealed-damage");
        {
            let mut log = Log::open(dir.path(), small()).unwrap();
            log.append(&entries(1, 1, 10, VALUE)).unwrap();
        }
        let path = log_path(dir.path(), idx(1));
        let len_before = fs::metadata(&path).unwrap().len();
        flip_byte(&path, HEADER_LEN + FRAME + 40);

        // With the index present the segment is not scanned on open. The
        // checksum catches the damage when the entry is read.
        let log = Log::open(dir.path(), small()).unwrap();
        assert_eq!(log.read(idx(1), 1, usize::MAX).unwrap().len(), 1);
        assert!(matches!(
            log.read(idx(2), 1, usize::MAX),
            Err(Error::Corrupt { offset, .. }) if offset == HEADER_LEN + FRAME
        ));
        assert!(log.read(idx(5), usize::MAX, usize::MAX).is_ok());
        drop(log);

        // Without the index the scan finds it, and open refuses to guess.
        fs::remove_file(idx_path(dir.path(), idx(1))).unwrap();
        assert!(matches!(
            Log::open(dir.path(), small()),
            Err(Error::Corrupt { .. })
        ));
        assert_eq!(fs::metadata(&path).unwrap().len(), len_before);
    }

    #[test]
    fn missing_or_bad_index_is_rebuilt() {
        let dir = TempDir::new("log-reindex");
        let want = entries(1, 1, 10, VALUE);
        {
            let mut log = Log::open(dir.path(), small()).unwrap();
            log.append(&want).unwrap();
        }
        let first = idx_path(dir.path(), idx(1));
        let second = idx_path(dir.path(), idx(5));
        let good = fs::read(&first).unwrap();
        fs::remove_file(&first).unwrap();
        fs::write(&second, b"not an index").unwrap();

        let log = Log::open(dir.path(), small()).unwrap();
        assert_eq!(log.recovery().rebuilt_indexes, 2);
        assert_eq!(read_all(&log), want);
        assert_eq!(fs::read(&first).unwrap(), good);
        drop(log);

        let log = Log::open(dir.path(), small()).unwrap();
        assert_eq!(log.recovery().rebuilt_indexes, 0);
    }

    #[test]
    fn crash_while_creating_a_segment_is_recovered() {
        let dir = TempDir::new("log-roll-crash");
        let want = entries(1, 1, 4, VALUE);
        {
            let mut log = Log::open(dir.path(), small()).unwrap();
            log.append(&want).unwrap();
        }
        // The next segment file exists but its header never made it.
        fs::write(log_path(dir.path(), idx(5)), [0x46, 0x4C, 0x53]).unwrap();

        let mut log = Log::open(dir.path(), small()).unwrap();
        assert_eq!(log.segment_count(), 2);
        assert_eq!(log.recovery().truncated_bytes, 3);
        assert_eq!(read_all(&log), want);

        log.append(&[entry(1, 5, VALUE)]).unwrap();
        drop(log);
        let log = Log::open(dir.path(), small()).unwrap();
        assert_eq!(log.last_index(), idx(5));
        assert_eq!(log.recovery().truncated_bytes, 0);
    }

    #[test]
    fn missing_segment_is_detected() {
        let dir = TempDir::new("log-gap");
        {
            let mut log = Log::open(dir.path(), small()).unwrap();
            log.append(&entries(1, 1, 10, VALUE)).unwrap();
        }
        fs::remove_file(log_path(dir.path(), idx(5))).unwrap();
        fs::remove_file(idx_path(dir.path(), idx(5))).unwrap();
        assert!(matches!(
            Log::open(dir.path(), small()),
            Err(Error::Corrupt { .. })
        ));
    }

    #[test]
    fn wrong_header_is_detected() {
        let dir = TempDir::new("log-header");
        {
            let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
            log.append(&entries(1, 1, 2, VALUE)).unwrap();
        }
        flip_byte(&log_path(dir.path(), idx(1)), 1);
        assert!(matches!(
            Log::open(dir.path(), LogConfig::default()),
            Err(Error::Corrupt { offset: 0, .. })
        ));
    }

    #[test]
    fn truncate_inside_the_active_segment() {
        let dir = TempDir::new("log-trunc-active");
        let want = entries(1, 1, 3, VALUE);
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        log.append(&want).unwrap();

        log.truncate_suffix(idx(4)).unwrap(); // past the end, nothing to do
        assert_eq!(log.last_index(), idx(3));

        log.truncate_suffix(idx(3)).unwrap();
        assert_eq!(read_all(&log), want[..2]);
        assert_eq!(log.size_bytes(), HEADER_LEN + 2 * FRAME);

        // A new leader's entries replace the removed one.
        let replacement = entries(2, 3, 4, VALUE);
        log.append(&replacement).unwrap();
        assert_eq!(log.term(idx(2)), Some(Term::new(1)));
        assert_eq!(log.term(idx(3)), Some(Term::new(2)));
        drop(log);

        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert_eq!(log.recovery().truncated_bytes, 0);
        assert_eq!(read_all(&log)[..2], want[..2]);
        assert_eq!(read_all(&log)[2..], replacement[..]);
        assert_eq!(log.last_id(), LogId::new(Term::new(2), idx(4)));
    }

    #[test]
    fn truncate_across_segments() {
        let dir = TempDir::new("log-trunc-across");
        let want = entries(1, 1, 10, VALUE);
        let mut log = Log::open(dir.path(), small()).unwrap();
        log.append(&want).unwrap();
        assert_eq!(log.segment_count(), 3);

        // Entry 7 lives in the second segment. The third goes away and the
        // second becomes active again, so its index is removed.
        log.truncate_suffix(idx(7)).unwrap();
        assert_eq!(log.segment_count(), 2);
        assert_eq!(read_all(&log), want[..6]);
        assert!(!log_path(dir.path(), idx(9)).exists());
        assert!(!idx_path(dir.path(), idx(9)).exists());
        assert!(!idx_path(dir.path(), idx(5)).exists());
        assert!(idx_path(dir.path(), idx(1)).exists());

        // Appending fills the reactivated segment and rolls again.
        let more = entries(4, 7, 11, VALUE);
        log.append(&more).unwrap();
        assert_eq!(log.segment_count(), 3);
        assert!(idx_path(dir.path(), idx(5)).exists());
        drop(log);

        let log = Log::open(dir.path(), small()).unwrap();
        assert_eq!(log.recovery().rebuilt_indexes, 0);
        let all = read_all(&log);
        assert_eq!(all[..6], want[..6]);
        assert_eq!(all[6..], more[..]);
        assert_eq!(log.term(idx(6)), Some(Term::new(1)));
        assert_eq!(log.term(idx(7)), Some(Term::new(4)));
    }

    #[test]
    fn truncate_at_a_segment_boundary_and_to_empty() {
        let dir = TempDir::new("log-trunc-boundary");
        let want = entries(1, 1, 10, VALUE);
        let mut log = Log::open(dir.path(), small()).unwrap();
        log.append(&want).unwrap();

        // Entry 5 is the first entry of the second segment. That segment
        // stays as an empty active segment.
        log.truncate_suffix(idx(5)).unwrap();
        assert_eq!(log.segment_count(), 2);
        assert_eq!(log.last_index(), idx(4));
        assert_eq!(read_all(&log), want[..4]);
        drop(log);

        let mut log = Log::open(dir.path(), small()).unwrap();
        assert_eq!(log.last_index(), idx(4));
        assert_eq!(log.last_id(), LogId::new(Term::new(1), idx(4)));

        log.truncate_suffix(idx(1)).unwrap();
        assert!(log.is_empty());
        assert_eq!(log.segment_count(), 1);
        assert_eq!(log.last_id(), LogId::ZERO);
        assert_eq!(log.term(idx(1)), None);
        assert!(matches!(
            log.truncate_suffix(LogIndex::ZERO),
            Err(Error::OutOfRange { .. })
        ));

        // An emptied log may restart in any term.
        log.append(&entries(1, 1, 2, VALUE)).unwrap();
        drop(log);
        let log = Log::open(dir.path(), small()).unwrap();
        assert_eq!(log.len(), 2);
    }

    #[test]
    fn manual_sync_policy_defers_the_flush() {
        let dir = TempDir::new("log-manual");
        let config = LogConfig {
            segment_bytes: 512,
            sync: SyncPolicy::Manual,
        };
        let want = entries(1, 1, 10, VALUE);
        let mut log = Log::open(dir.path(), config).unwrap();
        log.append(&want).unwrap();
        assert!(log.dirty);
        log.sync().unwrap();
        assert!(!log.dirty);
        log.sync().unwrap();
        drop(log);

        let log = Log::open(dir.path(), config).unwrap();
        assert_eq!(read_all(&log), want);
    }

    #[test]
    fn unrelated_files_in_the_directory_are_ignored() {
        let dir = TempDir::new("log-unrelated");
        fs::write(dir.path().join("hardstate"), b"x").unwrap();
        fs::write(dir.path().join("notes.txt"), b"x").unwrap();
        fs::write(dir.path().join("00000000000000000009.idx.tmp"), b"x").unwrap();

        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        log.append(&entries(1, 1, 2, VALUE)).unwrap();
        assert_eq!(log.segment_count(), 1);
        assert!(dir.path().join("hardstate").exists());
        assert!(!dir.path().join("00000000000000000009.idx.tmp").exists());
    }

    #[test]
    fn bad_config_is_rejected() {
        let dir = TempDir::new("log-config");
        for segment_bytes in [0, HEADER_LEN, MAX_SEGMENT_BYTES + 1] {
            let config = LogConfig {
                segment_bytes,
                sync: SyncPolicy::Always,
            };
            assert!(matches!(
                Log::open(dir.path(), config),
                Err(Error::InvalidConfig(_))
            ));
        }
    }
}
