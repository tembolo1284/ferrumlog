//! A read-only view of the sealed segments in a log directory.
//!
//! This is for tools that copy segments somewhere else, such as an
//! archiver. It does not open the log and takes no lock, so it is safe to
//! call while a node is running on the same directory. Sealed segments
//! are never written to again.

use std::fs;
use std::path::{Path, PathBuf};

use fl_types::LogIndex;

use crate::error::{Error, Result};
use crate::segment::{idx_path, log_path, parse_base};

/// One sealed segment: a log file that is complete, and its index file.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SealedSegment {
    /// Index of the first entry in the segment.
    pub first_index: LogIndex,
    /// Index of the last entry in the segment.
    pub last_index: LogIndex,
    /// The segment file.
    pub log_path: PathBuf,
    /// The index file that goes with it.
    pub index_path: PathBuf,
}

/// The file names a segment starting at `first_index` uses, log file
/// first and index file second.
pub fn segment_file_names(first_index: LogIndex) -> (String, String) {
    let name = |path: PathBuf| {
        path.file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default()
    };
    let dir = Path::new("");
    (
        name(log_path(dir, first_index)),
        name(idx_path(dir, first_index)),
    )
}

/// Lists the sealed segments in `dir`, oldest first.
///
/// The segment with the highest first index is the one being written to
/// and is left out. So is a segment whose index file is missing, and
/// everything after it, because the list must not have gaps.
pub fn sealed_segments(dir: &Path) -> Result<Vec<SealedSegment>> {
    let mut bases = Vec::new();
    for item in fs::read_dir(dir).map_err(Error::io(dir))? {
        let name = item.map_err(Error::io(dir))?.file_name();
        if let Some(base) = name.to_str().and_then(parse_base) {
            bases.push(base);
        }
    }
    bases.sort_unstable();

    let mut sealed = Vec::new();
    // Each segment ends just before the next one begins, so the last file
    // has no known end and is the active segment.
    for pair in bases.windows(2) {
        let (first, next) = (pair[0], pair[1]);
        let index_path = idx_path(dir, first);
        if !index_path.is_file() {
            break;
        }
        sealed.push(SealedSegment {
            first_index: first,
            last_index: next.prev(),
            log_path: log_path(dir, first),
            index_path,
        });
    }
    Ok(sealed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::{Log, LogConfig};
    use crate::testutil::{entries, TempDir};

    #[test]
    fn file_names_match_what_the_log_writes() {
        let (log, index) = segment_file_names(LogIndex::new(48_213));
        assert_eq!(log, "00000000000000048213.log");
        assert_eq!(index, "00000000000000048213.idx");
    }

    #[test]
    fn only_complete_segments_are_listed() {
        let dir = TempDir::new("sealed-list");
        assert!(sealed_segments(dir.path()).unwrap().is_empty());

        // Small segments, so a handful of entries fills several.
        let config = LogConfig {
            segment_bytes: 512,
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        log.append(&entries(1, 1, 20, 54)).unwrap();
        let count = log.segment_count();
        assert!(count >= 3, "expected several segments, got {count}");

        let sealed = sealed_segments(dir.path()).unwrap();
        // Everything but the segment still being written.
        assert_eq!(sealed.len(), count - 1);
        assert_eq!(sealed[0].first_index, LogIndex::FIRST);
        for pair in sealed.windows(2) {
            assert_eq!(pair[0].last_index.next(), pair[1].first_index);
        }
        for segment in &sealed {
            assert!(segment.log_path.is_file());
            assert!(segment.index_path.is_file());
            assert!(segment.last_index >= segment.first_index);
        }
        assert!(sealed.last().unwrap().last_index < log.last_index());

        // A segment without its index file ends the list.
        fs::remove_file(&sealed[1].index_path).unwrap();
        assert_eq!(sealed_segments(dir.path()).unwrap().len(), 1);
    }

    #[test]
    fn a_missing_directory_is_an_error() {
        let dir = TempDir::new("sealed-missing");
        let gone = dir.path().join("nope");
        assert!(matches!(sealed_segments(&gone), Err(Error::Io { .. })));
    }
}
