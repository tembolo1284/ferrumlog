//! Test-only helpers.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use fl_types::{Command, LogEntry, LogIndex, Record, RecordBatch, Term, TopicId};

/// A scratch directory that is deleted when dropped.
pub(crate) struct TempDir(PathBuf);

impl TempDir {
    pub(crate) fn new(tag: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("ferrumlog-{tag}-{}-{n}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        TempDir(path)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// An append entry whose single record has a value of `size` bytes. The
/// bytes depend on the index so that entries are distinguishable.
pub(crate) fn entry(term: u64, index: u64, size: usize) -> LogEntry {
    let value = vec![(index % 251) as u8; size];
    let batch = RecordBatch::new(TopicId::new(1), vec![Record::new(value)]);
    LogEntry::new(
        Term::new(term),
        LogIndex::new(index),
        Command::Append(batch),
    )
}

/// Entries `from..=to`, all in one term.
pub(crate) fn entries(term: u64, from: u64, to: u64, size: usize) -> Vec<LogEntry> {
    (from..=to).map(|i| entry(term, i, size)).collect()
}
