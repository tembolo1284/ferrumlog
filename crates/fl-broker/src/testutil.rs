//! Test-only helpers.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use fl_types::{
    Command, LogEntry, LogIndex, Membership, NodeId, Record, RecordBatch, Term, TopicId, TopicName,
};

/// A scratch directory that is deleted when dropped.
pub(crate) struct TempDir(PathBuf);

impl TempDir {
    pub(crate) fn new(tag: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("ferrumlog-broker-{tag}-{}-{n}", std::process::id()));
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

fn entry(index: u64, command: Command) -> LogEntry {
    LogEntry::new(Term::new(1), LogIndex::new(index), command)
}

/// A no-op entry.
pub(crate) fn noop(index: u64) -> LogEntry {
    entry(index, Command::Noop)
}

/// A create-topic entry.
pub(crate) fn create(index: u64, name: &str) -> LogEntry {
    let name = TopicName::new(name).unwrap();
    entry(index, Command::CreateTopic { name })
}

/// A membership-change entry.
pub(crate) fn reconfigure(index: u64) -> LogEntry {
    let members = Membership::new([NodeId::new(1)]).unwrap();
    entry(index, Command::Reconfigure(members))
}

/// An append of `count` records to topic `topic`. Each value is
/// `"<log index>:<position in batch>"`, so a test can tell exactly which
/// record it got back.
pub(crate) fn append(index: u64, topic: u32, count: usize) -> LogEntry {
    let records = (0..count)
        .map(|position| Record::new(format!("{index}:{position}")))
        .collect();
    let batch = RecordBatch::new(TopicId::new(topic), records);
    entry(index, Command::Append(batch))
}
