//! The on-disk log, as Raft wants to see it.

use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use fl_raft::LogStore;
use fl_storage::Log;
use fl_types::{LogEntry, LogId, LogIndex, Term};

/// A handle to the on-disk log that can be cloned and shared.
///
/// The Raft node writes through one clone. Request handlers read committed
/// entries through others. A read-write lock keeps a reader from seeing a
/// half-finished append or truncation.
///
/// Writes are not synced. The code that drives the node calls
/// [`SharedLog::sync`] when a [`fl_raft::Ready`] asks for it, so the log
/// should be opened with [`fl_storage::SyncPolicy::Manual`].
#[derive(Clone)]
pub struct SharedLog {
    log: Arc<RwLock<Log>>,
}

impl SharedLog {
    /// Wraps an open log.
    pub fn new(log: Log) -> Self {
        SharedLog {
            log: Arc::new(RwLock::new(log)),
        }
    }

    /// Shared access to the log, for reading.
    pub fn read(&self) -> RwLockReadGuard<'_, Log> {
        // A panic while holding the lock takes the node down anyway, so a
        // poisoned lock carries no extra information.
        self.log.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// Flushes everything appended so far to disk.
    pub fn sync(&self) -> fl_storage::Result<()> {
        self.write().sync()
    }

    fn write(&self) -> RwLockWriteGuard<'_, Log> {
        self.log.write().unwrap_or_else(PoisonError::into_inner)
    }
}

impl LogStore for SharedLog {
    type Error = fl_storage::Error;

    fn last_id(&self) -> LogId {
        self.read().last_id()
    }

    fn term(&self, index: LogIndex) -> Result<Option<Term>, Self::Error> {
        Ok(self.read().term(index))
    }

    fn entries(
        &self,
        from: LogIndex,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Vec<LogEntry>, Self::Error> {
        self.read()
            .read(from.max(LogIndex::FIRST), max_entries, max_bytes)
    }

    fn append(&mut self, entries: &[LogEntry]) -> Result<(), Self::Error> {
        self.write().append(entries)
    }

    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), Self::Error> {
        self.write().truncate_suffix(from)
    }
}

/// Converts the hard state read from disk into the form Raft uses.
pub fn hard_state_from_disk(state: fl_storage::HardState) -> fl_raft::HardState {
    fl_raft::HardState {
        term: state.term,
        voted_for: state.voted_for,
    }
}

/// Converts Raft's hard state into the form written to disk.
pub fn hard_state_to_disk(state: fl_raft::HardState) -> fl_storage::HardState {
    fl_storage::HardState {
        term: state.term,
        voted_for: state.voted_for,
    }
}
