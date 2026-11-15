//! The log as Raft sees it.

use std::convert::Infallible;

use fl_types::{Command, LogEntry, LogId, LogIndex, Term};

/// The operations Raft needs from a log.
///
/// The on-disk log in `fl-storage` sits behind this trait in production.
/// [`MemLog`] implements it in memory for tests and the simulator.
///
/// Writes do not have to be durable when they return. The driver syncs the
/// log when a [`Ready`](crate::Ready) says so.
///
/// If any method returns an error, the [`Node`](crate::Node) that called it
/// may be left half way through a step. Stop that node and restart it from
/// disk. Do not keep using it.
pub trait LogStore {
    /// The error a failed read or write produces.
    type Error;

    /// Term and index of the last entry, or [`LogId::ZERO`] for an empty
    /// log.
    fn last_id(&self) -> LogId;

    /// The term of the entry at `index`.
    ///
    /// Index zero, the position before the first entry, has term zero.
    /// Returns `None` for an index past the end of the log.
    fn term(&self, index: LogIndex) -> Result<Option<Term>, Self::Error>;

    /// Entries starting at `from`, in order.
    ///
    /// Returns at most `max_entries`, and stops before the payload total
    /// passes `max_bytes`, but always returns at least one entry if one
    /// exists at `from`.
    fn entries(
        &self,
        from: LogIndex,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Vec<LogEntry>, Self::Error>;

    /// Adds entries at the end. Their indexes continue directly from the
    /// current last index.
    fn append(&mut self, entries: &[LogEntry]) -> Result<(), Self::Error>;

    /// Removes the entry at `from` and every entry after it.
    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), Self::Error>;
}

/// Approximate size of an entry, used to keep messages under a byte limit.
pub fn entry_size(entry: &LogEntry) -> usize {
    // Term and index, plus the payload.
    16 + match &entry.command {
        Command::Noop => 0,
        Command::Append(batch) => batch.payload_len(),
        Command::CreateTopic { name } => name.as_str().len(),
        Command::Reconfigure(members) => members.len() * 8,
    }
}

/// A log held in memory.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct MemLog {
    entries: Vec<LogEntry>,
}

impl MemLog {
    /// An empty log.
    pub fn new() -> Self {
        MemLog::default()
    }

    /// A log holding `entries`.
    ///
    /// # Panics
    ///
    /// If the indexes do not run 1, 2, 3 and so on, or a term goes down.
    pub fn from_entries(entries: Vec<LogEntry>) -> Self {
        let mut term = Term::ZERO;
        for (position, entry) in entries.iter().enumerate() {
            assert_eq!(
                entry.index().get(),
                position as u64 + 1,
                "log indexes must be consecutive from 1"
            );
            assert!(entry.term() >= term, "log terms must not decrease");
            term = entry.term();
        }
        MemLog { entries }
    }

    /// Every entry, in order.
    pub fn as_slice(&self) -> &[LogEntry] {
        &self.entries
    }

    /// The entry at `index`, if there is one.
    pub fn get(&self, index: LogIndex) -> Option<&LogEntry> {
        let position = usize::try_from(index.get().checked_sub(1)?).ok()?;
        self.entries.get(position)
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True if the log holds no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl LogStore for MemLog {
    type Error = Infallible;

    fn last_id(&self) -> LogId {
        self.entries.last().map_or(LogId::ZERO, |entry| entry.id)
    }

    fn term(&self, index: LogIndex) -> Result<Option<Term>, Infallible> {
        if index.is_zero() {
            return Ok(Some(Term::ZERO));
        }
        Ok(self.get(index).map(LogEntry::term))
    }

    fn entries(
        &self,
        from: LogIndex,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Vec<LogEntry>, Infallible> {
        let start = usize::try_from(from.get().saturating_sub(1)).unwrap_or(usize::MAX);
        let mut out = Vec::new();
        let mut bytes = 0usize;
        for entry in self.entries.iter().skip(start).take(max_entries) {
            bytes = bytes.saturating_add(entry_size(entry));
            if !out.is_empty() && bytes > max_bytes {
                break;
            }
            out.push(entry.clone());
        }
        Ok(out)
    }

    fn append(&mut self, entries: &[LogEntry]) -> Result<(), Infallible> {
        for entry in entries {
            assert_eq!(
                entry.index().get(),
                self.entries.len() as u64 + 1,
                "appended entry does not continue the log"
            );
            self.entries.push(entry.clone());
        }
        Ok(())
    }

    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), Infallible> {
        let keep = usize::try_from(from.get().saturating_sub(1)).unwrap_or(usize::MAX);
        self.entries.truncate(keep);
        Ok(())
    }
}
