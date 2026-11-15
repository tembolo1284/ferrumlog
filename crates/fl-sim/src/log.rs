//! A log that distinguishes what was written from what was synced.

use std::convert::Infallible;

use fl_raft::{LogStore, MemLog};
use fl_types::{LogEntry, LogId, LogIndex, Term};

/// An in-memory log with a model of durability.
///
/// Writes go to a working copy. [`SimLog::sync`] brings the durable copy
/// up to date. A crash keeps only the durable copy, which is exactly what
/// a real disk does with writes that were never fsynced.
#[derive(Clone, Debug, Default)]
pub struct SimLog {
    working: MemLog,
    durable: MemLog,
    /// The lowest index written or removed since the last sync. Everything
    /// below it is the same in both copies.
    dirty_from: Option<LogIndex>,
}

impl SimLog {
    /// A log holding `durable`, as a node finds it after a restart.
    pub fn from_durable(durable: MemLog) -> Self {
        SimLog {
            working: durable.clone(),
            durable,
            dirty_from: None,
        }
    }

    /// Makes everything written so far durable.
    pub fn sync(&mut self) {
        let Some(from) = self.dirty_from.take() else {
            return;
        };
        self.durable
            .truncate_suffix(from)
            .unwrap_or_else(|never| match never {});
        let position = usize::try_from(from.get() - 1).expect("index fits in usize");
        let changed = self.working.as_slice().get(position..).unwrap_or(&[]);
        self.durable
            .append(changed)
            .unwrap_or_else(|never| match never {});
    }

    /// The log as the node sees it, including unsynced writes.
    pub fn working(&self) -> &MemLog {
        &self.working
    }

    /// What would survive a crash right now.
    pub fn durable(&self) -> &MemLog {
        &self.durable
    }

    /// Crashes the log. Unsynced writes are gone.
    pub fn into_durable(self) -> MemLog {
        self.durable
    }

    fn mark_dirty(&mut self, from: LogIndex) {
        self.dirty_from = Some(self.dirty_from.map_or(from, |current| current.min(from)));
    }
}

impl LogStore for SimLog {
    type Error = Infallible;

    fn last_id(&self) -> LogId {
        self.working.last_id()
    }

    fn term(&self, index: LogIndex) -> Result<Option<Term>, Infallible> {
        self.working.term(index)
    }

    fn entries(
        &self,
        from: LogIndex,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Vec<LogEntry>, Infallible> {
        self.working.entries(from, max_entries, max_bytes)
    }

    fn append(&mut self, entries: &[LogEntry]) -> Result<(), Infallible> {
        if let Some(first) = entries.first() {
            self.mark_dirty(first.index());
        }
        self.working.append(entries)
    }

    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), Infallible> {
        self.mark_dirty(from);
        self.working.truncate_suffix(from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fl_types::Command;

    fn entry(term: u64, index: u64) -> LogEntry {
        LogEntry::new(Term::new(term), LogIndex::new(index), Command::Noop)
    }

    #[test]
    fn unsynced_writes_do_not_survive_a_crash() {
        let mut log = SimLog::default();
        log.append(&[entry(1, 1), entry(1, 2)]).unwrap();
        assert_eq!(log.working().len(), 2);
        assert_eq!(log.durable().len(), 0);

        log.sync();
        log.append(&[entry(1, 3)]).unwrap();
        assert_eq!(log.durable().len(), 2);
        assert_eq!(log.into_durable().len(), 2);
    }

    #[test]
    fn a_synced_truncation_is_durable_and_an_unsynced_one_is_not() {
        let mut log = SimLog::default();
        log.append(&[entry(1, 1), entry(1, 2), entry(1, 3)])
            .unwrap();
        log.sync();

        // Replace the tail without syncing. The old tail is what survives.
        log.truncate_suffix(LogIndex::new(2)).unwrap();
        log.append(&[entry(2, 2)]).unwrap();
        assert_eq!(log.last_id().term, Term::new(2));
        assert_eq!(log.durable().len(), 3);
        assert_eq!(log.durable().last_id().term, Term::new(1));

        // After a sync both copies agree.
        log.sync();
        assert_eq!(log.durable(), log.working());
        // A second sync with nothing new changes nothing.
        log.sync();
        assert_eq!(log.durable().len(), 2);

        let reopened = SimLog::from_durable(log.into_durable());
        assert_eq!(reopened.working().len(), 2);
        assert_eq!(reopened.term(LogIndex::new(2)).unwrap(), Some(Term::new(2)));
        assert_eq!(reopened.entries(LogIndex::new(1), 9, 999).unwrap().len(), 2);
    }
}
