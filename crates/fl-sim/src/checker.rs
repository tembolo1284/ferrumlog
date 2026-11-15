//! The safety properties, checked against what the nodes actually do.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use fl_raft::MemLog;
use fl_types::{Command, LogEntry, LogId, LogIndex, NodeId, Record, RecordBatch, Term, TopicId};

/// Which property was broken.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ViolationKind {
    /// Two nodes led in the same term.
    TwoLeaders,
    /// A node applied an entry that differs from what another node applied
    /// at the same index, or skipped an index.
    CommittedEntryChanged,
    /// A leader's log lacks an entry committed in an earlier term.
    LeaderMissingCommitted,
    /// Two logs agree on a term at some index but differ at or before it.
    LogMismatch,
    /// The same proposal was committed twice.
    DuplicateCommand,
    /// Something was committed that no client proposed.
    UnknownCommand,
    /// A leader reported a proposal as committed, but the committed log
    /// holds something else at that position.
    AckedButLost,
    /// A node's durable term went down.
    TermWentBackwards,
    /// With every fault removed, the cluster still did not recover.
    Stalled,
    /// A node hit one of its own internal assertions.
    NodePanicked,
}

/// A broken property, with enough context to reproduce it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Violation {
    /// Which property was broken.
    pub kind: ViolationKind,
    /// What exactly was seen.
    pub detail: String,
    /// The seed of the run. Running it again reproduces the violation.
    pub seed: u64,
    /// The simulated time at which it was noticed.
    pub tick: u64,
    /// The most recent events before it, oldest first. Empty unless the
    /// run had tracing on.
    pub trace: Vec<String>,
}

impl Violation {
    pub(crate) fn new(kind: ViolationKind, detail: String) -> Self {
        Violation {
            kind,
            detail,
            seed: 0,
            tick: 0,
            trace: Vec::new(),
        }
    }
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?} at tick {} with seed {}: {}",
            self.kind, self.tick, self.seed, self.detail
        )
    }
}

impl std::error::Error for Violation {}

/// Builds the command a simulated client proposes. The number identifies
/// the proposal, so the checker can recognize it in the committed log.
pub(crate) fn client_command(number: u64) -> Command {
    let record = Record::new(number.to_be_bytes().to_vec());
    Command::Append(RecordBatch::new(TopicId::new(1), vec![record]))
}

/// Reads the proposal number back out of a client command.
pub(crate) fn command_number(command: &Command) -> Option<u64> {
    let Command::Append(batch) = command else {
        return None;
    };
    let bytes: [u8; 8] = batch.records.first()?.value.as_slice().try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

/// Watches a cluster and reports the first broken property.
///
/// The checker never looks inside a node's decision making. It sees only
/// what an outside observer could: who claims to lead, what each node
/// applies, and what is in each log.
#[derive(Clone, Debug, Default)]
pub struct Checker {
    /// The one leader seen for each term.
    leaders: BTreeMap<Term, NodeId>,
    /// The committed log, built from the first node to apply each index.
    committed: Vec<LogEntry>,
    /// For each committed entry, the term of the node that applied it
    /// first. Every leader of a later term must hold the entry.
    seen_in_term: Vec<Term>,
    proposed: BTreeSet<u64>,
    committed_numbers: BTreeSet<u64>,
}

impl Checker {
    /// A checker that has seen nothing yet.
    pub fn new() -> Self {
        Checker::default()
    }

    /// The committed log as observed so far.
    pub fn committed(&self) -> &[LogEntry] {
        &self.committed
    }

    /// The number of distinct terms that had a leader.
    pub fn terms_with_a_leader(&self) -> usize {
        self.leaders.len()
    }

    /// Records that a client sent this proposal.
    pub fn note_proposed(&mut self, number: u64) {
        self.proposed.insert(number);
    }

    /// Records that `node` leads `term`. Election safety.
    pub fn observe_leader(&mut self, term: Term, node: NodeId) -> Result<(), Violation> {
        match self.leaders.insert(term, node) {
            Some(earlier) if earlier != node => Err(Violation::new(
                ViolationKind::TwoLeaders,
                format!("{earlier} and {node} both led {term}"),
            )),
            _ => Ok(()),
        }
    }

    /// Records that `node`, currently in `node_term`, applied `entry`.
    /// State machine safety.
    pub fn observe_apply(
        &mut self,
        node: NodeId,
        node_term: Term,
        entry: &LogEntry,
    ) -> Result<(), Violation> {
        let position = entry.index().get() as usize;
        if let Some(known) = self.committed.get(position.wrapping_sub(1)) {
            if known == entry {
                return Ok(());
            }
            return Err(Violation::new(
                ViolationKind::CommittedEntryChanged,
                format!(
                    "{node} applied {} at {}, but {} was applied there before",
                    entry.id,
                    entry.index(),
                    known.id
                ),
            ));
        }
        if position != self.committed.len() + 1 {
            return Err(Violation::new(
                ViolationKind::CommittedEntryChanged,
                format!(
                    "{node} applied {} while only {} entries are committed",
                    entry.index(),
                    self.committed.len()
                ),
            ));
        }

        if let Some(number) = command_number(&entry.command) {
            if !self.proposed.contains(&number) {
                return Err(Violation::new(
                    ViolationKind::UnknownCommand,
                    format!("proposal {number} at {} was never sent", entry.id),
                ));
            }
            if !self.committed_numbers.insert(number) {
                return Err(Violation::new(
                    ViolationKind::DuplicateCommand,
                    format!("proposal {number} was committed again at {}", entry.id),
                ));
            }
        }
        self.committed.push(entry.clone());
        self.seen_in_term.push(node_term);
        Ok(())
    }

    /// Checks that a leader of `term` holds every entry that was committed
    /// in an earlier term. Leader completeness.
    pub fn check_leader_log(
        &self,
        node: NodeId,
        term: Term,
        log: &MemLog,
    ) -> Result<(), Violation> {
        for (entry, seen_in) in self.committed.iter().zip(&self.seen_in_term) {
            if *seen_in >= term {
                // Committed during this leader's own term or later, so the
                // leader is not required to have had it when elected.
                continue;
            }
            if log.get(entry.index()) != Some(entry) {
                return Err(Violation::new(
                    ViolationKind::LeaderMissingCommitted,
                    format!("{node} leads {term} without committed entry {}", entry.id),
                ));
            }
        }
        Ok(())
    }

    /// Checks a proposal that a leader has reported as committed.
    pub fn check_ack(&self, id: LogId, number: u64) -> Result<(), Violation> {
        let found = self
            .committed
            .get((id.index.get() as usize).wrapping_sub(1));
        let matches = found
            .is_some_and(|entry| entry.id == id && command_number(&entry.command) == Some(number));
        if matches {
            return Ok(());
        }
        Err(Violation::new(
            ViolationKind::AckedButLost,
            format!(
                "proposal {number} was acknowledged at {id}, but the committed log holds {:?} there",
                found.map(|entry| entry.id)
            ),
        ))
    }

    /// Compares every pair of logs. Log matching.
    pub fn check_logs(&self, logs: &[(NodeId, &MemLog)]) -> Result<(), Violation> {
        for (position, (a_id, a)) in logs.iter().enumerate() {
            for (b_id, b) in &logs[position + 1..] {
                let (a, b) = (a.as_slice(), b.as_slice());
                let shared = a.len().min(b.len());
                // The highest index where both hold an entry of the same
                // term. Everything up to there has to be identical.
                let Some(agree) = (0..shared).rev().find(|&at| a[at].term() == b[at].term()) else {
                    continue;
                };
                if let Some(at) = (0..=agree).find(|&at| a[at] != b[at]) {
                    return Err(Violation::new(
                        ViolationKind::LogMismatch,
                        format!(
                            "{a_id} and {b_id} agree at {} but differ at {}: {} against {}",
                            LogIndex::new(agree as u64 + 1),
                            LogIndex::new(at as u64 + 1),
                            a[at].id,
                            b[at].id
                        ),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(id: u64) -> NodeId {
        NodeId::new(id)
    }

    fn t(term: u64) -> Term {
        Term::new(term)
    }

    fn entry(term: u64, index: u64, number: u64) -> LogEntry {
        LogEntry::new(t(term), LogIndex::new(index), client_command(number))
    }

    fn checker_knowing(numbers: &[u64]) -> Checker {
        let mut checker = Checker::new();
        for number in numbers {
            checker.note_proposed(*number);
        }
        checker
    }

    fn kind(result: Result<(), Violation>) -> ViolationKind {
        result.unwrap_err().kind
    }

    #[test]
    fn command_numbers_round_trip() {
        assert_eq!(command_number(&client_command(77)), Some(77));
        assert_eq!(command_number(&Command::Noop), None);
    }

    #[test]
    fn one_leader_per_term() {
        let mut checker = Checker::new();
        checker.observe_leader(t(1), n(1)).unwrap();
        // Seeing the same leader again is fine.
        checker.observe_leader(t(1), n(1)).unwrap();
        checker.observe_leader(t(2), n(2)).unwrap();
        assert_eq!(checker.terms_with_a_leader(), 2);
        assert_eq!(
            kind(checker.observe_leader(t(2), n(3))),
            ViolationKind::TwoLeaders
        );
    }

    #[test]
    fn applied_entries_must_agree_and_be_in_order() {
        let mut checker = checker_knowing(&[1, 2, 3]);
        checker.observe_apply(n(1), t(1), &entry(1, 1, 1)).unwrap();
        checker.observe_apply(n(1), t(1), &entry(1, 2, 2)).unwrap();
        // A second node applying the same entries is fine.
        checker.observe_apply(n(2), t(1), &entry(1, 1, 1)).unwrap();
        assert_eq!(checker.committed().len(), 2);

        // A different entry at an applied index.
        assert_eq!(
            kind(checker.observe_apply(n(3), t(2), &entry(2, 2, 3))),
            ViolationKind::CommittedEntryChanged
        );
        // Skipping index 3.
        assert_eq!(
            kind(checker.observe_apply(n(3), t(2), &entry(2, 4, 3))),
            ViolationKind::CommittedEntryChanged
        );
    }

    #[test]
    fn commands_are_committed_once_and_only_if_proposed() {
        let mut checker = checker_knowing(&[1]);
        checker.observe_apply(n(1), t(1), &entry(1, 1, 1)).unwrap();
        assert_eq!(
            kind(checker.observe_apply(n(1), t(1), &entry(1, 2, 1))),
            ViolationKind::DuplicateCommand
        );
        assert_eq!(
            kind(checker.observe_apply(n(1), t(1), &entry(1, 2, 9))),
            ViolationKind::UnknownCommand
        );
        // A no-op carries no proposal and is always acceptable.
        let noop = LogEntry::new(t(1), LogIndex::new(2), Command::Noop);
        checker.observe_apply(n(1), t(1), &noop).unwrap();
    }

    #[test]
    fn a_later_leader_must_hold_earlier_commits() {
        let mut checker = checker_knowing(&[1, 2]);
        checker.observe_apply(n(1), t(1), &entry(1, 1, 1)).unwrap();
        checker.observe_apply(n(1), t(3), &entry(3, 2, 2)).unwrap();

        let full = MemLog::from_entries(vec![entry(1, 1, 1), entry(3, 2, 2)]);
        let short = MemLog::from_entries(vec![entry(1, 1, 1)]);
        let wrong = MemLog::from_entries(vec![entry(2, 1, 2)]);

        checker.check_leader_log(n(2), t(4), &full).unwrap();
        // The second entry was committed in term 3, so a leader of term 3
        // or earlier is not required to hold it yet.
        checker.check_leader_log(n(2), t(3), &short).unwrap();
        assert_eq!(
            kind(checker.check_leader_log(n(2), t(4), &short)),
            ViolationKind::LeaderMissingCommitted
        );
        assert_eq!(
            kind(checker.check_leader_log(n(2), t(2), &wrong)),
            ViolationKind::LeaderMissingCommitted
        );
    }

    #[test]
    fn an_acknowledged_proposal_must_be_where_the_leader_said() {
        let mut checker = checker_knowing(&[1, 2]);
        checker.observe_apply(n(1), t(1), &entry(1, 1, 1)).unwrap();

        let at = |term, index| LogId::new(t(term), LogIndex::new(index));
        checker.check_ack(at(1, 1), 1).unwrap();
        // Another proposal, another term, or an index nobody applied.
        for (id, number) in [(at(1, 1), 2), (at(2, 1), 1), (at(1, 2), 2)] {
            assert_eq!(
                kind(checker.check_ack(id, number)),
                ViolationKind::AckedButLost
            );
        }
    }

    #[test]
    fn logs_that_agree_on_a_term_must_agree_before_it() {
        let checker = Checker::new();
        let a = MemLog::from_entries(vec![entry(1, 1, 1), entry(1, 2, 2), entry(2, 3, 3)]);
        // Shorter, and a diverged tail in a different term. Both are fine.
        let prefix = MemLog::from_entries(vec![entry(1, 1, 1)]);
        let diverged = MemLog::from_entries(vec![entry(1, 1, 1), entry(1, 2, 2), entry(3, 3, 4)]);
        let empty = MemLog::new();
        checker
            .check_logs(&[
                (n(1), &a),
                (n(2), &prefix),
                (n(3), &diverged),
                (n(4), &empty),
            ])
            .unwrap();

        // Same term at index 3, but a different entry at index 2.
        let broken = MemLog::from_entries(vec![entry(1, 1, 1), entry(1, 2, 9), entry(2, 3, 3)]);
        let violation = checker
            .check_logs(&[(n(1), &a), (n(2), &broken)])
            .unwrap_err();
        assert_eq!(violation.kind, ViolationKind::LogMismatch);
        assert!(violation.to_string().contains("LogMismatch"));
    }
}
