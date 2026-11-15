//! Log entries and the commands they carry.

use crate::error::Result;
use crate::ids::{LogId, LogIndex, Term};
use crate::membership::Membership;
use crate::record::RecordBatch;
use crate::topic::TopicName;

/// A state machine command replicated through the Raft log.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Command {
    /// Does nothing when applied. A new leader appends one at the start of
    /// its term so it can commit entries left over from earlier terms.
    Noop,
    /// Appends a batch of records to a topic.
    Append(RecordBatch),
    /// Creates a topic. Applying it assigns the topic its `TopicId`.
    CreateTopic {
        /// Name of the new topic.
        name: TopicName,
    },
    /// Replaces the cluster membership.
    Reconfigure(Membership),
}

impl Command {
    /// A short stable name, used for log lines and metric labels.
    pub fn kind(&self) -> &'static str {
        match self {
            Command::Noop => "noop",
            Command::Append(_) => "append",
            Command::CreateTopic { .. } => "create_topic",
            Command::Reconfigure(_) => "reconfigure",
        }
    }

    /// Checks the command's payload. The leader calls this before proposing
    /// so an invalid command never enters the log.
    pub fn validate(&self) -> Result<()> {
        match self {
            Command::Append(batch) => batch.validate(),
            // TopicName and Membership are validated on construction.
            Command::Noop | Command::CreateTopic { .. } | Command::Reconfigure(_) => Ok(()),
        }
    }
}

/// One entry in the replicated log.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct LogEntry {
    /// Term and index of the entry.
    pub id: LogId,
    /// The command to apply once the entry commits.
    pub command: Command,
}

impl LogEntry {
    /// Builds an entry.
    pub fn new(term: Term, index: LogIndex, command: Command) -> Self {
        LogEntry {
            id: LogId::new(term, index),
            command,
        }
    }

    /// Term in which the entry was created.
    #[inline]
    pub fn term(&self) -> Term {
        self.id.term
    }

    /// Position of the entry in the log.
    #[inline]
    pub fn index(&self) -> LogIndex {
        self.id.index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::ids::{NodeId, TopicId};
    use crate::record::Record;

    #[test]
    fn kinds_are_stable() {
        let batch = RecordBatch::new(TopicId::new(1), vec![Record::new("x")]);
        let members = Membership::new([NodeId::new(1)]).unwrap();
        let name = TopicName::new("quotes").unwrap();

        assert_eq!(Command::Noop.kind(), "noop");
        assert_eq!(Command::Append(batch).kind(), "append");
        assert_eq!(Command::CreateTopic { name }.kind(), "create_topic");
        assert_eq!(Command::Reconfigure(members).kind(), "reconfigure");
    }

    #[test]
    fn validate_checks_append_payload() {
        let good = Command::Append(RecordBatch::new(TopicId::new(1), vec![Record::new("x")]));
        assert!(good.validate().is_ok());

        let empty = Command::Append(RecordBatch::new(TopicId::new(1), vec![]));
        assert_eq!(empty.validate(), Err(Error::EmptyBatch));

        assert!(Command::Noop.validate().is_ok());
    }

    #[test]
    fn entry_accessors() {
        let entry = LogEntry::new(Term::new(4), LogIndex::new(17), Command::Noop);
        assert_eq!(entry.term(), Term::new(4));
        assert_eq!(entry.index(), LogIndex::new(17));
        assert_eq!(entry.id.to_string(), "t4/i17");
    }
}
