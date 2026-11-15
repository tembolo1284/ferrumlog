//! Conversions between wire messages and `fl-types` domain types.
//!
//! Domain to wire is infallible. Wire to domain is checked, because the
//! bytes came from another process.

use std::fmt;

use fl_types::{
    Command, LogEntry, LogId, LogIndex, Membership, NodeId, Record, RecordBatch, Term, Timestamp,
    TopicId, TopicName,
};

use crate::pb;

/// A wire message could not be turned into a domain type.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ConvertError {
    /// A message field that must be present was not set.
    MissingField(&'static str),
    /// A field was present but its value is not allowed.
    Invalid(fl_types::Error),
}

impl fmt::Display for ConvertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConvertError::MissingField(field) => write!(f, "missing field `{field}`"),
            ConvertError::Invalid(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ConvertError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConvertError::MissingField(_) => None,
            ConvertError::Invalid(e) => Some(e),
        }
    }
}

impl From<fl_types::Error> for ConvertError {
    fn from(e: fl_types::Error) -> Self {
        ConvertError::Invalid(e)
    }
}

/// A malformed request is the caller's fault, so it maps to
/// `INVALID_ARGUMENT`.
impl From<ConvertError> for tonic::Status {
    fn from(e: ConvertError) -> Self {
        tonic::Status::invalid_argument(e.to_string())
    }
}

// ---- Record ----

impl From<Record> for pb::Record {
    fn from(record: Record) -> Self {
        pb::Record {
            key: record.key,
            value: record.value,
            timestamp_ms: record.timestamp.as_millis(),
        }
    }
}

impl From<pb::Record> for Record {
    fn from(record: pb::Record) -> Self {
        Record {
            key: record.key,
            value: record.value,
            timestamp: Timestamp::from_millis(record.timestamp_ms),
        }
    }
}

// ---- RecordBatch ----

impl From<RecordBatch> for pb::RecordBatch {
    fn from(batch: RecordBatch) -> Self {
        pb::RecordBatch {
            topic_id: batch.topic.get(),
            records: batch.records.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<pb::RecordBatch> for RecordBatch {
    fn from(batch: pb::RecordBatch) -> Self {
        RecordBatch::new(
            TopicId::new(batch.topic_id),
            batch.records.into_iter().map(Into::into).collect(),
        )
    }
}

// ---- Membership ----

impl From<&Membership> for pb::Membership {
    fn from(membership: &Membership) -> Self {
        pb::Membership {
            voters: membership.voters().map(NodeId::get).collect(),
        }
    }
}

impl TryFrom<pb::Membership> for Membership {
    type Error = ConvertError;

    fn try_from(membership: pb::Membership) -> Result<Self, ConvertError> {
        Ok(Membership::new(
            membership.voters.into_iter().map(NodeId::new),
        )?)
    }
}

// ---- LogId ----

impl From<LogId> for pb::LogId {
    fn from(id: LogId) -> Self {
        pb::LogId {
            term: id.term.get(),
            index: id.index.get(),
        }
    }
}

impl From<pb::LogId> for LogId {
    fn from(id: pb::LogId) -> Self {
        LogId::new(Term::new(id.term), LogIndex::new(id.index))
    }
}

/// Reads an optional [`pb::LogId`] field. Protobuf omits a message field
/// whose contents are all defaults, so an absent id means [`LogId::ZERO`].
pub(crate) fn log_id_or_zero(id: Option<pb::LogId>) -> LogId {
    id.map_or(LogId::ZERO, Into::into)
}

// ---- LogEntry ----

impl From<LogEntry> for pb::Entry {
    fn from(entry: LogEntry) -> Self {
        use pb::entry::Command as Wire;
        let command = match entry.command {
            Command::Noop => Wire::Noop(pb::NoopCommand {}),
            Command::Append(batch) => Wire::Append(batch.into()),
            Command::CreateTopic { name } => Wire::CreateTopic(pb::CreateTopicCommand {
                name: name.into_string(),
            }),
            Command::Reconfigure(membership) => Wire::Reconfigure((&membership).into()),
        };
        pb::Entry {
            term: entry.id.term.get(),
            index: entry.id.index.get(),
            command: Some(command),
        }
    }
}

impl TryFrom<pb::Entry> for LogEntry {
    type Error = ConvertError;

    fn try_from(entry: pb::Entry) -> Result<Self, ConvertError> {
        use pb::entry::Command as Wire;
        let command = match entry.command {
            None => return Err(ConvertError::MissingField("Entry.command")),
            Some(Wire::Noop(_)) => Command::Noop,
            Some(Wire::Append(batch)) => Command::Append(batch.into()),
            Some(Wire::CreateTopic(create)) => Command::CreateTopic {
                name: TopicName::new(create.name)?,
            },
            Some(Wire::Reconfigure(membership)) => Command::Reconfigure(membership.try_into()?),
        };
        Ok(LogEntry::new(
            Term::new(entry.term),
            LogIndex::new(entry.index),
            command,
        ))
    }
}

// ---- Raft request helpers ----

impl pb::RequestVoteRequest {
    /// Term and index of the candidate's last entry.
    pub fn last_log_id(&self) -> LogId {
        log_id_or_zero(self.last_log)
    }
}

impl pb::AppendEntriesRequest {
    /// Term and index of the entry just before the ones being sent.
    pub fn prev_log_id(&self) -> LogId {
        log_id_or_zero(self.prev_log)
    }
}

impl pb::StatusResponse {
    /// Term and index of the node's last entry.
    pub fn last_log_id(&self) -> LogId {
        log_id_or_zero(self.last_log)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    fn sample_entries() -> Vec<LogEntry> {
        let batch = RecordBatch::new(
            TopicId::new(7),
            vec![
                Record::new("px=99.5")
                    .with_key("UST10Y")
                    .with_timestamp(Timestamp::from_millis(1_700_000_000_000)),
                Record::new(""),
                Record::new(vec![0xAB; 300]).with_key(""),
            ],
        );
        let members = Membership::new([1, 2, 3].map(NodeId::new)).unwrap();
        vec![
            LogEntry::new(Term::new(1), LogIndex::new(1), Command::Noop),
            LogEntry::new(
                Term::new(1),
                LogIndex::new(2),
                Command::CreateTopic {
                    name: TopicName::new("ust.quotes").unwrap(),
                },
            ),
            LogEntry::new(Term::new(2), LogIndex::new(3), Command::Append(batch)),
            LogEntry::new(
                Term::new(u64::MAX),
                LogIndex::new(u64::MAX),
                Command::Reconfigure(members),
            ),
        ]
    }

    #[test]
    fn entries_round_trip_through_the_wire_types() {
        for entry in sample_entries() {
            let wire: pb::Entry = entry.clone().into();
            let back = LogEntry::try_from(wire).unwrap();
            assert_eq!(back, entry);
        }
    }

    #[test]
    fn entries_round_trip_through_encoded_bytes() {
        for entry in sample_entries() {
            let bytes = pb::Entry::from(entry.clone()).encode_to_vec();
            let wire = pb::Entry::decode(bytes.as_slice()).unwrap();
            assert_eq!(LogEntry::try_from(wire).unwrap(), entry);
        }
    }

    #[test]
    fn empty_key_and_missing_key_stay_distinct() {
        let with_empty = Record::new("v").with_key("");
        let without = Record::new("v");
        for record in [with_empty, without] {
            let bytes = pb::Record::from(record.clone()).encode_to_vec();
            let back: Record = pb::Record::decode(bytes.as_slice()).unwrap().into();
            assert_eq!(back, record);
        }
    }

    #[test]
    fn entry_without_a_command_is_rejected() {
        let wire = pb::Entry {
            term: 1,
            index: 1,
            command: None,
        };
        assert_eq!(
            LogEntry::try_from(wire),
            Err(ConvertError::MissingField("Entry.command"))
        );
    }

    #[test]
    fn bad_topic_name_is_rejected() {
        let wire = pb::Entry {
            term: 1,
            index: 1,
            command: Some(pb::entry::Command::CreateTopic(pb::CreateTopicCommand {
                name: "not a valid name".into(),
            })),
        };
        assert!(matches!(
            LogEntry::try_from(wire),
            Err(ConvertError::Invalid(
                fl_types::Error::InvalidTopicName { .. }
            ))
        ));
    }

    #[test]
    fn empty_membership_is_rejected() {
        let wire = pb::Membership { voters: vec![] };
        assert_eq!(
            Membership::try_from(wire),
            Err(ConvertError::Invalid(fl_types::Error::EmptyMembership))
        );
    }

    #[test]
    fn membership_round_trips_in_order() {
        let members = Membership::new([3, 1, 2].map(NodeId::new)).unwrap();
        let wire = pb::Membership::from(&members);
        assert_eq!(wire.voters, vec![1, 2, 3]);
        assert_eq!(Membership::try_from(wire).unwrap(), members);
    }

    #[test]
    fn log_id_round_trips_and_absent_means_zero() {
        let id = LogId::new(Term::new(4), LogIndex::new(17));
        assert_eq!(LogId::from(pb::LogId::from(id)), id);

        let request = pb::AppendEntriesRequest::default();
        assert_eq!(request.prev_log_id(), LogId::ZERO);
        let request = pb::RequestVoteRequest {
            last_log: Some(id.into()),
            ..Default::default()
        };
        assert_eq!(request.last_log_id(), id);
        assert_eq!(pb::StatusResponse::default().last_log_id(), LogId::ZERO);
    }

    #[test]
    fn convert_error_becomes_invalid_argument() {
        let status: tonic::Status = ConvertError::MissingField("Entry.command").into();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(status.message(), "missing field `Entry.command`");
    }
}
