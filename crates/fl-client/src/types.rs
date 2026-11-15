//! The values the client hands back, built from the wire messages.

use std::fmt;

use fl_proto::pb;
use fl_types::{LogId, LogIndex, NodeId, Offset, Record, Term, TopicId};

use crate::error::{Error, Result};

/// The result of [`Client::create_topic`](crate::Client::create_topic).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TopicCreated {
    /// The topic's id.
    pub id: TopicId,
    /// False if the topic already existed.
    pub created: bool,
}

/// A topic and how much it holds.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TopicInfo {
    /// The topic's id.
    pub id: TopicId,
    /// The topic's name.
    pub name: String,
    /// Offset the next record will receive. Equal to the number of records
    /// in the topic.
    pub high_watermark: Offset,
}

/// The result of [`Client::produce`](crate::Client::produce).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Produced {
    /// Offset given to the first record.
    pub base_offset: Offset,
    /// Number of records written.
    pub count: u32,
}

/// A record together with its position in the topic.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ConsumedRecord {
    /// The record's offset.
    pub offset: Offset,
    /// The record.
    pub record: Record,
}

/// Records returned by one fetch or one subscription message.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Batch {
    /// Records in offset order with no gaps.
    pub records: Vec<ConsumedRecord>,
    /// Offset the next record in the topic will receive, as of this reply.
    pub high_watermark: Offset,
}

impl Batch {
    /// The offset to ask for next: one past the last record here, or `None`
    /// if the batch is empty.
    pub fn next_offset(&self) -> Option<Offset> {
        self.records.last().map(|record| record.offset.next())
    }
}

/// Bounds on one fetch. Zero means "use the server's default".
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct FetchOptions {
    /// Most records to return.
    pub max_records: u32,
    /// Most record bytes to return.
    pub max_bytes: u32,
}

/// Where a subscription begins.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StartAt {
    /// The first record in the topic.
    Earliest,
    /// Only records written after the subscription opens.
    Latest,
    /// A specific offset.
    Offset(Offset),
}

impl StartAt {
    pub(crate) fn to_wire(self) -> pb::subscribe_request::Start {
        use pb::subscribe_request::Start;
        match self {
            StartAt::Earliest => Start::Position(pb::StartPosition::Earliest as i32),
            StartAt::Latest => Start::Position(pb::StartPosition::Latest as i32),
            StartAt::Offset(offset) => Start::Offset(offset.get()),
        }
    }
}

/// A node's role in the current term.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
    /// Replicating from a leader.
    Follower,
    /// Running for leader.
    Candidate,
    /// Accepting writes.
    Leader,
    /// The node reported a role this client does not know.
    Unknown,
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Role::Follower => "follower",
            Role::Candidate => "candidate",
            Role::Leader => "leader",
            Role::Unknown => "unknown",
        })
    }
}

/// A cluster member and the address clients reach it on.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Peer {
    /// The member's node id.
    pub node_id: NodeId,
    /// The member's client-facing address.
    pub addr: String,
}

/// One node's view of itself and the cluster.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct NodeStatus {
    /// The endpoint that answered.
    pub endpoint: String,
    /// The node's id.
    pub node_id: NodeId,
    /// The node's role.
    pub role: Role,
    /// The node's current term.
    pub term: Term,
    /// The leader the node follows, if it knows one.
    pub leader_id: Option<NodeId>,
    /// Index of the first entry the node still holds.
    pub first_log_index: LogIndex,
    /// Term and index of the node's last log entry.
    pub last_log: LogId,
    /// Highest index known to be committed.
    pub commit_index: LogIndex,
    /// Highest index applied to the state machine.
    pub applied_index: LogIndex,
    /// The voting members as the node knows them.
    pub voters: Vec<Peer>,
    /// Number of segment files on disk.
    pub segment_count: u32,
    /// Total size of the segment files in bytes.
    pub log_bytes: u64,
}

impl NodeStatus {
    pub(crate) fn from_wire(endpoint: String, status: pb::StatusResponse) -> Self {
        let role = match pb::Role::try_from(status.role) {
            Ok(pb::Role::Follower) => Role::Follower,
            Ok(pb::Role::Candidate) => Role::Candidate,
            Ok(pb::Role::Leader) => Role::Leader,
            Ok(pb::Role::Unspecified) | Err(_) => Role::Unknown,
        };
        NodeStatus {
            endpoint,
            node_id: NodeId::new(status.node_id),
            role,
            term: Term::new(status.term),
            leader_id: status.leader_id.map(NodeId::new),
            first_log_index: LogIndex::new(status.first_log_index),
            last_log: status.last_log_id(),
            commit_index: LogIndex::new(status.commit_index),
            applied_index: LogIndex::new(status.applied_index),
            voters: status
                .voters
                .into_iter()
                .map(|peer| Peer {
                    node_id: NodeId::new(peer.node_id),
                    addr: peer.addr,
                })
                .collect(),
            segment_count: status.segment_count,
            log_bytes: status.log_bytes,
        }
    }
}

impl From<pb::TopicInfo> for TopicInfo {
    fn from(topic: pb::TopicInfo) -> Self {
        TopicInfo {
            id: TopicId::new(topic.topic_id),
            name: topic.name,
            high_watermark: Offset::new(topic.high_watermark),
        }
    }
}

/// Builds a [`Batch`] from the records and watermark of a reply.
pub(crate) fn batch(records: Vec<pb::ConsumedRecord>, high_watermark: u64) -> Result<Batch> {
    let records = records
        .into_iter()
        .map(|consumed| {
            let record = consumed
                .record
                .ok_or(Error::Protocol("a consumed record has no record"))?;
            Ok(ConsumedRecord {
                offset: Offset::new(consumed.offset),
                record: record.into(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Batch {
        records,
        high_watermark: Offset::new(high_watermark),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wire(offset: u64, value: &str) -> pb::ConsumedRecord {
        pb::ConsumedRecord {
            offset,
            record: Some(pb::Record {
                key: None,
                value: value.as_bytes().to_vec(),
                timestamp_ms: 7,
            }),
        }
    }

    #[test]
    fn batch_converts_and_reports_the_next_offset() {
        let built = batch(vec![wire(4, "a"), wire(5, "b")], 9).unwrap();
        assert_eq!(built.records.len(), 2);
        assert_eq!(built.records[1].offset, Offset::new(5));
        assert_eq!(built.records[1].record.value, b"b");
        assert_eq!(built.high_watermark, Offset::new(9));
        assert_eq!(built.next_offset(), Some(Offset::new(6)));

        let empty = batch(vec![], 9).unwrap();
        assert_eq!(empty.next_offset(), None);
    }

    #[test]
    fn a_record_without_a_body_is_a_protocol_error() {
        let broken = pb::ConsumedRecord {
            offset: 1,
            record: None,
        };
        assert!(matches!(
            batch(vec![wire(0, "a"), broken], 2),
            Err(Error::Protocol(_))
        ));
    }

    #[test]
    fn status_converts_roles_and_missing_fields() {
        let status = pb::StatusResponse {
            node_id: 2,
            role: pb::Role::Follower as i32,
            term: 5,
            leader_id: Some(1),
            first_log_index: 1,
            last_log: Some(pb::LogId { term: 5, index: 40 }),
            commit_index: 39,
            applied_index: 38,
            voters: vec![pb::Peer {
                node_id: 1,
                addr: "n1:7001".into(),
            }],
            segment_count: 3,
            log_bytes: 1024,
        };
        let converted = NodeStatus::from_wire("http://n2:7001".into(), status);
        assert_eq!(converted.role, Role::Follower);
        assert_eq!(converted.role.to_string(), "follower");
        assert_eq!(converted.leader_id, Some(NodeId::new(1)));
        assert_eq!(
            converted.last_log,
            LogId::new(Term::new(5), LogIndex::new(40))
        );
        assert_eq!(converted.voters[0].addr, "n1:7001");

        // A reply from a newer server with a role this client has never
        // heard of, and nothing else filled in.
        let sparse = pb::StatusResponse {
            role: 99,
            ..Default::default()
        };
        let converted = NodeStatus::from_wire("x".into(), sparse);
        assert_eq!(converted.role, Role::Unknown);
        assert_eq!(converted.leader_id, None);
        assert_eq!(converted.last_log, LogId::ZERO);
    }

    #[test]
    fn start_positions_map_to_the_wire() {
        use pb::subscribe_request::Start;
        assert_eq!(
            StartAt::Earliest.to_wire(),
            Start::Position(pb::StartPosition::Earliest as i32)
        );
        assert_eq!(
            StartAt::Latest.to_wire(),
            Start::Position(pb::StartPosition::Latest as i32)
        );
        assert_eq!(
            StartAt::Offset(Offset::new(12)).to_wire(),
            Start::Offset(12)
        );
    }
}
