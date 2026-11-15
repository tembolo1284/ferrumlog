//! Core vocabulary types for ferrumlog.
//!
//! Every other crate in the workspace depends on this one, so it stays small
//! and has no dependencies. It does no I/O, reads no clock, and defines no
//! wire or disk encoding. Encoding lives in `fl-storage` (disk) and
//! `fl-proto` (network).
//!
//! Two kinds of position appear throughout the system and must not be mixed:
//!
//! * [`LogIndex`] is a position in the replicated Raft log. Every command
//!   gets one, including no-ops and membership changes.
//! * [`Offset`] is a position within one topic, as seen by consumers. Only
//!   records get one, and the state machine assigns it when an entry is
//!   applied.

mod entry;
mod error;
mod ids;
mod membership;
mod record;
mod topic;

pub use entry::{Command, LogEntry};
pub use error::{Error, Result};
pub use ids::{LogId, LogIndex, NodeId, Offset, Term, Timestamp, TopicId};
pub use membership::Membership;
pub use record::{Record, RecordBatch, MAX_BATCH_BYTES, MAX_BATCH_RECORDS, MAX_RECORD_BYTES};
pub use topic::{TopicName, MAX_TOPIC_NAME_LEN};
