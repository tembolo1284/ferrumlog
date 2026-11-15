//! The messages Raft nodes exchange.
//!
//! These mirror the `Raft` gRPC service in `fl-proto`, but as plain values.
//! The transport crate converts between the two.

use fl_types::{LogEntry, LogId, LogIndex, NodeId, Term};

/// One message between two nodes.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Message {
    /// The sender.
    pub from: NodeId,
    /// The intended receiver.
    pub to: NodeId,
    /// The sender's term when it sent the message.
    pub term: Term,
    /// What the message says.
    pub body: Body,
}

/// The four kinds of Raft message.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Body {
    /// A candidate asks for a vote.
    RequestVote {
        /// Term and index of the candidate's last log entry.
        last_log: LogId,
    },
    /// The answer to a vote request.
    RequestVoteResponse {
        /// True if the vote was given.
        granted: bool,
    },
    /// The leader replicates entries. With no entries it is a heartbeat.
    AppendEntries {
        /// Term and index of the entry just before `entries`.
        prev_log: LogId,
        /// Entries to store, in order.
        entries: Vec<LogEntry>,
        /// Highest index the leader knows to be committed.
        leader_commit: LogIndex,
    },
    /// The answer to an AppendEntries.
    AppendEntriesResponse {
        /// True if the follower held `prev_log` and stored the entries.
        success: bool,
        /// On success, the last index known to match the leader's log.
        match_index: LogIndex,
        /// On failure, where the leader should send from next.
        conflict_index: LogIndex,
    },
}

impl Body {
    /// A short stable name, for traces and metric labels.
    pub fn kind(&self) -> &'static str {
        match self {
            Body::RequestVote { .. } => "request_vote",
            Body::RequestVoteResponse { .. } => "request_vote_response",
            Body::AppendEntries { .. } => "append_entries",
            Body::AppendEntriesResponse { .. } => "append_entries_response",
        }
    }
}
