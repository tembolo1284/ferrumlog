//! Everything `fl-raft` needs to run on real disks and real networks.
//!
//! `fl-raft` is a pure state machine. It asks for a log through a trait
//! and hands back messages as plain values. This crate supplies the real
//! versions of both, and nothing else:
//!
//! * [`SharedLog`] puts the on-disk log from `fl-storage` behind the
//!   [`fl_raft::LogStore`] trait.
//! * [`wire`] converts Raft messages to and from the protobuf types of the
//!   `Raft` gRPC service.
//! * [`Transport`] sends a node's outgoing messages to its peers.
//! * [`RaftService`] receives the peers' messages.
//! * [`PeerMap`] is the list of cluster members and their addresses.
//!
//! The loop that ties these to a [`fl_raft::Node`] lives in `fl-server`.
//!
//! # Requests and messages
//!
//! Raft in `fl-raft` speaks in one-way messages. gRPC speaks in requests
//! and responses. The mapping is simple. A `RequestVote` or
//! `AppendEntries` message becomes a request. The receiving node's answer
//! to it becomes that request's response, and on the sending side the
//! response is turned back into a message and stepped into the node.
//!
//! # Losing messages is fine
//!
//! Raft does not need a reliable network, so this crate never retries and
//! never blocks. A message for a peer that is down, slow, or backed up is
//! dropped. The next heartbeat finds out what the peer is missing.
//!
//! # Security
//!
//! There is no TLS and no authentication. Any process that can reach the
//! port can speak the Raft protocol to a node. Keep the port closed to
//! everything but the other members of the cluster.

mod peers;
mod service;
mod storage;
mod transport;
pub mod wire;

pub use peers::{PeerError, PeerMap};
pub use service::{RaftService, RequestSink};
pub use storage::{hard_state_from_disk, hard_state_to_disk, SharedLog};
pub use transport::{ReplySink, Transport, TransportOptions};

/// Largest Raft message a node sends or accepts, in bytes.
///
/// One log entry can carry a 2 MiB record batch, and a message always
/// carries at least one entry, so this has to sit well above that.
pub const MAX_MESSAGE_BYTES: usize = 8 << 20;
