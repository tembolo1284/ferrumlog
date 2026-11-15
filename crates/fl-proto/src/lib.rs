//! Protobuf messages and gRPC services for ferrumlog.
//!
//! The `.proto` files under `proto/ferrumlog/v1` define three services:
//!
//! * `Raft` is the node-to-node protocol: `RequestVote` and `AppendEntries`.
//! * `Log` is the client API: topics, produce, fetch, and subscribe.
//! * `Admin` reports a node's status.
//!
//! The generated code lives in [`pb`]. The rest of this crate is the glue
//! between those wire types and the domain types in `fl-types`:
//!
//! * `From` and `TryFrom` impls convert in both directions. Converting from
//!   the wire is fallible and returns a [`ConvertError`], because a peer can
//!   send anything.
//! * [`redirect`] carries the "I am not the leader, try this node" hint in
//!   gRPC metadata.
//!
//! Nothing here opens a socket. Transport setup belongs to `fl-net`,
//! `fl-server`, and `fl-client`.

/// Code generated from the `ferrumlog.v1` protobuf package.
#[allow(missing_docs, clippy::all)]
pub mod pb {
    tonic::include_proto!("ferrumlog.v1");
}

mod convert;
pub mod redirect;

pub use convert::ConvertError;

pub use pb::admin_client::AdminClient;
pub use pb::admin_server::{Admin, AdminServer};
pub use pb::log_client::LogClient;
pub use pb::log_server::{Log, LogServer};
pub use pb::raft_client::RaftClient;
pub use pb::raft_server::{Raft, RaftServer};
