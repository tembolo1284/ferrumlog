//! The ferrumlog state machine.
//!
//! The replicated log is a flat sequence of commands. This crate gives that
//! sequence meaning: it turns committed entries into topics and per-topic
//! offsets, and it serves reads by offset.
//!
//! # The pieces
//!
//! * [`TopicState`] is the state machine. Feeding it committed entries in
//!   order with [`TopicState::apply`] is the only way it changes. It is
//!   deterministic, so every node that applies the same entries ends up
//!   with an equal `TopicState`.
//! * [`TopicState::prepare_create_topic`] and
//!   [`TopicState::prepare_produce`] validate a client request and turn it
//!   into a [`fl_types::Command`] to be proposed. They do not change
//!   anything.
//! * [`TopicState::fetch`] reads records by topic and offset.
//! * [`Standalone`] wires the above to a [`fl_storage::Log`] for a
//!   single-node deployment with no replication.
//!
//! # How offsets map to the log
//!
//! Records are not stored a second time. The log entry is the only copy.
//! For each topic the state machine keeps a small index with one row per
//! appended batch: the batch's first offset, how many records it has, and
//! the log index of the entry that holds it. A fetch finds the row by
//! binary search and reads that entry from the log.
//!
//! The index lives in memory and is rebuilt by replaying the log on
//! startup.
//!
//! # No clock, no network, no async
//!
//! Callers pass the current time in. Nothing here opens a socket or needs a
//! runtime, so the same code runs in the server and in the deterministic
//! simulator.

mod error;
mod fetch;
mod standalone;
mod state;

#[cfg(test)]
mod testutil;

pub use error::{Error, Result};
pub use fetch::{FetchLimits, Fetched, FetchedRecord};
pub use standalone::{Produced, Standalone};
pub use state::{Applied, CreateTopic, Rejection, TopicInfo, TopicState, MAX_TOPICS};
