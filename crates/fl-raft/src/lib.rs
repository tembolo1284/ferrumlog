//! Raft consensus as a pure state machine.
//!
//! This crate does no I/O. It never touches the network, a disk, a clock,
//! or a thread. A [`Node`] is driven entirely from outside:
//!
//! * [`Node::tick`] says that one unit of time has passed.
//! * [`Node::step`] hands it a message from another node.
//! * [`Node::propose`] asks the leader to replicate a command.
//!
//! After any of these, the driver calls [`Node::ready`] and gets back a
//! [`Ready`]: what to make durable, which messages to send, and how far the
//! log is committed. Because nothing here depends on real time or real
//! networks, the same inputs always give the same outputs. That is what
//! lets `fl-sim` replay a failing run from its seed.
//!
//! # The contract with the driver
//!
//! For every [`Ready`], in this order:
//!
//! 1. If `hard_state` is set, write it durably.
//! 2. If `must_sync` is set, sync the log.
//! 3. Only then send `messages`.
//! 4. Only then apply entries up to `commit_index` to the state machine and
//!    answer clients.
//!
//! Skipping the order breaks Raft. A node that sends a vote before its
//! hard state is on disk can vote twice after a crash. A follower that
//! acknowledges entries before they are on disk can lose data the leader
//! already counted as committed.
//!
//! # What is implemented
//!
//! Leader election, log replication, the commit rule, and repair of
//! diverged logs, as in the Raft paper. Two additions:
//!
//! * A new leader appends a no-op in its own term, so entries from earlier
//!   terms commit without waiting for a client write.
//! * A leader that cannot reach a majority for a full election timeout
//!   steps down, so clients stop waiting on a leader that can never commit.
//!
//! # What is not implemented yet
//!
//! * Snapshots and log compaction. The log only grows.
//! * Membership changes. The set of voters is fixed in [`Config`], and a
//!   `Command::Reconfigure` entry is replicated like any other command but
//!   has no effect on the cluster.
//! * Pre-vote. A node that was cut off can force one extra election when it
//!   rejoins.
//! * Linearizable reads. Reading from a leader that has just been replaced
//!   can return slightly stale data.

mod config;
mod log;
mod message;
mod node;
mod rng;

#[cfg(test)]
mod tests;

pub use config::{Config, ConfigError};
pub use log::{entry_size, LogStore, MemLog};
pub use message::{Body, Message};
pub use node::{HardState, Node, ProposeError, Ready, Role, Status};
