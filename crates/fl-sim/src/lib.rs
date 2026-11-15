//! A deterministic simulator for ferrumlog's Raft.
//!
//! Several [`fl_raft::Node`]s run inside one thread. The simulator plays
//! the part of everything outside them: the network, the disks, the
//! clocks, and the clients. All of it is driven by one seeded random
//! number generator, so a run is a pure function of its [`SimConfig`].
//! A failure found with seed 1234 fails the same way every time seed 1234
//! is run again.
//!
//! # What it does to the cluster
//!
//! * Drops, duplicates, delays, and reorders messages.
//! * Splits the cluster into two groups that cannot talk to each other.
//! * Crashes nodes, including half way through handling a step, and
//!   restarts them from what was durable at that moment.
//! * Lets clocks run slow or fast.
//! * Sends proposals to random nodes the whole time.
//!
//! # What it checks
//!
//! After every step, the [`Checker`] verifies the safety properties from
//! the Raft paper:
//!
//! * **Election safety.** At most one leader per term.
//! * **State machine safety.** Every node applies the same entry at each
//!   index. Once any node has applied an entry, it never changes.
//! * **Leader completeness.** Every leader holds every entry committed in
//!   an earlier term.
//! * **Log matching.** Two logs that agree on the term at an index agree
//!   on everything up to that index.
//!
//! and three properties a client relies on:
//!
//! * A proposal the leader reported as committed is in the committed log
//!   at exactly the position the leader gave.
//! * No proposal is committed twice, and nothing is committed that was
//!   never proposed.
//! * Once the faults stop, the cluster elects one leader, commits a new
//!   entry, and every node ends up with the same log.
//!
//! # Use
//!
//! ```
//! use fl_sim::{run, SimConfig};
//!
//! let report = run(SimConfig::new(7)).expect("seed 7 found a bug");
//! assert!(report.committed > 0);
//! ```

mod checker;
mod config;
mod log;
mod rng;
mod sim;

pub use checker::{Checker, Violation, ViolationKind};
pub use config::{Bug, SimConfig};
pub use log::SimLog;
pub use sim::{run, Report};
