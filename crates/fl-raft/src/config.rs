//! Static configuration of one Raft node.

use std::fmt;

use fl_types::{Membership, NodeId};

/// Settings for one node. Times are counted in ticks. The driver decides
/// how long a tick is, typically 50 to 100 milliseconds.
#[derive(Clone, Debug)]
pub struct Config {
    /// This node's id. Must be one of `voters`.
    pub id: NodeId,
    /// Every voting node in the cluster, this one included. Fixed for the
    /// life of the cluster.
    pub voters: Membership,
    /// Shortest time a follower waits without hearing from a leader before
    /// it starts an election.
    pub election_timeout_min: u32,
    /// Longest such wait. Each node picks a fresh random value between the
    /// two bounds every time it resets its timer, which keeps nodes from
    /// starting elections at the same moment over and over.
    pub election_timeout_max: u32,
    /// How often a leader sends heartbeats. Must be well below
    /// `election_timeout_min`.
    pub heartbeat_interval: u32,
    /// Most entries in one AppendEntries message.
    pub max_entries_per_message: usize,
    /// Most payload bytes in one AppendEntries message. A message always
    /// carries at least one entry when there is one to send, even if that
    /// entry alone is larger.
    pub max_bytes_per_message: usize,
    /// Seed for the election timeout randomness. Give each node a
    /// different seed in production. The node id is mixed in as well.
    pub seed: u64,
}

impl Config {
    /// A configuration with sensible defaults: elections after 10 to 20
    /// ticks of silence and a heartbeat every 2 ticks.
    pub fn new(id: NodeId, voters: Membership) -> Self {
        Config {
            id,
            voters,
            election_timeout_min: 10,
            election_timeout_max: 20,
            heartbeat_interval: 2,
            max_entries_per_message: 256,
            max_bytes_per_message: 4 * 1024 * 1024,
            seed: 0,
        }
    }

    /// Checks the settings against each other.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.voters.contains(self.id) {
            return Err(ConfigError::NotAVoter(self.id));
        }
        if self.heartbeat_interval == 0 {
            return Err(ConfigError::ZeroHeartbeat);
        }
        if self.election_timeout_min <= self.heartbeat_interval {
            return Err(ConfigError::ElectionTimeoutTooShort {
                election_timeout_min: self.election_timeout_min,
                heartbeat_interval: self.heartbeat_interval,
            });
        }
        if self.election_timeout_max <= self.election_timeout_min {
            return Err(ConfigError::EmptyElectionRange {
                min: self.election_timeout_min,
                max: self.election_timeout_max,
            });
        }
        if self.max_entries_per_message == 0 {
            return Err(ConfigError::ZeroMessageLimit);
        }
        Ok(())
    }
}

/// A [`Config`] that cannot work.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ConfigError {
    /// The node's own id is not in the voter set.
    NotAVoter(NodeId),
    /// The heartbeat interval is zero.
    ZeroHeartbeat,
    /// Followers would time out before the first heartbeat could arrive.
    ElectionTimeoutTooShort {
        /// The configured minimum election timeout.
        election_timeout_min: u32,
        /// The configured heartbeat interval.
        heartbeat_interval: u32,
    },
    /// The maximum election timeout is not above the minimum, which leaves
    /// no room for randomness.
    EmptyElectionRange {
        /// The configured minimum.
        min: u32,
        /// The configured maximum.
        max: u32,
    },
    /// The per-message entry limit is zero, so nothing could replicate.
    ZeroMessageLimit,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::NotAVoter(id) => write!(f, "node {id} is not in the voter set"),
            ConfigError::ZeroHeartbeat => f.write_str("heartbeat interval must be at least 1"),
            ConfigError::ElectionTimeoutTooShort {
                election_timeout_min,
                heartbeat_interval,
            } => write!(
                f,
                "election timeout minimum ({election_timeout_min}) must be above \
                 the heartbeat interval ({heartbeat_interval})"
            ),
            ConfigError::EmptyElectionRange { min, max } => write!(
                f,
                "election timeout maximum ({max}) must be above the minimum ({min})"
            ),
            ConfigError::ZeroMessageLimit => {
                f.write_str("max entries per message must be at least 1")
            }
        }
    }
}

impl std::error::Error for ConfigError {}
