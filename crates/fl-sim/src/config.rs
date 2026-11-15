//! What a simulation run looks like.

/// A deliberate mistake in the code that drives the nodes.
///
/// These exist to prove that the checker can catch real bugs. Each one
/// breaks the rule that a node's state must be on disk before it tells
/// anyone about it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Bug {
    /// Never sync the log. A crash loses every entry the node held.
    SkipLogSync,
    /// Never write the term and vote to disk. A crashed node forgets that
    /// it voted and can vote again in the same term.
    SkipHardStateWrite,
}

/// Settings for one run. Rates are probabilities between 0 and 1.
#[derive(Clone, PartialEq, Debug)]
pub struct SimConfig {
    /// Decides everything random in the run.
    pub seed: u64,
    /// Number of nodes in the cluster.
    pub nodes: u64,
    /// Length of the phase with faults, in ticks.
    pub ticks: u64,
    /// Length of the quiet phase afterwards, in which the cluster has to
    /// recover.
    pub settle_ticks: u64,
    /// Chance that a message is lost.
    pub drop_rate: f64,
    /// Chance that a message is delivered twice.
    pub duplicate_rate: f64,
    /// Shortest delivery time in ticks. At least 1.
    pub min_delay: u64,
    /// Longest delivery time in ticks. Different delays for different
    /// messages are what reorders them.
    pub max_delay: u64,
    /// Chance, per node per tick, that the node crashes.
    pub crash_rate: f64,
    /// Longest time a crashed node stays down, in ticks.
    pub max_downtime: u64,
    /// Chance, per tick, that a partition starts while there is none.
    pub partition_rate: f64,
    /// Longest time a partition lasts, in ticks.
    pub max_partition_ticks: u64,
    /// Chance, per tick, that a client sends a proposal to a random node.
    pub propose_rate: f64,
    /// Chance, per node per tick, that its clock skips the tick. Half as
    /// often, a clock ticks twice instead.
    pub clock_skew: f64,
    /// A mistake to inject, or `None` for a correct driver.
    pub bug: Option<Bug>,
    /// Keep the last few hundred events and attach them to a violation.
    pub trace: bool,
}

impl SimConfig {
    /// Three nodes under a moderate mix of every fault.
    pub fn new(seed: u64) -> Self {
        SimConfig {
            seed,
            nodes: 3,
            ticks: 3_000,
            settle_ticks: 400,
            drop_rate: 0.05,
            duplicate_rate: 0.02,
            min_delay: 1,
            max_delay: 4,
            crash_rate: 0.002,
            max_downtime: 80,
            partition_rate: 0.004,
            max_partition_ticks: 150,
            propose_rate: 0.3,
            clock_skew: 0.05,
            bug: None,
            trace: false,
        }
    }

    /// A healthy network and no crashes. Useful as a baseline.
    pub fn calm(seed: u64) -> Self {
        SimConfig {
            drop_rate: 0.0,
            duplicate_rate: 0.0,
            max_delay: 1,
            crash_rate: 0.0,
            partition_rate: 0.0,
            clock_skew: 0.0,
            ..SimConfig::new(seed)
        }
    }

    /// Faults several times more frequent than [`SimConfig::new`]. The
    /// cluster spends much of the run without a leader.
    pub fn harsh(seed: u64) -> Self {
        SimConfig {
            drop_rate: 0.2,
            duplicate_rate: 0.1,
            max_delay: 12,
            crash_rate: 0.01,
            max_downtime: 60,
            partition_rate: 0.02,
            max_partition_ticks: 120,
            clock_skew: 0.2,
            ..SimConfig::new(seed)
        }
    }

    /// The same run with a different cluster size.
    #[must_use]
    pub fn with_nodes(mut self, nodes: u64) -> Self {
        self.nodes = nodes;
        self
    }

    /// The same run with a deliberate mistake in the driver.
    #[must_use]
    pub fn with_bug(mut self, bug: Bug) -> Self {
        self.bug = Some(bug);
        self
    }

    /// Checks the settings.
    ///
    /// # Panics
    ///
    /// If a setting makes no sense, such as zero nodes or a rate above 1.
    pub fn validate(&self) {
        assert!(self.nodes >= 1, "a cluster needs at least one node");
        assert!(self.min_delay >= 1, "messages take at least one tick");
        assert!(
            self.max_delay >= self.min_delay,
            "max_delay is below min_delay"
        );
        assert!(self.max_downtime >= 1, "max_downtime must be at least 1");
        assert!(
            self.max_partition_ticks >= 1,
            "max_partition_ticks must be at least 1"
        );
        let rates = [
            ("drop_rate", self.drop_rate),
            ("duplicate_rate", self.duplicate_rate),
            ("crash_rate", self.crash_rate),
            ("partition_rate", self.partition_rate),
            ("propose_rate", self.propose_rate),
            ("clock_skew", self.clock_skew),
        ];
        for (name, rate) in rates {
            assert!(
                (0.0..=1.0).contains(&rate),
                "{name} must be between 0 and 1"
            );
        }
    }
}
