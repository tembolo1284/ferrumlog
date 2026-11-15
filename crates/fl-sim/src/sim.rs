//! The simulation loop.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};
use std::panic::{self, AssertUnwindSafe};

use fl_raft::{Config, HardState, MemLog, Message, Node, ProposeError};
use fl_types::{LogId, LogIndex, Membership, NodeId};

use crate::checker::{client_command, Checker, Violation, ViolationKind};
use crate::config::{Bug, SimConfig};
use crate::log::SimLog;
use crate::rng::Rng;

/// How many events a trace keeps.
const TRACE_LEN: usize = 400;
/// How often every pair of logs is compared, in ticks.
const LOG_CHECK_INTERVAL: u64 = 64;

/// What happened in a run that found no violation.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Report {
    /// The seed of the run.
    pub seed: u64,
    /// Total simulated time, both phases together.
    pub ticks: u64,
    /// Messages handed to the network.
    pub messages_sent: u64,
    /// Messages that reached a node.
    pub messages_delivered: u64,
    /// Messages lost to drops, partitions, or a crashed receiver.
    pub messages_lost: u64,
    /// Extra copies the network made.
    pub messages_duplicated: u64,
    /// Node crashes.
    pub crashes: u64,
    /// Partitions.
    pub partitions: u64,
    /// Terms in which some node became leader.
    pub leaders_elected: u64,
    /// The highest term any node reached.
    pub max_term: u64,
    /// Proposals clients sent, to any node.
    pub proposals: u64,
    /// Proposals a leader accepted into its log.
    pub accepted: u64,
    /// Proposals a leader then reported as committed.
    pub acknowledged: u64,
    /// Entries in the committed log at the end, no-ops included.
    pub committed: u64,
    /// A hash of the committed log and the counters above. Two runs with
    /// the same configuration always produce the same fingerprint.
    pub fingerprint: u64,
}

/// Runs one simulation.
///
/// Returns a report if every check held, and the first violation
/// otherwise. A panic inside a node, which is how the Raft code reports a
/// broken internal invariant, is caught and returned as a violation too.
pub fn run(config: SimConfig) -> Result<Report, Violation> {
    config.validate();
    let mut sim = Sim::new(config);
    let outcome = panic::catch_unwind(AssertUnwindSafe(|| sim.run()));
    match outcome {
        Ok(Ok(())) => Ok(sim.report()),
        Ok(Err(violation)) => Err(sim.stamp(violation)),
        Err(payload) => {
            let message = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_string()))
                .unwrap_or_else(|| "a node panicked".to_string());
            Err(sim.stamp(Violation::new(ViolationKind::NodePanicked, message)))
        }
    }
}

/// A point in the handling of a step at which a node can crash.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Doom {
    /// After the node changed its state, before anything was made durable.
    /// The change is lost and nobody hears of it.
    BeforePersist,
    /// After the state was made durable, before any message left. The
    /// change survives and nobody hears of it.
    BeforeSend,
}

/// One machine: a running node, or the disk of a crashed one.
struct Slot {
    /// `None` while the node is down.
    node: Option<Node<SimLog>>,
    /// The durable term and vote.
    hard: HardState,
    /// The durable log of a node that is down.
    disk: MemLog,
    /// How far the state machine has applied. Reset by a crash, because
    /// the state machine lives in memory.
    applied: LogIndex,
    restart_at: u64,
    /// Counts restarts, so each life of a node gets a different seed.
    incarnation: u64,
    doom: Option<Doom>,
    /// Proposals this node accepted as leader and has not yet resolved.
    pending: Vec<Pending>,
}

struct Pending {
    id: LogId,
    number: u64,
}

struct Envelope {
    deliver_at: u64,
    /// Breaks ties, so delivery order never depends on anything but the
    /// seed.
    sequence: u64,
    message: Message,
}

impl PartialEq for Envelope {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}
impl Eq for Envelope {}
impl PartialOrd for Envelope {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Envelope {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key().cmp(&other.key())
    }
}
impl Envelope {
    fn key(&self) -> Reverse<(u64, u64)> {
        // Reversed, because BinaryHeap pops its largest element and the
        // earliest message has to come out first.
        Reverse((self.deliver_at, self.sequence))
    }
}

struct Partition {
    /// One side. Every other node is on the other side.
    side: BTreeSet<NodeId>,
    heals_at: u64,
}

#[derive(Default)]
struct Stats {
    sent: u64,
    delivered: u64,
    lost: u64,
    duplicated: u64,
    crashes: u64,
    partitions: u64,
    max_term: u64,
    proposals: u64,
    accepted: u64,
    acknowledged: u64,
}

struct Sim {
    config: SimConfig,
    rng: Rng,
    now: u64,
    voters: Membership,
    slots: BTreeMap<NodeId, Slot>,
    network: BinaryHeap<Envelope>,
    sequence: u64,
    partition: Option<Partition>,
    checker: Checker,
    stats: Stats,
    next_number: u64,
    /// True in the second phase, when nothing goes wrong any more.
    quiet: bool,
    /// True at the very end, while the cluster drains what is in flight.
    clients_stopped: bool,
    trace: VecDeque<String>,
}

impl Sim {
    fn new(config: SimConfig) -> Self {
        let voters = Membership::new((1..=config.nodes).map(NodeId::new))
            .expect("validate() requires at least one node");
        let mut sim = Sim {
            rng: Rng::new(config.seed),
            now: 0,
            slots: BTreeMap::new(),
            network: BinaryHeap::new(),
            sequence: 0,
            partition: None,
            checker: Checker::new(),
            stats: Stats::default(),
            next_number: 0,
            quiet: false,
            clients_stopped: false,
            trace: VecDeque::new(),
            voters,
            config,
        };
        let ids: Vec<NodeId> = sim.voters.voters().collect();
        for id in ids {
            sim.slots.insert(
                id,
                Slot {
                    node: None,
                    hard: HardState::default(),
                    disk: MemLog::new(),
                    applied: LogIndex::ZERO,
                    restart_at: 0,
                    incarnation: 0,
                    doom: None,
                    pending: Vec::new(),
                },
            );
            sim.start(id);
        }
        sim
    }

    fn run(&mut self) -> Result<(), Violation> {
        for _ in 0..self.config.ticks {
            self.advance()?;
        }
        self.settle()
    }

    /// One tick of simulated time.
    fn advance(&mut self) -> Result<(), Violation> {
        self.now += 1;
        if !self.quiet {
            self.inject_faults();
        }
        self.restart_due_nodes();
        self.deliver_due_messages()?;
        if self.rng.chance(self.config.propose_rate) && !self.clients_stopped {
            self.client_proposes()?;
        }
        self.tick_nodes()?;
        if self.now.is_multiple_of(LOG_CHECK_INTERVAL) {
            self.check_all_logs()?;
        }
        Ok(())
    }

    /// The second phase. Every fault is removed, and the cluster has to
    /// get back to work: one leader, a new entry committed, and the same
    /// log on every node.
    fn settle(&mut self) -> Result<(), Violation> {
        self.quiet = true;
        self.partition = None;
        let ids: Vec<NodeId> = self.slots.keys().copied().collect();
        for id in &ids {
            self.slots.get_mut(id).expect("known node").doom = None;
            if self.slots[id].node.is_none() {
                self.start(*id);
            }
        }
        self.note(|| "faults off".to_string());

        for _ in 0..self.config.settle_ticks {
            self.advance()?;
        }

        let leaders: Vec<NodeId> = self
            .slots
            .iter()
            .filter(|(_, slot)| slot.node.as_ref().is_some_and(Node::is_leader))
            .map(|(id, _)| *id)
            .collect();
        let [leader] = leaders[..] else {
            return Err(Violation::new(
                ViolationKind::Stalled,
                format!("expected one leader after the faults stopped, found {leaders:?}"),
            ));
        };

        // One last proposal, straight to the leader, has to go through.
        // Clients then stop, so that nothing is still in flight when the
        // logs are compared.
        let number = self.next_number;
        self.propose_to(leader)?;
        self.clients_stopped = true;
        for _ in 0..self.config.settle_ticks.max(50) {
            self.advance()?;
        }

        let committed = self.checker.committed().len() as u64;
        let last = self.checker.committed().last();
        let landed = self
            .checker
            .committed()
            .iter()
            .any(|entry| crate::checker::command_number(&entry.command) == Some(number));
        if !landed {
            return Err(Violation::new(
                ViolationKind::Stalled,
                format!("the final proposal {number} was not committed"),
            ));
        }
        for (id, slot) in &self.slots {
            let node = slot.node.as_ref().expect("every node is up while settling");
            let log = node.log().working();
            if slot.applied.get() != committed || log.as_slice() != self.checker.committed() {
                return Err(Violation::new(
                    ViolationKind::Stalled,
                    format!(
                        "{id} applied {} of {committed} entries and its log ends at {}, \
                         the committed log ends at {:?}",
                        slot.applied.get(),
                        log.len(),
                        last.map(|entry| entry.id),
                    ),
                ));
            }
        }
        self.check_all_logs()
    }

    // ---- faults ----------------------------------------------------------

    fn inject_faults(&mut self) {
        // Partitions.
        if let Some(partition) = &self.partition {
            if self.now >= partition.heals_at {
                self.partition = None;
                self.note(|| "partition healed".to_string());
            }
        } else if self.rng.chance(self.config.partition_rate) {
            let mut side = BTreeSet::new();
            for id in self.voters.voters() {
                if self.rng.chance(0.5) {
                    side.insert(id);
                }
            }
            let heals_at = self.now + self.rng.range(1, self.config.max_partition_ticks);
            self.note(|| format!("partition {side:?} against the rest until {heals_at}"));
            self.partition = Some(Partition { side, heals_at });
            self.stats.partitions += 1;
        }

        // Crashes. A third happen right now, between steps. The others are
        // armed and go off in the middle of the node's next step.
        let ids: Vec<NodeId> = self.slots.keys().copied().collect();
        for id in ids {
            if !self.rng.chance(self.config.crash_rate) {
                continue;
            }
            let how = self.rng.range(0, 2);
            if self.slots[&id].node.is_none() {
                continue;
            }
            match how {
                0 => self.crash(id, "between steps"),
                1 => self.slots.get_mut(&id).expect("known node").doom = Some(Doom::BeforePersist),
                _ => self.slots.get_mut(&id).expect("known node").doom = Some(Doom::BeforeSend),
            }
        }
    }

    fn crash(&mut self, id: NodeId, when: &str) {
        let max_downtime = self.config.max_downtime;
        let downtime = self.rng.range(1, max_downtime);
        let slot = self.slots.get_mut(&id).expect("known node");
        let Some(node) = slot.node.take() else {
            return;
        };
        // Only what was synced survives.
        slot.disk = node.into_log().into_durable();
        slot.applied = LogIndex::ZERO;
        slot.restart_at = self.now + downtime;
        slot.doom = None;
        slot.pending.clear();
        self.stats.crashes += 1;
        let back_at = self.now + downtime;
        self.note(|| format!("{id} crashed {when}, back at {back_at}"));
    }

    fn restart_due_nodes(&mut self) {
        let due: Vec<NodeId> = self
            .slots
            .iter()
            .filter(|(_, slot)| slot.node.is_none() && slot.restart_at <= self.now)
            .map(|(id, _)| *id)
            .collect();
        for id in due {
            self.start(id);
        }
    }

    /// Starts a node from what is on its disk.
    fn start(&mut self, id: NodeId) {
        let slot = self.slots.get_mut(&id).expect("known node");
        slot.incarnation += 1;
        let mut config = Config::new(id, self.voters.clone());
        config.seed = self
            .config
            .seed
            .wrapping_mul(0x0100_0000_01B3)
            .wrapping_add(slot.incarnation);
        // Small messages, so catching a node up takes several round trips
        // and there is more room for something to go wrong in between.
        config.max_entries_per_message = 8;
        let log = SimLog::from_durable(std::mem::take(&mut slot.disk));
        let node = Node::new(config, slot.hard, log).expect("the simulator's config is valid");
        slot.node = Some(node);
        slot.applied = LogIndex::ZERO;
        let (hard, incarnation) = (slot.hard, slot.incarnation);
        self.note(|| format!("{id} started, life {incarnation}, {hard:?}"));
    }

    // ---- the network -----------------------------------------------------

    fn post(&mut self, message: Message) {
        self.stats.sent += 1;
        if !self.quiet && self.rng.chance(self.config.drop_rate) {
            self.stats.lost += 1;
            return;
        }
        let copies = if !self.quiet && self.rng.chance(self.config.duplicate_rate) {
            self.stats.duplicated += 1;
            2
        } else {
            1
        };
        for _ in 0..copies {
            let delay = if self.quiet {
                1
            } else {
                self.rng.range(self.config.min_delay, self.config.max_delay)
            };
            self.sequence += 1;
            self.network.push(Envelope {
                deliver_at: self.now + delay,
                sequence: self.sequence,
                message: message.clone(),
            });
        }
    }

    fn connected(&self, a: NodeId, b: NodeId) -> bool {
        match &self.partition {
            Some(partition) => partition.side.contains(&a) == partition.side.contains(&b),
            None => true,
        }
    }

    fn deliver_due_messages(&mut self) -> Result<(), Violation> {
        while self
            .network
            .peek()
            .is_some_and(|envelope| envelope.deliver_at <= self.now)
        {
            let message = self.network.pop().expect("peeked").message;
            let (from, to) = (message.from, message.to);
            let up = self.slots[&to].node.is_some();
            if !up || !self.connected(from, to) {
                self.stats.lost += 1;
                continue;
            }
            self.stats.delivered += 1;
            self.note(|| format!("{from} -> {to} {} in {}", message.body.kind(), message.term));
            let node = self.slots.get_mut(&to).and_then(|slot| slot.node.as_mut());
            node.expect("checked above")
                .step(message)
                .unwrap_or_else(|never| match never {});
            self.handle_ready(to)?;
        }
        Ok(())
    }

    // ---- clients and clocks ----------------------------------------------

    fn client_proposes(&mut self) -> Result<(), Violation> {
        let target = NodeId::new(self.rng.range(1, self.config.nodes));
        self.propose_to(target)
    }

    fn propose_to(&mut self, target: NodeId) -> Result<(), Violation> {
        let number = self.next_number;
        self.next_number += 1;
        self.stats.proposals += 1;
        self.checker.note_proposed(number);

        let slot = self.slots.get_mut(&target).expect("known node");
        let Some(node) = slot.node.as_mut() else {
            return Ok(());
        };
        match node.propose(client_command(number)) {
            Ok(id) => {
                slot.pending.push(Pending { id, number });
                self.stats.accepted += 1;
                self.note(|| format!("{target} accepted proposal {number} at {id}"));
                self.handle_ready(target)
            }
            Err(ProposeError::NotLeader { .. }) => Ok(()),
            Err(ProposeError::Storage(never)) => match never {},
        }
    }

    fn tick_nodes(&mut self) -> Result<(), Violation> {
        let ids: Vec<NodeId> = self.slots.keys().copied().collect();
        for id in ids {
            // A slow clock skips the tick. A fast one ticks twice.
            let skew = if self.quiet {
                0.0
            } else {
                self.config.clock_skew
            };
            let slow = self.rng.chance(skew);
            let fast = self.rng.chance(skew / 2.0);
            let ticks = match (slow, fast) {
                (true, _) => 0,
                (false, true) => 2,
                (false, false) => 1,
            };
            for _ in 0..ticks {
                let Some(node) = self.slots.get_mut(&id).and_then(|slot| slot.node.as_mut()) else {
                    break;
                };
                node.tick().unwrap_or_else(|never| match never {});
                self.handle_ready(id)?;
            }
        }
        Ok(())
    }

    // ---- driving one node ------------------------------------------------

    /// Does what a real driver does with a `Ready`, in the required order:
    /// persist, then send, then apply. Also where crashes in the middle of
    /// a step happen, and where the checker gets to look.
    fn handle_ready(&mut self, id: NodeId) -> Result<(), Violation> {
        let bug = self.config.bug;
        let slot = self.slots.get_mut(&id).expect("known node");
        let Some(node) = slot.node.as_mut() else {
            return Ok(());
        };
        let ready = node.ready().unwrap_or_else(|never| match never {});

        if !ready.is_idle() && slot.doom == Some(Doom::BeforePersist) {
            self.crash(id, "before persisting");
            return Ok(());
        }

        // 1. The term and vote.
        if let Some(hard) = ready.hard_state {
            if bug != Some(Bug::SkipHardStateWrite) {
                if hard.term < slot.hard.term {
                    return Err(Violation::new(
                        ViolationKind::TermWentBackwards,
                        format!("{id} went from {} back to {}", slot.hard.term, hard.term),
                    ));
                }
                slot.hard = hard;
            }
        }
        // 2. The log.
        if ready.must_sync && bug != Some(Bug::SkipLogSync) {
            node.log_mut().sync();
        }

        if !ready.is_idle() && slot.doom == Some(Doom::BeforeSend) {
            self.crash(id, "before sending");
            return Ok(());
        }

        // The checker looks at the node now that its state is durable.
        let term = node.term();
        self.stats.max_term = self.stats.max_term.max(term.get());
        if node.is_leader() {
            let newly_seen = self.checker.terms_with_a_leader();
            self.checker.observe_leader(term, id)?;
            if self.checker.terms_with_a_leader() > newly_seen {
                self.checker
                    .check_leader_log(id, term, node.log().working())?;
            }
        }

        // 3. Apply what is committed.
        while slot.applied < ready.commit_index {
            let next = slot.applied.next();
            let Some(entry) = node.log().working().get(next) else {
                return Err(Violation::new(
                    ViolationKind::CommittedEntryChanged,
                    format!(
                        "{id} has commit index {} but no entry at {next}",
                        ready.commit_index
                    ),
                ));
            };
            self.checker.observe_apply(id, term, entry)?;
            slot.applied = next;
        }

        // A proposal is acknowledged to its client once the leader that
        // accepted it, still leading the same term, sees it committed.
        let is_leader = node.is_leader();
        let mut still_pending = Vec::new();
        for pending in slot.pending.drain(..) {
            if !is_leader || pending.id.term != term {
                // Leadership was lost. The outcome is unknown to this
                // node, and a real client would have to ask the new
                // leader.
                continue;
            }
            if pending.id.index <= ready.commit_index {
                self.checker.check_ack(pending.id, pending.number)?;
                self.stats.acknowledged += 1;
            } else {
                still_pending.push(pending);
            }
        }
        slot.pending = still_pending;

        // 4. Send.
        for message in ready.messages {
            self.post(message);
        }
        Ok(())
    }

    // ---- whole-cluster checks --------------------------------------------

    fn check_all_logs(&self) -> Result<(), Violation> {
        let mut logs: Vec<(NodeId, &MemLog)> = Vec::new();
        for (id, slot) in &self.slots {
            match &slot.node {
                Some(node) => {
                    logs.push((*id, node.log().working()));
                    if node.is_leader() {
                        self.checker
                            .check_leader_log(*id, node.term(), node.log().working())?;
                    }
                }
                None => logs.push((*id, &slot.disk)),
            }
        }
        self.checker.check_logs(&logs)
    }

    // ---- reporting -------------------------------------------------------

    fn note(&mut self, event: impl FnOnce() -> String) {
        if !self.config.trace {
            return;
        }
        if self.trace.len() == TRACE_LEN {
            self.trace.pop_front();
        }
        let line = format!("{:>6} {}", self.now, event());
        self.trace.push_back(line);
    }

    fn stamp(&self, mut violation: Violation) -> Violation {
        violation.seed = self.config.seed;
        violation.tick = self.now;
        violation.trace = self.trace.iter().cloned().collect();
        violation
    }

    fn report(&self) -> Report {
        let mut report = Report {
            seed: self.config.seed,
            ticks: self.now,
            messages_sent: self.stats.sent,
            messages_delivered: self.stats.delivered,
            messages_lost: self.stats.lost,
            messages_duplicated: self.stats.duplicated,
            crashes: self.stats.crashes,
            partitions: self.stats.partitions,
            leaders_elected: self.checker.terms_with_a_leader() as u64,
            max_term: self.stats.max_term,
            proposals: self.stats.proposals,
            accepted: self.stats.accepted,
            acknowledged: self.stats.acknowledged,
            committed: self.checker.committed().len() as u64,
            fingerprint: 0,
        };
        report.fingerprint = self.fingerprint(&report);
        report
    }

    /// FNV-1a over the committed log and the counters.
    fn fingerprint(&self, report: &Report) -> u64 {
        let mut hash: u64 = 0xCBF2_9CE4_8422_2325;
        let mut mix = |value: u64| {
            for byte in value.to_le_bytes() {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(0x0100_0000_01B3);
            }
        };
        for entry in self.checker.committed() {
            mix(entry.term().get());
            mix(entry.index().get());
            mix(crate::checker::command_number(&entry.command).map_or(u64::MAX, |n| n));
        }
        for counter in [
            report.ticks,
            report.messages_sent,
            report.messages_delivered,
            report.messages_lost,
            report.messages_duplicated,
            report.crashes,
            report.partitions,
            report.leaders_elected,
            report.max_term,
            report.proposals,
            report.accepted,
            report.acknowledged,
        ] {
            mix(counter);
        }
        hash
    }
}
