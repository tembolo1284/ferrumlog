//! The bridge between async request handlers and the replicated log.
//!
//! # One thread drives Raft
//!
//! A single thread owns the Raft node. Everything that can change the node
//! reaches it through one bounded queue: clock ticks, messages from peers,
//! and writes from clients. The thread takes whatever is queued, up to a
//! limit, and handles it as one batch:
//!
//! 1. Step every input into the node.
//! 2. Write the term and vote to disk if they changed, and sync the log
//!    once if anything was appended.
//! 3. Send the outgoing messages.
//! 4. Apply newly committed entries to the topic state and answer the
//!    clients whose writes those were.
//!
//! The order is the contract `fl-raft` asks for. Nothing leaves the node
//! before the state behind it is durable, and no client hears "written"
//! before a majority holds the write.
//!
//! # Group commit
//!
//! Because a batch is synced once, many writes share one fsync. On the
//! leader they also share one message to each follower. Under light load a
//! write gets its own fsync. Under heavy load throughput grows with
//! concurrency instead of being capped at one fsync per request.
//!
//! # A cluster of one
//!
//! A node started without peers is a cluster with one member. It elects
//! itself at startup and commits every write on its own, so it behaves like
//! a plain durable log. There is no separate code path for it.
//!
//! # Reads
//!
//! Any node serves reads, from what it has applied. Only committed entries
//! are ever applied, so a read never returns a record that could be rolled
//! back. A follower can lag the leader by a moment. Reads run on tokio's
//! blocking pool, because they may hit the disk.
//!
//! # Failure
//!
//! A storage error is fatal. The thread fails every waiting write,
//! triggers shutdown, and exits. Restarting the process runs log recovery,
//! which is the only way back to a state that is known to be good.

use std::collections::VecDeque;
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use fl_broker::{Applied, CreateTopic, FetchLimits, Fetched, Produced, Rejection, TopicInfo};
use fl_broker::{Error, TopicState};
use fl_net::{PeerMap, ReplySink, RequestSink, SharedLog, Transport, TransportOptions};
use fl_proto::redirect::{self, LeaderHint};
use fl_raft::{Node, ProposeError, Ready, Role};
use fl_storage::{HardStateStore, Log, LogConfig, SyncPolicy};
use fl_types::{Command, LogId, LogIndex, NodeId, Offset, Record, Term, Timestamp, TopicId};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::MissedTickBehavior;
use tonic::Status;
use tracing::{debug, error, info};

use crate::config::Config;
use crate::metrics::Metrics;
use crate::shutdown::Shutdown;
use crate::BoxError;

/// Inputs that may wait in the queue. Client writes wait for room. Ticks
/// and Raft messages are dropped when it is full, which Raft tolerates.
const INPUT_QUEUE: usize = 4096;

/// Writes that may be waiting for a majority before new ones are refused.
/// This is what stops memory from growing when the followers are gone.
const MAX_PENDING: usize = 16_384;

/// Most entries and bytes in one message to a follower. One entry can be
/// 2 MiB on its own, and a message always carries at least one.
const MAX_ENTRIES_PER_MESSAGE: usize = 1024;
const MAX_BYTES_PER_MESSAGE: usize = 1 << 20;

/// A write to be committed.
pub(crate) enum WriteOp {
    CreateTopic { name: String },
    Produce { topic: String, records: Vec<Record> },
}

/// The result of a committed write.
pub(crate) enum WriteOutcome {
    TopicCreated { id: TopicId, created: bool },
    Produced(Produced),
}

type WriteReply = oneshot::Sender<Result<WriteOutcome, Status>>;

enum Input {
    /// One unit of Raft time has passed.
    Tick,
    /// A client wants something written.
    Write { op: WriteOp, reply: WriteReply },
    /// A peer sent a request and is waiting for the answer.
    Request(fl_raft::Message, oneshot::Sender<fl_raft::Message>),
    /// A peer answered one of our requests.
    Reply(fl_raft::Message),
    /// Finish everything queued ahead of this, then exit.
    Stop,
}

/// What the node currently believes about the cluster.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct RaftView {
    role: Role,
    term: Term,
    leader: Option<NodeId>,
    commit: LogIndex,
}

/// A point-in-time view of the node, for the Admin service.
pub(crate) struct NodeSnapshot {
    pub role: Role,
    pub term: Term,
    pub leader: Option<NodeId>,
    pub commit: LogIndex,
    pub applied: LogIndex,
    pub first_index: LogIndex,
    pub last: LogId,
    pub segments: usize,
    pub bytes: u64,
}

/// The Raft thread's handle. Joining it yields the error that stopped the
/// thread, if any.
pub(crate) type WriterHandle = thread::JoinHandle<Result<(), Error>>;

/// A cloneable handle to the node.
#[derive(Clone)]
pub(crate) struct Engine {
    state: Arc<RwLock<TopicState>>,
    log: SharedLog,
    inputs: mpsc::Sender<Input>,
    /// Holds the index of the last applied entry. Subscriptions wait on it
    /// to learn that there is something new to read.
    committed: watch::Receiver<u64>,
    view: watch::Receiver<RaftView>,
    metrics: Arc<Metrics>,
}

impl Engine {
    /// Opens the data directory, recovers the log, and starts the Raft
    /// thread.
    ///
    /// A cluster of one has elected itself and replayed its log by the time
    /// this returns. A node with peers returns at once and catches up when
    /// it hears from a leader.
    pub(crate) async fn open(
        config: &Config,
        peers: Arc<PeerMap>,
        metrics: Arc<Metrics>,
        shutdown: Shutdown,
    ) -> Result<(Engine, WriterHandle), BoxError> {
        let me = NodeId::new(config.node_id);
        let dir = config.data_dir.clone();
        // The Raft thread syncs once per batch, so the log itself must not
        // sync on every append.
        let log_config = LogConfig {
            segment_bytes: config.segment_bytes,
            sync: SyncPolicy::Manual,
        };
        let (log, hard) = tokio::task::spawn_blocking(move || {
            let log = Log::open(&dir, log_config)?;
            let hard = HardStateStore::open(&dir)?;
            Ok::<_, fl_storage::Error>((log, hard))
        })
        .await??;
        let log = SharedLog::new(log);

        let mut raft = fl_raft::Config::new(me, peers.membership());
        raft.max_entries_per_message = MAX_ENTRIES_PER_MESSAGE;
        raft.max_bytes_per_message = MAX_BYTES_PER_MESSAGE;
        raft.seed = election_seed();
        let node = Node::new(raft, fl_net::hard_state_from_disk(hard.get()), log.clone())?;

        let (inputs, queue) = mpsc::channel(INPUT_QUEUE);
        let transport = {
            let inputs = inputs.clone();
            let sink: ReplySink = Arc::new(move |message| {
                // A full queue drops the answer, like a lost packet.
                let _ = inputs.try_send(Input::Reply(message));
            });
            Transport::start(me, &peers, TransportOptions::default(), sink)
        };
        spawn_ticker(inputs.clone(), Duration::from_millis(config.tick_ms.max(1)));

        let state = Arc::new(RwLock::new(TopicState::new()));
        let (committed_tx, committed) = watch::channel(0);
        let (view_tx, view) = watch::channel(RaftView {
            role: Role::Follower,
            term: node.term(),
            leader: None,
            commit: LogIndex::ZERO,
        });

        let driver = Driver {
            me,
            node,
            log: log.clone(),
            hard,
            state: Arc::clone(&state),
            queue,
            transport,
            peers,
            committed: committed_tx,
            view: view_tx,
            metrics: Arc::clone(&metrics),
            max_batch: config.max_commit_batch.max(1),
            fsync: !config.unsafe_no_fsync,
            applied: LogIndex::ZERO,
            applied_term: Term::ZERO,
            pending: VecDeque::new(),
            batch: Batch::default(),
        };
        let (started_tx, started) = oneshot::channel();
        let handle = thread::Builder::new()
            .name("fl-raft".into())
            .spawn(move || {
                // Whatever ends this thread, the node cannot go on without
                // it. This also covers a panic from a broken invariant.
                let _stop_node = TriggerOnExit(shutdown);
                let result = driver.run(started_tx);
                if let Err(e) = &result {
                    error!(error = %e, "storage failure, shutting the node down");
                }
                result
            })?;

        if started.await.is_err() {
            // The thread ended before it finished starting. Its result
            // says why.
            return match tokio::task::spawn_blocking(move || handle.join()).await? {
                Ok(Err(e)) => Err(e.into()),
                _ => Err("the raft thread stopped during startup".into()),
            };
        }

        let engine = Engine {
            state,
            log,
            inputs,
            committed,
            view,
            metrics,
        };
        Ok((engine, handle))
    }

    /// Queues a write and waits until it is committed.
    pub(crate) async fn write(&self, op: WriteOp) -> Result<WriteOutcome, Status> {
        let (reply, response) = oneshot::channel();
        if self.inputs.send(Input::Write { op, reply }).await.is_err() {
            return Err(stopping());
        }
        // The thread dropping the reply without answering means it exited.
        response.await.unwrap_or_else(|_| Err(stopping()))
    }

    /// Tells the Raft thread to finish what is queued and exit.
    pub(crate) async fn stop(&self) {
        let _ = self.inputs.send(Input::Stop).await;
    }

    /// Where the `Raft` gRPC service hands over requests from peers.
    pub(crate) fn raft_sink(&self) -> RequestSink {
        let inputs = self.inputs.clone();
        Arc::new(move |message, answer| inputs.try_send(Input::Request(message, answer)).is_ok())
    }

    /// A receiver that changes every time entries are applied.
    pub(crate) fn committed(&self) -> watch::Receiver<u64> {
        self.committed.clone()
    }

    /// Reads records from a topic.
    pub(crate) async fn fetch(
        &self,
        topic: String,
        offset: Offset,
        limits: FetchLimits,
    ) -> Result<Fetched, Status> {
        let fetched = self
            .read(move |state, log| state.fetch(log, &topic, offset, limits))
            .await?
            .map_err(|e| to_status(&e))?;
        self.metrics.fetches.inc();
        self.metrics
            .records_fetched
            .add(fetched.records.len() as u64);
        Ok(fetched)
    }

    /// Every topic, in id order.
    pub(crate) async fn topics(&self) -> Result<Vec<TopicInfo>, Status> {
        self.read(|state, _| state.topics()).await
    }

    /// The high watermark of a topic, or `NOT_FOUND`.
    pub(crate) async fn high_watermark(&self, topic: String) -> Result<Offset, Status> {
        self.read(move |state, _| {
            state
                .topic_id(&topic)
                .and_then(|id| state.high_watermark(id))
                .ok_or_else(|| to_status(&Error::UnknownTopic(topic)))
        })
        .await?
    }

    /// The node's role and the log's position and size.
    pub(crate) async fn snapshot(&self) -> Result<NodeSnapshot, Status> {
        let view = *self.view.borrow();
        self.read(move |state, log| NodeSnapshot {
            role: view.role,
            term: view.term,
            leader: view.leader,
            commit: view.commit,
            applied: state.applied(),
            first_index: log.first_index(),
            last: log.last_id(),
            segments: log.segment_count(),
            bytes: log.size_bytes(),
        })
        .await
    }

    /// Runs `f` with shared access to the topic state and the log, on the
    /// blocking pool.
    async fn read<T, F>(&self, f: F) -> Result<T, Status>
    where
        T: Send + 'static,
        F: FnOnce(&TopicState, &Log) -> T + Send + 'static,
    {
        let state = Arc::clone(&self.state);
        let log = self.log.clone();
        tokio::task::spawn_blocking(move || {
            // Always the state first and the log second. The Raft thread
            // takes them in the same order when it applies.
            let state = read_lock(&state);
            let log = log.read();
            f(&state, &log)
        })
        .await
        .map_err(|_| Status::internal("a read task failed"))
    }
}

/// Feeds a tick into the queue at a steady rate until the Raft thread is
/// gone.
fn spawn_ticker(inputs: mpsc::Sender<Input>, every: Duration) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(every);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if let Err(TrySendError::Closed(_)) = inputs.try_send(Input::Tick) {
                break;
            }
        }
    });
}

/// A seed for election timeouts that differs between nodes and restarts.
fn election_seed() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos());
    (nanos as u64) ^ (u64::from(std::process::id()) << 32)
}

/// Triggers shutdown when dropped.
struct TriggerOnExit(Shutdown);

impl Drop for TriggerOnExit {
    fn drop(&mut self) {
        self.0.trigger();
    }
}

/// A write that is in the leader's log and waiting for a majority.
struct Pending {
    id: LogId,
    reply: WriteReply,
}

/// What one batch of inputs has asked the driver to do.
#[derive(Default)]
struct Batch {
    hard_state: Option<fl_raft::HardState>,
    must_sync: bool,
    outbound: Vec<fl_raft::Message>,
    /// Answers to peers' requests, to send once the state is durable.
    answers: Vec<(oneshot::Sender<fl_raft::Message>, fl_raft::Message)>,
    commit: LogIndex,
    /// Client writes proposed in this batch.
    writes: usize,
}

impl Batch {
    fn absorb(&mut self, ready: Ready) {
        if ready.hard_state.is_some() {
            self.hard_state = ready.hard_state;
        }
        self.must_sync |= ready.must_sync;
        self.outbound.extend(ready.messages);
        self.commit = self.commit.max(ready.commit_index);
    }
}

struct Driver {
    me: NodeId,
    node: Node<SharedLog>,
    log: SharedLog,
    hard: HardStateStore,
    state: Arc<RwLock<TopicState>>,
    queue: mpsc::Receiver<Input>,
    transport: Transport,
    peers: Arc<PeerMap>,
    committed: watch::Sender<u64>,
    view: watch::Sender<RaftView>,
    metrics: Arc<Metrics>,
    max_batch: usize,
    fsync: bool,
    /// How far the topic state has applied.
    applied: LogIndex,
    /// The term of the last applied entry. A leader takes writes only once
    /// this is its own term. See [`Driver::write`].
    applied_term: Term,
    pending: VecDeque<Pending>,
    batch: Batch,
}

impl Driver {
    fn run(mut self, started: oneshot::Sender<()>) -> Result<(), Error> {
        if self.peers.len() == 1 {
            // Nobody else can win, so there is no reason to wait for the
            // election timeout. This commits the whole log and replays it.
            self.node.campaign()?;
        }
        self.finish_batch()?;
        info!(
            node = %self.me,
            term = %self.node.term(),
            last = %self.log.read().last_id(),
            applied = %self.applied,
            members = self.peers.len(),
            "raft started"
        );
        let _ = started.send(());

        let mut stop = false;
        while !stop {
            // Block for the first input, then take whatever else is
            // already waiting. Those are the inputs that arrived during
            // the previous fsync.
            let Some(first) = self.queue.blocking_recv() else {
                break;
            };
            stop = self.handle(first)?;
            let mut taken = 1;
            while !stop && taken < self.max_batch {
                match self.queue.try_recv() {
                    Ok(input) => stop = self.handle(input)?,
                    Err(_) => break,
                }
                taken += 1;
            }
            self.finish_batch()?;
        }

        self.fail_pending(&stopping());
        Ok(())
    }

    /// Steps one input into the node. Returns true for a stop request.
    fn handle(&mut self, input: Input) -> Result<bool, Error> {
        match input {
            Input::Tick => self.node.tick()?,
            Input::Reply(message) => self.node.step(message)?,
            Input::Request(message, answer) => self.request(message, answer)?,
            Input::Write { op, reply } => self.write(op, reply)?,
            Input::Stop => return Ok(true),
        }
        Ok(false)
    }

    /// Handles a request from a peer and keeps the node's answer aside.
    fn request(
        &mut self,
        message: fl_raft::Message,
        answer: oneshot::Sender<fl_raft::Message>,
    ) -> Result<(), Error> {
        let from = message.from;
        self.node.step(message)?;
        // Take the node's output right away. The one response addressed
        // to the sender is the answer to this request. It is held back
        // with everything else until the batch is durable.
        let mut ready = self.node.ready()?;
        let position = ready
            .messages
            .iter()
            .position(|m| m.to == from && fl_net::wire::is_response(m));
        if let Some(position) = position {
            let response = ready.messages.remove(position);
            self.batch.answers.push((answer, response));
        }
        // With no response, dropping `answer` fails the peer's request,
        // which it treats like a lost message.
        self.batch.absorb(ready);
        Ok(())
    }

    /// Turns a client write into a proposal.
    fn write(&mut self, op: WriteOp, reply: WriteReply) -> Result<(), Error> {
        // A new leader's topic state can be behind: entries from earlier
        // terms may be committed but not yet applied here. Checking a
        // request against that state could, for example, report a topic
        // as missing when it exists. Applying an entry of the leader's own
        // term means everything before it is applied too, so wait for
        // that. It takes one round trip after an election.
        let ready = self.node.is_leader() && self.applied_term == self.node.term();
        if !ready {
            let _ = reply.send(Err(self.not_leader()));
            return Ok(());
        }
        if self.pending.len() >= MAX_PENDING {
            let _ = reply.send(Err(Status::unavailable(
                "too many writes are waiting to be committed",
            )));
            return Ok(());
        }

        let command = match self.prepare(op) {
            Ok(Prepared::Propose(command)) => command,
            Ok(Prepared::Done(outcome)) => {
                let _ = reply.send(Ok(outcome));
                return Ok(());
            }
            Err(e) => {
                let _ = reply.send(Err(to_status(&e)));
                return Ok(());
            }
        };
        match self.node.propose(command) {
            Ok(id) => {
                self.pending.push_back(Pending { id, reply });
                self.batch.writes += 1;
            }
            Err(ProposeError::NotLeader { .. }) => {
                let _ = reply.send(Err(self.not_leader()));
            }
            Err(ProposeError::Storage(e)) => {
                let _ = reply.send(Err(not_durable()));
                return Err(e.into());
            }
        }
        Ok(())
    }

    /// Checks a write against the topic state and builds its command.
    fn prepare(&self, op: WriteOp) -> Result<Prepared, Error> {
        let state = read_lock(&self.state);
        match op {
            WriteOp::CreateTopic { name } => match state.prepare_create_topic(&name)? {
                CreateTopic::Exists(id) => Ok(Prepared::Done(WriteOutcome::TopicCreated {
                    id,
                    created: false,
                })),
                CreateTopic::Propose(command) => Ok(Prepared::Propose(command)),
            },
            WriteOp::Produce { topic, records } => {
                // Stamped here, before the command enters the log, so
                // every replica stores the same timestamps.
                let now = Timestamp::from_system_time(SystemTime::now());
                state
                    .prepare_produce(&topic, records, now)
                    .map(Prepared::Propose)
            }
        }
    }

    /// Does what the batch asked for, in the order Raft requires.
    fn finish_batch(&mut self) -> Result<(), Error> {
        let ready = self.node.ready()?;
        self.batch.absorb(ready);
        let batch = std::mem::take(&mut self.batch);
        // The commit index only moves forward, so the next batch starts
        // from this one's.
        self.batch.commit = batch.commit;

        // 1 and 2. Make the term, the vote, and the log durable.
        let started = Instant::now();
        if let Some(hard_state) = batch.hard_state {
            self.hard.save(fl_net::hard_state_to_disk(hard_state))?;
        }
        if batch.must_sync {
            if self.fsync {
                self.log.sync()?;
            }
            self.metrics.commits.inc();
            self.metrics
                .commit_seconds
                .observe_duration(started.elapsed());
        }
        if batch.writes > 0 {
            self.metrics.commit_batch_size.observe(batch.writes as f64);
        }

        // 3. Only now may anyone hear about it.
        for (answer, response) in batch.answers {
            // The peer may have given up waiting. That is not our problem.
            let _ = answer.send(response);
        }
        for message in batch.outbound {
            self.transport.send(message);
        }

        // 4. Apply what is committed and answer the clients.
        self.apply(batch.commit)?;
        self.drop_orphans();
        self.publish();
        Ok(())
    }

    /// Applies entries up to `commit` and resolves the writes among them.
    fn apply(&mut self, commit: LogIndex) -> Result<(), Error> {
        if commit <= self.applied {
            return Ok(());
        }
        // Results are only needed for entries a client is waiting on.
        // Skipping the rest matters at startup, when the whole log is
        // replayed.
        let wanted_from = self.pending.front().map(|pending| pending.id.index);
        let mut results: Vec<Applied> = Vec::new();
        {
            // The state first and the log second, as in `Engine::read`.
            let mut state = write_lock(&self.state);
            let log = self.log.read();
            state.apply_through(&log, commit, |entry, applied| {
                if wanted_from.is_some_and(|from| entry.index() >= from) {
                    results.push(*applied);
                }
            })?;
            self.metrics.topics.set(state.topic_count() as u64);
            self.applied_term = log.term(commit).unwrap_or(Term::ZERO);
        }
        self.applied = commit;
        debug!(applied = %commit, "applied");

        while let Some(front) = self.pending.front() {
            if front.id.index > commit {
                break;
            }
            let Some(pending) = self.pending.pop_front() else {
                break;
            };
            // The write is committed only if the entry at its index is
            // still the one this node proposed. If another leader took
            // over first, something else is there now.
            let ours = self.log.read().term(pending.id.index) == Some(pending.id.term);
            let result = if ours {
                let offset = wanted_from.map_or(0, |from| pending.id.index.entries_since(from));
                match usize::try_from(offset).ok().and_then(|at| results.get(at)) {
                    Some(applied) => outcome(applied),
                    None => Err(Status::internal("a committed write has no result")),
                }
            } else {
                Err(leadership_lost())
            };
            // The caller may have gone away. That is not our problem.
            let _ = pending.reply.send(result);
        }

        self.committed.send_replace(commit.get());
        Ok(())
    }

    /// Fails the writes this node can no longer vouch for. Once it stops
    /// leading the term they were proposed in, it will never learn their
    /// fate. A new leader may still commit them.
    fn drop_orphans(&mut self) {
        let Some(front) = self.pending.front() else {
            return;
        };
        if !self.node.is_leader() || front.id.term != self.node.term() {
            self.fail_pending(&leadership_lost());
        }
    }

    fn fail_pending(&mut self, status: &Status) {
        for pending in self.pending.drain(..) {
            let _ = pending.reply.send(Err(status.clone()));
        }
    }

    /// The rejection for a write this node cannot take, naming the leader
    /// if it knows one.
    fn not_leader(&self) -> Status {
        let hint = self
            .node
            .leader()
            .filter(|leader| *leader != self.me)
            .map(|leader| LeaderHint {
                node_id: leader,
                addr: self.peers.addr(leader).map(str::to_owned),
            });
        redirect::not_leader(hint.as_ref())
    }

    /// Updates what the Admin service and the metrics report.
    fn publish(&self) {
        let status = self.node.status();
        let view = RaftView {
            role: status.role,
            term: status.term,
            leader: status.leader,
            commit: status.commit_index,
        };
        self.view.send_if_modified(|current| {
            if *current == view {
                return false;
            }
            if current.role != view.role || current.leader != view.leader {
                info!(
                    role = view.role.as_str(),
                    term = %view.term,
                    leader = ?view.leader.map(NodeId::get),
                    "raft state changed"
                );
            }
            *current = view;
            true
        });

        let metrics = &self.metrics;
        metrics.raft_term.set(status.term.get());
        metrics
            .raft_leader
            .set(u64::from(status.role == Role::Leader));
        metrics.commit_index.set(status.commit_index.get());
        let log = self.log.read();
        metrics.log_last_index.set(log.last_index().get());
        metrics.log_bytes.set(log.size_bytes());
        metrics.log_segments.set(log.segment_count() as u64);
    }
}

enum Prepared {
    /// Nothing to replicate. The answer is already known.
    Done(WriteOutcome),
    /// Replicate this command.
    Propose(Command),
}

/// Turns what applying an entry did into the answer for its client.
fn outcome(applied: &Applied) -> Result<WriteOutcome, Status> {
    match *applied {
        Applied::TopicCreated { id, created } => Ok(WriteOutcome::TopicCreated { id, created }),
        Applied::Appended {
            base_offset, count, ..
        } => Ok(WriteOutcome::Produced(Produced { base_offset, count })),
        Applied::Rejected(Rejection::TopicLimit) => Err(to_status(&Error::TooManyTopics)),
        Applied::Rejected(Rejection::UnknownTopic(id)) => {
            Err(Status::not_found(format!("{id} does not exist")))
        }
        Applied::Nothing => Err(Status::internal("the write had no effect")),
    }
}

/// Maps a broker error to the gRPC status a client sees.
pub(crate) fn to_status(error: &Error) -> Status {
    let message = error.to_string();
    match error {
        Error::UnknownTopic(_) => Status::not_found(message),
        Error::Invalid(_) => Status::invalid_argument(message),
        Error::TooManyTopics => Status::resource_exhausted(message),
        Error::Storage(_) | Error::OutOfOrder { .. } | Error::Inconsistent { .. } => {
            Status::internal(message)
        }
    }
}

fn stopping() -> Status {
    Status::unavailable("the node is shutting down")
}

fn not_durable() -> Status {
    Status::internal("the write could not be made durable")
}

/// The answer when leadership changed under a write. The write may or may
/// not end up committed, so the client must not blindly send it again.
fn leadership_lost() -> Status {
    Status::unavailable("leadership changed before the write was confirmed")
}

// A panic while holding the lock already takes the node down through the
// Raft thread's exit path, so a poisoned lock carries no extra information.

fn read_lock(lock: &RwLock<TopicState>) -> RwLockReadGuard<'_, TopicState> {
    lock.read().unwrap_or_else(PoisonError::into_inner)
}

fn write_lock(lock: &RwLock<TopicState>) -> RwLockWriteGuard<'_, TopicState> {
    lock.write().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use tonic::Code;

    fn produce(value: &str) -> WriteOp {
        WriteOp::Produce {
            topic: "quotes".into(),
            records: vec![Record::new(value)],
        }
    }

    async fn open(tag: &str) -> (Engine, WriterHandle, Arc<Metrics>, std::path::PathBuf) {
        let dir =
            std::env::temp_dir().join(format!("ferrumlog-engine-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let config = Config::parse_from(["fl-server", "--data-dir", dir.to_str().unwrap()]);
        let peers = Arc::new(PeerMap::single(NodeId::new(1), "unused"));
        let metrics = Arc::new(Metrics::default());
        let (engine, handle) = Engine::open(&config, peers, Arc::clone(&metrics), Shutdown::new())
            .await
            .unwrap();
        (engine, handle, metrics, dir)
    }

    async fn close(engine: &Engine, handle: WriterHandle) {
        engine.stop().await;
        tokio::task::spawn_blocking(move || handle.join().unwrap().unwrap())
            .await
            .unwrap();
    }

    // The test holds a std lock across an await on purpose: blocking the
    // Raft thread is the whole point. The runtime is single-threaded and
    // nothing else here takes that lock.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn queued_writes_share_one_commit() {
        const WRITES: usize = 50;
        let (engine, handle, metrics, dir) = open("group-commit").await;

        let name = "quotes".to_owned();
        assert!(engine.write(WriteOp::CreateTopic { name }).await.is_ok());
        let before = metrics.commits.get();

        // Hold a read lock on the topic state so the Raft thread cannot
        // apply, and queue writes behind it. The thread takes the first
        // one, syncs it, and then blocks while the rest pile up.
        let reader = read_lock(&engine.state);
        let mut tasks = Vec::new();
        for i in 0..WRITES {
            let engine = engine.clone();
            tasks.push(tokio::spawn(async move {
                engine.write(produce(&format!("v{i}"))).await
            }));
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(tasks.iter().all(|task| !task.is_finished()));
        drop(reader);

        let mut offsets = Vec::new();
        for task in tasks {
            match task.await.unwrap() {
                Ok(WriteOutcome::Produced(produced)) => offsets.push(produced.base_offset.get()),
                _ => panic!("a queued write failed"),
            }
        }
        offsets.sort_unstable();
        assert_eq!(offsets, (0..WRITES as u64).collect::<Vec<_>>());

        // The batch the thread was holding, plus very few for everything
        // that queued up behind it. Fifty writes do not cost fifty fsyncs.
        let commits = metrics.commits.get() - before;
        assert!(commits <= 3, "{commits} commits for {WRITES} writes");
        // The election no-op, the topic, and the fifty writes.
        assert_eq!(*engine.committed().borrow(), WRITES as u64 + 2);

        close(&engine, handle).await;
        // With the thread gone, new writes are refused, not left hanging.
        let refused = engine.write(produce("late")).await;
        assert!(matches!(refused, Err(status) if status.code() == Code::Unavailable));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_single_node_is_a_leader_as_soon_as_it_opens() {
        let (engine, handle, metrics, dir) = open("single").await;
        let snapshot = engine.snapshot().await.unwrap();
        assert_eq!(snapshot.role, Role::Leader);
        assert_eq!(snapshot.leader, Some(NodeId::new(1)));
        assert_eq!(snapshot.term, Term::new(1));
        // The no-op from taking office is committed and applied.
        assert_eq!(snapshot.commit, LogIndex::new(1));
        assert_eq!(snapshot.applied, LogIndex::new(1));
        assert_eq!(metrics.raft_leader.get(), 1);
        assert_eq!(metrics.raft_term.get(), 1);

        let name = "quotes".to_owned();
        assert!(engine.write(WriteOp::CreateTopic { name }).await.is_ok());
        assert!(engine.write(produce("a")).await.is_ok());
        close(&engine, handle).await;

        // After a restart it leads a new term, and what was written is
        // back before `open` returns.
        let config = Config::parse_from(["fl-server", "--data-dir", dir.to_str().unwrap()]);
        let peers = Arc::new(PeerMap::single(NodeId::new(1), "unused"));
        let (engine, handle) = Engine::open(
            &config,
            peers,
            Arc::new(Metrics::default()),
            Shutdown::new(),
        )
        .await
        .unwrap();
        let snapshot = engine.snapshot().await.unwrap();
        assert_eq!(snapshot.term, Term::new(2));
        assert_eq!(snapshot.applied, LogIndex::new(4));
        let topics = engine.topics().await.unwrap();
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].high_watermark, Offset::new(1));
        close(&engine, handle).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_node_outside_its_own_peer_list_does_not_start() {
        let dir =
            std::env::temp_dir().join(format!("ferrumlog-engine-outsider-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let config = Config::parse_from(["fl-server", "--data-dir", dir.to_str().unwrap()]);
        let peers = Arc::new(PeerMap::parse(["2=a:1", "3=b:1"]).unwrap());
        let result = Engine::open(
            &config,
            peers,
            Arc::new(Metrics::default()),
            Shutdown::new(),
        )
        .await;
        let error = result.err().expect("node 1 is not in the list").to_string();
        assert!(error.contains("not in the voter set"), "{error}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn apply_results_become_client_answers() {
        let created = Applied::TopicCreated {
            id: TopicId::new(2),
            created: true,
        };
        assert!(matches!(
            outcome(&created),
            Ok(WriteOutcome::TopicCreated { created: true, .. })
        ));
        let appended = Applied::Appended {
            topic: TopicId::new(1),
            base_offset: Offset::new(7),
            count: 3,
        };
        match outcome(&appended) {
            Ok(WriteOutcome::Produced(produced)) => {
                assert_eq!(produced.base_offset, Offset::new(7));
                assert_eq!(produced.count, 3);
            }
            _ => panic!("expected a produce result"),
        }
        let code = |applied: Applied| outcome(&applied).err().map(|status| status.code());
        assert_eq!(
            code(Applied::Rejected(Rejection::TopicLimit)),
            Some(Code::ResourceExhausted)
        );
        assert_eq!(
            code(Applied::Rejected(Rejection::UnknownTopic(TopicId::new(9)))),
            Some(Code::NotFound)
        );
        assert_eq!(code(Applied::Nothing), Some(Code::Internal));
    }

    #[test]
    fn errors_map_to_the_right_status() {
        let cases = [
            (Error::UnknownTopic("t".into()), Code::NotFound),
            (
                Error::Invalid(fl_types::Error::EmptyBatch),
                Code::InvalidArgument,
            ),
            (Error::TooManyTopics, Code::ResourceExhausted),
            (
                Error::Inconsistent {
                    index: LogIndex::new(3),
                    reason: "test",
                },
                Code::Internal,
            ),
            (
                Error::OutOfOrder {
                    expected: LogIndex::new(1),
                    got: LogIndex::new(2),
                },
                Code::Internal,
            ),
            (
                Error::Storage(fl_storage::Error::InvalidConfig("test")),
                Code::Internal,
            ),
        ];
        for (error, code) in cases {
            assert_eq!(to_status(&error).code(), code, "{error}");
        }
        // Losing leadership is reported as unavailable, which tells a
        // client the outcome is unknown.
        assert_eq!(leadership_lost().code(), Code::Unavailable);
        assert_eq!(not_durable().code(), Code::Internal);
    }
}
