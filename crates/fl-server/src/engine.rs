//! The bridge between async request handlers and the synchronous broker.
//!
//! # Writes: one thread, group commit
//!
//! Every write goes through a bounded queue to a single writer thread. The
//! thread takes whatever is queued, up to a limit, appends all of it to the
//! log, and calls fsync once. Only then does it answer the callers.
//!
//! Under light load each write gets its own fsync. Under heavy load many
//! writes share one, so throughput grows with concurrency instead of being
//! capped at one fsync per request. The queue is bounded, so when the disk
//! cannot keep up, handlers wait in `send` and the pressure reaches the
//! clients.
//!
//! # Reads: shared lock, blocking pool
//!
//! Reads take a shared lock and run on tokio's blocking pool, because a
//! read may hit the disk. The writer holds the exclusive lock for a whole
//! batch including the fsync, so a reader never sees a record that is not
//! yet durable.
//!
//! # Failure
//!
//! A storage error is fatal. The writer fails every request in the batch,
//! triggers shutdown, and exits. Restarting the process runs log recovery,
//! which is the only way back to a state that is known to be good.

use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread;
use std::time::{Instant, SystemTime};

use fl_broker::{FetchLimits, Fetched, Produced, Standalone, TopicInfo};
use fl_storage::{LogConfig, SyncPolicy};
use fl_types::{LogId, LogIndex, Offset, Record, Timestamp, TopicId};
use tokio::sync::{mpsc, oneshot, watch};
use tonic::Status;
use tracing::{debug, error};

use crate::config::Config;
use crate::metrics::Metrics;
use crate::shutdown::Shutdown;
use crate::BoxError;

/// Write requests that may wait in the queue before handlers block.
const WRITE_QUEUE: usize = 1024;

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

struct WriteRequest {
    op: WriteOp,
    reply: oneshot::Sender<Result<WriteOutcome, Status>>,
}

enum Message {
    Write(WriteRequest),
    /// Finish everything queued ahead of this message, then exit.
    Stop,
}

/// A point-in-time view of the log, for the Admin service.
pub(crate) struct LogSnapshot {
    pub first_index: LogIndex,
    pub last: LogId,
    pub applied: LogIndex,
    pub segments: usize,
    pub bytes: u64,
}

/// The writer thread's handle. Joining it yields the error that stopped the
/// writer, if any.
pub(crate) type WriterHandle = thread::JoinHandle<Result<(), fl_broker::Error>>;

/// A cloneable handle to the broker.
#[derive(Clone)]
pub(crate) struct Engine {
    broker: Arc<RwLock<Standalone>>,
    writes: mpsc::Sender<Message>,
    /// Holds the index of the last committed entry. Subscriptions wait on
    /// it to learn that there is something new to read.
    committed: watch::Receiver<u64>,
    metrics: Arc<Metrics>,
}

impl Engine {
    /// Opens the data directory, replays the log, and starts the writer
    /// thread.
    pub(crate) async fn open(
        config: &Config,
        metrics: Arc<Metrics>,
        shutdown: Shutdown,
    ) -> Result<(Engine, WriterHandle), BoxError> {
        let dir = config.data_dir.clone();
        // The writer syncs once per batch, so the log itself must not sync
        // on every append.
        let log_config = LogConfig {
            segment_bytes: config.segment_bytes,
            sync: SyncPolicy::Manual,
        };
        let broker =
            tokio::task::spawn_blocking(move || Standalone::open(dir, log_config)).await??;

        refresh_gauges(&metrics, &broker);
        let last = broker.log().last_index().get();
        let broker = Arc::new(RwLock::new(broker));
        let (writes, queue) = mpsc::channel(WRITE_QUEUE);
        let (committed_tx, committed) = watch::channel(last);

        let writer = Writer {
            broker: Arc::clone(&broker),
            queue,
            committed: committed_tx,
            metrics: Arc::clone(&metrics),
            max_batch: config.max_commit_batch.max(1),
            fsync: !config.unsafe_no_fsync,
        };
        let handle = thread::Builder::new()
            .name("fl-writer".into())
            .spawn(move || {
                let result = writer.run();
                if let Err(e) = &result {
                    error!(error = %e, "storage failure, shutting the node down");
                    shutdown.trigger();
                }
                result
            })?;

        let engine = Engine {
            broker,
            writes,
            committed,
            metrics,
        };
        Ok((engine, handle))
    }

    /// Queues a write and waits until it is committed.
    pub(crate) async fn write(&self, op: WriteOp) -> Result<WriteOutcome, Status> {
        let (reply, response) = oneshot::channel();
        let request = Message::Write(WriteRequest { op, reply });
        if self.writes.send(request).await.is_err() {
            return Err(stopping());
        }
        // The writer dropping the reply without answering means it exited.
        response.await.unwrap_or_else(|_| Err(stopping()))
    }

    /// Tells the writer to finish what is queued and exit.
    pub(crate) async fn stop(&self) {
        let _ = self.writes.send(Message::Stop).await;
    }

    /// A receiver that changes every time a batch commits.
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
            .read(move |broker| broker.fetch(&topic, offset, limits))
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
        self.read(|broker| broker.topics()).await
    }

    /// The high watermark of a topic, or `NOT_FOUND`.
    pub(crate) async fn high_watermark(&self, topic: String) -> Result<Offset, Status> {
        self.read(move |broker| {
            let state = broker.state();
            state
                .topic_id(&topic)
                .and_then(|id| state.high_watermark(id))
                .ok_or_else(|| to_status(&fl_broker::Error::UnknownTopic(topic)))
        })
        .await?
    }

    /// A consistent view of the log's position and size.
    pub(crate) async fn snapshot(&self) -> Result<LogSnapshot, Status> {
        self.read(|broker| {
            let log = broker.log();
            LogSnapshot {
                first_index: log.first_index(),
                last: log.last_id(),
                applied: broker.state().applied(),
                segments: log.segment_count(),
                bytes: log.size_bytes(),
            }
        })
        .await
    }

    /// Runs `f` with shared access to the broker, on the blocking pool.
    async fn read<T, F>(&self, f: F) -> Result<T, Status>
    where
        T: Send + 'static,
        F: FnOnce(&Standalone) -> T + Send + 'static,
    {
        let broker = Arc::clone(&self.broker);
        tokio::task::spawn_blocking(move || f(&read_lock(&broker)))
            .await
            .map_err(|_| Status::internal("a read task failed"))
    }
}

struct Writer {
    broker: Arc<RwLock<Standalone>>,
    queue: mpsc::Receiver<Message>,
    committed: watch::Sender<u64>,
    metrics: Arc<Metrics>,
    max_batch: usize,
    fsync: bool,
}

impl Writer {
    fn run(mut self) -> Result<(), fl_broker::Error> {
        let mut batch = Vec::with_capacity(self.max_batch);
        let mut stopping = false;
        while !stopping {
            // Block for the first request, then take whatever else is
            // already waiting. Those are the requests that arrived during
            // the previous fsync.
            match self.queue.blocking_recv() {
                Some(Message::Write(request)) => batch.push(request),
                Some(Message::Stop) | None => break,
            }
            while batch.len() < self.max_batch {
                match self.queue.try_recv() {
                    Ok(Message::Write(request)) => batch.push(request),
                    Ok(Message::Stop) => {
                        stopping = true;
                        break;
                    }
                    Err(_) => break,
                }
            }
            self.commit(&mut batch)?;
        }
        Ok(())
    }

    /// Applies a batch, syncs once, and answers every request in it.
    fn commit(&self, batch: &mut Vec<WriteRequest>) -> Result<(), fl_broker::Error> {
        let started = Instant::now();
        let size = batch.len();
        let now = Timestamp::from_system_time(SystemTime::now());
        let mut replies = Vec::with_capacity(size);
        let mut fatal: Option<fl_broker::Error> = None;

        let last = {
            let mut broker = write_lock(&self.broker);
            for request in batch.drain(..) {
                let result = if fatal.is_some() {
                    Err(not_durable())
                } else {
                    match apply(&mut broker, request.op, now) {
                        Ok(outcome) => Ok(outcome),
                        Err(e) => {
                            let status = to_status(&e);
                            if is_fatal(&e) {
                                fatal = Some(e);
                            }
                            Err(status)
                        }
                    }
                };
                replies.push((request.reply, result));
            }
            if fatal.is_none() && self.fsync {
                fatal = broker.sync().err();
            }
            refresh_gauges(&self.metrics, &broker);
            broker.log().last_index()
        };

        self.metrics.commits.inc();
        self.metrics.commit_batch_size.observe(size as f64);
        self.metrics
            .commit_seconds
            .observe_duration(started.elapsed());
        debug!(size, last = %last, "committed a batch");

        for (reply, result) in replies {
            // A write that was applied but not synced must not be
            // acknowledged, even though it is in the log.
            let result = match (&fatal, result) {
                (Some(_), Ok(_)) => Err(not_durable()),
                (_, result) => result,
            };
            // The caller may have gone away. That is not our problem.
            let _ = reply.send(result);
        }

        match fatal {
            Some(e) => Err(e),
            None => {
                self.committed.send_replace(last.get());
                Ok(())
            }
        }
    }
}

fn apply(
    broker: &mut Standalone,
    op: WriteOp,
    now: Timestamp,
) -> Result<WriteOutcome, fl_broker::Error> {
    match op {
        WriteOp::CreateTopic { name } => {
            let (id, created) = broker.create_topic(&name)?;
            Ok(WriteOutcome::TopicCreated { id, created })
        }
        WriteOp::Produce { topic, records } => Ok(WriteOutcome::Produced(
            broker.produce(&topic, records, now)?,
        )),
    }
}

fn refresh_gauges(metrics: &Metrics, broker: &Standalone) {
    let log = broker.log();
    metrics.log_last_index.set(log.last_index().get());
    metrics.log_bytes.set(log.size_bytes());
    metrics.log_segments.set(log.segment_count() as u64);
    metrics.topics.set(broker.state().topic_count() as u64);
}

/// True for errors that mean the node can no longer trust its own storage.
fn is_fatal(error: &fl_broker::Error) -> bool {
    use fl_broker::Error;
    match error {
        Error::UnknownTopic(_) | Error::Invalid(_) | Error::TooManyTopics => false,
        Error::Storage(_) | Error::OutOfOrder { .. } | Error::Inconsistent { .. } => true,
    }
}

/// Maps a broker error to the gRPC status a client sees.
pub(crate) fn to_status(error: &fl_broker::Error) -> Status {
    use fl_broker::Error;
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

// A panic while holding the lock already takes the node down through the
// writer's exit path, so a poisoned lock carries no extra information.

fn read_lock(lock: &RwLock<Standalone>) -> RwLockReadGuard<'_, Standalone> {
    lock.read().unwrap_or_else(PoisonError::into_inner)
}

fn write_lock(lock: &RwLock<Standalone>) -> RwLockWriteGuard<'_, Standalone> {
    lock.write().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::time::Duration;
    use tonic::Code;

    fn produce(value: &str) -> WriteOp {
        WriteOp::Produce {
            topic: "quotes".into(),
            records: vec![Record::new(value)],
        }
    }

    // The test holds a std lock across an await on purpose: blocking the
    // writer is the whole point. The runtime is single-threaded and nothing
    // else here takes that lock.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn queued_writes_share_one_commit() {
        const WRITES: usize = 50;

        let dir = std::env::temp_dir().join(format!(
            "ferrumlog-engine-group-commit-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let config = Config::parse_from(["fl-server", "--data-dir", dir.to_str().unwrap()]);
        let metrics = Arc::new(Metrics::default());
        let (engine, writer) = Engine::open(&config, Arc::clone(&metrics), Shutdown::new())
            .await
            .unwrap();

        let name = "quotes".to_owned();
        assert!(engine.write(WriteOp::CreateTopic { name }).await.is_ok());
        assert_eq!(metrics.commits.get(), 1);

        // Hold a read lock so the writer cannot commit, and queue writes
        // behind it. The writer picks up the first one and then blocks on
        // the lock while the rest pile up in the queue.
        let reader = read_lock(&engine.broker);
        let mut tasks = Vec::new();
        for i in 0..WRITES {
            let engine = engine.clone();
            tasks.push(tokio::spawn(async move {
                engine.write(produce(&format!("v{i}"))).await
            }));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
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

        // The batch the writer was holding, plus one for everything that
        // queued up behind it. Fifty writes cost at most two fsyncs.
        let commits = metrics.commits.get() - 1;
        assert!(commits <= 2, "{commits} commits for {WRITES} writes");
        assert_eq!(*engine.committed().borrow(), WRITES as u64 + 1);

        engine.stop().await;
        tokio::task::spawn_blocking(move || writer.join().unwrap().unwrap())
            .await
            .unwrap();
        // With the writer gone, new writes are refused, not left hanging.
        let refused = engine.write(produce("late")).await;
        assert!(matches!(refused, Err(status) if status.code() == Code::Unavailable));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn client_mistakes_are_not_fatal() {
        use fl_broker::Error;
        let cases = [
            (Error::UnknownTopic("t".into()), Code::NotFound, false),
            (
                Error::Invalid(fl_types::Error::EmptyBatch),
                Code::InvalidArgument,
                false,
            ),
            (Error::TooManyTopics, Code::ResourceExhausted, false),
            (
                Error::Inconsistent {
                    index: LogIndex::new(3),
                    reason: "test",
                },
                Code::Internal,
                true,
            ),
            (
                Error::OutOfOrder {
                    expected: LogIndex::new(1),
                    got: LogIndex::new(2),
                },
                Code::Internal,
                true,
            ),
            (
                Error::Storage(fl_storage::Error::InvalidConfig("test")),
                Code::Internal,
                true,
            ),
        ];
        for (error, code, fatal) in cases {
            assert_eq!(to_status(&error).code(), code, "{error}");
            assert_eq!(is_fatal(&error), fatal, "{error}");
        }
    }
}
