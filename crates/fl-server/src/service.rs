//! The gRPC services: `Log` for clients and `Admin` for operators.

use std::sync::Arc;
use std::time::Instant;

use fl_broker::{FetchLimits, Fetched, FetchedRecord};
use fl_net::PeerMap;
use fl_proto::pb;
use fl_raft::Role;
use fl_types::{NodeId, Offset, Record};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::engine::{Engine, WriteOp, WriteOutcome};
use crate::metrics::Metrics;
use crate::shutdown::Shutdown;

/// Most records one fetch or one subscription message may carry.
const MAX_FETCH_RECORDS: usize = 10_000;

/// Most record bytes one fetch or one subscription message may carry. Kept
/// under gRPC's default 4 MiB message limit with room for framing.
const MAX_FETCH_BYTES: usize = 2 << 20;

/// Subscription messages buffered per stream before the reader task waits
/// for the client to catch up.
const SUBSCRIBE_BUFFER: usize = 4;

/// Turns the limits in a request into limits the server will honour. Zero
/// means "use the default", and anything above the cap is lowered to it.
fn limits(max_records: u32, max_bytes: u32) -> FetchLimits {
    let defaults = FetchLimits::default();
    let pick = |requested: u32, default: usize, cap: usize| match requested {
        0 => default,
        n => (n as usize).min(cap),
    };
    FetchLimits {
        max_records: pick(max_records, defaults.max_records, MAX_FETCH_RECORDS),
        max_bytes: pick(max_bytes, defaults.max_bytes, MAX_FETCH_BYTES),
    }
}

fn consumed(record: FetchedRecord) -> pb::ConsumedRecord {
    pb::ConsumedRecord {
        offset: record.offset.get(),
        record: Some(record.record.into()),
    }
}

fn unexpected() -> Status {
    Status::internal("the write produced an unexpected result")
}

/// Implements the client-facing `Log` service.
pub(crate) struct LogService {
    pub engine: Engine,
    pub metrics: Arc<Metrics>,
    pub shutdown: Shutdown,
}

#[tonic::async_trait]
impl fl_proto::Log for LogService {
    async fn create_topic(
        &self,
        request: Request<pb::CreateTopicRequest>,
    ) -> Result<Response<pb::CreateTopicResponse>, Status> {
        let name = request.into_inner().name;
        match self.engine.write(WriteOp::CreateTopic { name }).await? {
            WriteOutcome::TopicCreated { id, created } => {
                Ok(Response::new(pb::CreateTopicResponse {
                    topic_id: id.get(),
                    created,
                }))
            }
            WriteOutcome::Produced(_) => Err(unexpected()),
        }
    }

    async fn list_topics(
        &self,
        _request: Request<pb::ListTopicsRequest>,
    ) -> Result<Response<pb::ListTopicsResponse>, Status> {
        let topics = self
            .engine
            .topics()
            .await?
            .into_iter()
            .map(|topic| pb::TopicInfo {
                name: topic.name.into_string(),
                topic_id: topic.id.get(),
                high_watermark: topic.high_watermark.get(),
            })
            .collect();
        Ok(Response::new(pb::ListTopicsResponse { topics }))
    }

    async fn produce(
        &self,
        request: Request<pb::ProduceRequest>,
    ) -> Result<Response<pb::ProduceResponse>, Status> {
        let started = Instant::now();
        let request = request.into_inner();
        let records: Vec<Record> = request.records.into_iter().map(Into::into).collect();
        let bytes: usize = records.iter().map(Record::payload_len).sum();

        let op = WriteOp::Produce {
            topic: request.topic,
            records,
        };
        let result = match self.engine.write(op).await {
            Ok(WriteOutcome::Produced(produced)) => Ok(produced),
            Ok(WriteOutcome::TopicCreated { .. }) => Err(unexpected()),
            Err(status) => Err(status),
        };
        self.metrics
            .produce_seconds
            .observe_duration(started.elapsed());

        match result {
            Ok(produced) => {
                self.metrics.produce_ok.inc();
                self.metrics.records_produced.add(u64::from(produced.count));
                self.metrics.bytes_produced.add(bytes as u64);
                Ok(Response::new(pb::ProduceResponse {
                    base_offset: produced.base_offset.get(),
                    count: produced.count,
                }))
            }
            Err(status) => {
                self.metrics.produce_err.inc();
                Err(status)
            }
        }
    }

    async fn fetch(
        &self,
        request: Request<pb::FetchRequest>,
    ) -> Result<Response<pb::FetchResponse>, Status> {
        let request = request.into_inner();
        let fetched = self
            .engine
            .fetch(
                request.topic,
                Offset::new(request.offset),
                limits(request.max_records, request.max_bytes),
            )
            .await?;
        Ok(Response::new(pb::FetchResponse {
            high_watermark: fetched.high_watermark.get(),
            records: fetched.records.into_iter().map(consumed).collect(),
        }))
    }

    type SubscribeStream = ReceiverStream<Result<pb::SubscribeResponse, Status>>;

    async fn subscribe(
        &self,
        request: Request<pb::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        use pb::subscribe_request::Start;

        let request = request.into_inner();
        // Checking the topic here fails a bad request up front with
        // NOT_FOUND, instead of opening a stream that ends at once.
        let high_watermark = self.engine.high_watermark(request.topic.clone()).await?;
        let start = match request.start {
            None => Offset::ZERO,
            Some(Start::Offset(offset)) => Offset::new(offset),
            Some(Start::Position(position)) => match pb::StartPosition::try_from(position) {
                Ok(pb::StartPosition::Latest) => high_watermark,
                Ok(pb::StartPosition::Earliest | pb::StartPosition::Unspecified) => Offset::ZERO,
                Err(_) => return Err(Status::invalid_argument("unknown start position")),
            },
        };

        let (tx, rx) = mpsc::channel(SUBSCRIBE_BUFFER);
        let subscription = Subscription {
            engine: self.engine.clone(),
            metrics: Arc::clone(&self.metrics),
            shutdown: self.shutdown.clone(),
            topic: request.topic,
            next: start,
            tx,
        };
        tokio::spawn(subscription.run());
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// The task behind one Subscribe stream.
struct Subscription {
    engine: Engine,
    metrics: Arc<Metrics>,
    shutdown: Shutdown,
    topic: String,
    /// Offset of the next record to send.
    next: Offset,
    tx: mpsc::Sender<Result<pb::SubscribeResponse, Status>>,
}

impl Subscription {
    /// Sends everything from `next` onward, then waits for more, until the
    /// client goes away or the node shuts down.
    async fn run(mut self) {
        self.metrics.subscriptions.inc();
        let mut committed = self.engine.committed();
        let limits = limits(0, 0);

        loop {
            // Mark the current commit as seen before reading. A commit that
            // lands after this point makes `changed` below return at once,
            // so a record can never slip between the read and the wait.
            committed.borrow_and_update();

            let fetched = match self
                .engine
                .fetch(self.topic.clone(), self.next, limits)
                .await
            {
                Ok(fetched) => fetched,
                Err(status) => {
                    let _ = self.tx.send(Err(status)).await;
                    break;
                }
            };

            if let Some(message) = self.message(fetched) {
                // A full buffer means the client is reading slowly. Waiting
                // here is the backpressure: this task stops reading the log
                // until the client catches up.
                tokio::select! {
                    sent = self.tx.send(Ok(message)) => {
                        if sent.is_err() {
                            break;
                        }
                    }
                    () = self.shutdown.wait() => break,
                }
                continue;
            }

            // Caught up. Sleep until something commits.
            tokio::select! {
                changed = committed.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
                () = self.tx.closed() => break,
                () = self.shutdown.wait() => break,
            }
        }
        self.metrics.subscriptions.dec();
    }

    /// Builds the next stream message and advances `next`. Returns `None`
    /// when there was nothing new.
    fn message(&mut self, fetched: Fetched) -> Option<pb::SubscribeResponse> {
        let last = fetched.records.last()?;
        self.next = last.offset.next();
        Some(pb::SubscribeResponse {
            high_watermark: fetched.high_watermark.get(),
            records: fetched.records.into_iter().map(consumed).collect(),
        })
    }
}

/// Implements the operator-facing `Admin` service.
pub(crate) struct AdminService {
    pub engine: Engine,
    pub node_id: u64,
    /// The address clients use to reach this node.
    pub advertise: String,
    /// Every member of the cluster.
    pub peers: Arc<PeerMap>,
}

#[tonic::async_trait]
impl fl_proto::Admin for AdminService {
    async fn status(
        &self,
        _request: Request<pb::StatusRequest>,
    ) -> Result<Response<pb::StatusResponse>, Status> {
        let node = self.engine.snapshot().await?;
        let role = match node.role {
            Role::Follower => pb::Role::Follower,
            Role::Candidate => pb::Role::Candidate,
            Role::Leader => pb::Role::Leader,
        };
        let me = NodeId::new(self.node_id);
        let voters = self
            .peers
            .iter()
            .map(|(id, addr)| pb::Peer {
                node_id: id.get(),
                // This node reports the address it was told to advertise,
                // which may differ from its entry in the peer list.
                addr: if id == me {
                    self.advertise.clone()
                } else {
                    addr.to_owned()
                },
            })
            .collect();
        Ok(Response::new(pb::StatusResponse {
            node_id: self.node_id,
            role: role as i32,
            term: node.term.get(),
            leader_id: node.leader.map(NodeId::get),
            first_log_index: node.first_index.get(),
            last_log: Some(node.last.into()),
            commit_index: node.commit.get(),
            applied_index: node.applied.get(),
            voters,
            segment_count: u32::try_from(node.segments).unwrap_or(u32::MAX),
            log_bytes: node.bytes,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_limits_mean_the_defaults() {
        assert_eq!(limits(0, 0), FetchLimits::default());
    }

    #[test]
    fn requested_limits_are_honoured_up_to_the_cap() {
        let small = limits(10, 4096);
        assert_eq!(small.max_records, 10);
        assert_eq!(small.max_bytes, 4096);

        let huge = limits(u32::MAX, u32::MAX);
        assert_eq!(huge.max_records, MAX_FETCH_RECORDS);
        assert_eq!(huge.max_bytes, MAX_FETCH_BYTES);
    }
}
