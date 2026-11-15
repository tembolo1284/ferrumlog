//! Client tests against real nodes, plus a scripted node for the paths a
//! single real node cannot produce: redirects and hangs.

use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fl_client::{
    Client, ClientConfig, ConsumedRecord, Error, FetchOptions, Role, StartAt, Subscription,
};
use fl_proto::pb;
use fl_proto::redirect::{self, LeaderHint};
use fl_proto::LogServer;
use fl_server::{Config, Server};
use fl_types::{NodeId, Offset, Record, TopicId};
use tokio::net::TcpListener;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::server::TcpIncoming;
use tonic::{Code, Request, Response, Status};

/// A scratch directory that is deleted when dropped.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("ferrumlog-client-{tag}-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        TempDir(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn server_config(dir: &Path, listen: &str) -> Config {
    Config {
        node_id: 1,
        data_dir: dir.to_path_buf(),
        listen: listen.parse().unwrap(),
        advertise: None,
        metrics_listen: "127.0.0.1:0".parse().unwrap(),
        no_metrics: true,
        segment_bytes: 64 << 20,
        max_commit_batch: 256,
        unsafe_no_fsync: false,
    }
}

async fn start_server(dir: &TempDir) -> Server {
    Server::start(server_config(dir.path(), "127.0.0.1:0"))
        .await
        .unwrap()
}

/// An address nothing is listening on: connecting to it is refused.
async fn dead_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

/// A client config with short waits so failure tests finish quickly.
fn fast<I, S>(endpoints: I) -> ClientConfig
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut config = ClientConfig::new(endpoints);
    config.connect_timeout = Duration::from_secs(2);
    config.request_timeout = Duration::from_secs(5);
    config.max_retries = 3;
    config.backoff_base = Duration::from_millis(10);
    config.backoff_max = Duration::from_millis(50);
    config
}

/// Fails the test instead of hanging if something never completes.
async fn soon<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(20), future)
        .await
        .expect("timed out")
}

fn records(values: &[&str]) -> Vec<Record> {
    values.iter().map(|v| Record::new(*v)).collect()
}

/// `offset=value` for each record.
fn summary(records: &[ConsumedRecord]) -> Vec<String> {
    records
        .iter()
        .map(|r| {
            format!(
                "{}={}",
                r.offset.get(),
                String::from_utf8_lossy(&r.record.value)
            )
        })
        .collect()
}

/// Reads from a subscription until `count` records have arrived.
async fn take(subscription: &mut Subscription, count: usize) -> Vec<ConsumedRecord> {
    let mut records = Vec::new();
    while records.len() < count {
        let batch = soon(subscription.next()).await.unwrap();
        assert!(!batch.records.is_empty());
        records.extend(batch.records);
    }
    records
}

/// A node that never does any work. It either tells every caller it is not
/// the leader, or it does not answer at all.
struct Scripted {
    hint: Option<LeaderHint>,
    delay: Duration,
    calls: Arc<AtomicU32>,
}

impl Scripted {
    async fn reject<T>(&self) -> Result<Response<T>, Status> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        Err(redirect::not_leader(self.hint.as_ref()))
    }
}

#[tonic::async_trait]
impl fl_proto::Log for Scripted {
    async fn create_topic(
        &self,
        _: Request<pb::CreateTopicRequest>,
    ) -> Result<Response<pb::CreateTopicResponse>, Status> {
        self.reject().await
    }

    async fn list_topics(
        &self,
        _: Request<pb::ListTopicsRequest>,
    ) -> Result<Response<pb::ListTopicsResponse>, Status> {
        self.reject().await
    }

    async fn produce(
        &self,
        _: Request<pb::ProduceRequest>,
    ) -> Result<Response<pb::ProduceResponse>, Status> {
        self.reject().await
    }

    async fn fetch(
        &self,
        _: Request<pb::FetchRequest>,
    ) -> Result<Response<pb::FetchResponse>, Status> {
        self.reject().await
    }

    type SubscribeStream = ReceiverStream<Result<pb::SubscribeResponse, Status>>;

    async fn subscribe(
        &self,
        _: Request<pb::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        self.reject().await
    }
}

/// Starts a scripted node and returns its address and call counter.
async fn start_scripted(hint: Option<LeaderHint>, delay: Duration) -> (SocketAddr, Arc<AtomicU32>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let calls = Arc::new(AtomicU32::new(0));
    let service = Scripted {
        hint,
        delay,
        calls: Arc::clone(&calls),
    };
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(LogServer::new(service))
            .serve_with_incoming(TcpIncoming::from(listener))
            .await
            .unwrap();
    });
    (addr, calls)
}

fn leader_at(addr: SocketAddr) -> Option<LeaderHint> {
    Some(LeaderHint {
        node_id: NodeId::new(1),
        addr: Some(addr.to_string()),
    })
}

#[tokio::test]
async fn round_trip_against_a_real_node() {
    let dir = TempDir::new("round-trip");
    let server = start_server(&dir).await;
    let client = soon(Client::connect(fast([server.grpc_addr().to_string()])))
        .await
        .unwrap();

    let created = client.create_topic("quotes").await.unwrap();
    assert_eq!((created.id, created.created), (TopicId::new(1), true));
    let again = client.create_topic("quotes").await.unwrap();
    assert_eq!((again.id, again.created), (TopicId::new(1), false));

    let keyed = Record::new("99.5").with_key("UST10Y");
    let produced = client
        .produce("quotes", vec![keyed.clone(), Record::new("b")])
        .await
        .unwrap();
    assert_eq!(produced.base_offset, Offset::ZERO);
    assert_eq!(produced.count, 2);
    let produced = client.produce("quotes", records(&["c"])).await.unwrap();
    assert_eq!(produced.base_offset, Offset::new(2));

    let batch = client
        .fetch("quotes", Offset::ZERO, FetchOptions::default())
        .await
        .unwrap();
    assert_eq!(summary(&batch.records), ["0=99.5", "1=b", "2=c"]);
    assert_eq!(batch.records[0].record.key, keyed.key);
    assert!(batch.records[0].record.timestamp.as_millis() > 0);
    assert_eq!(batch.high_watermark, Offset::new(3));
    assert_eq!(batch.next_offset(), Some(Offset::new(3)));

    let limited = FetchOptions {
        max_records: 1,
        max_bytes: 0,
    };
    let one = client
        .fetch("quotes", Offset::new(1), limited)
        .await
        .unwrap();
    assert_eq!(summary(&one.records), ["1=b"]);

    let topics = client.list_topics().await.unwrap();
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].name, "quotes");
    assert_eq!(topics[0].high_watermark, Offset::new(3));

    let status = client.status().await.unwrap();
    assert_eq!(status.node_id, NodeId::new(1));
    assert_eq!(status.role, Role::Leader);
    assert_eq!(status.leader_id, Some(NodeId::new(1)));
    assert_eq!(status.commit_index.get(), 3);
    assert_eq!(status.endpoint, format!("http://{}", server.grpc_addr()));
    assert_eq!(status.voters.len(), 1);

    soon(server.stop()).await.unwrap();
}

#[tokio::test]
async fn a_rejection_is_returned_at_once() {
    let dir = TempDir::new("rejected");
    let server = start_server(&dir).await;
    let client = Client::new(fast([server.grpc_addr().to_string()])).unwrap();

    let error = client.produce("nope", records(&["a"])).await.unwrap_err();
    assert!(matches!(error, Error::Rejected(_)), "{error:?}");
    assert_eq!(error.code(), Some(Code::NotFound));
    assert!(error.to_string().contains("does not exist"), "{error}");

    let error = client.create_topic("bad name").await.unwrap_err();
    assert_eq!(error.code(), Some(Code::InvalidArgument));

    let error = client
        .fetch("nope", Offset::ZERO, FetchOptions::default())
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some(Code::NotFound));

    let error = match client.subscribe("nope", StartAt::Earliest).await {
        Ok(_) => panic!("subscribing to a missing topic should fail"),
        Err(error) => error,
    };
    assert_eq!(error.code(), Some(Code::NotFound));

    soon(server.stop()).await.unwrap();
}

#[tokio::test]
async fn reads_fail_over_to_a_live_endpoint() {
    let dir = TempDir::new("failover");
    let server = start_server(&dir).await;
    let live = server.grpc_addr().to_string();
    let dead = dead_addr().await.to_string();

    let client = Client::new(fast([dead.clone(), live.clone()])).unwrap();
    assert_eq!(client.endpoints()[0], format!("http://{dead}"));

    assert!(soon(client.list_topics()).await.unwrap().is_empty());
    // The client now starts with the node that answered.
    assert_eq!(client.endpoints()[0], format!("http://{live}"));

    // A call that is safe to repeat fails over too, and so does a write
    // once the client has settled on a live node.
    client.create_topic("quotes").await.unwrap();
    client.produce("quotes", records(&["a"])).await.unwrap();

    soon(server.stop()).await.unwrap();
}

#[tokio::test]
async fn ambiguous_writes_are_retried_only_when_asked() {
    let dead = dead_addr().await.to_string();

    // Default: one try, then the caller decides.
    let client = Client::new(fast([dead.clone()])).unwrap();
    let error = soon(client.produce("quotes", records(&["a"])))
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::Unavailable { attempts: 1, .. }),
        "{error:?}"
    );

    // A call that is safe to repeat uses every retry.
    let error = soon(client.create_topic("quotes")).await.unwrap_err();
    assert!(
        matches!(error, Error::Unavailable { attempts: 4, .. }),
        "{error:?}"
    );
    let error = soon(client.list_topics()).await.unwrap_err();
    assert!(
        matches!(error, Error::Unavailable { attempts: 4, .. }),
        "{error:?}"
    );

    // Opted in: writes use every retry as well.
    let mut config = fast([dead]);
    config.retry_writes = true;
    let client = Client::new(config).unwrap();
    let error = soon(client.produce("quotes", records(&["a"])))
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::Unavailable { attempts: 4, .. }),
        "{error:?}"
    );
}

#[tokio::test]
async fn connect_reports_an_unreachable_cluster() {
    let dead = dead_addr().await.to_string();
    let result = soon(Client::connect(fast([dead]))).await;
    assert!(matches!(result, Err(Error::Unavailable { .. })));
}

#[tokio::test]
async fn redirects_are_followed_and_remembered() {
    let dir = TempDir::new("redirect");
    let server = start_server(&dir).await;
    let leader = server.grpc_addr();
    let (follower, calls) = start_scripted(leader_at(leader), Duration::ZERO).await;

    // The client only knows the follower.
    let client = Client::new(fast([follower.to_string()])).unwrap();
    client.create_topic("quotes").await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // It learned the leader's address and now goes there directly. That
    // includes writes: a redirect means nothing was written, so following
    // one is safe even though blind retries of writes are off.
    assert_eq!(
        client.endpoints(),
        [format!("http://{leader}"), format!("http://{follower}")]
    );
    let produced = client
        .produce("quotes", records(&["a", "b"]))
        .await
        .unwrap();
    assert_eq!(produced.base_offset, Offset::ZERO);
    let batch = client
        .fetch("quotes", Offset::ZERO, FetchOptions::default())
        .await
        .unwrap();
    assert_eq!(summary(&batch.records), ["0=a", "1=b"]);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    soon(server.stop()).await.unwrap();
}

#[tokio::test]
async fn a_write_sent_to_a_follower_first_is_redirected() {
    let dir = TempDir::new("redirect-write");
    let server = start_server(&dir).await;
    let leader = server.grpc_addr();
    let (follower, calls) = start_scripted(leader_at(leader), Duration::ZERO).await;

    // Set the topic up through the leader, then write through a client
    // whose first stop is the follower.
    let setup = Client::new(fast([leader.to_string()])).unwrap();
    setup.create_topic("quotes").await.unwrap();

    let client = Client::new(fast([follower.to_string()])).unwrap();
    let produced = client.produce("quotes", records(&["a"])).await.unwrap();
    assert_eq!(produced.base_offset, Offset::ZERO);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // Exactly one copy was written.
    let batch = setup
        .fetch("quotes", Offset::ZERO, FetchOptions::default())
        .await
        .unwrap();
    assert_eq!(summary(&batch.records), ["0=a"]);

    soon(server.stop()).await.unwrap();
}

#[tokio::test]
async fn a_redirect_without_a_leader_tries_the_next_node() {
    let dir = TempDir::new("no-hint");
    let server = start_server(&dir).await;
    let leader = server.grpc_addr();
    // Mid-election a follower knows it is not the leader but not who is.
    let (follower, calls) = start_scripted(None, Duration::ZERO).await;

    let client = Client::new(fast([follower.to_string(), leader.to_string()])).unwrap();
    client.create_topic("quotes").await.unwrap();
    client.produce("quotes", records(&["a"])).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(client.endpoints()[0], format!("http://{leader}"));

    soon(server.stop()).await.unwrap();
}

#[tokio::test]
async fn redirects_that_lead_nowhere_give_up() {
    // A lone node that keeps saying "not me" and names nobody.
    let (lonely, calls) = start_scripted(None, Duration::ZERO).await;
    let client = Client::new(fast([lonely.to_string()])).unwrap();
    let error = soon(client.produce("quotes", records(&["a"])))
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::Unavailable { attempts: 4, .. }),
        "{error:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 4);

    // A node that points at another node, which in turn names nobody.
    let (dead_end, _) = start_scripted(None, Duration::ZERO).await;
    let (pointer, calls) = start_scripted(leader_at(dead_end), Duration::ZERO).await;
    let client = Client::new(fast([pointer.to_string()])).unwrap();
    let error = soon(client.list_topics()).await.unwrap_err();
    assert!(
        matches!(error, Error::Unavailable { attempts: 4, .. }),
        "{error:?}"
    );
    assert!(calls.load(Ordering::SeqCst) >= 1);
}

#[tokio::test]
async fn a_node_that_does_not_answer_times_out() {
    let dir = TempDir::new("timeout");
    let server = start_server(&dir).await;
    let live = server.grpc_addr().to_string();
    let (slow, calls) = start_scripted(None, Duration::from_secs(30)).await;

    let mut config = fast([slow.to_string(), live.clone()]);
    config.request_timeout = Duration::from_millis(200);
    let client = Client::new(config).unwrap();

    // The read gives up on the silent node and gets its answer elsewhere.
    assert!(soon(client.list_topics()).await.unwrap().is_empty());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(client.endpoints()[0], format!("http://{live}"));

    // A write that times out is ambiguous, so it is reported, not retried.
    let mut config = fast([slow.to_string()]);
    config.request_timeout = Duration::from_millis(200);
    let client = Client::new(config).unwrap();
    let error = soon(client.produce("quotes", records(&["a"])))
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::Unavailable { attempts: 1, .. }),
        "{error:?}"
    );
    assert_eq!(error.code(), Some(Code::DeadlineExceeded));

    soon(server.stop()).await.unwrap();
}

#[tokio::test]
async fn status_of_looks_at_one_node_without_failover() {
    let dir = TempDir::new("status-of");
    let server = start_server(&dir).await;
    let live = server.grpc_addr().to_string();
    let dead = dead_addr().await.to_string();
    let client = Client::new(fast([live.clone(), dead.clone()])).unwrap();

    let status = soon(client.status_of(&live)).await.unwrap();
    assert_eq!(status.role, Role::Leader);
    assert_eq!(status.endpoint, format!("http://{live}"));

    let error = soon(client.status_of(&dead)).await.unwrap_err();
    assert!(
        matches!(error, Error::Unavailable { attempts: 1, .. }),
        "{error:?}"
    );
    // Looking at a dead node does not change where calls go.
    assert_eq!(client.endpoints()[0], format!("http://{live}"));

    // An endpoint the client was not configured with works too.
    let other = Client::new(fast([dead])).unwrap();
    assert!(soon(other.status_of(&live)).await.is_ok());

    soon(server.stop()).await.unwrap();
}

#[tokio::test]
async fn subscription_delivers_live_records_in_order() {
    let dir = TempDir::new("subscribe");
    let server = start_server(&dir).await;
    let client = Client::new(fast([server.grpc_addr().to_string()])).unwrap();
    client.create_topic("quotes").await.unwrap();
    client
        .produce("quotes", records(&["a", "b"]))
        .await
        .unwrap();

    let mut earliest = client.subscribe("quotes", StartAt::Earliest).await.unwrap();
    assert_eq!(earliest.topic(), "quotes");
    assert_eq!(summary(&take(&mut earliest, 2).await), ["0=a", "1=b"]);

    let mut latest = client.subscribe("quotes", StartAt::Latest).await.unwrap();
    let mut from_one = client
        .subscribe("quotes", StartAt::Offset(Offset::new(1)))
        .await
        .unwrap();
    assert_eq!(summary(&take(&mut from_one, 1).await), ["1=b"]);

    client.produce("quotes", records(&["c"])).await.unwrap();
    assert_eq!(summary(&take(&mut earliest, 1).await), ["2=c"]);
    assert_eq!(summary(&take(&mut latest, 1).await), ["2=c"]);
    assert_eq!(summary(&take(&mut from_one, 1).await), ["2=c"]);

    drop((earliest, latest, from_one));
    soon(server.stop()).await.unwrap();
}

#[tokio::test]
async fn subscription_resumes_after_the_node_restarts() {
    let dir = TempDir::new("resume");
    // A fixed port, so the restarted node is reachable at the same address.
    let addr = dead_addr().await.to_string();

    let server = Server::start(server_config(dir.path(), &addr))
        .await
        .unwrap();
    let mut config = fast([addr.clone()]);
    config.max_retries = 50;
    let client = Client::new(config).unwrap();
    client.create_topic("quotes").await.unwrap();
    client
        .produce("quotes", records(&["a", "b"]))
        .await
        .unwrap();

    let mut subscription = client.subscribe("quotes", StartAt::Earliest).await.unwrap();
    assert_eq!(summary(&take(&mut subscription, 2).await), ["0=a", "1=b"]);

    // The node goes away while the subscription is idle.
    soon(server.stop()).await.unwrap();
    let waiting = tokio::spawn(async move {
        let records = take(&mut subscription, 2).await;
        (records, subscription)
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!waiting.is_finished());

    // It comes back with the same data, and more is written.
    let server = Server::start(server_config(dir.path(), &addr))
        .await
        .unwrap();
    let mut produced = None;
    for _ in 0..100 {
        match client.produce("quotes", records(&["c", "d"])).await {
            Ok(result) => {
                produced = Some(result);
                break;
            }
            Err(Error::Unavailable { .. }) => tokio::time::sleep(Duration::from_millis(20)).await,
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }
    assert_eq!(produced.unwrap().base_offset, Offset::new(2));

    // The subscription picks up exactly where it stopped: no repeat of the
    // records it already delivered, and nothing skipped.
    let (resumed, mut subscription) = soon(waiting).await.unwrap();
    assert_eq!(summary(&resumed), ["2=c", "3=d"]);

    client.produce("quotes", records(&["e"])).await.unwrap();
    assert_eq!(summary(&take(&mut subscription, 1).await), ["4=e"]);

    drop(subscription);
    soon(server.stop()).await.unwrap();
}

#[tokio::test]
async fn subscription_reports_a_cluster_that_stays_down() {
    let dir = TempDir::new("stays-down");
    let server = start_server(&dir).await;
    let client = Client::new(fast([server.grpc_addr().to_string()])).unwrap();
    client.create_topic("quotes").await.unwrap();
    client.produce("quotes", records(&["a"])).await.unwrap();

    let mut subscription = client.subscribe("quotes", StartAt::Earliest).await.unwrap();
    assert_eq!(summary(&take(&mut subscription, 1).await), ["0=a"]);

    soon(server.stop()).await.unwrap();
    let error = soon(subscription.next()).await.unwrap_err();
    assert!(matches!(error, Error::Unavailable { .. }), "{error:?}");
    // The subscription is still usable and fails the same way again.
    let error = soon(subscription.next()).await.unwrap_err();
    assert!(matches!(error, Error::Unavailable { .. }), "{error:?}");
}
