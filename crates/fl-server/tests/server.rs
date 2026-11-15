//! End-to-end tests: a real node on a loopback port, driven by the
//! generated gRPC clients.

use std::collections::BTreeSet;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fl_proto::pb;
use fl_proto::{AdminClient, LogClient};
use fl_server::{Config, Server};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::transport::Channel;
use tonic::{Code, Streaming};

/// A scratch directory that is deleted when dropped.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("ferrumlog-server-{tag}-{}-{n}", std::process::id()));
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

/// A config that binds both servers to free loopback ports.
fn config(dir: &Path) -> Config {
    Config {
        node_id: 1,
        data_dir: dir.to_path_buf(),
        listen: "127.0.0.1:0".parse().unwrap(),
        advertise: None,
        metrics_listen: "127.0.0.1:0".parse().unwrap(),
        no_metrics: false,
        segment_bytes: 64 << 20,
        max_commit_batch: 256,
        unsafe_no_fsync: false,
    }
}

async fn start(dir: &TempDir) -> Server {
    Server::start(config(dir.path())).await.unwrap()
}

async fn log_client(server: &Server) -> LogClient<Channel> {
    LogClient::connect(format!("http://{}", server.grpc_addr()))
        .await
        .unwrap()
}

/// Fails the test instead of hanging if something never completes.
async fn soon<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("timed out")
}

fn record(value: &str) -> pb::Record {
    pb::Record {
        key: None,
        value: value.as_bytes().to_vec(),
        timestamp_ms: 0,
    }
}

async fn create(client: &mut LogClient<Channel>, name: &str) -> pb::CreateTopicResponse {
    let request = pb::CreateTopicRequest { name: name.into() };
    client.create_topic(request).await.unwrap().into_inner()
}

async fn produce(
    client: &mut LogClient<Channel>,
    topic: &str,
    values: &[&str],
) -> pb::ProduceResponse {
    let request = pb::ProduceRequest {
        topic: topic.into(),
        records: values.iter().map(|v| record(v)).collect(),
    };
    client.produce(request).await.unwrap().into_inner()
}

async fn fetch(client: &mut LogClient<Channel>, topic: &str, offset: u64) -> pb::FetchResponse {
    let request = pb::FetchRequest {
        topic: topic.into(),
        offset,
        max_records: 0,
        max_bytes: 0,
    };
    client.fetch(request).await.unwrap().into_inner()
}

/// `offset=value` for each record, which makes assertions easy to read.
fn summary(records: &[pb::ConsumedRecord]) -> Vec<String> {
    records
        .iter()
        .map(|r| {
            let value = &r.record.as_ref().unwrap().value;
            format!("{}={}", r.offset, String::from_utf8_lossy(value))
        })
        .collect()
}

/// Reads stream messages until `count` records have arrived.
async fn take(
    stream: &mut Streaming<pb::SubscribeResponse>,
    count: usize,
) -> Vec<pb::ConsumedRecord> {
    let mut records = Vec::new();
    while records.len() < count {
        let message = soon(stream.message()).await.unwrap().expect("stream ended");
        assert!(!message.records.is_empty(), "empty subscription message");
        records.extend(message.records);
    }
    records
}

/// A minimal HTTP GET, enough to read the metrics and health endpoints.
async fn http_get(addr: std::net::SocketAddr, path: &str) -> (String, String) {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    soon(stream.read_to_string(&mut response)).await.unwrap();
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    let status_line = head.lines().next().unwrap().to_owned();
    (status_line, body.to_owned())
}

#[tokio::test]
async fn topics_produce_and_fetch() {
    let dir = TempDir::new("basic");
    let server = start(&dir).await;
    let mut client = log_client(&server).await;

    let created = create(&mut client, "quotes").await;
    assert_eq!((created.topic_id, created.created), (1, true));
    let again = create(&mut client, "quotes").await;
    assert_eq!((again.topic_id, again.created), (1, false));

    let first = produce(&mut client, "quotes", &["a", "b"]).await;
    assert_eq!((first.base_offset, first.count), (0, 2));
    let second = produce(&mut client, "quotes", &["c"]).await;
    assert_eq!((second.base_offset, second.count), (2, 1));

    let fetched = fetch(&mut client, "quotes", 0).await;
    assert_eq!(summary(&fetched.records), ["0=a", "1=b", "2=c"]);
    assert_eq!(fetched.high_watermark, 3);
    // The server stamped the records, since the client sent no timestamp.
    assert!(fetched
        .records
        .iter()
        .all(|r| r.record.as_ref().unwrap().timestamp_ms > 0));

    let request = pb::FetchRequest {
        topic: "quotes".into(),
        offset: 1,
        max_records: 1,
        max_bytes: 0,
    };
    let one = client.fetch(request).await.unwrap().into_inner();
    assert_eq!(summary(&one.records), ["1=b"]);

    let past = fetch(&mut client, "quotes", 3).await;
    assert!(past.records.is_empty());
    assert_eq!(past.high_watermark, 3);

    let topics = client
        .list_topics(pb::ListTopicsRequest {})
        .await
        .unwrap()
        .into_inner()
        .topics;
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].name, "quotes");
    assert_eq!(topics[0].high_watermark, 3);

    soon(server.stop()).await.unwrap();
}

#[tokio::test]
async fn keys_and_client_timestamps_round_trip() {
    let dir = TempDir::new("keys");
    let server = start(&dir).await;
    let mut client = log_client(&server).await;
    create(&mut client, "quotes").await;

    let sent = vec![
        pb::Record {
            key: Some(b"UST10Y".to_vec()),
            value: b"99.5".to_vec(),
            timestamp_ms: 1_700_000_000_000,
        },
        pb::Record {
            key: Some(Vec::new()),
            value: Vec::new(),
            timestamp_ms: 42,
        },
    ];
    let request = pb::ProduceRequest {
        topic: "quotes".into(),
        records: sent.clone(),
    };
    client.produce(request).await.unwrap();

    let fetched = fetch(&mut client, "quotes", 0).await;
    let got: Vec<pb::Record> = fetched
        .records
        .into_iter()
        .map(|r| r.record.unwrap())
        .collect();
    assert_eq!(got, sent);

    soon(server.stop()).await.unwrap();
}

#[tokio::test]
async fn errors_map_to_grpc_codes() {
    let dir = TempDir::new("errors");
    let server = start(&dir).await;
    let mut client = log_client(&server).await;
    create(&mut client, "quotes").await;

    let bad_name = pb::CreateTopicRequest {
        name: "bad name".into(),
    };
    let status = client.create_topic(bad_name).await.unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);

    let missing = pb::ProduceRequest {
        topic: "nope".into(),
        records: vec![record("a")],
    };
    let status = client.produce(missing).await.unwrap_err();
    assert_eq!(status.code(), Code::NotFound);

    let empty = pb::ProduceRequest {
        topic: "quotes".into(),
        records: vec![],
    };
    let status = client.produce(empty).await.unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);

    let request = pb::FetchRequest {
        topic: "nope".into(),
        ..Default::default()
    };
    let status = client.fetch(request).await.unwrap_err();
    assert_eq!(status.code(), Code::NotFound);

    let request = pb::SubscribeRequest {
        topic: "nope".into(),
        start: None,
    };
    let status = client.subscribe(request).await.unwrap_err();
    assert_eq!(status.code(), Code::NotFound);

    // The node is still healthy after all of that.
    let produced = produce(&mut client, "quotes", &["ok"]).await;
    assert_eq!(produced.base_offset, 0);
    assert_eq!(server.metrics().produce_err.get(), 2);
    assert_eq!(server.metrics().produce_ok.get(), 1);

    soon(server.stop()).await.unwrap();
}

#[tokio::test]
async fn subscribe_delivers_the_backlog_then_live_records() {
    let dir = TempDir::new("subscribe");
    let server = start(&dir).await;
    let mut client = log_client(&server).await;
    create(&mut client, "quotes").await;
    produce(&mut client, "quotes", &["a", "b"]).await;

    let request = pb::SubscribeRequest {
        topic: "quotes".into(),
        start: None,
    };
    let mut stream = client.subscribe(request).await.unwrap().into_inner();
    assert_eq!(summary(&take(&mut stream, 2).await), ["0=a", "1=b"]);

    // Nothing more arrives until something is produced.
    let idle = tokio::time::timeout(Duration::from_millis(200), stream.message()).await;
    assert!(idle.is_err(), "a caught-up stream should be quiet");

    let mut producer = log_client(&server).await;
    produce(&mut producer, "quotes", &["c"]).await;
    produce(&mut producer, "quotes", &["d", "e"]).await;
    assert_eq!(summary(&take(&mut stream, 3).await), ["2=c", "3=d", "4=e"]);

    // Records for another topic do not show up on this stream.
    create(&mut producer, "trades").await;
    produce(&mut producer, "trades", &["t"]).await;
    produce(&mut producer, "quotes", &["f"]).await;
    assert_eq!(summary(&take(&mut stream, 1).await), ["5=f"]);

    assert_eq!(server.metrics().subscriptions.get(), 1);
    drop(stream);
    soon(server.stop()).await.unwrap();
}

#[tokio::test]
async fn subscribe_can_start_at_an_offset_or_at_the_end() {
    use pb::subscribe_request::Start;

    let dir = TempDir::new("subscribe-start");
    let server = start(&dir).await;
    let mut client = log_client(&server).await;
    create(&mut client, "quotes").await;
    produce(&mut client, "quotes", &["a", "b", "c"]).await;

    let from_offset = pb::SubscribeRequest {
        topic: "quotes".into(),
        start: Some(Start::Offset(2)),
    };
    let mut stream = client.subscribe(from_offset).await.unwrap().into_inner();
    assert_eq!(summary(&take(&mut stream, 1).await), ["2=c"]);

    let earliest = pb::SubscribeRequest {
        topic: "quotes".into(),
        start: Some(Start::Position(pb::StartPosition::Earliest as i32)),
    };
    let mut stream = client.subscribe(earliest).await.unwrap().into_inner();
    assert_eq!(summary(&take(&mut stream, 3).await), ["0=a", "1=b", "2=c"]);

    let latest = pb::SubscribeRequest {
        topic: "quotes".into(),
        start: Some(Start::Position(pb::StartPosition::Latest as i32)),
    };
    let mut stream = client.subscribe(latest).await.unwrap().into_inner();
    let mut producer = log_client(&server).await;
    produce(&mut producer, "quotes", &["d"]).await;
    assert_eq!(summary(&take(&mut stream, 1).await), ["3=d"]);

    let unknown = pb::SubscribeRequest {
        topic: "quotes".into(),
        start: Some(Start::Position(99)),
    };
    let status = client.subscribe(unknown).await.unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);

    soon(server.stop()).await.unwrap();
}

#[tokio::test]
async fn data_survives_a_restart() {
    let dir = TempDir::new("restart");
    {
        let server = start(&dir).await;
        let mut client = log_client(&server).await;
        create(&mut client, "quotes").await;
        create(&mut client, "trades").await;
        produce(&mut client, "quotes", &["a", "b"]).await;
        produce(&mut client, "trades", &["t"]).await;
        soon(server.stop()).await.unwrap();
    }

    let server = start(&dir).await;
    let mut client = log_client(&server).await;
    assert_eq!(
        summary(&fetch(&mut client, "quotes", 0).await.records),
        ["0=a", "1=b"]
    );
    assert_eq!(
        summary(&fetch(&mut client, "trades", 0).await.records),
        ["0=t"]
    );
    // Offsets continue from where they stopped.
    let produced = produce(&mut client, "quotes", &["c"]).await;
    assert_eq!(produced.base_offset, 2);
    // The gauges were filled in from the recovered log.
    assert_eq!(server.metrics().topics.get(), 2);
    assert_eq!(server.metrics().log_last_index.get(), 5);

    soon(server.stop()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_producers_get_distinct_contiguous_offsets() {
    const PRODUCERS: u64 = 200;

    let dir = TempDir::new("concurrent");
    let server = start(&dir).await;
    let mut client = log_client(&server).await;
    create(&mut client, "quotes").await;

    let mut tasks = Vec::new();
    for i in 0..PRODUCERS {
        // Clones share one HTTP/2 connection and send in parallel.
        let mut client = client.clone();
        tasks.push(tokio::spawn(async move {
            let value = format!("v{i}");
            let produced = produce(&mut client, "quotes", &[value.as_str()]).await;
            (produced.base_offset, value)
        }));
    }
    let mut assigned = Vec::new();
    for task in tasks {
        assigned.push(soon(task).await.unwrap());
    }

    // Every producer got its own offset, and together they cover 0..N.
    let offsets: BTreeSet<u64> = assigned.iter().map(|(offset, _)| *offset).collect();
    assert_eq!(offsets, (0..PRODUCERS).collect::<BTreeSet<_>>());

    // Each offset holds the value of the producer that was told it.
    let request = pb::FetchRequest {
        topic: "quotes".into(),
        offset: 0,
        max_records: PRODUCERS as u32,
        max_bytes: 0,
    };
    let fetched = client.fetch(request).await.unwrap().into_inner();
    assert_eq!(fetched.records.len() as u64, PRODUCERS);
    let stored: BTreeSet<(u64, String)> = fetched
        .records
        .iter()
        .map(|r| {
            let value = String::from_utf8(r.record.as_ref().unwrap().value.clone()).unwrap();
            (r.offset, value)
        })
        .collect();
    assert_eq!(stored, assigned.into_iter().collect::<BTreeSet<_>>());

    let metrics = server.metrics();
    assert_eq!(metrics.produce_ok.get(), PRODUCERS);
    assert_eq!(metrics.records_produced.get(), PRODUCERS);
    // One commit for the topic, and at most one per produce.
    assert!(metrics.commits.get() <= PRODUCERS + 1);

    soon(server.stop()).await.unwrap();
}

#[tokio::test]
async fn admin_status_and_http_endpoints() {
    let dir = TempDir::new("admin");
    let server = start(&dir).await;
    let mut client = log_client(&server).await;
    create(&mut client, "quotes").await;
    produce(&mut client, "quotes", &["a", "b"]).await;

    let mut admin = AdminClient::connect(format!("http://{}", server.grpc_addr()))
        .await
        .unwrap();
    let status = admin
        .status(pb::StatusRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(status.node_id, 1);
    assert_eq!(status.role, pb::Role::Leader as i32);
    assert_eq!(status.leader_id, Some(1));
    assert_eq!(status.term, 1);
    assert_eq!(status.first_log_index, 1);
    assert_eq!(status.last_log, Some(pb::LogId { term: 1, index: 2 }));
    assert_eq!(status.commit_index, 2);
    assert_eq!(status.applied_index, 2);
    assert_eq!(status.segment_count, 1);
    assert!(status.log_bytes > 16);
    assert_eq!(status.voters.len(), 1);
    assert_eq!(status.voters[0].addr, server.grpc_addr().to_string());

    let http = server.metrics_addr().unwrap();
    let (line, body) = http_get(http, "/healthz").await;
    assert!(line.contains("200"), "{line}");
    assert_eq!(body, "ok\n");

    let (line, body) = http_get(http, "/metrics").await;
    assert!(line.contains("200"), "{line}");
    assert!(body.contains("ferrumlog_produce_requests_total{result=\"ok\"} 1\n"));
    assert!(body.contains("ferrumlog_records_produced_total 2\n"));
    assert!(body.contains("ferrumlog_topics 1\n"));
    assert!(body.contains("ferrumlog_log_last_index 2\n"));
    assert!(body.contains("ferrumlog_commit_duration_seconds_count 2\n"));

    let (line, _) = http_get(http, "/nope").await;
    assert!(line.contains("404"), "{line}");

    soon(server.stop()).await.unwrap();
}

#[tokio::test]
async fn metrics_server_can_be_disabled() {
    let dir = TempDir::new("no-metrics");
    let mut config = config(dir.path());
    config.no_metrics = true;
    config.advertise = Some("node1.example:7001".into());
    let server = Server::start(config).await.unwrap();
    assert_eq!(server.metrics_addr(), None);

    let mut admin = AdminClient::connect(format!("http://{}", server.grpc_addr()))
        .await
        .unwrap();
    let status = admin
        .status(pb::StatusRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(status.voters[0].addr, "node1.example:7001");
    // An empty log reports the zero id and still names a term.
    assert_eq!(status.last_log, Some(pb::LogId { term: 0, index: 0 }));
    assert_eq!(status.term, 1);

    soon(server.stop()).await.unwrap();
}

#[tokio::test]
async fn shutdown_ends_open_subscriptions_and_refuses_new_work() {
    let dir = TempDir::new("shutdown");
    let server = start(&dir).await;
    let addr = server.grpc_addr();
    let mut client = log_client(&server).await;
    create(&mut client, "quotes").await;
    produce(&mut client, "quotes", &["a"]).await;

    let request = pb::SubscribeRequest {
        topic: "quotes".into(),
        start: None,
    };
    let mut stream = client.subscribe(request).await.unwrap().into_inner();
    assert_eq!(summary(&take(&mut stream, 1).await), ["0=a"]);

    // An idle subscription must not hold the shutdown up.
    soon(server.stop()).await.unwrap();

    // The stream ends, cleanly or with an error, but it does end.
    match soon(stream.message()).await {
        Ok(None) | Err(_) => {}
        Ok(Some(message)) => panic!("unexpected message after shutdown: {message:?}"),
    }
    assert!(LogClient::connect(format!("http://{addr}")).await.is_err());
}
