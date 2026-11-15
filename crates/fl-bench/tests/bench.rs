//! The benchmarks against a real node.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fl_bench::{consume, produce, ConsumeOptions, Limit, ProduceOptions};
use fl_client::{Client, ClientConfig, Error};
use fl_server::{Config, Server};

struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("ferrumlog-bench-{tag}-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        TempDir(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn start(dir: &TempDir) -> Server {
    let config = Config {
        node_id: 1,
        data_dir: dir.0.clone(),
        listen: "127.0.0.1:0".parse().unwrap(),
        advertise: None,
        metrics_listen: "127.0.0.1:0".parse().unwrap(),
        no_metrics: true,
        segment_bytes: 64 << 20,
        max_commit_batch: 256,
        // The tests check counts, not durability, and run much faster
        // without a real fsync per batch.
        unsafe_no_fsync: true,
        peers: Vec::new(),
        tick_ms: 20,
    };
    Server::start(config).await.unwrap()
}

fn client_for(server: &Server) -> ClientConfig {
    ClientConfig::new([server.grpc_addr().to_string()])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_limit_writes_exactly_that_many() {
    let dir = TempDir::new("requests");
    let server = start(&dir).await;

    let mut options = ProduceOptions::new(client_for(&server), "bench");
    options.workers = 4;
    options.batch = 5;
    options.record_bytes = 64;
    options.limit = Limit::Requests(200);
    let written = produce(options).await.unwrap();

    assert_eq!(written.requests, 200);
    assert_eq!(written.errors, 0);
    assert_eq!(written.records, 1_000);
    assert_eq!(written.bytes, 64_000);
    assert_eq!(written.latency.count(), 200);
    assert!(written.latency.max() > 0);
    assert!(written.records_per_second() > 0.0);

    // The topic holds what the benchmark says it wrote.
    let topics = Client::new(client_for(&server))
        .unwrap()
        .list_topics()
        .await
        .unwrap();
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].high_watermark.get(), 1_000);

    // Reading it back finds every record, once per reader.
    let mut options = ConsumeOptions::new(client_for(&server), "bench");
    options.workers = 2;
    options.max_records = 300;
    let read = consume(options).await.unwrap();
    assert_eq!(read.records, 2_000);
    assert_eq!(read.bytes, 128_000);
    assert_eq!(read.errors, 0);
    // A thousand records at three hundred per fetch is four fetches each.
    assert_eq!(read.requests, 8);

    server.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_time_limit_stops_the_run() {
    let dir = TempDir::new("time");
    let server = start(&dir).await;

    let mut options = ProduceOptions::new(client_for(&server), "bench");
    options.workers = 2;
    options.limit = Limit::Time(Duration::from_millis(400));
    let written = produce(options).await.unwrap();

    assert!(written.requests > 0);
    assert_eq!(written.records, written.requests);
    assert!(written.elapsed >= Duration::from_millis(400));
    assert!(
        written.elapsed < Duration::from_secs(5),
        "{:?}",
        written.elapsed
    );
    server.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_target_rate_paces_the_requests() {
    let dir = TempDir::new("rate");
    let server = start(&dir).await;

    // 100 requests at 200 per second should take about half a second. A
    // closed loop against a local node would finish far sooner.
    let mut options = ProduceOptions::new(client_for(&server), "bench");
    options.workers = 2;
    options.limit = Limit::Requests(100);
    options.rate = Some(200.0);
    let written = produce(options).await.unwrap();

    assert_eq!(written.requests, 100);
    assert!(
        written.elapsed >= Duration::from_millis(400),
        "{:?}",
        written.elapsed
    );
    assert!(
        written.elapsed < Duration::from_secs(5),
        "{:?}",
        written.elapsed
    );
    let rate = written.requests_per_second();
    assert!((100.0..=260.0).contains(&rate), "{rate}");
    server.stop().await.unwrap();
}

#[tokio::test]
async fn an_unreachable_cluster_or_a_missing_topic_is_an_error() {
    // Nothing listens here.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let dead = listener.local_addr().unwrap().to_string();
    drop(listener);
    let mut unreachable = ClientConfig::new([dead]);
    unreachable.max_retries = 1;
    unreachable.backoff_base = Duration::from_millis(10);
    let result = produce(ProduceOptions::new(unreachable, "bench")).await;
    assert!(matches!(result, Err(Error::Unavailable { .. })));

    let dir = TempDir::new("missing");
    let server = start(&dir).await;
    let result = consume(ConsumeOptions::new(client_for(&server), "nope")).await;
    assert!(matches!(result, Err(Error::Rejected(_))));
    server.stop().await.unwrap();
}
