//! Runs `fl` commands against a real node and checks what they print.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use clap::Parser;
use fl_cli::{run, Cli};
use fl_client::{Client, ClientConfig};
use fl_server::{Config, Server};

/// A scratch directory that is deleted when dropped.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("ferrumlog-cli-{tag}-{}-{n}", std::process::id()));
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

async fn start_server(dir: &TempDir) -> Server {
    let config = Config {
        node_id: 1,
        data_dir: dir.path().to_path_buf(),
        listen: "127.0.0.1:0".parse().unwrap(),
        advertise: None,
        metrics_listen: "127.0.0.1:0".parse().unwrap(),
        no_metrics: true,
        segment_bytes: 64 << 20,
        max_commit_batch: 256,
        unsafe_no_fsync: false,
    };
    Server::start(config).await.unwrap()
}

/// Fails the test instead of hanging if something never completes.
async fn soon<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(20), future)
        .await
        .expect("timed out")
}

/// Runs `fl --endpoints <endpoint> <args>` with `stdin` as its input.
/// Returns what it printed, and the error message if it failed.
async fn fl(endpoint: &str, args: &[&str], stdin: &str) -> (String, Option<String>) {
    let argv = ["fl", "--endpoints", endpoint]
        .into_iter()
        .chain(args.iter().copied());
    let cli = Cli::try_parse_from(argv).unwrap();

    let mut config = ClientConfig::new(cli.endpoints);
    config.max_retries = 2;
    config.backoff_base = Duration::from_millis(10);
    let client = Client::new(config).unwrap();

    let mut lines = stdin.lines().map(|line| Ok(line.to_owned()));
    let mut out = Vec::new();
    let result = soon(run(cli.command, &client, &mut lines, &mut out)).await;
    let printed = String::from_utf8(out).unwrap();
    (printed, result.err().map(|e| e.to_string()))
}

/// Runs a command that is expected to succeed and returns its output.
async fn ok(endpoint: &str, args: &[&str]) -> String {
    let (printed, error) = fl(endpoint, args, "").await;
    assert_eq!(error, None, "{args:?} failed");
    printed
}

#[tokio::test]
async fn a_full_session() {
    let dir = TempDir::new("session");
    let server = start_server(&dir).await;
    let node = server.grpc_addr().to_string();

    assert_eq!(ok(&node, &["topics"]).await, "no topics\n");
    assert_eq!(
        ok(&node, &["create-topic", "quotes"]).await,
        "created topic quotes (id 1)\n"
    );
    assert_eq!(
        ok(&node, &["create-topic", "quotes"]).await,
        "topic quotes already exists (id 1)\n"
    );

    assert_eq!(
        ok(&node, &["produce", "quotes", "first", "second value"]).await,
        "wrote 2 records to quotes, offsets 0 to 1\n"
    );
    assert_eq!(
        ok(&node, &["produce", "quotes", "third", "--key", "UST10Y"]).await,
        "wrote 1 record to quotes, offsets 2 to 2\n"
    );

    assert_eq!(
        ok(&node, &["consume", "quotes"]).await,
        "0\t-\tfirst\n1\t-\tsecond value\n2\tUST10Y\tthird\n"
    );
    assert_eq!(
        ok(&node, &["consume", "quotes", "--from", "1", "-n", "1"]).await,
        "1\t-\tsecond value\n"
    );
    assert_eq!(
        ok(&node, &["consume", "quotes", "--from", "2"]).await,
        "2\tUST10Y\tthird\n"
    );
    assert_eq!(
        ok(&node, &["consume", "quotes", "--from", "latest"]).await,
        ""
    );
    assert_eq!(ok(&node, &["consume", "quotes", "--from", "99"]).await, "");
    assert_eq!(ok(&node, &["consume", "quotes", "-n", "0"]).await, "");

    ok(&node, &["create-topic", "ust.trades"]).await;
    assert_eq!(
        ok(&node, &["topics"]).await,
        "ID  NAME        RECORDS\n\
         1   quotes      3\n\
         2   ust.trades  0\n"
    );

    soon(server.stop()).await.unwrap();
}

#[tokio::test]
async fn produce_reads_lines_from_standard_input() {
    let dir = TempDir::new("stdin");
    let server = start_server(&dir).await;
    let node = server.grpc_addr().to_string();
    ok(&node, &["create-topic", "quotes"]).await;

    // Seven records with blank lines mixed in, sent three per request.
    let input = "a\nb\n\nc\nd\ne\n\n\nf\ng\n";
    let (printed, error) = fl(&node, &["produce", "quotes", "--batch", "3"], input).await;
    assert_eq!(error, None);
    assert_eq!(printed, "wrote 7 records to quotes, offsets 0 to 6\n");
    // Three requests: 3 + 3 + 1. The first commit created the topic.
    assert_eq!(server.metrics().produce_ok.get(), 3);

    let consumed = ok(&node, &["consume", "quotes"]).await;
    let values: Vec<&str> = consumed
        .lines()
        .map(|line| line.rsplit('\t').next().unwrap())
        .collect();
    assert_eq!(values, ["a", "b", "c", "d", "e", "f", "g"]);

    // Arguments win over standard input when both are present.
    let (printed, _) = fl(&node, &["produce", "quotes", "h"], "ignored\n").await;
    assert_eq!(printed, "wrote 1 record to quotes, offsets 7 to 7\n");

    soon(server.stop()).await.unwrap();
}

#[tokio::test]
async fn follow_prints_new_records_until_the_limit() {
    let dir = TempDir::new("follow");
    let server = start_server(&dir).await;
    let node = server.grpc_addr().to_string();
    ok(&node, &["create-topic", "quotes"]).await;
    ok(&node, &["produce", "quotes", "old-1", "old-2"]).await;

    // From the start: the backlog satisfies the limit.
    assert_eq!(
        ok(&node, &["consume", "quotes", "--follow", "-n", "2"]).await,
        "0\t-\told-1\n1\t-\told-2\n"
    );
    // The limit cuts a batch short.
    assert_eq!(
        ok(&node, &["consume", "quotes", "-f", "-n", "1"]).await,
        "0\t-\told-1\n"
    );

    // From the end: nothing until something new is written. The follower
    // and the writer run side by side, and the writer starts late so the
    // follower is already waiting.
    let follow = ok(
        &node,
        &["consume", "quotes", "-f", "--from", "latest", "-n", "2"],
    );
    let write = async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        ok(&node, &["produce", "quotes", "new-1"]).await;
        ok(&node, &["produce", "quotes", "new-2"]).await;
    };
    let (printed, ()) = tokio::join!(follow, write);
    assert_eq!(printed, "2\t-\tnew-1\n3\t-\tnew-2\n");

    soon(server.stop()).await.unwrap();
}

#[tokio::test]
async fn status_shows_every_node() {
    let dir = TempDir::new("status");
    let server = start_server(&dir).await;
    let node = server.grpc_addr().to_string();
    ok(&node, &["create-topic", "quotes"]).await;
    ok(&node, &["produce", "quotes", "a"]).await;

    let printed = ok(&node, &["status"]).await;
    let lines: Vec<&str> = printed.lines().collect();
    assert_eq!(lines.len(), 2, "{printed}");
    let header: Vec<&str> = lines[0].split_whitespace().collect();
    assert_eq!(
        header,
        [
            "ENDPOINT", "NODE", "ROLE", "TERM", "LEADER", "LAST", "COMMIT", "APPLIED", "SEGMENTS",
            "SIZE"
        ]
    );
    let row: Vec<&str> = lines[1].split_whitespace().collect();
    let endpoint = format!("http://{node}");
    assert_eq!(
        row[..9],
        [
            endpoint.as_str(),
            "1",
            "leader",
            "1",
            "1",
            "2",
            "2",
            "2",
            "1"
        ]
    );
    assert_eq!(row[10], "B");

    // One live node and one dead one: the live one is shown, the dead one
    // is reported, and the command still succeeds.
    let dead = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap().to_string()
    };
    let both = format!("{node},{dead}");
    let (printed, error) = fl(&both, &["status"], "").await;
    assert_eq!(error, None);
    assert!(printed.contains("leader"), "{printed}");
    assert!(
        printed.contains(&format!("http://{dead}: unreachable")),
        "{printed}"
    );

    // No live node at all is a failure.
    let (printed, error) = fl(&dead, &["status"], "").await;
    assert_eq!(error.as_deref(), Some("no node answered"));
    assert!(printed.contains("unreachable"), "{printed}");

    soon(server.stop()).await.unwrap();
}

#[tokio::test]
async fn failures_are_reported_as_errors() {
    let dir = TempDir::new("failures");
    let server = start_server(&dir).await;
    let node = server.grpc_addr().to_string();
    ok(&node, &["create-topic", "quotes"]).await;

    let (printed, error) = fl(&node, &["produce", "nope", "a"], "").await;
    assert_eq!(printed, "");
    assert_eq!(error.as_deref(), Some("topic \"nope\" does not exist"));

    let (_, error) = fl(&node, &["consume", "nope"], "").await;
    assert_eq!(error.as_deref(), Some("topic \"nope\" does not exist"));

    let (_, error) = fl(&node, &["consume", "nope", "--follow"], "").await;
    assert_eq!(error.as_deref(), Some("topic \"nope\" does not exist"));

    let (_, error) = fl(&node, &["create-topic", "bad name"], "").await;
    assert!(error.unwrap().contains("invalid topic name"));

    // No arguments and nothing on standard input.
    let (_, error) = fl(&node, &["produce", "quotes"], "\n\n").await;
    assert_eq!(
        error.as_deref(),
        Some("nothing to write: no records were given")
    );

    soon(server.stop()).await.unwrap();

    // With the node gone, a read fails after its retries.
    let (_, error) = fl(&node, &["topics"], "").await;
    assert!(
        error.unwrap().contains("no node could complete the call"),
        "unexpected message"
    );
}
