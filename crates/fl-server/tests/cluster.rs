//! Three real nodes on loopback ports, replicating through Raft.

use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fl_proto::redirect;
use fl_proto::{pb, AdminClient, LogClient};
use fl_server::{Config, Server};
use tonic::transport::Channel;
use tonic::Code;

/// A scratch directory that is deleted when dropped.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "ferrumlog-cluster-{tag}-{}-{n}",
            std::process::id()
        ));
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

/// Fails the test instead of hanging if something never completes.
async fn soon<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .expect("timed out")
}

/// Reserves a loopback port by binding it and letting it go again.
fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

fn record(value: &str) -> pb::Record {
    pb::Record {
        key: None,
        value: value.as_bytes().to_vec(),
        timestamp_ms: 0,
    }
}

/// A cluster whose nodes can be stopped and started one at a time. Nodes
/// are numbered from zero here and have ids from one.
struct Cluster {
    dirs: Vec<TempDir>,
    addrs: Vec<SocketAddr>,
    servers: Vec<Option<Server>>,
}

impl Cluster {
    async fn start(tag: &str, size: usize) -> Cluster {
        let mut cluster = Cluster {
            dirs: (0..size).map(|_| TempDir::new(tag)).collect(),
            addrs: (0..size).map(|_| free_addr()).collect(),
            servers: (0..size).map(|_| None).collect(),
        };
        for node in 0..size {
            cluster.start_node(node).await;
        }
        cluster
    }

    fn config(&self, node: usize) -> Config {
        let peers = self
            .addrs
            .iter()
            .enumerate()
            .map(|(index, addr)| format!("{}={addr}", index + 1))
            .collect();
        Config {
            node_id: node as u64 + 1,
            data_dir: self.dirs[node].path().to_path_buf(),
            listen: self.addrs[node],
            advertise: None,
            metrics_listen: "127.0.0.1:0".parse().unwrap(),
            no_metrics: true,
            segment_bytes: 64 << 20,
            max_commit_batch: 256,
            unsafe_no_fsync: false,
            peers,
            // Short ticks, so elections take a fraction of a second.
            tick_ms: 20,
        }
    }

    async fn start_node(&mut self, node: usize) {
        assert!(self.servers[node].is_none(), "node {node} is running");
        self.servers[node] = Some(Server::start(self.config(node)).await.unwrap());
    }

    async fn stop_node(&mut self, node: usize) {
        let server = self.servers[node].take().expect("node is not running");
        soon(server.stop()).await.unwrap();
    }

    async fn stop_all(&mut self) {
        for node in 0..self.servers.len() {
            if self.servers[node].is_some() {
                self.stop_node(node).await;
            }
        }
    }

    fn running(&self) -> Vec<usize> {
        (0..self.servers.len())
            .filter(|node| self.servers[*node].is_some())
            .collect()
    }

    async fn log(&self, node: usize) -> LogClient<Channel> {
        LogClient::connect(format!("http://{}", self.addrs[node]))
            .await
            .unwrap()
    }

    async fn status(&self, node: usize) -> pb::StatusResponse {
        let mut admin = AdminClient::connect(format!("http://{}", self.addrs[node]))
            .await
            .unwrap();
        admin
            .status(pb::StatusRequest {})
            .await
            .unwrap()
            .into_inner()
    }

    /// Waits until the running nodes agree on one leader, and returns it.
    async fn leader(&self) -> usize {
        soon(async {
            loop {
                let mut statuses = Vec::new();
                for node in self.running() {
                    statuses.push((node, self.status(node).await));
                }
                let leaders: Vec<usize> = statuses
                    .iter()
                    .filter(|(_, status)| status.role == pb::Role::Leader as i32)
                    .map(|(node, _)| *node)
                    .collect();
                if let [leader] = leaders[..] {
                    let id = leader as u64 + 1;
                    if statuses.iter().all(|(_, s)| s.leader_id == Some(id)) {
                        return leader;
                    }
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
    }

    /// Waits until `node` has applied everything the leader has committed.
    async fn caught_up(&self, node: usize) {
        soon(async {
            loop {
                let leader = self.leader().await;
                let target = self.status(leader).await.commit_index;
                if self.status(node).await.applied_index >= target {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await;
    }

    /// Creates a topic through whichever node currently leads.
    async fn create(&self, name: &str) {
        soon(async {
            loop {
                let leader = self.leader().await;
                let request = pb::CreateTopicRequest { name: name.into() };
                if self.log(leader).await.create_topic(request).await.is_ok() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await;
    }

    /// Produces through whichever node currently leads. A fresh leader
    /// turns writes away for a moment, so this tries until one is taken.
    async fn produce(&self, topic: &str, values: &[&str]) -> pb::ProduceResponse {
        soon(async {
            loop {
                let leader = self.leader().await;
                let request = pb::ProduceRequest {
                    topic: topic.into(),
                    records: values.iter().map(|value| record(value)).collect(),
                };
                match self.log(leader).await.produce(request).await {
                    Ok(response) => return response.into_inner(),
                    Err(status) if redirect::is_not_leader(&status) => {}
                    Err(status) => panic!("produce failed: {status}"),
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
    }

    /// Every value in a topic, read from `node`.
    async fn values(&self, node: usize, topic: &str) -> Vec<String> {
        let request = pb::FetchRequest {
            topic: topic.into(),
            offset: 0,
            max_records: 0,
            max_bytes: 0,
        };
        let fetched = self.log(node).await.fetch(request).await.unwrap();
        fetched
            .into_inner()
            .records
            .into_iter()
            .map(|consumed| String::from_utf8(consumed.record.unwrap().value).unwrap())
            .collect()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_elect_a_leader_and_replicate() {
    let mut cluster = Cluster::start("replicate", 3).await;
    let leader = cluster.leader().await;

    // One leader, two followers, and everyone lists the same three voters.
    let mut roles = Vec::new();
    for node in 0..3 {
        let status = cluster.status(node).await;
        assert_eq!(status.voters.len(), 3);
        assert_eq!(status.leader_id, Some(leader as u64 + 1));
        roles.push(status.role);
    }
    roles.sort_unstable();
    let follower = pb::Role::Follower as i32;
    assert_eq!(roles, [follower, follower, pb::Role::Leader as i32]);

    cluster.create("quotes").await;
    let produced = cluster.produce("quotes", &["a", "b"]).await;
    assert_eq!((produced.base_offset, produced.count), (0, 2));
    let produced = cluster.produce("quotes", &["c"]).await;
    assert_eq!(produced.base_offset, 2);

    // Every node serves the records once it has caught up, the followers
    // included.
    for node in 0..3 {
        cluster.caught_up(node).await;
        assert_eq!(cluster.values(node, "quotes").await, ["a", "b", "c"]);
    }
    // All three logs end at the same entry.
    let last = cluster.status(leader).await.last_log;
    for node in 0..3 {
        assert_eq!(cluster.status(node).await.last_log, last);
    }
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_redirects_writes_to_the_leader() {
    let mut cluster = Cluster::start("redirect", 3).await;
    let leader = cluster.leader().await;
    cluster.create("quotes").await;
    let follower = (leader + 1) % 3;

    let request = pb::ProduceRequest {
        topic: "quotes".into(),
        records: vec![record("x")],
    };
    let status = cluster
        .log(follower)
        .await
        .produce(request)
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(redirect::is_not_leader(&status));
    let hint = redirect::leader_hint(&status).expect("the follower knows the leader");
    assert_eq!(hint.node_id.get(), leader as u64 + 1);
    assert_eq!(hint.addr, Some(cluster.addrs[leader].to_string()));

    // The refused write left no trace anywhere.
    for node in 0..3 {
        cluster.caught_up(node).await;
        assert!(cluster.values(node, "quotes").await.is_empty());
    }
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_cluster_survives_losing_its_leader() {
    let mut cluster = Cluster::start("failover", 3).await;
    cluster.create("quotes").await;
    cluster.produce("quotes", &["before"]).await;
    let old_leader = cluster.leader().await;
    let old_term = cluster.status(old_leader).await.term;

    cluster.stop_node(old_leader).await;

    // The two that are left elect a new leader in a later term, and
    // nothing that was acknowledged is missing.
    let new_leader = cluster.leader().await;
    assert_ne!(new_leader, old_leader);
    assert!(cluster.status(new_leader).await.term > old_term);
    cluster.produce("quotes", &["during"]).await;
    assert_eq!(
        cluster.values(new_leader, "quotes").await,
        ["before", "during"]
    );

    // The old leader comes back as a follower and catches up on what it
    // missed.
    cluster.start_node(old_leader).await;
    cluster.caught_up(old_leader).await;
    assert_eq!(
        cluster.values(old_leader, "quotes").await,
        ["before", "during"]
    );
    cluster.produce("quotes", &["after"]).await;
    for node in 0..3 {
        cluster.caught_up(node).await;
        assert_eq!(
            cluster.values(node, "quotes").await,
            ["before", "during", "after"]
        );
    }
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_without_a_majority_cannot_commit() {
    let mut cluster = Cluster::start("minority", 3).await;
    cluster.create("quotes").await;
    cluster.produce("quotes", &["safe"]).await;
    let leader = cluster.leader().await;
    for node in 0..3 {
        if node != leader {
            cluster.stop_node(node).await;
        }
    }

    // Alone, the leader can take the write into its log but never commit
    // it. It notices it has lost its majority, steps down, and tells the
    // client the outcome is unknown. The call does not hang.
    let request = pb::ProduceRequest {
        topic: "quotes".into(),
        records: vec![record("stranded")],
    };
    let status = soon(cluster.log(leader).await.produce(request))
        .await
        .unwrap_err();
    assert!(
        status.code() == Code::Unavailable || redirect::is_not_leader(&status),
        "{status}"
    );
    soon(async {
        while cluster.status(leader).await.role == pb::Role::Leader as i32 {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    // It still serves what was committed, and nothing more.
    assert_eq!(cluster.values(leader, "quotes").await, ["safe"]);

    // With a second node back there is a majority again and writes go
    // through.
    cluster.start_node((leader + 1) % 3).await;
    cluster.produce("quotes", &["again"]).await;
    let values = cluster.values(cluster.leader().await, "quotes").await;
    assert_eq!(values.first().map(String::as_str), Some("safe"));
    assert_eq!(values.last().map(String::as_str), Some("again"));
    // The stranded write may or may not have made it. That is what
    // "outcome unknown" means. It is never there twice.
    assert!(values.iter().filter(|value| *value == "stranded").count() <= 1);
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_whole_cluster_restart_keeps_the_data() {
    let mut cluster = Cluster::start("restart", 3).await;
    cluster.create("quotes").await;
    cluster.create("trades").await;
    for value in ["a", "b", "c", "d"] {
        cluster.produce("quotes", &[value]).await;
    }
    cluster.produce("trades", &["t1", "t2"]).await;
    let term = cluster.status(cluster.leader().await).await.term;

    cluster.stop_all().await;
    for node in 0..3 {
        cluster.start_node(node).await;
    }

    // A new term, and every node rebuilds the same topics from its log.
    let leader = cluster.leader().await;
    assert!(cluster.status(leader).await.term > term);
    cluster.produce("quotes", &["e"]).await;
    for node in 0..3 {
        cluster.caught_up(node).await;
        assert_eq!(
            cluster.values(node, "quotes").await,
            ["a", "b", "c", "d", "e"]
        );
        assert_eq!(cluster.values(node, "trades").await, ["t1", "t2"]);
    }
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_all_land_exactly_once() {
    const WRITERS: usize = 8;
    const EACH: usize = 25;

    let mut cluster = Cluster::start("concurrent", 3).await;
    cluster.create("quotes").await;
    let leader = cluster.leader().await;
    // Make sure the leader is taking writes before the race starts.
    cluster.produce("quotes", &["first"]).await;

    let mut tasks = Vec::new();
    for writer in 0..WRITERS {
        let mut client = cluster.log(leader).await;
        tasks.push(tokio::spawn(async move {
            for sequence in 0..EACH {
                let request = pb::ProduceRequest {
                    topic: "quotes".into(),
                    records: vec![record(&format!("w{writer}-{sequence}"))],
                };
                client.produce(request).await.unwrap();
            }
        }));
    }
    for task in tasks {
        soon(task).await.unwrap();
    }

    let mut expected: Vec<String> = (0..WRITERS)
        .flat_map(|writer| (0..EACH).map(move |sequence| format!("w{writer}-{sequence}")))
        .collect();
    expected.push("first".into());
    expected.sort();
    for node in 0..3 {
        cluster.caught_up(node).await;
        let mut values = cluster.values(node, "quotes").await;
        // Each writer's records are in the order it sent them.
        for writer in 0..WRITERS {
            let prefix = format!("w{writer}-");
            let own: Vec<usize> = values
                .iter()
                .filter_map(|value| value.strip_prefix(&prefix))
                .map(|sequence| sequence.parse().unwrap())
                .collect();
            assert_eq!(own, (0..EACH).collect::<Vec<_>>(), "writer {writer}");
        }
        values.sort();
        assert_eq!(values, expected, "node {node}");
    }
    cluster.stop_all().await;
}

#[tokio::test]
async fn a_node_missing_from_its_own_peer_list_refuses_to_start() {
    let dir = TempDir::new("outsider");
    let config = Config {
        node_id: 4,
        data_dir: dir.path().to_path_buf(),
        listen: "127.0.0.1:0".parse().unwrap(),
        advertise: None,
        metrics_listen: "127.0.0.1:0".parse().unwrap(),
        no_metrics: true,
        segment_bytes: 64 << 20,
        max_commit_batch: 256,
        unsafe_no_fsync: false,
        peers: vec!["1=127.0.0.1:1".into(), "2=127.0.0.1:2".into()],
        tick_ms: 20,
    };
    let error = Server::start(config.clone()).await.err().unwrap();
    assert!(error.to_string().contains("not in --peers"), "{error}");

    let mut malformed = config;
    malformed.peers = vec!["nonsense".into()];
    let error = Server::start(malformed).await.err().unwrap();
    assert!(error.to_string().contains("id=host:port"), "{error}");
}
