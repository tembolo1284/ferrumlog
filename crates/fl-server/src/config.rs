//! Node configuration, from command-line flags or environment variables.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;

/// Settings for one ferrumlog node.
///
/// Every flag can also be set through the environment variable shown next
/// to it in `--help`, which is the convenient form inside a container.
#[derive(Parser, Clone, Debug)]
#[command(
    name = "fl-server",
    version,
    about = "Runs one ferrumlog node",
    long_about = "Runs one ferrumlog node.\n\nEvery flag can also be set through the environment variable shown beside it."
)]
pub struct Config {
    /// This node's id. Must be unique within a cluster.
    #[arg(long, env = "FERRUMLOG_NODE_ID", default_value_t = 1)]
    pub node_id: u64,

    /// Directory for the log segments and node state. Created if missing.
    #[arg(long, env = "FERRUMLOG_DATA_DIR", default_value = "data")]
    pub data_dir: PathBuf,

    /// Address the gRPC server listens on. Use 0.0.0.0 to accept
    /// connections from other machines.
    #[arg(long, env = "FERRUMLOG_LISTEN", default_value = "127.0.0.1:7001")]
    pub listen: SocketAddr,

    /// Address clients should use to reach this node, as reported by the
    /// Admin service. Defaults to the listen address.
    #[arg(long, env = "FERRUMLOG_ADVERTISE")]
    pub advertise: Option<String>,

    /// Every node in the cluster as id=host:port, separated by commas,
    /// this node included. Leave it out to run a single node.
    ///
    /// The address is each node's gRPC address. Peers use it for Raft and
    /// clients are redirected to it, so both must be able to reach it.
    #[arg(long, env = "FERRUMLOG_PEERS", value_delimiter = ',')]
    pub peers: Vec<String>,

    /// Length of one Raft tick in milliseconds. A leader sends a heartbeat
    /// every 2 ticks. A follower that hears nothing for 10 to 20 ticks
    /// starts an election. With the default, a dead leader is replaced in
    /// about a second.
    #[arg(long, env = "FERRUMLOG_TICK_MS", default_value_t = 50)]
    pub tick_ms: u64,

    /// Address the HTTP server for /metrics and /healthz listens on.
    #[arg(
        long,
        env = "FERRUMLOG_METRICS_LISTEN",
        default_value = "127.0.0.1:9101"
    )]
    pub metrics_listen: SocketAddr,

    /// Do not start the HTTP server for /metrics and /healthz.
    #[arg(long, env = "FERRUMLOG_NO_METRICS")]
    pub no_metrics: bool,

    /// Size in bytes at which a log segment is sealed and a new one started.
    #[arg(long, env = "FERRUMLOG_SEGMENT_BYTES", default_value_t = 64 << 20)]
    pub segment_bytes: u64,

    /// Most write requests committed with a single fsync.
    #[arg(long, env = "FERRUMLOG_MAX_COMMIT_BATCH", default_value_t = 256)]
    pub max_commit_batch: usize,

    /// Acknowledge writes without waiting for fsync. A crash or power loss
    /// can then lose acknowledged records. For benchmarking only.
    #[arg(long, env = "FERRUMLOG_UNSAFE_NO_FSYNC")]
    pub unsafe_no_fsync: bool,
}

impl Config {
    /// The address reported to clients for this node.
    pub fn advertised_addr(&self, bound: SocketAddr) -> String {
        match &self.advertise {
            Some(addr) => addr.clone(),
            None => bound.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_local_and_durable() {
        let config = Config::parse_from(["fl-server"]);
        assert_eq!(config.node_id, 1);
        assert_eq!(config.data_dir, PathBuf::from("data"));
        assert!(config.listen.ip().is_loopback());
        assert!(config.metrics_listen.ip().is_loopback());
        assert!(!config.no_metrics);
        assert_eq!(config.segment_bytes, 64 << 20);
        assert_eq!(config.max_commit_batch, 256);
        assert!(!config.unsafe_no_fsync);
        assert!(config.peers.is_empty());
        assert_eq!(config.tick_ms, 50);
    }

    #[test]
    fn peers_are_split_on_commas() {
        let config = Config::parse_from([
            "fl-server",
            "--node-id",
            "2",
            "--peers",
            "1=a:7001,2=b:7001,3=c:7001",
            "--tick-ms",
            "20",
        ]);
        assert_eq!(config.peers, ["1=a:7001", "2=b:7001", "3=c:7001"]);
        assert_eq!(config.tick_ms, 20);
    }

    #[test]
    fn flags_override_defaults() {
        let config = Config::parse_from([
            "fl-server",
            "--node-id",
            "3",
            "--data-dir",
            "/var/lib/ferrumlog",
            "--listen",
            "0.0.0.0:7001",
            "--advertise",
            "node3.internal:7001",
            "--no-metrics",
            "--segment-bytes",
            "1048576",
        ]);
        assert_eq!(config.node_id, 3);
        assert_eq!(config.listen.to_string(), "0.0.0.0:7001");
        assert!(config.no_metrics);
        assert_eq!(config.segment_bytes, 1 << 20);
        assert_eq!(config.advertised_addr(config.listen), "node3.internal:7001");
    }

    #[test]
    fn advertised_address_defaults_to_the_bound_one() {
        let config = Config::parse_from(["fl-server"]);
        let bound: SocketAddr = "127.0.0.1:54321".parse().unwrap();
        assert_eq!(config.advertised_addr(bound), "127.0.0.1:54321");
    }
}
