//! The ferrumlog node.
//!
//! This crate turns the synchronous broker into a network service:
//!
//! * a gRPC server for the `Log` and `Admin` services,
//! * a writer thread that group-commits writes with one fsync per batch,
//! * an HTTP server for Prometheus metrics and health checks,
//! * graceful shutdown that finishes queued writes before exiting.
//!
//! The node currently runs a single-node [`fl_broker::Standalone`] broker.
//! Replication is added by putting Raft between the writer and the log. The
//! services, metrics, and shutdown path stay as they are.
//!
//! # Security
//!
//! There is no TLS and no authentication yet. The defaults bind to
//! loopback. If you listen on a public interface, restrict who can connect
//! with a firewall or security group.

mod config;
mod engine;
mod http;
mod metrics;
mod service;
mod shutdown;

use std::net::SocketAddr;
use std::sync::Arc;

use fl_proto::{AdminServer, LogServer};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;
use tracing::{error, info, warn};

pub use config::Config;
pub use metrics::Metrics;
pub use shutdown::Shutdown;

use engine::{Engine, WriterHandle};
use service::{AdminService, LogService};

/// The error type for starting and stopping a node.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// A running node.
///
/// Dropping a `Server` without calling [`Server::join`] leaves its tasks
/// running until the runtime shuts down. Call [`Server::stop`] for an
/// orderly exit.
pub struct Server {
    grpc_addr: SocketAddr,
    metrics_addr: Option<SocketAddr>,
    shutdown: Shutdown,
    metrics: Arc<Metrics>,
    engine: Engine,
    writer: WriterHandle,
    grpc: JoinHandle<Result<(), BoxError>>,
    http: Option<JoinHandle<Result<(), BoxError>>>,
}

impl Server {
    /// Opens the data directory, recovers the log, and starts serving.
    ///
    /// Returns once the listeners are bound, so a caller can connect as
    /// soon as this resolves.
    pub async fn start(config: Config) -> Result<Server, BoxError> {
        let shutdown = Shutdown::new();
        let metrics = Arc::new(Metrics::default());

        if config.unsafe_no_fsync {
            warn!("fsync is disabled: acknowledged writes can be lost in a crash");
        }
        let (engine, writer) =
            Engine::open(&config, Arc::clone(&metrics), shutdown.clone()).await?;

        let listener = TcpListener::bind(config.listen).await?;
        let grpc_addr = listener.local_addr()?;
        // Small request and response messages should go out immediately
        // instead of waiting on Nagle's algorithm.
        let incoming = TcpIncoming::from(listener).with_nodelay(Some(true));

        let log_service = LogService {
            engine: engine.clone(),
            metrics: Arc::clone(&metrics),
            shutdown: shutdown.clone(),
        };
        let admin_service = AdminService {
            engine: engine.clone(),
            node_id: config.node_id,
            advertise: config.advertised_addr(grpc_addr),
        };
        let grpc = {
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                let result = tonic::transport::Server::builder()
                    .add_service(LogServer::new(log_service))
                    .add_service(AdminServer::new(admin_service))
                    .serve_with_incoming_shutdown(incoming, shutdown.wait())
                    .await;
                // If the server stops for any reason, the node stops.
                shutdown.trigger();
                result.map_err(BoxError::from)
            })
        };

        let (metrics_addr, http) = if config.no_metrics {
            (None, None)
        } else {
            let listener = TcpListener::bind(config.metrics_listen).await?;
            let addr = listener.local_addr()?;
            let router = http::router(Arc::clone(&metrics), shutdown.clone());
            let shutdown = shutdown.clone();
            let task = tokio::spawn(async move {
                let result = axum::serve(listener, router)
                    .with_graceful_shutdown({
                        let shutdown = shutdown.clone();
                        async move { shutdown.wait().await }
                    })
                    .await;
                shutdown.trigger();
                result.map_err(BoxError::from)
            });
            (Some(addr), Some(task))
        };

        info!(
            node_id = config.node_id,
            grpc = %grpc_addr,
            metrics = ?metrics_addr,
            data_dir = %config.data_dir.display(),
            "ferrumlog node started"
        );

        Ok(Server {
            grpc_addr,
            metrics_addr,
            shutdown,
            metrics,
            engine,
            writer,
            grpc,
            http,
        })
    }

    /// The address the gRPC server is listening on.
    pub fn grpc_addr(&self) -> SocketAddr {
        self.grpc_addr
    }

    /// The address the HTTP server is listening on, if it is enabled.
    pub fn metrics_addr(&self) -> Option<SocketAddr> {
        self.metrics_addr
    }

    /// The node's metrics.
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    /// A handle that can trigger shutdown from another task.
    pub fn shutdown_handle(&self) -> Shutdown {
        self.shutdown.clone()
    }

    /// Triggers shutdown and waits for the node to stop.
    pub async fn stop(self) -> Result<(), BoxError> {
        self.shutdown.trigger();
        self.join().await
    }

    /// Waits until shutdown is triggered, then stops in order: the servers
    /// stop accepting requests and finish the ones in flight, the writer
    /// commits what is still queued, and the log is closed.
    ///
    /// Returns the error that brought the node down, if there was one.
    pub async fn join(self) -> Result<(), BoxError> {
        self.shutdown.wait().await;
        info!("shutting down");

        let grpc = flatten(self.grpc.await);
        let http = match self.http {
            Some(task) => flatten(task.await),
            None => Ok(()),
        };

        // Every request handler has returned, so nothing new can be queued
        // behind the stop message.
        self.engine.stop().await;
        let writer = self.writer;
        let writer = match tokio::task::spawn_blocking(move || writer.join()).await {
            Ok(Ok(result)) => result.map_err(BoxError::from),
            Ok(Err(_)) => Err(BoxError::from("the writer thread panicked")),
            Err(e) => Err(BoxError::from(e)),
        };

        // A storage failure is the root cause when there is one, so it is
        // reported ahead of whatever the servers returned.
        let result = writer.and(grpc).and(http);
        match &result {
            Ok(()) => info!("stopped cleanly"),
            Err(e) => error!(error = %e, "stopped with an error"),
        }
        result
    }
}

fn flatten(joined: Result<Result<(), BoxError>, tokio::task::JoinError>) -> Result<(), BoxError> {
    match joined {
        Ok(result) => result,
        Err(e) => Err(BoxError::from(e)),
    }
}
