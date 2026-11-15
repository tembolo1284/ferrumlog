//! The `fl-server` binary: parse the configuration, run a node, and stop
//! cleanly on Ctrl-C or SIGTERM.

use std::process::ExitCode;

use clap::Parser;
use fl_server::{Config, Server};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> ExitCode {
    // `RUST_LOG=debug` or `RUST_LOG=fl_server=debug` raises the level.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let config = Config::parse();
    let server = match Server::start(config).await {
        Ok(server) => server,
        Err(e) => {
            error!(error = %e, "failed to start");
            return ExitCode::FAILURE;
        }
    };

    let shutdown = server.shutdown_handle();
    tokio::spawn(async move {
        wait_for_signal().await;
        info!("signal received");
        shutdown.trigger();
    });

    match server.join().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(_) => ExitCode::FAILURE,
    }
}

/// Completes on Ctrl-C, or on SIGTERM, which is what `docker stop` and
/// systemd send.
async fn wait_for_signal() {
    use tokio::signal::unix::{signal, SignalKind};

    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(stream) => stream,
        Err(e) => {
            error!(error = %e, "cannot listen for SIGTERM, Ctrl-C still works");
            let _ = tokio::signal::ctrl_c().await;
            return;
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}
