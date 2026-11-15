//! The `fl-tier` binary.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use fl_client::{Client, ClientConfig};
use fl_tier::{archive, restore, verify, Error, Manifest, Store};

/// Archives a ferrumlog node's sealed log segments to a directory or an
/// S3 bucket, and restores a data directory from the archive.
#[derive(Parser, Debug)]
#[command(name = "fl-tier", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Upload sealed segments that are committed and not yet archived.
    Archive {
        /// The node's data directory. Run this on the node's machine.
        #[arg(long, env = "FERRUMLOG_DATA_DIR", default_value = "data")]
        data_dir: PathBuf,
        /// The node's own gRPC address, to ask it for its commit index.
        #[arg(long, default_value = "127.0.0.1:7001")]
        endpoint: String,
        /// Where to archive to: a path, file:///dir, or s3://bucket/prefix.
        #[arg(long, env = "FERRUMLOG_ARCHIVE")]
        dest: String,
        /// Keep running and make a pass every this many seconds. Without
        /// it, one pass is made.
        #[arg(long)]
        every: Option<u64>,
    },
    /// Show what an archive holds.
    List {
        /// The archive: a path, file:///dir, or s3://bucket/prefix.
        #[arg(long, env = "FERRUMLOG_ARCHIVE")]
        source: String,
    },
    /// Download every archived file and check it against the manifest.
    Verify {
        /// The archive: a path, file:///dir, or s3://bucket/prefix.
        #[arg(long, env = "FERRUMLOG_ARCHIVE")]
        source: String,
    },
    /// Rebuild an empty data directory from an archive.
    Restore {
        /// The archive: a path, file:///dir, or s3://bucket/prefix.
        #[arg(long, env = "FERRUMLOG_ARCHIVE")]
        source: String,
        /// The directory to restore into. It must not hold a log.
        #[arg(long)]
        data_dir: PathBuf,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(Cli::parse().command).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

type AnyError = Box<dyn std::error::Error + Send + Sync>;

async fn run(command: Command) -> Result<(), AnyError> {
    match command {
        Command::Archive {
            data_dir,
            endpoint,
            dest,
            every,
        } => {
            let store = Store::open(&dest).await?;
            let client = Client::new(ClientConfig::new([endpoint.clone()]))?;
            let Some(every) = every else {
                return pass(&client, &endpoint, &data_dir, &store).await;
            };
            let mut timer = tokio::time::interval(Duration::from_secs(every.max(1)));
            loop {
                timer.tick().await;
                // A failed pass is reported and tried again next time. The
                // node may simply be restarting.
                if let Err(e) = pass(&client, &endpoint, &data_dir, &store).await {
                    eprintln!("error: {e}");
                }
            }
        }
        Command::List { source } => {
            let store = Store::open(&source).await?;
            let manifest = match store.get("manifest").await? {
                Some(bytes) => Manifest::decode(&String::from_utf8_lossy(&bytes))?,
                None => Manifest::default(),
            };
            println!("{:>12} {:>12} {:>14}", "FIRST", "LAST", "BYTES");
            for segment in &manifest.segments {
                println!(
                    "{:>12} {:>12} {:>14}",
                    segment.first_index.get(),
                    segment.last_index.get(),
                    segment.log_bytes + segment.index_bytes
                );
            }
            println!(
                "{} segments, {} bytes, through index {}{}",
                manifest.segments.len(),
                manifest.total_bytes(),
                manifest.archived_through().get(),
                manifest
                    .node
                    .map_or(String::new(), |node| format!(", from node {}", node.get())),
            );
            Ok(())
        }
        Command::Verify { source } => {
            let store = Store::open(&source).await?;
            let report = verify(&store).await?;
            println!(
                "ok: {} segments, {} bytes, through index {}",
                report.segments,
                report.bytes,
                report.through.get()
            );
            Ok(())
        }
        Command::Restore { source, data_dir } => {
            let store = Store::open(&source).await?;
            let report = restore(&store, &data_dir).await?;
            println!(
                "restored {} segments, {} bytes, through index {} into {}",
                report.segments,
                report.bytes,
                report.through.get(),
                data_dir.display()
            );
            Ok(())
        }
    }
}

/// One archive pass: ask the node how far it has committed, then upload.
async fn pass(
    client: &Client,
    endpoint: &str,
    data_dir: &std::path::Path,
    store: &Store,
) -> Result<(), AnyError> {
    let status = client.status_of(endpoint).await?;
    let report = archive(data_dir, status.node_id, status.commit_index, store)
        .await
        .map_err(|e: Error| -> AnyError { e.into() })?;
    println!(
        "archived {} segments ({} bytes) to {}, now through index {} of {} committed{}",
        report.uploaded.len(),
        report.bytes,
        store.describe(),
        report.archived_through.get(),
        status.commit_index.get(),
        if report.held_back > 0 {
            format!(", {} sealed segments not yet committed", report.held_back)
        } else {
            String::new()
        },
    );
    Ok(())
}
