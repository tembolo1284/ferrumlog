//! The `fl-bench` binary.

use std::process::ExitCode;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use fl_bench::{consume, produce, ConsumeOptions, Limit, ProduceOptions, Summary};
use fl_client::ClientConfig;

/// Measures how fast a ferrumlog cluster takes writes and serves reads.
#[derive(Parser, Debug)]
#[command(name = "fl-bench", version)]
struct Cli {
    /// Nodes to talk to, as host:port, separated by commas.
    #[arg(
        long,
        short,
        env = "FERRUMLOG_ENDPOINTS",
        value_delimiter = ',',
        default_value = "127.0.0.1:7001",
        global = true
    )]
    endpoints: Vec<String>,

    /// Seconds to wait for one request.
    #[arg(long, default_value_t = 10, global = true)]
    timeout: u64,

    /// Print one line of JSON instead of a table.
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Write records and measure the time to an acknowledgement.
    Produce(ProduceArgs),
    /// Read a topic from start to end and measure the fetches.
    Consume(ConsumeArgs),
}

#[derive(Args, Debug)]
struct ProduceArgs {
    /// Topic to write to. Created if missing.
    #[arg(long, default_value = "bench")]
    topic: String,
    /// Concurrent writers, each with its own connection.
    #[arg(long, short, default_value_t = 8)]
    workers: usize,
    /// Size of each record's value in bytes.
    #[arg(long, default_value_t = 100)]
    size: usize,
    /// Records per request.
    #[arg(long, default_value_t = 1)]
    batch: usize,
    /// Run for this many seconds.
    #[arg(long, default_value_t = 10, conflicts_with = "requests")]
    seconds: u64,
    /// Stop after this many requests instead of after a time.
    #[arg(long)]
    requests: Option<u64>,
    /// Requests per second to aim for, across all workers. Without it the
    /// workers send as fast as they are answered. With it, latency is
    /// measured from when each request was due, so stalls are not hidden.
    #[arg(long)]
    rate: Option<f64>,
}

#[derive(Args, Debug)]
struct ConsumeArgs {
    /// Topic to read.
    #[arg(long, default_value = "bench")]
    topic: String,
    /// Concurrent readers. Each one reads the whole topic.
    #[arg(long, short, default_value_t = 1)]
    workers: usize,
    /// Most records per fetch. Zero leaves it to the server.
    #[arg(long, default_value_t = 0)]
    max_records: u32,
    /// Most record bytes per fetch. Zero leaves it to the server.
    #[arg(long, default_value_t = 0)]
    max_bytes: u32,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let mut client = ClientConfig::new(cli.endpoints.clone());
    client.request_timeout = Duration::from_secs(cli.timeout.max(1));

    let (name, title, result) = match cli.command {
        Command::Produce(args) => {
            let title = format!(
                "produce: {} workers, {}-byte records, {} per request{}",
                args.workers.max(1),
                args.size.max(8),
                args.batch.max(1),
                args.rate
                    .map_or(String::new(), |rate| format!(", {rate} requests/s target")),
            );
            let options = ProduceOptions {
                client,
                topic: args.topic,
                workers: args.workers,
                record_bytes: args.size,
                batch: args.batch,
                limit: match args.requests {
                    Some(requests) => Limit::Requests(requests),
                    None => Limit::Time(Duration::from_secs(args.seconds)),
                },
                rate: args.rate,
            };
            ("produce", title, produce(options).await)
        }
        Command::Consume(args) => {
            let title = format!("consume: {} readers", args.workers.max(1));
            let options = ConsumeOptions {
                client,
                topic: args.topic,
                workers: args.workers,
                max_records: args.max_records,
                max_bytes: args.max_bytes,
            };
            ("consume", title, consume(options).await)
        }
    };

    match result {
        Ok(summary) => {
            print(&summary, name, &title, cli.json);
            // A run in which nothing worked is a failure, whatever the
            // numbers say.
            if summary.requests == 0 && summary.errors > 0 {
                ExitCode::FAILURE
            } else {
                ExitCode::SUCCESS
            }
        }
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn print(summary: &Summary, name: &str, title: &str, json: bool) {
    if json {
        println!("{}", summary.to_json(name));
    } else {
        print!("{}", summary.to_text(title));
    }
}
