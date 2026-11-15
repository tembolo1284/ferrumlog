//! The `fl` binary.

use std::io::{self, BufRead};
use std::process::ExitCode;
use std::time::Duration;

use clap::Parser;
use fl_cli::{run, Cli, CliError};
use fl_client::{Client, ClientConfig};

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match execute(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        // The reader went away, as in `fl consume quotes | head`. That is
        // how pipelines normally end, not a failure.
        Err(e) if is_broken_pipe(&e) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn execute(cli: Cli) -> Result<(), CliError> {
    let mut config = ClientConfig::new(cli.endpoints);
    config.request_timeout = Duration::from_secs(cli.timeout.max(1));
    let client = Client::new(config)?;

    let stdin = io::stdin();
    let mut lines = stdin.lock().lines();
    let mut out = io::stdout();

    tokio::select! {
        result = run(cli.command, &client, &mut lines, &mut out) => result,
        // Ctrl-C is the normal way to leave `consume --follow`.
        _ = tokio::signal::ctrl_c() => Ok(()),
    }
}

fn is_broken_pipe(error: &CliError) -> bool {
    error
        .downcast_ref::<io::Error>()
        .is_some_and(|e| e.kind() == io::ErrorKind::BrokenPipe)
}
