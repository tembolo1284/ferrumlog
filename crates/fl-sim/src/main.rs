//! Runs the simulator over a range of seeds.

use std::process::ExitCode;
use std::time::Instant;

use clap::Parser;
use fl_sim::{run, SimConfig};

/// Runs ferrumlog's Raft under simulated faults and checks its safety
/// properties. Every run is decided by its seed, so a failure can be
/// replayed exactly.
#[derive(Parser, Debug)]
#[command(name = "fl-sim", version)]
struct Args {
    /// First seed to run.
    #[arg(long, default_value_t = 0)]
    seed: u64,
    /// How many seeds to run, counting up from --seed.
    #[arg(long, default_value_t = 100)]
    count: u64,
    /// Nodes in the cluster.
    #[arg(long, default_value_t = 3)]
    nodes: u64,
    /// Length of the phase with faults, in ticks.
    #[arg(long)]
    ticks: Option<u64>,
    /// How rough the faults are.
    #[arg(long, value_enum, default_value_t = Profile::Normal)]
    profile: Profile,
    /// Record recent events and print them if a run fails.
    #[arg(long)]
    trace: bool,
    /// Print one line per seed instead of only a summary.
    #[arg(long, short)]
    verbose: bool,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
enum Profile {
    /// No faults at all.
    Calm,
    /// A moderate mix of every fault.
    Normal,
    /// Faults several times more frequent.
    Harsh,
}

fn main() -> ExitCode {
    let args = Args::parse();
    let started = Instant::now();
    let mut committed = 0u64;
    let mut crashes = 0u64;
    let mut partitions = 0u64;
    let mut elections = 0u64;

    // A node reports a broken internal invariant by panicking. The
    // simulator turns that into a violation, so the default panic message
    // would only be noise.
    std::panic::set_hook(Box::new(|_| {}));

    for seed in args.seed..args.seed.saturating_add(args.count) {
        let mut config = match args.profile {
            Profile::Calm => SimConfig::calm(seed),
            Profile::Normal => SimConfig::new(seed),
            Profile::Harsh => SimConfig::harsh(seed),
        }
        .with_nodes(args.nodes);
        if let Some(ticks) = args.ticks {
            config.ticks = ticks;
        }
        config.trace = args.trace;

        match run(config) {
            Ok(report) => {
                committed += report.committed;
                crashes += report.crashes;
                partitions += report.partitions;
                elections += report.leaders_elected;
                if args.verbose {
                    println!(
                        "seed {seed}: ok, {} committed, {} leaders, {} crashes, \
                         {} partitions, {} of {} messages lost, fingerprint {:016x}",
                        report.committed,
                        report.leaders_elected,
                        report.crashes,
                        report.partitions,
                        report.messages_lost,
                        report.messages_sent,
                        report.fingerprint,
                    );
                }
            }
            Err(violation) => {
                for line in &violation.trace {
                    println!("{line}");
                }
                println!("FAILED: {violation}");
                println!(
                    "replay with: fl-sim --seed {seed} --count 1 --nodes {} --profile {} --trace",
                    args.nodes,
                    format!("{:?}", args.profile).to_lowercase(),
                );
                return ExitCode::FAILURE;
            }
        }
    }

    println!(
        "{} seeds passed in {:.1}s: {committed} entries committed through \
         {elections} leaders, {crashes} crashes, and {partitions} partitions",
        args.count,
        started.elapsed().as_secs_f64(),
    );
    ExitCode::SUCCESS
}
