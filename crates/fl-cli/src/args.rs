//! Command-line arguments.

use clap::{Parser, Subcommand};

/// The parsed command line.
#[derive(Parser, Debug)]
#[command(
    name = "fl",
    version,
    about = "Command-line client for ferrumlog",
    long_about = None
)]
pub struct Cli {
    /// Nodes to contact, separated by commas. List every node you know.
    #[arg(
        short,
        long,
        global = true,
        env = "FERRUMLOG_ENDPOINTS",
        value_delimiter = ',',
        default_value = "127.0.0.1:7001"
    )]
    pub endpoints: Vec<String>,

    /// Seconds to wait for a reply before giving up on a node.
    #[arg(long, global = true, default_value_t = 10)]
    pub timeout: u64,

    /// What to do.
    #[command(subcommand)]
    pub command: Command,
}

/// One `fl` subcommand.
#[derive(Subcommand, Clone, PartialEq, Eq, Debug)]
pub enum Command {
    /// List the topics and how many records each holds.
    Topics,

    /// Create a topic. Does nothing if it already exists.
    CreateTopic {
        /// Name of the topic.
        name: String,
    },

    /// Append records to a topic.
    Produce {
        /// Topic to write to.
        topic: String,

        /// Records to write, one per argument. With none, reads one record
        /// per line from standard input and skips blank lines.
        values: Vec<String>,

        /// Key to attach to every record.
        #[arg(short, long)]
        key: Option<String>,

        /// Most records sent in one request.
        #[arg(long, default_value_t = 100)]
        batch: usize,
    },

    /// Read records from a topic, printing offset, key, and value.
    Consume {
        /// Topic to read from.
        topic: String,

        /// Where to start: earliest, latest, or an offset.
        #[arg(long, default_value = "earliest", value_parser = parse_start)]
        from: Start,

        /// Keep running and print new records as they arrive.
        #[arg(short, long)]
        follow: bool,

        /// Stop after this many records.
        #[arg(short = 'n', long)]
        max: Option<u64>,
    },

    /// Show the status of every node.
    Status,
}

/// Where `fl consume` starts reading.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Start {
    /// The first record in the topic.
    Earliest,
    /// Past the last record, so only new ones are shown.
    Latest,
    /// A specific offset.
    Offset(u64),
}

fn parse_start(value: &str) -> Result<Start, String> {
    match value {
        "earliest" => Ok(Start::Earliest),
        "latest" => Ok(Start::Latest),
        other => other
            .parse()
            .map(Start::Offset)
            .map_err(|_| format!("expected earliest, latest, or an offset, got {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("fl").chain(args.iter().copied())).unwrap()
    }

    #[test]
    fn defaults() {
        let cli = parse(&["topics"]);
        assert_eq!(cli.endpoints, ["127.0.0.1:7001"]);
        assert_eq!(cli.timeout, 10);
        assert_eq!(cli.command, Command::Topics);
    }

    #[test]
    fn endpoints_split_on_commas_before_or_after_the_command() {
        let before = parse(&["--endpoints", "n1:7001,n2:7001", "status"]);
        assert_eq!(before.endpoints, ["n1:7001", "n2:7001"]);
        let after = parse(&["status", "-e", "n1:7001,n2:7001,n3:7001"]);
        assert_eq!(after.endpoints.len(), 3);
        assert_eq!(after.command, Command::Status);
    }

    #[test]
    fn produce_arguments() {
        let cli = parse(&[
            "produce", "quotes", "a", "b c", "-k", "UST10Y", "--batch", "5",
        ]);
        assert_eq!(
            cli.command,
            Command::Produce {
                topic: "quotes".into(),
                values: vec!["a".into(), "b c".into()],
                key: Some("UST10Y".into()),
                batch: 5,
            }
        );

        let from_stdin = parse(&["produce", "quotes"]);
        assert_eq!(
            from_stdin.command,
            Command::Produce {
                topic: "quotes".into(),
                values: vec![],
                key: None,
                batch: 100,
            }
        );
    }

    #[test]
    fn consume_arguments() {
        let cli = parse(&["consume", "quotes"]);
        assert_eq!(
            cli.command,
            Command::Consume {
                topic: "quotes".into(),
                from: Start::Earliest,
                follow: false,
                max: None,
            }
        );

        let cli = parse(&["consume", "quotes", "--from", "42", "-f", "-n", "10"]);
        assert_eq!(
            cli.command,
            Command::Consume {
                topic: "quotes".into(),
                from: Start::Offset(42),
                follow: true,
                max: Some(10),
            }
        );

        let cli = parse(&["consume", "quotes", "--from", "latest"]);
        assert!(matches!(
            cli.command,
            Command::Consume {
                from: Start::Latest,
                ..
            }
        ));
    }

    #[test]
    fn bad_input_is_rejected() {
        for args in [
            vec!["fl"],
            vec!["fl", "consume"],
            vec!["fl", "consume", "quotes", "--from", "yesterday"],
            vec!["fl", "consume", "quotes", "--from", "-3"],
            vec!["fl", "create-topic"],
            vec!["fl", "frobnicate"],
        ] {
            assert!(Cli::try_parse_from(&args).is_err(), "{args:?}");
        }
    }
}
