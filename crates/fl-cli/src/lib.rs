//! The `fl` command-line client.
//!
//! The binary in `main.rs` is a thin shell. Parsing lives in [`Cli`] and
//! the commands live in [`run`], which takes its input and output as
//! arguments. That lets the tests drive every command against a real node
//! and check exactly what would be printed.
//!
//! ```text
//! fl create-topic quotes
//! fl produce quotes "UST10Y 99.5" "UST2Y 100.1"
//! cat ticks.txt | fl produce quotes
//! fl consume quotes --from earliest
//! fl consume quotes --from latest --follow
//! fl topics
//! fl --endpoints n1:7001,n2:7001,n3:7001 status
//! ```

mod args;
mod commands;

pub use args::{Cli, Command, Start};
pub use commands::{run, CliError, Lines};
