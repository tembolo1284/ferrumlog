//! Broker errors.

use std::fmt;

use fl_types::LogIndex;

/// Result alias for this crate's [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

/// A broker operation failed.
#[derive(Debug)]
pub enum Error {
    /// The request named a topic that does not exist.
    UnknownTopic(String),
    /// The request itself is not allowed: a bad topic name, an empty batch,
    /// a record that is too large, and so on.
    Invalid(fl_types::Error),
    /// The cluster already has the maximum number of topics.
    TooManyTopics,
    /// The log could not be read or written.
    Storage(fl_storage::Error),
    /// An entry was applied out of order. Entries must be applied one at a
    /// time in index order with no gaps.
    OutOfOrder {
        /// The index the next applied entry must have.
        expected: LogIndex,
        /// The index it had.
        got: LogIndex,
    },
    /// The offset index and the log disagree. This is a bug or on-disk
    /// damage, never a client mistake.
    Inconsistent {
        /// Log index where the disagreement was found.
        index: LogIndex,
        /// What was wrong.
        reason: &'static str,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::UnknownTopic(name) => write!(f, "topic {name:?} does not exist"),
            Error::Invalid(e) => write!(f, "{e}"),
            Error::TooManyTopics => f.write_str("the topic limit has been reached"),
            Error::Storage(e) => write!(f, "{e}"),
            Error::OutOfOrder { expected, got } => {
                write!(f, "apply out of order: expected {expected}, got {got}")
            }
            Error::Inconsistent { index, reason } => {
                write!(f, "state machine and log disagree at {index}: {reason}")
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Invalid(e) => Some(e),
            Error::Storage(e) => Some(e),
            _ => None,
        }
    }
}

impl From<fl_types::Error> for Error {
    fn from(e: fl_types::Error) -> Self {
        Error::Invalid(e)
    }
}

impl From<fl_storage::Error> for Error {
    fn from(e: fl_storage::Error) -> Self {
        Error::Storage(e)
    }
}
