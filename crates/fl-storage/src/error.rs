//! Storage errors.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use fl_types::{LogIndex, Term};

/// Result alias for this crate's [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

/// A storage operation failed.
#[derive(Debug)]
pub enum Error {
    /// The operating system reported an I/O error.
    Io {
        /// File or directory being accessed.
        path: PathBuf,
        /// The underlying error.
        source: io::Error,
    },
    /// Stored data failed validation and cannot be a torn write.
    Corrupt {
        /// File holding the bad data.
        path: PathBuf,
        /// Byte offset where the problem was found.
        offset: u64,
        /// What was wrong.
        reason: String,
    },
    /// An appended entry does not directly follow the last entry.
    NonContiguous {
        /// The index the next entry must have.
        expected: LogIndex,
        /// The index it had.
        got: LogIndex,
    },
    /// An appended entry has a lower term than the entry before it.
    TermRegression {
        /// Term of the preceding entry.
        last: Term,
        /// Term of the rejected entry.
        got: Term,
    },
    /// An index is before the first entry the log still holds.
    OutOfRange {
        /// The requested index.
        index: LogIndex,
        /// First index in the log.
        first: LogIndex,
        /// Last index in the log.
        last: LogIndex,
    },
    /// An encoded entry is larger than a frame may be.
    EntryTooLarge {
        /// Encoded payload size in bytes.
        size: usize,
        /// The limit in bytes.
        max: usize,
    },
    /// The configuration is unusable.
    InvalidConfig(&'static str),
}

impl Error {
    /// Returns a closure that wraps an I/O error with the path it came from.
    pub(crate) fn io(path: &Path) -> impl FnOnce(io::Error) -> Error + '_ {
        move |source| Error::Io {
            path: path.to_path_buf(),
            source,
        }
    }

    pub(crate) fn corrupt(path: &Path, offset: u64, reason: impl Into<String>) -> Error {
        Error::Corrupt {
            path: path.to_path_buf(),
            offset,
            reason: reason.into(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io { path, source } => write!(f, "{}: {source}", path.display()),
            Error::Corrupt {
                path,
                offset,
                reason,
            } => write!(
                f,
                "{} is corrupt at byte {offset}: {reason}",
                path.display()
            ),
            Error::NonContiguous { expected, got } => {
                write!(f, "append out of order: expected {expected}, got {got}")
            }
            Error::TermRegression { last, got } => {
                write!(f, "append with term {got} after term {last}")
            }
            Error::OutOfRange { index, first, last } => {
                write!(f, "{index} is outside the log range {first} to {last}")
            }
            Error::EntryTooLarge { size, max } => {
                write!(f, "encoded entry is {size} bytes, limit is {max}")
            }
            Error::InvalidConfig(reason) => write!(f, "invalid log config: {reason}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}
