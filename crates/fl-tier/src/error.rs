//! Errors from archiving and restoring.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use fl_types::{LogIndex, NodeId};

/// Result alias for this crate's [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

/// An archive or restore operation failed.
#[derive(Debug)]
pub enum Error {
    /// A local file or directory could not be read or written.
    Io {
        /// The file or directory.
        path: PathBuf,
        /// The underlying error.
        source: io::Error,
    },
    /// The data directory could not be listed.
    Storage(fl_storage::Error),
    /// The object store refused or failed a request.
    Store(String),
    /// The store location is not understood, or its backend is not built
    /// into this binary.
    Unsupported(String),
    /// The manifest in the store cannot be parsed.
    BadManifest(String),
    /// The store holds another node's archive.
    WrongNode {
        /// The node whose segments are in the store.
        archived: NodeId,
        /// The node being archived now.
        local: NodeId,
    },
    /// The next local segment does not continue the archive.
    Gap {
        /// The index the next archived segment must start at.
        expected: LogIndex,
        /// Where the next local segment starts.
        found: LogIndex,
    },
    /// An archived file is missing, or is not what the manifest says.
    Damaged {
        /// The file's key in the store.
        key: String,
        /// What is wrong with it.
        reason: String,
    },
    /// The directory to restore into already holds a log.
    NotEmpty(PathBuf),
}

impl Error {
    pub(crate) fn io(path: &Path) -> impl FnOnce(io::Error) -> Error + '_ {
        move |source| Error::Io {
            path: path.to_path_buf(),
            source,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io { path, source } => write!(f, "{}: {source}", path.display()),
            Error::Storage(e) => write!(f, "{e}"),
            Error::Store(reason) => write!(f, "object store: {reason}"),
            Error::Unsupported(reason) => f.write_str(reason),
            Error::BadManifest(reason) => write!(f, "the archive manifest is invalid: {reason}"),
            Error::WrongNode { archived, local } => write!(
                f,
                "the store holds the archive of node {archived}, and this is node {local}"
            ),
            Error::Gap { expected, found } => write!(
                f,
                "the archive ends before {expected}, but the next local segment starts at {found}"
            ),
            Error::Damaged { key, reason } => write!(f, "archived file {key}: {reason}"),
            Error::NotEmpty(path) => write!(
                f,
                "{} already holds log files, refusing to restore over them",
                path.display()
            ),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io { source, .. } => Some(source),
            Error::Storage(e) => Some(e),
            _ => None,
        }
    }
}

impl From<fl_storage::Error> for Error {
    fn from(e: fl_storage::Error) -> Self {
        Error::Storage(e)
    }
}
