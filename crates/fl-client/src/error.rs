//! Client errors.

use std::fmt;

use tonic::{Code, Status};

/// Result alias for this crate's [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

/// A client call failed.
#[derive(Debug)]
pub enum Error {
    /// The configuration lists no endpoints.
    NoEndpoints,
    /// An endpoint could not be parsed as an address.
    InvalidEndpoint {
        /// The endpoint as given.
        endpoint: String,
        /// Why it was rejected.
        reason: String,
    },
    /// The server understood the request and said no. Retrying the same
    /// request will get the same answer.
    Rejected(Status),
    /// No node could complete the call.
    ///
    /// For a read, the retries ran out. For a write this is returned on the
    /// first transient failure unless
    /// [`retry_writes`](crate::ClientConfig::retry_writes) is set, and it
    /// means the outcome is unknown: the records may or may not have been
    /// written.
    Unavailable {
        /// How many times the call was tried.
        attempts: u32,
        /// The last failure.
        status: Status,
    },
    /// A reply was missing something the protocol requires.
    Protocol(&'static str),
}

impl Error {
    /// The gRPC status behind the error, if there is one.
    pub fn status(&self) -> Option<&Status> {
        match self {
            Error::Rejected(status) | Error::Unavailable { status, .. } => Some(status),
            _ => None,
        }
    }

    /// The gRPC status code behind the error, if there is one.
    pub fn code(&self) -> Option<Code> {
        self.status().map(Status::code)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::NoEndpoints => f.write_str("no endpoints were given"),
            Error::InvalidEndpoint { endpoint, reason } => {
                write!(f, "invalid endpoint {endpoint:?}: {reason}")
            }
            Error::Rejected(status) => write!(f, "{}", status.message()),
            Error::Unavailable { attempts, status } => {
                let tries = if *attempts == 1 { "try" } else { "tries" };
                write!(
                    f,
                    "no node could complete the call after {attempts} {tries}: {}",
                    status.message()
                )
            }
            Error::Protocol(reason) => write!(f, "malformed reply: {reason}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.status()
            .map(|status| status as &(dyn std::error::Error + 'static))
    }
}

/// True if a different node, or the same node a moment later, might give a
/// different answer.
///
/// The codes treated as final are the ones a server returns after it has
/// looked at the request and decided: a bad argument, a missing topic, and
/// so on. Everything else can come from a connection that broke or a node
/// that is restarting.
pub(crate) fn is_transient(code: Code) -> bool {
    !matches!(
        code,
        Code::Ok
            | Code::InvalidArgument
            | Code::NotFound
            | Code::AlreadyExists
            | Code::PermissionDenied
            | Code::Unauthenticated
            | Code::ResourceExhausted
            | Code::FailedPrecondition
            | Code::OutOfRange
            | Code::Unimplemented
            | Code::DataLoss
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decisions_are_final_and_breakage_is_transient() {
        for code in [
            Code::InvalidArgument,
            Code::NotFound,
            Code::ResourceExhausted,
            Code::FailedPrecondition,
            Code::Unimplemented,
        ] {
            assert!(!is_transient(code), "{code:?}");
        }
        for code in [
            Code::Unavailable,
            Code::DeadlineExceeded,
            Code::Cancelled,
            Code::Unknown,
            Code::Internal,
            Code::Aborted,
        ] {
            assert!(is_transient(code), "{code:?}");
        }
    }

    #[test]
    fn messages_read_well() {
        let rejected = Error::Rejected(Status::not_found("topic \"x\" does not exist"));
        assert_eq!(rejected.to_string(), "topic \"x\" does not exist");
        assert_eq!(rejected.code(), Some(Code::NotFound));

        let once = Error::Unavailable {
            attempts: 1,
            status: Status::unavailable("connection refused"),
        };
        assert_eq!(
            once.to_string(),
            "no node could complete the call after 1 try: connection refused"
        );
        let many = Error::Unavailable {
            attempts: 6,
            status: Status::unavailable("connection refused"),
        };
        assert!(many.to_string().contains("after 6 tries"));

        assert_eq!(Error::NoEndpoints.code(), None);
        assert_eq!(
            Error::Protocol("record missing").to_string(),
            "malformed reply: record missing"
        );
    }
}
