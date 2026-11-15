//! Validation errors for the core types.

use std::fmt;

/// Result alias for this crate's [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

/// A value failed validation.
///
/// These are all caller mistakes, not runtime faults. I/O, Raft, and network
/// errors are defined in the crates that produce them.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Error {
    /// A topic name broke the naming rules.
    InvalidTopicName {
        /// The rejected name.
        name: String,
        /// Which rule it broke.
        reason: &'static str,
    },
    /// A record's key plus value is over the limit.
    RecordTooLarge {
        /// Size of the record in bytes.
        size: usize,
        /// The limit in bytes.
        max: usize,
    },
    /// A batch has no records.
    EmptyBatch,
    /// A batch's total payload is over the limit.
    BatchTooLarge {
        /// Size of the batch in bytes.
        size: usize,
        /// The limit in bytes.
        max: usize,
    },
    /// A batch has more records than allowed.
    TooManyRecords {
        /// Number of records in the batch.
        count: usize,
        /// The limit.
        max: usize,
    },
    /// A membership has no voters.
    EmptyMembership,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::InvalidTopicName { name, reason } => {
                write!(f, "invalid topic name {name:?}: {reason}")
            }
            Error::RecordTooLarge { size, max } => {
                write!(f, "record is {size} bytes, limit is {max}")
            }
            Error::EmptyBatch => f.write_str("record batch is empty"),
            Error::BatchTooLarge { size, max } => {
                write!(f, "record batch is {size} bytes, limit is {max}")
            }
            Error::TooManyRecords { count, max } => {
                write!(f, "record batch has {count} records, limit is {max}")
            }
            Error::EmptyMembership => f.write_str("membership has no voters"),
        }
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_read_well() {
        let e = Error::InvalidTopicName {
            name: "a b".into(),
            reason: "bad",
        };
        assert_eq!(e.to_string(), "invalid topic name \"a b\": bad");
        assert_eq!(
            Error::RecordTooLarge { size: 9, max: 8 }.to_string(),
            "record is 9 bytes, limit is 8"
        );
        assert_eq!(Error::EmptyBatch.to_string(), "record batch is empty");
    }

    #[test]
    fn is_a_std_error() {
        fn takes(_: &dyn std::error::Error) {}
        takes(&Error::EmptyMembership);
    }
}
