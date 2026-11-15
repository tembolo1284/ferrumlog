//! Identifier and position newtypes.
//!
//! Each wraps a plain integer so the compiler rejects mixing a term with an
//! index, or a log index with a topic offset.

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

macro_rules! newtype {
    ($(#[$meta:meta])* $name:ident($inner:ty), $prefix:literal) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
        pub struct $name($inner);

        impl $name {
            /// Wraps a raw value.
            #[inline]
            pub const fn new(raw: $inner) -> Self {
                Self(raw)
            }

            /// Returns the raw value.
            #[inline]
            pub const fn get(self) -> $inner {
                self.0
            }
        }

        impl From<$inner> for $name {
            #[inline]
            fn from(raw: $inner) -> Self {
                Self(raw)
            }
        }

        impl From<$name> for $inner {
            #[inline]
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!($prefix, "{}"), self.0)
            }
        }
    };
}

newtype!(
    /// Identifies one node in the cluster. Assigned by the operator and
    /// stable for the life of the node.
    NodeId(u64),
    "n"
);

newtype!(
    /// A Raft term. Terms only increase, and each term has at most one leader.
    Term(u64),
    "t"
);

newtype!(
    /// A position in the replicated Raft log.
    ///
    /// The first real entry is at index 1. Index 0 means "before the first
    /// entry" and is used as the previous index of an empty log.
    LogIndex(u64),
    "i"
);

newtype!(
    /// A position within one topic, as seen by consumers. The first record
    /// in a topic has offset 0.
    Offset(u64),
    "o"
);

newtype!(
    /// Identifies a topic. Assigned by the state machine when the topic is
    /// created, so every node agrees on it.
    TopicId(u32),
    "topic#"
);

newtype!(
    /// Milliseconds since the Unix epoch.
    ///
    /// This crate never reads the clock. Callers convert from a
    /// [`SystemTime`] they obtained themselves, which keeps the Raft core
    /// and the simulator deterministic.
    Timestamp(u64),
    "ms:"
);

impl Term {
    /// The term every node starts in, before any election.
    pub const ZERO: Term = Term(0);

    /// The following term.
    #[inline]
    #[must_use]
    pub fn next(self) -> Term {
        Term(self.0.checked_add(1).expect("term overflow"))
    }
}

impl LogIndex {
    /// The position before the first entry.
    pub const ZERO: LogIndex = LogIndex(0);

    /// The index of the first real entry.
    pub const FIRST: LogIndex = LogIndex(1);

    /// The following index.
    #[inline]
    #[must_use]
    pub fn next(self) -> LogIndex {
        LogIndex(self.0.checked_add(1).expect("log index overflow"))
    }

    /// The preceding index. Stays at [`LogIndex::ZERO`] rather than
    /// underflowing.
    #[inline]
    #[must_use]
    pub fn prev(self) -> LogIndex {
        LogIndex(self.0.saturating_sub(1))
    }

    /// True for the position before the first entry.
    #[inline]
    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    /// Number of entries in the half-open range `(earlier, self]`. Zero if
    /// `earlier` is not actually earlier.
    #[inline]
    pub fn entries_since(self, earlier: LogIndex) -> u64 {
        self.0.saturating_sub(earlier.0)
    }
}

impl Offset {
    /// The offset of the first record in a topic.
    pub const ZERO: Offset = Offset(0);

    /// The following offset.
    #[inline]
    #[must_use]
    pub fn next(self) -> Offset {
        self.advance(1)
    }

    /// The offset `count` records later.
    #[inline]
    #[must_use]
    pub fn advance(self, count: u64) -> Offset {
        Offset(self.0.checked_add(count).expect("offset overflow"))
    }
}

impl Timestamp {
    /// An unset timestamp. The leader stamps records that carry this value
    /// before proposing them.
    pub const UNSET: Timestamp = Timestamp(0);

    /// Wraps a millisecond count.
    #[inline]
    pub const fn from_millis(millis: u64) -> Self {
        Timestamp(millis)
    }

    /// Converts a wall-clock time. Times before the epoch become
    /// [`Timestamp::UNSET`].
    pub fn from_system_time(time: SystemTime) -> Self {
        let millis = time
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        Timestamp(u64::try_from(millis).unwrap_or(u64::MAX))
    }

    /// Milliseconds since the Unix epoch.
    #[inline]
    pub const fn as_millis(self) -> u64 {
        self.0
    }

    /// True if no timestamp has been assigned.
    #[inline]
    pub fn is_unset(self) -> bool {
        self.0 == 0
    }
}

/// The term and index of one log entry.
///
/// Field order matters. The derived ordering compares the term first and the
/// index second, which is exactly Raft's "at least as up to date" rule used
/// when deciding whether to grant a vote.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct LogId {
    /// Term in which the entry was created.
    pub term: Term,
    /// Position of the entry in the log.
    pub index: LogIndex,
}

impl LogId {
    /// The id of the position before the first entry.
    pub const ZERO: LogId = LogId {
        term: Term::ZERO,
        index: LogIndex::ZERO,
    };

    /// Builds an id from its parts.
    #[inline]
    pub const fn new(term: Term, index: LogIndex) -> Self {
        LogId { term, index }
    }
}

impl fmt::Display for LogId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.term, self.index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn display_uses_prefixes() {
        assert_eq!(NodeId::new(3).to_string(), "n3");
        assert_eq!(Term::new(7).to_string(), "t7");
        assert_eq!(LogIndex::new(42).to_string(), "i42");
        assert_eq!(Offset::new(9).to_string(), "o9");
        assert_eq!(TopicId::new(1).to_string(), "topic#1");
        assert_eq!(
            LogId::new(Term::new(2), LogIndex::new(5)).to_string(),
            "t2/i5"
        );
    }

    #[test]
    fn round_trips_through_raw() {
        let n: NodeId = 11u64.into();
        assert_eq!(u64::from(n), 11);
        assert_eq!(n.get(), 11);
    }

    #[test]
    fn term_and_index_step() {
        assert_eq!(Term::ZERO.next(), Term::new(1));
        assert_eq!(LogIndex::ZERO.next(), LogIndex::FIRST);
        assert_eq!(LogIndex::FIRST.prev(), LogIndex::ZERO);
        assert_eq!(LogIndex::ZERO.prev(), LogIndex::ZERO);
        assert!(LogIndex::ZERO.is_zero());
        assert!(!LogIndex::FIRST.is_zero());
    }

    #[test]
    fn entries_since_saturates() {
        let a = LogIndex::new(10);
        let b = LogIndex::new(4);
        assert_eq!(a.entries_since(b), 6);
        assert_eq!(b.entries_since(a), 0);
    }

    #[test]
    fn offset_advances() {
        assert_eq!(Offset::ZERO.next(), Offset::new(1));
        assert_eq!(Offset::new(5).advance(10), Offset::new(15));
    }

    #[test]
    fn log_id_orders_by_term_then_index() {
        let old_long = LogId::new(Term::new(1), LogIndex::new(100));
        let new_short = LogId::new(Term::new(2), LogIndex::new(3));
        assert!(new_short > old_long);

        let same_term_longer = LogId::new(Term::new(2), LogIndex::new(4));
        assert!(same_term_longer > new_short);
        assert!(LogId::ZERO < old_long);
    }

    #[test]
    fn timestamp_from_system_time() {
        let t = UNIX_EPOCH + Duration::from_millis(1_700_000_000_123);
        assert_eq!(
            Timestamp::from_system_time(t).as_millis(),
            1_700_000_000_123
        );

        let before = UNIX_EPOCH - Duration::from_secs(1);
        assert!(Timestamp::from_system_time(before).is_unset());
        assert!(Timestamp::UNSET.is_unset());
    }
}
