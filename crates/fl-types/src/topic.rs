//! Validated topic names.

use std::fmt;
use std::str::FromStr;

use crate::error::{Error, Result};

/// Longest allowed topic name, in bytes.
pub const MAX_TOPIC_NAME_LEN: usize = 255;

/// A validated topic name.
///
/// Names are 1 to [`MAX_TOPIC_NAME_LEN`] ASCII characters drawn from letters,
/// digits, `.`, `_`, and `-`. The names `.` and `..` are rejected because
/// storage uses the topic name as a directory name.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct TopicName(String);

impl TopicName {
    /// Validates and wraps a name.
    pub fn new(name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        match check(&name) {
            Ok(()) => Ok(TopicName(name)),
            Err(reason) => Err(Error::InvalidTopicName { name, reason }),
        }
    }

    /// The name as a string slice.
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Unwraps the name.
    #[inline]
    pub fn into_string(self) -> String {
        self.0
    }
}

fn check(name: &str) -> std::result::Result<(), &'static str> {
    if name.is_empty() {
        return Err("must not be empty");
    }
    if name.len() > MAX_TOPIC_NAME_LEN {
        return Err("longer than 255 bytes");
    }
    if name == "." || name == ".." {
        return Err("must not be '.' or '..'");
    }
    let allowed = |b: u8| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-');
    if !name.bytes().all(allowed) {
        return Err("only ASCII letters, digits, '.', '_' and '-' are allowed");
    }
    Ok(())
}

impl fmt::Display for TopicName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for TopicName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl FromStr for TopicName {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        TopicName::new(s)
    }
}

impl TryFrom<String> for TopicName {
    type Error = Error;

    fn try_from(name: String) -> Result<Self> {
        TopicName::new(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_reasonable_names() {
        for name in ["quotes", "ust.quotes-v1", "trades_2026", "a", "A.b-C_9"] {
            let topic = TopicName::new(name).unwrap();
            assert_eq!(topic.as_str(), name);
            assert_eq!(topic.to_string(), name);
        }
    }

    #[test]
    fn rejects_bad_names() {
        for name in ["", ".", "..", "has space", "slash/name", "caf\u{e9}", "a:b"] {
            let err = TopicName::new(name).unwrap_err();
            assert!(
                matches!(err, Error::InvalidTopicName { .. }),
                "{name:?} gave {err:?}"
            );
        }
    }

    #[test]
    fn enforces_length_limit() {
        assert!(TopicName::new("x".repeat(MAX_TOPIC_NAME_LEN)).is_ok());
        assert!(TopicName::new("x".repeat(MAX_TOPIC_NAME_LEN + 1)).is_err());
    }

    #[test]
    fn parses_from_str() {
        let topic: TopicName = "quotes".parse().unwrap();
        assert_eq!(topic.into_string(), "quotes");
        assert!("bad name".parse::<TopicName>().is_err());
    }
}
