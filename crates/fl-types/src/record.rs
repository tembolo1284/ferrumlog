//! Records and record batches, the payload that producers write.

use crate::error::{Error, Result};
use crate::ids::{Timestamp, TopicId};

/// Largest allowed record, counting key and value bytes.
pub const MAX_RECORD_BYTES: usize = 1 << 20;

/// Largest allowed batch, counting key and value bytes of every record.
///
/// Kept well under tonic's default 4 MiB message limit so a batch always
/// fits in one gRPC message with room for framing.
pub const MAX_BATCH_BYTES: usize = 2 << 20;

/// Largest allowed number of records in one batch.
pub const MAX_BATCH_RECORDS: usize = 10_000;

/// One message in a topic.
#[derive(Clone, PartialEq, Eq, Hash, Debug, Default)]
pub struct Record {
    /// Optional key. Opaque to the log.
    pub key: Option<Vec<u8>>,
    /// The message body. Opaque to the log.
    pub value: Vec<u8>,
    /// Producer timestamp. If [`Timestamp::UNSET`], the leader stamps the
    /// record before proposing it, so every replica stores the same value.
    pub timestamp: Timestamp,
}

impl Record {
    /// A record with a value and no key or timestamp.
    pub fn new(value: impl Into<Vec<u8>>) -> Self {
        Record {
            key: None,
            value: value.into(),
            timestamp: Timestamp::UNSET,
        }
    }

    /// Sets the key.
    #[must_use]
    pub fn with_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Sets the timestamp.
    #[must_use]
    pub fn with_timestamp(mut self, timestamp: Timestamp) -> Self {
        self.timestamp = timestamp;
        self
    }

    /// Key bytes plus value bytes.
    pub fn payload_len(&self) -> usize {
        self.key.as_ref().map_or(0, Vec::len) + self.value.len()
    }

    /// Checks the record against [`MAX_RECORD_BYTES`].
    pub fn validate(&self) -> Result<()> {
        let size = self.payload_len();
        if size > MAX_RECORD_BYTES {
            return Err(Error::RecordTooLarge {
                size,
                max: MAX_RECORD_BYTES,
            });
        }
        Ok(())
    }
}

/// A group of records appended to one topic as a single log entry.
///
/// A batch commits or fails as a unit. Its records receive consecutive
/// offsets when the entry is applied.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct RecordBatch {
    /// Destination topic.
    pub topic: TopicId,
    /// The records, in the order they will appear in the topic.
    pub records: Vec<Record>,
}

impl RecordBatch {
    /// Builds a batch. Call [`RecordBatch::validate`] before proposing it.
    pub fn new(topic: TopicId, records: Vec<Record>) -> Self {
        RecordBatch { topic, records }
    }

    /// Number of records.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// True if the batch holds no records.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Key and value bytes summed over every record.
    pub fn payload_len(&self) -> usize {
        self.records.iter().map(Record::payload_len).sum()
    }

    /// Checks the batch and each record against the size limits.
    pub fn validate(&self) -> Result<()> {
        if self.records.is_empty() {
            return Err(Error::EmptyBatch);
        }
        if self.records.len() > MAX_BATCH_RECORDS {
            return Err(Error::TooManyRecords {
                count: self.records.len(),
                max: MAX_BATCH_RECORDS,
            });
        }
        let mut total = 0usize;
        for record in &self.records {
            record.validate()?;
            total += record.payload_len();
        }
        if total > MAX_BATCH_BYTES {
            return Err(Error::BatchTooLarge {
                size: total,
                max: MAX_BATCH_BYTES,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic() -> TopicId {
        TopicId::new(1)
    }

    #[test]
    fn builder_sets_fields() {
        let r = Record::new("px=99.5")
            .with_key("UST10Y")
            .with_timestamp(Timestamp::from_millis(5));
        assert_eq!(r.key.as_deref(), Some(b"UST10Y".as_slice()));
        assert_eq!(r.value, b"px=99.5");
        assert_eq!(r.timestamp.as_millis(), 5);
        assert_eq!(r.payload_len(), 6 + 7);
    }

    #[test]
    fn record_size_limit() {
        assert!(Record::new(vec![0u8; MAX_RECORD_BYTES]).validate().is_ok());

        let over = Record::new(vec![0u8; MAX_RECORD_BYTES]).with_key("k");
        assert_eq!(
            over.validate(),
            Err(Error::RecordTooLarge {
                size: MAX_RECORD_BYTES + 1,
                max: MAX_RECORD_BYTES
            })
        );
    }

    #[test]
    fn empty_batch_rejected() {
        let batch = RecordBatch::new(topic(), vec![]);
        assert!(batch.is_empty());
        assert_eq!(batch.validate(), Err(Error::EmptyBatch));
    }

    #[test]
    fn batch_byte_limit() {
        let half = MAX_BATCH_BYTES / 2;
        let ok = RecordBatch::new(topic(), vec![Record::new(vec![0u8; half]); 2]);
        assert_eq!(ok.payload_len(), MAX_BATCH_BYTES);
        assert!(ok.validate().is_ok());

        let mut records = ok.records;
        records.push(Record::new("x"));
        let over = RecordBatch::new(topic(), records);
        assert_eq!(
            over.validate(),
            Err(Error::BatchTooLarge {
                size: MAX_BATCH_BYTES + 1,
                max: MAX_BATCH_BYTES
            })
        );
    }

    #[test]
    fn batch_count_limit() {
        let batch = RecordBatch::new(topic(), vec![Record::new("x"); MAX_BATCH_RECORDS + 1]);
        assert_eq!(batch.len(), MAX_BATCH_RECORDS + 1);
        assert_eq!(
            batch.validate(),
            Err(Error::TooManyRecords {
                count: MAX_BATCH_RECORDS + 1,
                max: MAX_BATCH_RECORDS
            })
        );
    }

    #[test]
    fn oversized_record_fails_the_batch() {
        let batch = RecordBatch::new(
            topic(),
            vec![
                Record::new("ok"),
                Record::new(vec![0u8; MAX_RECORD_BYTES + 1]),
            ],
        );
        assert!(matches!(
            batch.validate(),
            Err(Error::RecordTooLarge { .. })
        ));
    }
}
