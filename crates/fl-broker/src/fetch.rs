//! Reading records by topic and offset.

use fl_storage::Log;
use fl_types::{Command, Offset, Record};

use crate::error::{Error, Result};
use crate::state::TopicState;

/// Bounds on one fetch.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct FetchLimits {
    /// Most records to return.
    pub max_records: usize,
    /// Most record bytes to return, counting keys and values. If a record
    /// exists at the requested offset it is returned even when it alone is
    /// larger than this, so a consumer can always make progress.
    pub max_bytes: usize,
}

impl Default for FetchLimits {
    fn default() -> Self {
        FetchLimits {
            max_records: 500,
            max_bytes: 1 << 20,
        }
    }
}

/// A record together with its position in the topic.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FetchedRecord {
    /// The record's offset.
    pub offset: Offset,
    /// The record.
    pub record: Record,
}

/// The result of a fetch.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Fetched {
    /// Records in offset order with no gaps, starting at the requested
    /// offset. Empty if that offset is at or past the high watermark.
    pub records: Vec<FetchedRecord>,
    /// Offset the next committed record will receive.
    pub high_watermark: Offset,
}

impl TopicState {
    /// Reads records from `topic` starting at `offset`.
    ///
    /// Only applied entries are visible, and only committed entries are
    /// ever applied, so a fetch never returns a record that could later be
    /// rolled back.
    ///
    /// `log` must be the log this state was built from.
    pub fn fetch(
        &self,
        log: &Log,
        topic: &str,
        offset: Offset,
        limits: FetchLimits,
    ) -> Result<Fetched> {
        let id = self
            .topic_id(topic)
            .ok_or_else(|| Error::UnknownTopic(topic.to_owned()))?;
        let Some(topic) = self.topic(id) else {
            return Err(Error::UnknownTopic(topic.to_owned()));
        };
        let mut fetched = Fetched {
            records: Vec::new(),
            high_watermark: topic.next_offset,
        };
        if limits.max_records == 0 {
            return Ok(fetched);
        }
        let Some(first) = topic.locate(offset) else {
            return Ok(fetched);
        };

        let mut bytes = 0usize;
        let mut next = offset;
        'batches: for row in &topic.batches[first..] {
            let inconsistent = |reason| Error::Inconsistent {
                index: row.index,
                reason,
            };
            let entry = log
                .entry(row.index)?
                .ok_or_else(|| inconsistent("the offset index points outside the log"))?;
            let Command::Append(batch) = entry.command else {
                return Err(inconsistent(
                    "the offset index points at a non-append entry",
                ));
            };
            if batch.topic != id || batch.records.len() != row.count as usize {
                return Err(inconsistent(
                    "the entry does not match its offset index row",
                ));
            }

            // `next` is inside this batch on the first pass and equal to
            // its base on every later one.
            let skip = (next.get() - row.base.get()) as usize;
            for (position, record) in batch.records.into_iter().enumerate().skip(skip) {
                let size = record.payload_len();
                let over_bytes = !fetched.records.is_empty() && bytes + size > limits.max_bytes;
                if fetched.records.len() >= limits.max_records || over_bytes {
                    break 'batches;
                }
                bytes += size;
                fetched.records.push(FetchedRecord {
                    offset: row.base.advance(position as u64),
                    record,
                });
            }
            next = row.base.advance(u64::from(row.count));
        }
        Ok(fetched)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{append, create, noop, TempDir};
    use fl_storage::LogConfig;
    use fl_types::{LogEntry, LogIndex};

    /// Builds a log and a state from the same entries.
    fn setup(dir: &TempDir, entries: &[LogEntry]) -> (Log, TopicState) {
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        log.append(entries).unwrap();
        let mut state = TopicState::new();
        state
            .apply_through(&log, log.last_index(), |_, _| {})
            .unwrap();
        (log, state)
    }

    /// Topic 1 gets batches of 3, 1, and 4 records (offsets 0..8) with
    /// other entries interleaved. Topic 2 gets one batch of 2.
    fn interleaved() -> Vec<LogEntry> {
        vec![
            create(1, "quotes"),
            create(2, "trades"),
            append(3, 1, 3),
            noop(4),
            append(5, 2, 2),
            append(6, 1, 1),
            append(7, 1, 4),
        ]
    }

    fn all() -> FetchLimits {
        FetchLimits {
            max_records: usize::MAX,
            max_bytes: usize::MAX,
        }
    }

    fn offsets(fetched: &Fetched) -> Vec<u64> {
        fetched.records.iter().map(|r| r.offset.get()).collect()
    }

    #[test]
    fn fetch_returns_records_in_offset_order_across_batches() {
        let dir = TempDir::new("fetch-order");
        let (log, state) = setup(&dir, &interleaved());

        let fetched = state.fetch(&log, "quotes", Offset::ZERO, all()).unwrap();
        assert_eq!(offsets(&fetched), [0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(fetched.high_watermark, Offset::new(8));

        // The test helper tags each value with its log index and position,
        // which shows the right record came back for each offset.
        let values: Vec<&[u8]> = fetched
            .records
            .iter()
            .map(|r| r.record.value.as_slice())
            .collect();
        assert_eq!(
            values,
            [
                b"3:0".as_slice(),
                b"3:1",
                b"3:2",
                b"6:0",
                b"7:0",
                b"7:1",
                b"7:2",
                b"7:3"
            ]
        );

        let trades = state.fetch(&log, "trades", Offset::ZERO, all()).unwrap();
        assert_eq!(offsets(&trades), [0, 1]);
        assert_eq!(trades.high_watermark, Offset::new(2));
    }

    #[test]
    fn fetch_can_start_anywhere() {
        let dir = TempDir::new("fetch-start");
        let (log, state) = setup(&dir, &interleaved());
        for start in 0..8u64 {
            let fetched = state
                .fetch(&log, "quotes", Offset::new(start), all())
                .unwrap();
            assert_eq!(offsets(&fetched), (start..8).collect::<Vec<_>>());
        }
    }

    #[test]
    fn fetch_at_or_past_the_high_watermark_is_empty() {
        let dir = TempDir::new("fetch-past");
        let (log, state) = setup(&dir, &interleaved());
        for start in [8, 9, u64::MAX] {
            let fetched = state
                .fetch(&log, "quotes", Offset::new(start), all())
                .unwrap();
            assert!(fetched.records.is_empty());
            assert_eq!(fetched.high_watermark, Offset::new(8));
        }
    }

    #[test]
    fn fetch_honours_the_record_limit() {
        let dir = TempDir::new("fetch-records");
        let (log, state) = setup(&dir, &interleaved());
        let limit = |max_records| FetchLimits {
            max_records,
            max_bytes: usize::MAX,
        };
        let fetch = |start, max| {
            let fetched = state
                .fetch(&log, "quotes", Offset::new(start), limit(max))
                .unwrap();
            offsets(&fetched)
        };
        assert_eq!(fetch(0, 0), Vec::<u64>::new());
        assert_eq!(fetch(0, 1), [0]);
        assert_eq!(fetch(0, 2), [0, 1]);
        // stops in the middle of the third batch
        assert_eq!(fetch(1, 5), [1, 2, 3, 4, 5]);
        assert_eq!(fetch(6, 100), [6, 7]);
    }

    #[test]
    fn fetch_honours_the_byte_limit_but_always_returns_one() {
        let dir = TempDir::new("fetch-bytes");
        let (log, state) = setup(&dir, &interleaved());
        // every test record's value is three bytes
        let limit = |max_bytes| FetchLimits {
            max_records: usize::MAX,
            max_bytes,
        };
        let fetch = |start, max| {
            let fetched = state
                .fetch(&log, "quotes", Offset::new(start), limit(max))
                .unwrap();
            offsets(&fetched)
        };
        assert_eq!(fetch(0, 0), [0]);
        assert_eq!(fetch(0, 2), [0]);
        assert_eq!(fetch(0, 3), [0]);
        assert_eq!(fetch(0, 8), [0, 1]);
        assert_eq!(fetch(0, 9), [0, 1, 2]);
        // crosses from the first batch into the second and third
        assert_eq!(fetch(2, 9), [2, 3, 4]);
    }

    #[test]
    fn fetch_from_an_empty_or_missing_topic() {
        let dir = TempDir::new("fetch-empty");
        let (log, state) = setup(&dir, &[create(1, "quotes")]);
        let fetched = state.fetch(&log, "quotes", Offset::ZERO, all()).unwrap();
        assert!(fetched.records.is_empty());
        assert_eq!(fetched.high_watermark, Offset::ZERO);

        assert!(matches!(
            state.fetch(&log, "nope", Offset::ZERO, all()),
            Err(Error::UnknownTopic(name)) if name == "nope"
        ));
    }

    #[test]
    fn unapplied_entries_are_invisible() {
        let dir = TempDir::new("fetch-uncommitted");
        let entries = interleaved();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        log.append(&entries).unwrap();

        // Only the first five entries are committed.
        let mut state = TopicState::new();
        state
            .apply_through(&log, LogIndex::new(5), |_, _| {})
            .unwrap();
        let fetched = state.fetch(&log, "quotes", Offset::ZERO, all()).unwrap();
        assert_eq!(offsets(&fetched), [0, 1, 2]);
        assert_eq!(fetched.high_watermark, Offset::new(3));

        // The rest commits later.
        state
            .apply_through(&log, LogIndex::new(7), |_, _| {})
            .unwrap();
        let fetched = state.fetch(&log, "quotes", Offset::new(3), all()).unwrap();
        assert_eq!(offsets(&fetched), [3, 4, 5, 6, 7]);
    }

    #[test]
    fn apply_through_reports_each_entry_and_stops_at_the_log_end() {
        let dir = TempDir::new("fetch-observer");
        let entries = interleaved();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        log.append(&entries).unwrap();

        let mut seen = Vec::new();
        let mut state = TopicState::new();
        state
            .apply_through(&log, LogIndex::new(7), |entry, applied| {
                seen.push((entry.index().get(), *applied));
            })
            .unwrap();
        assert_eq!(seen.len(), 7);
        assert_eq!(
            seen.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
            (1..=7).collect::<Vec<_>>()
        );
        assert_eq!(seen[3].1, crate::Applied::Nothing);

        // Already there, so nothing happens.
        state
            .apply_through(&log, LogIndex::new(7), |_, _| panic!("no entries expected"))
            .unwrap();
        state
            .apply_through(&log, LogIndex::new(3), |_, _| panic!("no entries expected"))
            .unwrap();

        // Asking for more than the log holds is a caller bug.
        assert!(matches!(
            state.apply_through(&log, LogIndex::new(8), |_, _| {}),
            Err(Error::Inconsistent { index, .. }) if index == LogIndex::new(8)
        ));
    }

    #[test]
    fn a_state_built_from_another_log_is_detected() {
        let dir_a = TempDir::new("fetch-mismatch-a");
        let dir_b = TempDir::new("fetch-mismatch-b");
        let (_log_a, state) = setup(&dir_a, &interleaved());

        // A log where index 3 is not an append to topic 1.
        let (log_b, _) = setup(&dir_b, &[create(1, "quotes"), create(2, "trades"), noop(3)]);
        assert!(matches!(
            state.fetch(&log_b, "quotes", Offset::ZERO, all()),
            Err(Error::Inconsistent { index, .. }) if index == LogIndex::new(3)
        ));
        // And one that is simply too short.
        assert!(matches!(
            state.fetch(&log_b, "quotes", Offset::new(3), all()),
            Err(Error::Inconsistent { index, .. }) if index == LogIndex::new(6)
        ));
    }
}
