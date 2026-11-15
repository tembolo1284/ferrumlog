//! A single-node broker with no replication.

use std::path::Path;

use fl_storage::{Log, LogConfig};
use fl_types::{Command, LogEntry, Offset, Record, Term, Timestamp, TopicId};

use crate::error::{Error, Result};
use crate::fetch::{FetchLimits, Fetched};
use crate::state::{Applied, CreateTopic, Rejection, TopicInfo, TopicState};

/// The result of a successful produce.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Produced {
    /// Offset given to the first record.
    pub base_offset: Offset,
    /// Number of records written.
    pub count: u32,
}

/// One node, one log, no consensus.
///
/// With a single node there is nobody to agree with, so an entry is
/// committed as soon as it is on disk. Each write is appended to the log
/// and applied to the state machine in one step.
///
/// This is the engine behind the first deployable server. The replicated
/// server keeps [`TopicState`] and the read path exactly as they are, and
/// replaces only the step in the middle: instead of appending directly, it
/// hands the command to Raft and applies it once Raft reports it committed.
pub struct Standalone {
    log: Log,
    state: TopicState,
}

impl Standalone {
    /// Opens the data directory, recovers the log, and rebuilds the topic
    /// state by replaying every entry.
    pub fn open(dir: impl AsRef<Path>, config: LogConfig) -> Result<Self> {
        let log = Log::open(dir, config)?;
        let mut state = TopicState::new();
        state.apply_through(&log, log.last_index(), |_, _| {})?;
        Ok(Standalone { log, state })
    }

    /// The underlying log.
    pub fn log(&self) -> &Log {
        &self.log
    }

    /// The topic state.
    pub fn state(&self) -> &TopicState {
        &self.state
    }

    /// Every topic, in id order.
    pub fn topics(&self) -> Vec<TopicInfo> {
        self.state.topics()
    }

    /// Creates a topic. Returns its id and whether it was newly created.
    /// Creating a topic that already exists succeeds and changes nothing.
    pub fn create_topic(&mut self, name: &str) -> Result<(TopicId, bool)> {
        let command = match self.state.prepare_create_topic(name)? {
            CreateTopic::Exists(id) => return Ok((id, false)),
            CreateTopic::Propose(command) => command,
        };
        match self.commit(command)? {
            Applied::TopicCreated { id, created } => Ok((id, created)),
            Applied::Rejected(Rejection::TopicLimit) => Err(Error::TooManyTopics),
            _ => Err(self.unexpected("create-topic applied as something else")),
        }
    }

    /// Appends records to a topic. They are on disk and readable when this
    /// returns.
    ///
    /// `now` is stamped on every record that has no timestamp of its own.
    pub fn produce(
        &mut self,
        topic: &str,
        records: Vec<Record>,
        now: Timestamp,
    ) -> Result<Produced> {
        let command = self.state.prepare_produce(topic, records, now)?;
        match self.commit(command)? {
            Applied::Appended {
                base_offset, count, ..
            } => Ok(Produced { base_offset, count }),
            Applied::Rejected(Rejection::UnknownTopic(_)) => {
                Err(Error::UnknownTopic(topic.to_owned()))
            }
            _ => Err(self.unexpected("append applied as something else")),
        }
    }

    /// Reads records from `topic` starting at `offset`.
    pub fn fetch(&self, topic: &str, offset: Offset, limits: FetchLimits) -> Result<Fetched> {
        self.state.fetch(&self.log, topic, offset, limits)
    }

    /// Appends one command to the log and applies it.
    fn commit(&mut self, command: Command) -> Result<Applied> {
        command.validate()?;
        let last = self.log.last_id();
        // There are no elections here, so the term never needs to move.
        // Term 1 is used for a fresh log because term 0 means "no entry".
        let term = last.term.max(Term::new(1));
        let entry = LogEntry::new(term, last.index.next(), command);
        self.log.append(std::slice::from_ref(&entry))?;
        self.state.apply(&entry)
    }

    fn unexpected(&self, reason: &'static str) -> Error {
        Error::Inconsistent {
            index: self.state.applied(),
            reason,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::TempDir;
    use fl_storage::SyncPolicy;
    use fl_types::LogIndex;

    fn ts(ms: u64) -> Timestamp {
        Timestamp::from_millis(ms)
    }

    fn records(values: &[&str]) -> Vec<Record> {
        values.iter().map(|v| Record::new(*v)).collect()
    }

    fn values(fetched: &Fetched) -> Vec<String> {
        fetched
            .records
            .iter()
            .map(|r| String::from_utf8(r.record.value.clone()).unwrap())
            .collect()
    }

    fn fetch_all(broker: &Standalone, topic: &str) -> Fetched {
        let limits = FetchLimits {
            max_records: usize::MAX,
            max_bytes: usize::MAX,
        };
        broker.fetch(topic, Offset::ZERO, limits).unwrap()
    }

    #[test]
    fn produce_then_fetch() {
        let dir = TempDir::new("standalone-basic");
        let mut broker = Standalone::open(dir.path(), LogConfig::default()).unwrap();
        assert!(broker.topics().is_empty());

        assert_eq!(
            broker.create_topic("quotes").unwrap(),
            (TopicId::new(1), true)
        );
        let first = broker
            .produce("quotes", records(&["a", "b"]), ts(10))
            .unwrap();
        let second = broker.produce("quotes", records(&["c"]), ts(20)).unwrap();
        assert_eq!(
            first,
            Produced {
                base_offset: Offset::new(0),
                count: 2
            }
        );
        assert_eq!(
            second,
            Produced {
                base_offset: Offset::new(2),
                count: 1
            }
        );

        let fetched = fetch_all(&broker, "quotes");
        assert_eq!(values(&fetched), ["a", "b", "c"]);
        assert_eq!(fetched.high_watermark, Offset::new(3));
        let stamps: Vec<u64> = fetched
            .records
            .iter()
            .map(|r| r.record.timestamp.as_millis())
            .collect();
        assert_eq!(stamps, [10, 10, 20]);
    }

    #[test]
    fn create_topic_is_idempotent() {
        let dir = TempDir::new("standalone-create");
        let mut broker = Standalone::open(dir.path(), LogConfig::default()).unwrap();
        assert_eq!(
            broker.create_topic("quotes").unwrap(),
            (TopicId::new(1), true)
        );
        assert_eq!(
            broker.create_topic("trades").unwrap(),
            (TopicId::new(2), true)
        );
        let entries = broker.log().len();
        assert_eq!(
            broker.create_topic("quotes").unwrap(),
            (TopicId::new(1), false)
        );
        // Nothing was written for the repeat.
        assert_eq!(broker.log().len(), entries);
        assert_eq!(broker.topics().len(), 2);
    }

    #[test]
    fn bad_requests_write_nothing() {
        let dir = TempDir::new("standalone-bad");
        let mut broker = Standalone::open(dir.path(), LogConfig::default()).unwrap();
        broker.create_topic("quotes").unwrap();
        let entries = broker.log().len();

        assert!(matches!(
            broker.create_topic("bad name"),
            Err(Error::Invalid(_))
        ));
        assert!(matches!(
            broker.produce("nope", records(&["a"]), ts(1)),
            Err(Error::UnknownTopic(_))
        ));
        assert!(matches!(
            broker.produce("quotes", vec![], ts(1)),
            Err(Error::Invalid(fl_types::Error::EmptyBatch))
        ));
        assert!(matches!(
            broker.fetch("nope", Offset::ZERO, FetchLimits::default()),
            Err(Error::UnknownTopic(_))
        ));
        assert_eq!(broker.log().len(), entries);
    }

    #[test]
    fn topics_keep_separate_offsets() {
        let dir = TempDir::new("standalone-topics");
        let mut broker = Standalone::open(dir.path(), LogConfig::default()).unwrap();
        broker.create_topic("quotes").unwrap();
        broker.create_topic("trades").unwrap();
        broker.produce("quotes", records(&["q0"]), ts(1)).unwrap();
        broker
            .produce("trades", records(&["t0", "t1"]), ts(1))
            .unwrap();
        let produced = broker.produce("quotes", records(&["q1"]), ts(1)).unwrap();
        assert_eq!(produced.base_offset, Offset::new(1));

        assert_eq!(values(&fetch_all(&broker, "quotes")), ["q0", "q1"]);
        assert_eq!(values(&fetch_all(&broker, "trades")), ["t0", "t1"]);
        let marks: Vec<u64> = broker
            .topics()
            .iter()
            .map(|t| t.high_watermark.get())
            .collect();
        assert_eq!(marks, [2, 2]);
    }

    #[test]
    fn state_is_rebuilt_on_reopen() {
        let dir = TempDir::new("standalone-reopen");
        // Small segments so the replay crosses segment files.
        let config = LogConfig {
            segment_bytes: 256,
            sync: SyncPolicy::Always,
        };
        let before = {
            let mut broker = Standalone::open(dir.path(), config).unwrap();
            broker.create_topic("quotes").unwrap();
            broker.create_topic("trades").unwrap();
            for i in 0..40 {
                let topic = if i % 3 == 0 { "trades" } else { "quotes" };
                let value = format!("{topic}-{i}");
                broker
                    .produce(topic, records(&[value.as_str()]), ts(i))
                    .unwrap();
            }
            assert!(broker.log().segment_count() > 3);
            broker.state().clone()
        };

        let mut broker = Standalone::open(dir.path(), config).unwrap();
        assert_eq!(broker.state(), &before);
        assert_eq!(broker.state().applied(), LogIndex::new(42));

        let quotes = fetch_all(&broker, "quotes");
        assert_eq!(quotes.records.len(), 26);
        assert_eq!(values(&quotes)[0], "quotes-1");
        assert_eq!(values(&fetch_all(&broker, "trades")).len(), 14);

        // Writing continues where it left off.
        let produced = broker
            .produce("quotes", records(&["later"]), ts(99))
            .unwrap();
        assert_eq!(produced.base_offset, Offset::new(26));
        assert!(!broker.create_topic("trades").unwrap().1);
    }

    #[test]
    fn entries_are_written_in_term_one() {
        let dir = TempDir::new("standalone-term");
        let mut broker = Standalone::open(dir.path(), LogConfig::default()).unwrap();
        broker.create_topic("quotes").unwrap();
        broker.produce("quotes", records(&["a"]), ts(1)).unwrap();
        assert_eq!(broker.log().last_id().term, Term::new(1));
        assert_eq!(broker.log().term(LogIndex::new(1)), Some(Term::new(1)));
    }
}
