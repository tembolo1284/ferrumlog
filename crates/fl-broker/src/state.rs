//! The deterministic state machine: topics and their offset indexes.

use std::collections::BTreeMap;

use fl_storage::Log;
use fl_types::{
    Command, LogEntry, LogIndex, Offset, Record, RecordBatch, Timestamp, TopicId, TopicName,
};

use crate::error::{Error, Result};

/// Largest number of topics a cluster may hold.
pub const MAX_TOPICS: usize = 10_000;

/// Entries read from the log per call while catching up.
const REPLAY_ENTRIES: usize = 1024;

/// Frame bytes read from the log per call while catching up.
const REPLAY_BYTES: usize = 4 << 20;

/// A topic as seen by clients.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TopicInfo {
    /// The topic's id.
    pub id: TopicId,
    /// The topic's name.
    pub name: TopicName,
    /// Offset the next committed record will receive. Equal to the number
    /// of records in the topic.
    pub high_watermark: Offset,
}

/// What applying one entry did.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Applied {
    /// The entry does not concern the broker: a no-op or a membership
    /// change.
    Nothing,
    /// A create-topic command was applied.
    TopicCreated {
        /// The topic's id.
        id: TopicId,
        /// False if the topic already existed and nothing changed.
        created: bool,
    },
    /// Records were appended to a topic.
    Appended {
        /// The topic written to.
        topic: TopicId,
        /// Offset given to the first record.
        base_offset: Offset,
        /// Number of records appended.
        count: u32,
    },
    /// The command was valid log content but could not take effect. It is
    /// skipped the same way on every node.
    Rejected(Rejection),
}

/// Why a committed command had no effect.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Rejection {
    /// An append named a topic id that does not exist.
    UnknownTopic(TopicId),
    /// A create-topic arrived with [`MAX_TOPICS`] topics already present.
    TopicLimit,
}

/// The outcome of [`TopicState::prepare_create_topic`].
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum CreateTopic {
    /// The topic already exists. Nothing needs to be proposed.
    Exists(TopicId),
    /// The topic is new. Propose this command to create it.
    Propose(Command),
}

/// One row of a topic's offset index: where one appended batch lives.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct BatchRef {
    /// Offset of the batch's first record.
    pub base: Offset,
    /// Log index of the entry that holds the batch.
    pub index: LogIndex,
    /// Number of records in the batch. Never zero.
    pub count: u32,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) struct Topic {
    pub name: TopicName,
    /// Ordered by `base`, contiguous, with no empty batches.
    pub batches: Vec<BatchRef>,
    pub next_offset: Offset,
}

impl Topic {
    /// Position in `batches` of the batch that holds `offset`.
    pub(crate) fn locate(&self, offset: Offset) -> Option<usize> {
        if offset >= self.next_offset {
            return None;
        }
        // The first batch has base 0, so at least one batch satisfies the
        // predicate and the subtraction cannot underflow.
        Some(self.batches.partition_point(|b| b.base <= offset) - 1)
    }
}

/// Topics and their offset indexes, built by applying committed entries.
///
/// Two states that applied the same entries compare equal. The simulator
/// relies on that to check that nodes agree.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct TopicState {
    /// Indexed by topic id minus one. Topics are never deleted.
    topics: Vec<Topic>,
    /// A `BTreeMap` so iteration order is the same on every node.
    by_name: BTreeMap<String, TopicId>,
    /// Index of the last entry applied.
    applied: LogIndex,
}

impl TopicState {
    /// An empty state that has applied nothing.
    pub fn new() -> Self {
        TopicState::default()
    }

    /// Index of the last entry applied, or [`LogIndex::ZERO`] if none.
    pub fn applied(&self) -> LogIndex {
        self.applied
    }

    /// Number of topics.
    pub fn topic_count(&self) -> usize {
        self.topics.len()
    }

    /// Every topic, in id order.
    pub fn topics(&self) -> Vec<TopicInfo> {
        self.topics
            .iter()
            .enumerate()
            .map(|(slot, topic)| TopicInfo {
                id: id_for_slot(slot),
                name: topic.name.clone(),
                high_watermark: topic.next_offset,
            })
            .collect()
    }

    /// Looks a topic up by name.
    pub fn topic_id(&self, name: &str) -> Option<TopicId> {
        self.by_name.get(name).copied()
    }

    /// Offset the next committed record in `topic` will receive.
    pub fn high_watermark(&self, topic: TopicId) -> Option<Offset> {
        self.topic(topic).map(|t| t.next_offset)
    }

    pub(crate) fn topic(&self, id: TopicId) -> Option<&Topic> {
        let slot = (id.get() as usize).checked_sub(1)?;
        self.topics.get(slot)
    }

    /// Checks a create-topic request and says what to do about it.
    ///
    /// This only reads the state. The topic exists once the returned
    /// command has been committed and applied.
    pub fn prepare_create_topic(&self, name: &str) -> Result<CreateTopic> {
        let name = TopicName::new(name)?;
        if let Some(id) = self.topic_id(name.as_str()) {
            return Ok(CreateTopic::Exists(id));
        }
        if self.topics.len() >= MAX_TOPICS {
            return Err(Error::TooManyTopics);
        }
        Ok(CreateTopic::Propose(Command::CreateTopic { name }))
    }

    /// Checks a produce request and builds the command to propose.
    ///
    /// Resolves the topic name to its id, stamps `now` on every record that
    /// has no timestamp, and enforces the size limits. Stamping happens
    /// here, before the command enters the log, so every replica stores the
    /// same timestamps.
    pub fn prepare_produce(
        &self,
        topic: &str,
        mut records: Vec<Record>,
        now: Timestamp,
    ) -> Result<Command> {
        let id = self
            .topic_id(topic)
            .ok_or_else(|| Error::UnknownTopic(topic.to_owned()))?;
        for record in &mut records {
            if record.timestamp.is_unset() {
                record.timestamp = now;
            }
        }
        let batch = RecordBatch::new(id, records);
        batch.validate()?;
        Ok(Command::Append(batch))
    }

    /// Applies one committed entry.
    ///
    /// Entries must arrive in index order with no gaps, starting right
    /// after [`TopicState::applied`]. Anything else is
    /// [`Error::OutOfOrder`] and leaves the state untouched.
    ///
    /// A command that cannot take effect is not an error. It yields
    /// [`Applied::Rejected`] and still advances the applied index, because
    /// every node must skip it identically.
    pub fn apply(&mut self, entry: &LogEntry) -> Result<Applied> {
        let expected = self.applied.next();
        if entry.index() != expected {
            return Err(Error::OutOfOrder {
                expected,
                got: entry.index(),
            });
        }

        let applied = match &entry.command {
            Command::Noop | Command::Reconfigure(_) => Applied::Nothing,
            Command::CreateTopic { name } => self.apply_create(name),
            Command::Append(batch) => self.apply_append(batch, entry.index()),
        };
        self.applied = entry.index();
        Ok(applied)
    }

    fn apply_create(&mut self, name: &TopicName) -> Applied {
        if let Some(id) = self.topic_id(name.as_str()) {
            return Applied::TopicCreated { id, created: false };
        }
        if self.topics.len() >= MAX_TOPICS {
            return Applied::Rejected(Rejection::TopicLimit);
        }
        let id = id_for_slot(self.topics.len());
        self.topics.push(Topic {
            name: name.clone(),
            batches: Vec::new(),
            next_offset: Offset::ZERO,
        });
        self.by_name.insert(name.as_str().to_owned(), id);
        Applied::TopicCreated { id, created: true }
    }

    fn apply_append(&mut self, batch: &RecordBatch, index: LogIndex) -> Applied {
        let slot = (batch.topic.get() as usize).wrapping_sub(1);
        let Some(topic) = self.topics.get_mut(slot) else {
            return Applied::Rejected(Rejection::UnknownTopic(batch.topic));
        };
        // The leader never proposes an empty batch. If one is in the log
        // anyway it gets no index row, which keeps `locate` sound.
        let Ok(count) = u32::try_from(batch.records.len()) else {
            return Applied::Nothing;
        };
        if count == 0 {
            return Applied::Nothing;
        }
        let base = topic.next_offset;
        topic.batches.push(BatchRef { base, index, count });
        topic.next_offset = base.advance(u64::from(count));
        Applied::Appended {
            topic: batch.topic,
            base_offset: base,
            count,
        }
    }

    /// Reads entries from `log` and applies them until the state has
    /// applied `through`.
    ///
    /// This is how the state catches up: on startup it replays the whole
    /// log, and in a cluster it runs each time the commit index advances.
    /// `observer` is called once per entry, after it is applied.
    ///
    /// `through` must not exceed what `log` holds. Only committed entries
    /// may be applied, so the caller passes its commit index.
    pub fn apply_through(
        &mut self,
        log: &Log,
        through: LogIndex,
        mut observer: impl FnMut(&LogEntry, &Applied),
    ) -> Result<()> {
        while self.applied < through {
            let wanted = through.entries_since(self.applied);
            let max_entries = usize::try_from(wanted)
                .unwrap_or(REPLAY_ENTRIES)
                .min(REPLAY_ENTRIES);
            let entries = log.read(self.applied.next(), max_entries, REPLAY_BYTES)?;
            if entries.is_empty() {
                return Err(Error::Inconsistent {
                    index: self.applied.next(),
                    reason: "the log ends before the index to apply through",
                });
            }
            for entry in &entries {
                let applied = self.apply(entry)?;
                observer(entry, &applied);
            }
        }
        Ok(())
    }
}

fn id_for_slot(slot: usize) -> TopicId {
    // `MAX_TOPICS` keeps the slot far below `u32::MAX`.
    TopicId::new(slot as u32 + 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{append, create, noop, reconfigure};
    use fl_types::Term;

    fn ts(ms: u64) -> Timestamp {
        Timestamp::from_millis(ms)
    }

    /// A state with topics `quotes` (id 1) and `trades` (id 2).
    fn two_topics() -> TopicState {
        let mut state = TopicState::new();
        state.apply(&create(1, "quotes")).unwrap();
        state.apply(&create(2, "trades")).unwrap();
        state
    }

    #[test]
    fn new_state_is_empty() {
        let state = TopicState::new();
        assert_eq!(state.applied(), LogIndex::ZERO);
        assert_eq!(state.topic_count(), 0);
        assert!(state.topics().is_empty());
        assert_eq!(state.topic_id("quotes"), None);
        assert_eq!(state.high_watermark(TopicId::new(1)), None);
        assert_eq!(state.high_watermark(TopicId::new(0)), None);
    }

    #[test]
    fn topics_get_sequential_ids() {
        let mut state = TopicState::new();
        assert_eq!(
            state.apply(&create(1, "quotes")).unwrap(),
            Applied::TopicCreated {
                id: TopicId::new(1),
                created: true
            }
        );
        assert_eq!(
            state.apply(&create(2, "trades")).unwrap(),
            Applied::TopicCreated {
                id: TopicId::new(2),
                created: true
            }
        );
        assert_eq!(state.topic_id("trades"), Some(TopicId::new(2)));
        let names: Vec<String> = state
            .topics()
            .iter()
            .map(|t| format!("{}={}", t.id.get(), t.name))
            .collect();
        assert_eq!(names, ["1=quotes", "2=trades"]);
    }

    #[test]
    fn creating_an_existing_topic_changes_nothing() {
        let mut state = two_topics();
        let before = state.topics();
        assert_eq!(
            state.apply(&create(3, "quotes")).unwrap(),
            Applied::TopicCreated {
                id: TopicId::new(1),
                created: false
            }
        );
        assert_eq!(state.topics(), before);
        assert_eq!(state.applied(), LogIndex::new(3));
    }

    #[test]
    fn appends_assign_consecutive_offsets_per_topic() {
        let mut state = two_topics();
        let results = [
            state.apply(&append(3, 1, 3)).unwrap(),
            state.apply(&append(4, 2, 5)).unwrap(),
            state.apply(&append(5, 1, 2)).unwrap(),
        ];
        let appended = |topic: u32, base: u64, count: u32| Applied::Appended {
            topic: TopicId::new(topic),
            base_offset: Offset::new(base),
            count,
        };
        assert_eq!(
            results,
            [appended(1, 0, 3), appended(2, 0, 5), appended(1, 3, 2)]
        );
        assert_eq!(state.high_watermark(TopicId::new(1)), Some(Offset::new(5)));
        assert_eq!(state.high_watermark(TopicId::new(2)), Some(Offset::new(5)));
    }

    #[test]
    fn noop_and_reconfigure_use_a_log_index_but_no_offset() {
        let mut state = two_topics();
        assert_eq!(state.apply(&noop(3)).unwrap(), Applied::Nothing);
        assert_eq!(state.apply(&reconfigure(4)).unwrap(), Applied::Nothing);
        state.apply(&append(5, 1, 1)).unwrap();
        assert_eq!(state.applied(), LogIndex::new(5));
        assert_eq!(state.high_watermark(TopicId::new(1)), Some(Offset::new(1)));
    }

    #[test]
    fn append_to_a_missing_topic_is_rejected_but_consumed() {
        let mut state = two_topics();
        for topic in [0, 3, u32::MAX] {
            let index = state.applied().next().get();
            assert_eq!(
                state.apply(&append(index, topic, 2)).unwrap(),
                Applied::Rejected(Rejection::UnknownTopic(TopicId::new(topic)))
            );
            assert_eq!(state.applied(), LogIndex::new(index));
        }
        assert_eq!(state.high_watermark(TopicId::new(1)), Some(Offset::ZERO));
    }

    #[test]
    fn empty_batch_gets_no_index_row() {
        let mut state = two_topics();
        assert_eq!(state.apply(&append(3, 1, 0)).unwrap(), Applied::Nothing);
        state.apply(&append(4, 1, 2)).unwrap();
        let topic = state.topic(TopicId::new(1)).unwrap();
        assert_eq!(topic.batches.len(), 1);
        assert_eq!(topic.locate(Offset::ZERO), Some(0));
    }

    #[test]
    fn apply_enforces_index_order() {
        let mut state = two_topics();
        let before = state.clone();
        for index in [0, 1, 2, 4, 100] {
            let err = state.apply(&noop(index)).unwrap_err();
            assert!(
                matches!(err, Error::OutOfOrder { expected, got }
                    if expected == LogIndex::new(3) && got == LogIndex::new(index)),
                "index {index} gave {err:?}"
            );
        }
        assert_eq!(state, before);
        state.apply(&noop(3)).unwrap();
    }

    #[test]
    fn same_entries_give_equal_states() {
        let entries = [
            create(1, "quotes"),
            noop(2),
            append(3, 1, 4),
            create(4, "trades"),
            append(5, 9, 1),
            append(6, 2, 2),
            create(7, "quotes"),
            append(8, 1, 1),
        ];
        let mut a = TopicState::new();
        let mut b = TopicState::new();
        for entry in &entries {
            a.apply(entry).unwrap();
        }
        for entry in &entries {
            b.apply(entry).unwrap();
        }
        assert_eq!(a, b);

        // A state that is one entry behind is not equal.
        let mut c = TopicState::new();
        for entry in &entries[..7] {
            c.apply(entry).unwrap();
        }
        assert_ne!(a, c);
    }

    #[test]
    fn term_does_not_affect_the_state() {
        let mut a = TopicState::new();
        let mut b = TopicState::new();
        let mut entry = create(1, "quotes");
        a.apply(&entry).unwrap();
        entry.id.term = Term::new(9);
        b.apply(&entry).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn locate_finds_the_batch_holding_an_offset() {
        let mut state = two_topics();
        // batches of 3, 1, 4 records: offsets 0..3, 3..4, 4..8
        state.apply(&append(3, 1, 3)).unwrap();
        state.apply(&append(4, 1, 1)).unwrap();
        state.apply(&append(5, 1, 4)).unwrap();
        let topic = state.topic(TopicId::new(1)).unwrap();
        let slots: Vec<Option<usize>> = (0..=8).map(|o| topic.locate(Offset::new(o))).collect();
        assert_eq!(
            slots,
            [
                Some(0),
                Some(0),
                Some(0),
                Some(1),
                Some(2),
                Some(2),
                Some(2),
                Some(2),
                None
            ]
        );
        assert_eq!(topic.batches[2].index, LogIndex::new(5));

        let empty = state.topic(TopicId::new(2)).unwrap();
        assert_eq!(empty.locate(Offset::ZERO), None);
    }

    #[test]
    fn prepare_create_topic_checks_name_and_existence() {
        let state = two_topics();
        assert_eq!(
            state.prepare_create_topic("quotes").unwrap(),
            CreateTopic::Exists(TopicId::new(1))
        );
        match state.prepare_create_topic("curves").unwrap() {
            CreateTopic::Propose(Command::CreateTopic { name }) => {
                assert_eq!(name.as_str(), "curves");
            }
            other => panic!("unexpected {other:?}"),
        }
        assert!(matches!(
            state.prepare_create_topic("bad name"),
            Err(Error::Invalid(fl_types::Error::InvalidTopicName { .. }))
        ));
        // preparing never changes the state
        assert_eq!(state, two_topics());
    }

    #[test]
    fn topic_limit_is_enforced_when_preparing_and_when_applying() {
        let mut state = TopicState::new();
        for i in 0..MAX_TOPICS {
            state
                .apply(&create(i as u64 + 1, &format!("t{i}")))
                .unwrap();
        }
        assert_eq!(state.topic_count(), MAX_TOPICS);
        assert!(matches!(
            state.prepare_create_topic("one-more"),
            Err(Error::TooManyTopics)
        ));
        // an existing name is still fine
        assert!(matches!(
            state.prepare_create_topic("t0"),
            Ok(CreateTopic::Exists(_))
        ));
        let next = MAX_TOPICS as u64 + 1;
        assert_eq!(
            state.apply(&create(next, "one-more")).unwrap(),
            Applied::Rejected(Rejection::TopicLimit)
        );
        assert_eq!(state.topic_count(), MAX_TOPICS);
        assert_eq!(state.applied(), LogIndex::new(next));
    }

    #[test]
    fn prepare_produce_resolves_the_topic_and_stamps_time() {
        let state = two_topics();
        let records = vec![
            Record::new("a"),
            Record::new("b").with_timestamp(ts(5)),
            Record::new("c").with_key("k"),
        ];
        let command = state.prepare_produce("trades", records, ts(1_000)).unwrap();
        let Command::Append(batch) = command else {
            panic!("expected an append");
        };
        assert_eq!(batch.topic, TopicId::new(2));
        let stamps: Vec<u64> = batch
            .records
            .iter()
            .map(|r| r.timestamp.as_millis())
            .collect();
        // a producer-supplied timestamp is kept
        assert_eq!(stamps, [1_000, 5, 1_000]);
        assert_eq!(batch.records[2].key.as_deref(), Some(b"k".as_slice()));
    }

    #[test]
    fn prepare_produce_rejects_bad_requests() {
        let state = two_topics();
        assert!(matches!(
            state.prepare_produce("nope", vec![Record::new("a")], ts(1)),
            Err(Error::UnknownTopic(name)) if name == "nope"
        ));
        assert!(matches!(
            state.prepare_produce("quotes", vec![], ts(1)),
            Err(Error::Invalid(fl_types::Error::EmptyBatch))
        ));
        let huge = Record::new(vec![0u8; fl_types::MAX_RECORD_BYTES + 1]);
        assert!(matches!(
            state.prepare_produce("quotes", vec![huge], ts(1)),
            Err(Error::Invalid(fl_types::Error::RecordTooLarge { .. }))
        ));
    }
}
