//! The Raft node.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use fl_types::{Command, LogEntry, LogId, LogIndex, Membership, NodeId, Term};

use crate::config::{Config, ConfigError};
use crate::log::LogStore;
use crate::message::{Body, Message};
use crate::rng::Rng;

/// The part of a node's state that must survive a crash.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct HardState {
    /// The latest term this node has seen.
    pub term: Term,
    /// The candidate this node voted for in `term`, if any.
    pub voted_for: Option<NodeId>,
}

/// What a node is doing right now.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
    /// Accepts entries from a leader.
    Follower,
    /// Asking for votes.
    Candidate,
    /// Accepts proposals and replicates them.
    Leader,
}

impl Role {
    /// A lower-case name, for logs and status output.
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Follower => "follower",
            Role::Candidate => "candidate",
            Role::Leader => "leader",
        }
    }
}

/// Work for the driver, produced by [`Node::ready`].
///
/// Handle the fields in the order they are declared. The crate
/// documentation explains why the order matters.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Ready {
    /// Set when the term or vote changed. Write it durably first.
    pub hard_state: Option<HardState>,
    /// True if the log was written to since the last `Ready`. Sync it
    /// before going further.
    pub must_sync: bool,
    /// Messages to send, once the two fields above are dealt with.
    pub messages: Vec<Message>,
    /// Every entry up to this index is committed and safe to apply.
    pub commit_index: LogIndex,
}

impl Ready {
    /// True if there is nothing to persist and nothing to send.
    pub fn is_idle(&self) -> bool {
        self.hard_state.is_none() && !self.must_sync && self.messages.is_empty()
    }
}

/// A snapshot of a node, for status endpoints, metrics, and tests.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Status {
    /// The node's id.
    pub id: NodeId,
    /// Its current term.
    pub term: Term,
    /// Its current role.
    pub role: Role,
    /// The leader of the current term, if this node knows it.
    pub leader: Option<NodeId>,
    /// Who it voted for in the current term.
    pub voted_for: Option<NodeId>,
    /// Term and index of its last log entry.
    pub last_log: LogId,
    /// The highest index it knows to be committed.
    pub commit_index: LogIndex,
}

/// Why a proposal was not accepted.
#[derive(Debug)]
pub enum ProposeError<E> {
    /// This node is not the leader. Retry against `leader` if it is set.
    NotLeader {
        /// The leader this node believes in, if any.
        leader: Option<NodeId>,
    },
    /// The log failed. The node must be stopped.
    Storage(E),
}

impl<E: fmt::Display> fmt::Display for ProposeError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProposeError::NotLeader { leader: Some(id) } => {
                write!(f, "not the leader, try {id}")
            }
            ProposeError::NotLeader { leader: None } => {
                f.write_str("not the leader, and no leader is known")
            }
            ProposeError::Storage(e) => write!(f, "log storage failed: {e}"),
        }
    }
}

impl<E: fmt::Debug + fmt::Display> std::error::Error for ProposeError<E> {}

/// What the leader knows about one follower's log.
#[derive(Clone, Copy, Debug)]
struct Progress {
    /// The next index to send. Advanced as soon as entries are sent,
    /// without waiting for the answer, and pulled back if the follower
    /// turns out not to have them.
    next: LogIndex,
    /// The highest index known to be stored on the follower.
    matched: LogIndex,
}

/// State that exists only while leading.
#[derive(Debug)]
struct Leading {
    progress: BTreeMap<NodeId, Progress>,
    /// Followers that have answered since the last quorum check.
    heard: BTreeSet<NodeId>,
}

#[derive(Debug)]
enum State {
    Follower { leader: Option<NodeId> },
    Candidate { votes: BTreeSet<NodeId> },
    Leader(Leading),
}

/// One Raft node. See the crate documentation for how to drive it.
#[derive(Debug)]
pub struct Node<L: LogStore> {
    id: NodeId,
    voters: Membership,
    config: Config,
    log: L,
    hard: HardState,
    state: State,
    commit: LogIndex,
    /// Ticks since the election timer was last reset. A leader uses the
    /// same counter to time its quorum check.
    election_elapsed: u32,
    /// The current, randomly chosen election timeout.
    election_timeout: u32,
    heartbeat_elapsed: u32,
    rng: Rng,
    // Output collected until the next call to `ready`.
    messages: Vec<Message>,
    hard_dirty: bool,
    log_dirty: bool,
    /// The leader has new entries or a new commit index to tell followers
    /// about. Acted on in `ready`, so that many proposals made together go
    /// out in one message per follower.
    broadcast: bool,
}

impl<L: LogStore> Node<L> {
    /// Builds a node from what it had on disk.
    ///
    /// For a brand-new node pass `HardState::default()` and an empty log.
    /// A node always starts as a follower with nothing committed. It
    /// learns the commit index again from the leader.
    pub fn new(config: Config, hard_state: HardState, log: L) -> Result<Self, ConfigError> {
        config.validate()?;
        let mut rng = Rng::new(config.seed ^ config.id.get().rotate_left(32));
        let election_timeout = rng.range(config.election_timeout_min, config.election_timeout_max);
        Ok(Node {
            id: config.id,
            voters: config.voters.clone(),
            config,
            log,
            hard: hard_state,
            state: State::Follower { leader: None },
            commit: LogIndex::ZERO,
            election_elapsed: 0,
            election_timeout,
            heartbeat_elapsed: 0,
            rng,
            messages: Vec::new(),
            hard_dirty: false,
            log_dirty: false,
            broadcast: false,
        })
    }

    /// This node's id.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// The current term.
    pub fn term(&self) -> Term {
        self.hard.term
    }

    /// The current role.
    pub fn role(&self) -> Role {
        match self.state {
            State::Follower { .. } => Role::Follower,
            State::Candidate { .. } => Role::Candidate,
            State::Leader(_) => Role::Leader,
        }
    }

    /// True if this node is the leader.
    pub fn is_leader(&self) -> bool {
        matches!(self.state, State::Leader(_))
    }

    /// The leader of the current term, if known. A leader names itself.
    pub fn leader(&self) -> Option<NodeId> {
        match self.state {
            State::Follower { leader } => leader,
            State::Candidate { .. } => None,
            State::Leader(_) => Some(self.id),
        }
    }

    /// The highest index known to be committed.
    ///
    /// Drivers should act on [`Ready::commit_index`] instead, which is only
    /// handed over together with the instruction to sync first.
    pub fn commit_index(&self) -> LogIndex {
        self.commit
    }

    /// The term and vote as they should be on disk.
    pub fn hard_state(&self) -> HardState {
        self.hard
    }

    /// The log.
    pub fn log(&self) -> &L {
        &self.log
    }

    /// The log, for the driver to sync.
    pub fn log_mut(&mut self) -> &mut L {
        &mut self.log
    }

    /// Takes the node apart and returns its log. Used when a node is shut
    /// down and its storage outlives it.
    pub fn into_log(self) -> L {
        self.log
    }

    /// The configuration this node was built with.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// A snapshot of the node.
    pub fn status(&self) -> Status {
        Status {
            id: self.id,
            term: self.hard.term,
            role: self.role(),
            leader: self.leader(),
            voted_for: self.hard.voted_for,
            last_log: self.log.last_id(),
            commit_index: self.commit,
        }
    }

    /// Advances time by one tick.
    pub fn tick(&mut self) -> Result<(), L::Error> {
        self.election_elapsed += 1;

        if let State::Leader(leading) = &mut self.state {
            // Step down if a majority has been silent for a whole election
            // timeout. Followers on the other side of a partition will have
            // elected someone else by now, and clients should stop waiting
            // on this node.
            if self.election_elapsed >= self.config.election_timeout_min {
                self.election_elapsed = 0;
                let heard = std::mem::take(&mut leading.heard);
                let reachable = heard.into_iter().chain(std::iter::once(self.id));
                if !self.voters.has_quorum(reachable) {
                    self.become_follower(self.hard.term, None);
                    return Ok(());
                }
            }

            self.heartbeat_elapsed += 1;
            if self.heartbeat_elapsed >= self.config.heartbeat_interval {
                self.heartbeat_elapsed = 0;
                self.send_append_to_all()?;
            }
            return Ok(());
        }

        if self.election_elapsed >= self.election_timeout {
            self.start_election()?;
        }
        Ok(())
    }

    /// Starts an election now, without waiting for the timeout.
    ///
    /// A cluster of one uses this at startup, so it is ready to serve at
    /// once. Does nothing on a node that already leads.
    pub fn campaign(&mut self) -> Result<(), L::Error> {
        if self.is_leader() {
            return Ok(());
        }
        self.start_election()
    }

    /// Handles a message from another node.
    ///
    /// Messages that are not addressed to this node, or that come from a
    /// node outside the voter set, are dropped.
    pub fn step(&mut self, message: Message) -> Result<(), L::Error> {
        let Message {
            from,
            to,
            term,
            body,
        } = message;
        if to != self.id || from == self.id || !self.voters.contains(from) {
            return Ok(());
        }

        if term > self.hard.term {
            // Someone is ahead of us. Whatever we were doing belongs to an
            // older term.
            self.become_follower(term, None);
        } else if term < self.hard.term {
            // The sender is behind. Answer requests with our term so it
            // catches up, and ignore its answers to things we no longer
            // care about.
            match body {
                Body::RequestVote { .. } => {
                    self.send(from, Body::RequestVoteResponse { granted: false });
                }
                Body::AppendEntries { .. } => self.send(
                    from,
                    Body::AppendEntriesResponse {
                        success: false,
                        match_index: LogIndex::ZERO,
                        conflict_index: LogIndex::ZERO,
                    },
                ),
                Body::RequestVoteResponse { .. } | Body::AppendEntriesResponse { .. } => {}
            }
            return Ok(());
        }

        // From here on the message is from our own term.
        match body {
            Body::RequestVote { last_log } => self.handle_request_vote(from, last_log),
            Body::RequestVoteResponse { granted } => self.handle_vote_response(from, granted),
            Body::AppendEntries {
                prev_log,
                entries,
                leader_commit,
            } => self.handle_append(from, prev_log, entries, leader_commit),
            Body::AppendEntriesResponse {
                success,
                match_index,
                conflict_index,
            } => self.handle_append_response(from, success, match_index, conflict_index),
        }
    }

    /// Asks this node to replicate `command`.
    ///
    /// On success the command is in the leader's log at the returned
    /// position. That does not mean it is committed. It is committed once
    /// a later [`Ready::commit_index`] reaches that index while the entry
    /// there still has the returned term. If a different leader takes over
    /// first, the entry may be replaced, and the caller has to find out
    /// from the new leader.
    ///
    /// The caller is expected to have validated the command.
    pub fn propose(&mut self, command: Command) -> Result<LogId, ProposeError<L::Error>> {
        if !self.is_leader() {
            return Err(ProposeError::NotLeader {
                leader: self.leader(),
            });
        }
        let id = self.append_own(command).map_err(ProposeError::Storage)?;
        // A cluster of one commits on its own.
        self.advance_commit().map_err(ProposeError::Storage)?;
        Ok(id)
    }

    /// Hands over everything the driver has to do, and clears it.
    pub fn ready(&mut self) -> Result<Ready, L::Error> {
        if std::mem::take(&mut self.broadcast) {
            self.send_append_to_all()?;
        }
        Ok(Ready {
            hard_state: std::mem::take(&mut self.hard_dirty).then_some(self.hard),
            must_sync: std::mem::take(&mut self.log_dirty),
            messages: std::mem::take(&mut self.messages),
            commit_index: self.commit,
        })
    }

    // ---- elections -------------------------------------------------------

    fn start_election(&mut self) -> Result<(), L::Error> {
        self.hard.term = self.hard.term.next();
        self.hard.voted_for = Some(self.id);
        self.hard_dirty = true;
        self.state = State::Candidate {
            votes: BTreeSet::from([self.id]),
        };
        self.reset_election_timer();

        if self.voters.has_quorum([self.id]) {
            // A cluster of one.
            return self.become_leader();
        }
        let last_log = self.log.last_id();
        for peer in self.peers() {
            self.send(peer, Body::RequestVote { last_log });
        }
        Ok(())
    }

    fn handle_request_vote(&mut self, candidate: NodeId, last_log: LogId) -> Result<(), L::Error> {
        // One vote per term. Asking again gets the same answer, which
        // covers a candidate whose first request or our reply was lost.
        let free_to_vote = match self.hard.voted_for {
            None => true,
            Some(already) => already == candidate,
        };
        // Only vote for a log at least as up to date as ours. LogId orders
        // by term first and index second, which is the rule from the
        // paper. This is what keeps committed entries from being lost: a
        // candidate missing one cannot collect a majority.
        let up_to_date = last_log >= self.log.last_id();
        let granted = free_to_vote && up_to_date;

        if granted {
            if self.hard.voted_for != Some(candidate) {
                self.hard.voted_for = Some(candidate);
                self.hard_dirty = true;
            }
            // Give the candidate time to win before we try ourselves.
            self.election_elapsed = 0;
        }
        self.send(candidate, Body::RequestVoteResponse { granted });
        Ok(())
    }

    fn handle_vote_response(&mut self, from: NodeId, granted: bool) -> Result<(), L::Error> {
        let State::Candidate { votes } = &mut self.state else {
            return Ok(());
        };
        if !granted {
            return Ok(());
        }
        votes.insert(from);
        if self.voters.has_quorum(votes.iter().copied()) {
            self.become_leader()?;
        }
        Ok(())
    }

    fn become_leader(&mut self) -> Result<(), L::Error> {
        let next = self.log.last_id().index.next();
        let progress = self
            .peers()
            .map(|peer| {
                let start = Progress {
                    next,
                    matched: LogIndex::ZERO,
                };
                (peer, start)
            })
            .collect();
        self.state = State::Leader(Leading {
            progress,
            heard: BTreeSet::new(),
        });
        self.election_elapsed = 0;
        self.heartbeat_elapsed = 0;

        // A leader may only count replicas for entries of its own term.
        // Appending a no-op right away gives it such an entry, and
        // committing that commits everything before it.
        self.append_own(Command::Noop)?;
        self.advance_commit()
    }

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        debug_assert!(term >= self.hard.term);
        if term > self.hard.term {
            self.hard.term = term;
            self.hard.voted_for = None;
            self.hard_dirty = true;
        }
        if self.is_leader() {
            // A leader's counter was timing quorum checks. Start a clean
            // election timeout so it does not campaign straight away.
            self.reset_election_timer();
        }
        self.state = State::Follower { leader };
        self.broadcast = false;
    }

    fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        self.election_timeout = self.rng.range(
            self.config.election_timeout_min,
            self.config.election_timeout_max,
        );
    }

    // ---- replication, follower side --------------------------------------

    fn handle_append(
        &mut self,
        leader: NodeId,
        prev_log: LogId,
        entries: Vec<LogEntry>,
        leader_commit: LogIndex,
    ) -> Result<(), L::Error> {
        // Two leaders in one term would mean two majorities voted in that
        // term, which the one-vote rule makes impossible.
        assert!(
            !self.is_leader(),
            "{} and {leader} both lead {}",
            self.id,
            self.hard.term
        );
        // A candidate that hears from a leader of its own term has lost.
        self.state = State::Follower {
            leader: Some(leader),
        };
        self.election_elapsed = 0;

        // Reject anything whose entries do not follow on from prev_log.
        let contiguous = entries
            .iter()
            .zip(prev_log.index.get() + 1..)
            .all(|(entry, index)| entry.index().get() == index);
        if !contiguous {
            return Ok(());
        }

        let last_index = self.log.last_id().index;
        let conflict_index = match self.log.term(prev_log.index)? {
            Some(term) if term == prev_log.term => None,
            // Our log is too short. Ask for everything after our end.
            None => Some(last_index.next()),
            // We hold a different entry there. Point the leader at the
            // start of that whole term, so one round trip skips all of it.
            Some(term) => {
                assert!(
                    prev_log.index > self.commit,
                    "{}: leader {leader} disagrees with committed entry {}",
                    self.id,
                    prev_log.index
                );
                let mut first = prev_log.index;
                while first.prev() > self.commit && self.log.term(first.prev())? == Some(term) {
                    first = first.prev();
                }
                Some(first)
            }
        };
        if let Some(conflict_index) = conflict_index {
            self.send(
                leader,
                Body::AppendEntriesResponse {
                    success: false,
                    match_index: LogIndex::ZERO,
                    conflict_index,
                },
            );
            return Ok(());
        }

        // Skip entries we already hold. At the first one that differs,
        // drop it and everything after it, then append the rest. Entries
        // that match are left alone, so a delayed or repeated message can
        // never shorten the log.
        let mut keep_from = entries.len();
        for (position, entry) in entries.iter().enumerate() {
            match self.log.term(entry.index())? {
                Some(term) if term == entry.term() => continue,
                Some(_) => {
                    assert!(
                        entry.index() > self.commit,
                        "{}: leader {leader} would overwrite committed entry {}",
                        self.id,
                        entry.index()
                    );
                    self.log.truncate_suffix(entry.index())?;
                    self.log_dirty = true;
                }
                None => {}
            }
            keep_from = position;
            break;
        }
        if keep_from < entries.len() {
            self.log.append(&entries[keep_from..])?;
            self.log_dirty = true;
        }

        // Only the part of our log that this message vouched for is known
        // to match the leader. Anything beyond it could be left over from
        // an old term, so it must not be committed on the leader's say-so.
        let match_index = LogIndex::new(prev_log.index.get() + entries.len() as u64);
        let commit = leader_commit.min(match_index);
        if commit > self.commit {
            self.commit = commit;
        }
        self.send(
            leader,
            Body::AppendEntriesResponse {
                success: true,
                match_index,
                conflict_index: LogIndex::ZERO,
            },
        );
        Ok(())
    }

    // ---- replication, leader side ----------------------------------------

    fn append_own(&mut self, command: Command) -> Result<LogId, L::Error> {
        let id = LogId::new(self.hard.term, self.log.last_id().index.next());
        self.log.append(&[LogEntry { id, command }])?;
        self.log_dirty = true;
        self.broadcast = true;
        Ok(id)
    }

    fn handle_append_response(
        &mut self,
        from: NodeId,
        success: bool,
        match_index: LogIndex,
        conflict_index: LogIndex,
    ) -> Result<(), L::Error> {
        let last_index = self.log.last_id().index;
        let State::Leader(leading) = &mut self.state else {
            return Ok(());
        };
        let Some(progress) = leading.progress.get_mut(&from) else {
            return Ok(());
        };
        leading.heard.insert(from);

        if success {
            // Answers can arrive late or twice, so only ever move forward.
            if match_index > progress.matched {
                progress.matched = match_index;
            }
            if progress.next <= progress.matched {
                progress.next = progress.matched.next();
            }
            let more_to_send = progress.next <= last_index;
            self.advance_commit()?;
            if more_to_send {
                self.send_append(from)?;
            }
        } else if conflict_index < progress.next {
            // The follower does not hold what we assumed. Back up to where
            // it says to resume, but never below what it has confirmed. A
            // rejection that does not move `next` back is an old one, and
            // is ignored.
            progress.next = conflict_index.max(progress.matched.next());
            self.send_append(from)?;
        }
        Ok(())
    }

    /// Moves the commit index forward if a majority now holds more.
    fn advance_commit(&mut self) -> Result<(), L::Error> {
        let State::Leader(leading) = &self.state else {
            return Ok(());
        };
        // Sort what each node holds, highest first. The value at position
        // quorum - 1 is held by at least a quorum of nodes.
        let mut matched: Vec<LogIndex> = leading
            .progress
            .values()
            .map(|progress| progress.matched)
            .collect();
        matched.push(self.log.last_id().index);
        matched.sort_unstable_by(|a, b| b.cmp(a));
        let candidate = matched[self.voters.quorum() - 1];

        // Never commit an entry from an earlier term by counting replicas.
        // Such an entry can still be overwritten even when a majority
        // holds it. It commits only as a side effect of committing an
        // entry from the current term after it.
        if candidate > self.commit && self.log.term(candidate)? == Some(self.hard.term) {
            self.commit = candidate;
            // Tell the followers.
            self.broadcast = true;
        }
        Ok(())
    }

    fn send_append_to_all(&mut self) -> Result<(), L::Error> {
        let peers: Vec<NodeId> = self.peers().collect();
        for peer in peers {
            self.send_append(peer)?;
        }
        Ok(())
    }

    /// Sends the follower whatever comes after what it was last sent. With
    /// nothing new, this is a heartbeat.
    fn send_append(&mut self, to: NodeId) -> Result<(), L::Error> {
        let State::Leader(leading) = &mut self.state else {
            return Ok(());
        };
        let Some(progress) = leading.progress.get_mut(&to) else {
            return Ok(());
        };
        let prev_index = progress.next.prev();
        let prev_term = self
            .log
            .term(prev_index)?
            .expect("the leader's log reaches every follower's next index");
        let entries = self.log.entries(
            progress.next,
            self.config.max_entries_per_message,
            self.config.max_bytes_per_message,
        )?;
        // Assume the entries will arrive. If they do not, the follower
        // rejects the next message and `next` is pulled back.
        progress.next = LogIndex::new(progress.next.get() + entries.len() as u64);

        self.messages.push(Message {
            from: self.id,
            to,
            term: self.hard.term,
            body: Body::AppendEntries {
                prev_log: LogId::new(prev_term, prev_index),
                entries,
                leader_commit: self.commit,
            },
        });
        Ok(())
    }

    // ---- helpers ---------------------------------------------------------

    fn peers(&self) -> impl Iterator<Item = NodeId> {
        let peers: Vec<NodeId> = self.voters.peers(self.id).collect();
        peers.into_iter()
    }

    fn send(&mut self, to: NodeId, body: Body) {
        self.messages.push(Message {
            from: self.id,
            to,
            term: self.hard.term,
            body,
        });
    }
}
