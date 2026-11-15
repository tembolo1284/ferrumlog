//! Scripted scenarios against small in-memory clusters.
//!
//! The network here is a queue the test controls by hand. Randomized
//! testing with faults lives in `fl-sim`.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use fl_types::{Command, LogEntry, LogId, LogIndex, Membership, NodeId, Term, TopicName};

use crate::rng::Rng;
use crate::{
    Body, Config, ConfigError, HardState, LogStore, MemLog, Message, Node, ProposeError, Role,
};

fn n(id: u64) -> NodeId {
    NodeId::new(id)
}

fn t(term: u64) -> Term {
    Term::new(term)
}

fn i(index: u64) -> LogIndex {
    LogIndex::new(index)
}

fn voters(count: u64) -> Membership {
    Membership::new((1..=count).map(NodeId::new)).unwrap()
}

/// A command that is easy to tell apart from others.
fn cmd(name: &str) -> Command {
    Command::CreateTopic {
        name: TopicName::new(name).unwrap(),
    }
}

/// A log whose entries have the given terms, at indexes 1, 2, 3 and so on.
fn log_with_terms(terms: &[u64]) -> MemLog {
    let entries = terms
        .iter()
        .zip(1u64..)
        .map(|(term, index)| LogEntry::new(t(*term), i(index), cmd(&format!("t{term}-i{index}"))))
        .collect();
    MemLog::from_entries(entries)
}

fn terms_of(node: &Node<MemLog>) -> Vec<u64> {
    node.log()
        .as_slice()
        .iter()
        .map(|entry| entry.term().get())
        .collect()
}

struct Cluster {
    nodes: BTreeMap<NodeId, Node<MemLog>>,
    queue: VecDeque<Message>,
    /// Nodes whose messages, in both directions, are thrown away.
    isolated: BTreeSet<NodeId>,
    /// Every message that was delivered, for assertions.
    delivered: Vec<Message>,
}

impl Cluster {
    fn new(count: u64) -> Self {
        Self::with(count, |_| {})
    }

    fn with(count: u64, adjust: impl Fn(&mut Config)) -> Self {
        let mut cluster = Cluster {
            nodes: BTreeMap::new(),
            queue: VecDeque::new(),
            isolated: BTreeSet::new(),
            delivered: Vec::new(),
        };
        for id in 1..=count {
            let mut config = Config::new(n(id), voters(count));
            adjust(&mut config);
            cluster.add(config, HardState::default(), MemLog::new());
        }
        cluster
    }

    fn add(&mut self, config: Config, hard: HardState, log: MemLog) {
        let id = config.id;
        self.nodes.insert(id, Node::new(config, hard, log).unwrap());
    }

    fn node(&self, id: u64) -> &Node<MemLog> {
        &self.nodes[&n(id)]
    }

    fn node_mut(&mut self, id: u64) -> &mut Node<MemLog> {
        self.nodes.get_mut(&n(id)).unwrap()
    }

    /// Moves a node's outgoing messages onto the queue.
    fn collect(&mut self, id: u64) {
        let ready = self.node_mut(id).ready().unwrap();
        self.queue.extend(ready.messages);
    }

    fn tick(&mut self, id: u64, times: u32) {
        for _ in 0..times {
            self.node_mut(id).tick().unwrap();
            self.collect(id);
        }
    }

    /// Ticks one node until it has started an election.
    fn campaign(&mut self, id: u64) {
        let term = self.node(id).term();
        for _ in 0..100 {
            if self.node(id).term() > term {
                return;
            }
            self.tick(id, 1);
        }
        panic!("node {id} never started an election");
    }

    /// Delivers queued messages, and the messages those cause, until the
    /// network is quiet.
    fn deliver_all(&mut self) {
        let mut budget = 10_000;
        while let Some(message) = self.queue.pop_front() {
            budget -= 1;
            assert!(budget > 0, "the cluster never went quiet");
            if self.isolated.contains(&message.from) || self.isolated.contains(&message.to) {
                continue;
            }
            let to = message.to;
            self.delivered.push(message.clone());
            let node = self.nodes.get_mut(&to).unwrap();
            node.step(message).unwrap();
            let ready = node.ready().unwrap();
            self.queue.extend(ready.messages);
        }
    }

    /// Makes `id` the leader and lets the cluster settle.
    fn elect(&mut self, id: u64) {
        self.campaign(id);
        self.deliver_all();
        assert!(self.node(id).is_leader(), "node {id} did not win");
    }

    fn propose(&mut self, id: u64, name: &str) -> LogId {
        let log_id = self.node_mut(id).propose(cmd(name)).unwrap();
        self.collect(id);
        log_id
    }

    fn leaders(&self) -> Vec<u64> {
        self.nodes
            .values()
            .filter(|node| node.is_leader())
            .map(|node| node.id().get())
            .collect()
    }
}

// ---- configuration -------------------------------------------------------

#[test]
fn config_is_validated() {
    let good = Config::new(n(1), voters(3));
    assert_eq!(good.validate(), Ok(()));

    let mut outsider = good.clone();
    outsider.id = n(9);
    assert_eq!(outsider.validate(), Err(ConfigError::NotAVoter(n(9))));

    let mut no_heartbeat = good.clone();
    no_heartbeat.heartbeat_interval = 0;
    assert_eq!(no_heartbeat.validate(), Err(ConfigError::ZeroHeartbeat));

    let mut short = good.clone();
    short.election_timeout_min = 2;
    assert!(matches!(
        short.validate(),
        Err(ConfigError::ElectionTimeoutTooShort { .. })
    ));

    let mut flat = good.clone();
    flat.election_timeout_max = flat.election_timeout_min;
    assert!(matches!(
        flat.validate(),
        Err(ConfigError::EmptyElectionRange { .. })
    ));

    let mut no_entries = good.clone();
    no_entries.max_entries_per_message = 0;
    assert_eq!(no_entries.validate(), Err(ConfigError::ZeroMessageLimit));

    assert!(Node::new(outsider, HardState::default(), MemLog::new()).is_err());
    assert!(ConfigError::ZeroHeartbeat.to_string().contains("heartbeat"));
}

#[test]
fn rng_is_deterministic_and_stays_in_range() {
    let mut a = Rng::new(42);
    let mut b = Rng::new(42);
    let mut c = Rng::new(43);
    let from_a: Vec<u32> = (0..100).map(|_| a.range(10, 20)).collect();
    let from_b: Vec<u32> = (0..100).map(|_| b.range(10, 20)).collect();
    let from_c: Vec<u32> = (0..100).map(|_| c.range(10, 20)).collect();
    assert_eq!(from_a, from_b);
    assert_ne!(from_a, from_c);
    assert!(from_a.iter().all(|value| (10..=20).contains(value)));
    // Every value in the range turns up.
    let seen: BTreeSet<u32> = from_a.into_iter().collect();
    assert_eq!(seen.len(), 11);
    // A zero seed still produces numbers.
    assert_ne!(Rng::new(0).next_u64(), 0);
}

// ---- elections -----------------------------------------------------------

#[test]
fn a_single_node_elects_itself_and_commits() {
    let mut cluster = Cluster::new(1);
    cluster.campaign(1);
    assert!(cluster.node(1).is_leader());
    assert_eq!(cluster.node(1).term(), t(1));
    // The no-op from taking office is already committed.
    assert_eq!(cluster.node(1).commit_index(), i(1));

    let id = cluster.node_mut(1).propose(cmd("a")).unwrap();
    assert_eq!(id, LogId::new(t(1), i(2)));
    let ready = cluster.node_mut(1).ready().unwrap();
    assert!(ready.must_sync);
    assert_eq!(ready.commit_index, i(2));
    assert!(ready.messages.is_empty());
}

#[test]
fn campaigning_on_request_skips_the_wait() {
    let mut single = Cluster::new(1);
    single.node_mut(1).campaign().unwrap();
    assert!(single.node(1).is_leader());
    // Asking again changes nothing.
    single.node_mut(1).campaign().unwrap();
    assert_eq!(single.node(1).term(), t(1));

    let mut cluster = Cluster::new(3);
    cluster.node_mut(2).campaign().unwrap();
    cluster.collect(2);
    cluster.deliver_all();
    assert_eq!(cluster.leaders(), vec![2]);
}

#[test]
fn a_node_starts_as_a_follower_and_waits_before_campaigning() {
    let mut cluster = Cluster::new(3);
    let min = cluster.node(1).config().election_timeout_min;
    cluster.tick(1, min - 1);
    assert_eq!(cluster.node(1).role(), Role::Follower);
    assert_eq!(cluster.node(1).term(), t(0));
    assert!(cluster.queue.is_empty());
}

#[test]
fn three_nodes_elect_a_leader() {
    let mut cluster = Cluster::new(3);
    cluster.campaign(1);
    assert_eq!(cluster.node(1).role(), Role::Candidate);
    assert_eq!(cluster.node(1).leader(), None);

    cluster.deliver_all();
    assert_eq!(cluster.leaders(), vec![1]);
    for id in 1..=3 {
        let status = cluster.node(id).status();
        assert_eq!(status.term, t(1));
        assert_eq!(status.leader, Some(n(1)));
        assert_eq!(status.voted_for, Some(n(1)));
        // The no-op.
        assert_eq!(status.last_log, LogId::new(t(1), i(1)));
    }
    assert_eq!(cluster.node(1).commit_index(), i(1));
    assert_eq!(cluster.node(2).role().as_str(), "follower");
}

#[test]
fn starting_an_election_persists_the_vote_before_asking() {
    let mut cluster = Cluster::new(3);
    let term = cluster.node(1).term();
    let mut ready = cluster.node_mut(1).ready().unwrap();
    while cluster.node(1).term() == term {
        cluster.node_mut(1).tick().unwrap();
        ready = cluster.node_mut(1).ready().unwrap();
    }
    // The same Ready that carries the vote requests also carries the hard
    // state, so the driver writes the vote down before sending anything.
    assert_eq!(
        ready.hard_state,
        Some(HardState {
            term: t(1),
            voted_for: Some(n(1)),
        })
    );
    assert_eq!(ready.messages.len(), 2);
    assert!(ready
        .messages
        .iter()
        .all(|message| matches!(message.body, Body::RequestVote { .. })));
    // Nothing changed since, so the next Ready is empty.
    assert!(cluster.node_mut(1).ready().unwrap().is_idle());
}

#[test]
fn a_node_votes_once_per_term() {
    let mut cluster = Cluster::new(3);
    let request = |from: u64| Message {
        from: n(from),
        to: n(3),
        term: t(1),
        body: Body::RequestVote {
            last_log: LogId::ZERO,
        },
    };
    let granted = |cluster: &mut Cluster| -> bool {
        let ready = cluster.node_mut(3).ready().unwrap();
        match ready.messages[0].body {
            Body::RequestVoteResponse { granted } => granted,
            ref other => panic!("unexpected {other:?}"),
        }
    };

    cluster.node_mut(3).step(request(1)).unwrap();
    assert!(granted(&mut cluster));
    // A second candidate in the same term is refused.
    cluster.node_mut(3).step(request(2)).unwrap();
    assert!(!granted(&mut cluster));
    // The first candidate asking again gets the same yes.
    cluster.node_mut(3).step(request(1)).unwrap();
    assert!(granted(&mut cluster));
    assert_eq!(cluster.node(3).hard_state().voted_for, Some(n(1)));
}

#[test]
fn a_restarted_node_remembers_its_vote() {
    let config = Config::new(n(3), voters(3));
    let hard = HardState {
        term: t(4),
        voted_for: Some(n(1)),
    };
    let mut node = Node::new(config, hard, MemLog::new()).unwrap();
    node.step(Message {
        from: n(2),
        to: n(3),
        term: t(4),
        body: Body::RequestVote {
            last_log: LogId::ZERO,
        },
    })
    .unwrap();
    let ready = node.ready().unwrap();
    assert_eq!(
        ready.messages[0].body,
        Body::RequestVoteResponse { granted: false }
    );
    // Nothing changed, so there is nothing to write.
    assert_eq!(ready.hard_state, None);
}

#[test]
fn a_vote_is_refused_to_a_candidate_with_an_older_log() {
    // The voter's last entry is from term 2.
    let config = Config::new(n(3), voters(3));
    let hard = HardState {
        term: t(2),
        voted_for: None,
    };
    let ask = |last_log: LogId| -> bool {
        let mut node = Node::new(config.clone(), hard, log_with_terms(&[1, 2, 2])).unwrap();
        node.step(Message {
            from: n(1),
            to: n(3),
            term: t(3),
            body: Body::RequestVote { last_log },
        })
        .unwrap();
        let ready = node.ready().unwrap();
        // The higher term is adopted whether or not the vote is given.
        assert_eq!(ready.hard_state.unwrap().term, t(3));
        match ready.messages[0].body {
            Body::RequestVoteResponse { granted } => granted,
            ref other => panic!("unexpected {other:?}"),
        }
    };

    // An older last term loses, however long the log is.
    assert!(!ask(LogId::new(t(1), i(50))));
    // The same last term but a shorter log loses.
    assert!(!ask(LogId::new(t(2), i(2))));
    // An identical log wins.
    assert!(ask(LogId::new(t(2), i(3))));
    // A newer last term wins even with a shorter log.
    assert!(ask(LogId::new(t(3), i(1))));
}

#[test]
fn a_candidate_that_hears_from_a_leader_of_its_term_steps_back() {
    let mut cluster = Cluster::new(3);
    cluster.campaign(2);
    assert_eq!(cluster.node(2).role(), Role::Candidate);
    cluster.queue.clear();

    cluster
        .node_mut(2)
        .step(Message {
            from: n(1),
            to: n(2),
            term: t(1),
            body: Body::AppendEntries {
                prev_log: LogId::ZERO,
                entries: vec![],
                leader_commit: i(0),
            },
        })
        .unwrap();
    assert_eq!(cluster.node(2).role(), Role::Follower);
    assert_eq!(cluster.node(2).leader(), Some(n(1)));
}

#[test]
fn a_split_vote_is_settled_by_a_later_term() {
    let mut cluster = Cluster::new(3);
    // Nodes 1 and 2 both campaign in term 1 and each has its own vote.
    // Node 3 is cut off, so neither reaches two votes.
    cluster.isolated.insert(n(3));
    cluster.campaign(1);
    cluster.campaign(2);
    cluster.deliver_all();
    assert!(cluster.leaders().is_empty());

    cluster.isolated.clear();
    cluster.campaign(1);
    cluster.deliver_all();
    assert_eq!(cluster.leaders(), vec![1]);
    assert_eq!(cluster.node(1).term(), t(2));
}

// ---- replication ---------------------------------------------------------

#[test]
fn a_proposal_replicates_and_commits_everywhere() {
    let mut cluster = Cluster::new(3);
    cluster.elect(1);

    let id = cluster.propose(1, "a");
    assert_eq!(id, LogId::new(t(1), i(2)));
    // Not committed until a follower confirms it.
    assert_eq!(cluster.node(1).commit_index(), i(1));

    cluster.deliver_all();
    for node in 1..=3 {
        assert_eq!(cluster.node(node).commit_index(), i(2), "node {node}");
        assert_eq!(
            cluster.node(node).log().get(i(2)).unwrap().command,
            cmd("a")
        );
    }
}

#[test]
fn proposals_made_together_travel_together() {
    let mut cluster = Cluster::new(3);
    cluster.elect(1);
    cluster.delivered.clear();

    for name in ["a", "b", "c"] {
        cluster.node_mut(1).propose(cmd(name)).unwrap();
    }
    cluster.collect(1);
    // One message per follower, each carrying all three entries.
    assert_eq!(cluster.queue.len(), 2);
    for message in &cluster.queue {
        match &message.body {
            Body::AppendEntries { entries, .. } => assert_eq!(entries.len(), 3),
            other => panic!("unexpected {other:?}"),
        }
    }
    cluster.deliver_all();
    assert_eq!(cluster.node(3).commit_index(), i(4));
}

#[test]
fn a_follower_refuses_proposals_and_names_the_leader() {
    let mut cluster = Cluster::new(3);
    cluster.elect(1);
    match cluster.node_mut(2).propose(cmd("a")) {
        Err(ProposeError::NotLeader { leader }) => assert_eq!(leader, Some(n(1))),
        other => panic!("unexpected {other:?}"),
    }
    // A node that has never heard of a leader says so.
    let mut fresh = Cluster::new(3);
    let error = fresh.node_mut(1).propose(cmd("a")).unwrap_err();
    assert!(matches!(error, ProposeError::NotLeader { leader: None }));
    assert!(error.to_string().contains("no leader is known"));
}

#[test]
fn a_commit_needs_a_majority() {
    let mut cluster = Cluster::new(3);
    cluster.elect(1);
    cluster.isolated.extend([n(2), n(3)]);

    cluster.propose(1, "a");
    cluster.deliver_all();
    assert_eq!(cluster.node(1).commit_index(), i(1));

    // One follower back is enough for two out of three.
    cluster.isolated.remove(&n(2));
    let heartbeat = cluster.node(1).config().heartbeat_interval;
    cluster.tick(1, heartbeat);
    cluster.deliver_all();
    assert_eq!(cluster.node(1).commit_index(), i(2));
    assert_eq!(cluster.node(2).commit_index(), i(2));
    // Node 3 still only knows about the no-op from the election.
    assert_eq!(cluster.node(3).commit_index(), i(1));
}

#[test]
fn a_lost_append_is_repaired_by_the_next_heartbeat() {
    let mut cluster = Cluster::new(3);
    cluster.elect(1);

    // The entries for node 3 are lost on the way.
    cluster.isolated.insert(n(3));
    cluster.propose(1, "a");
    cluster.propose(1, "b");
    cluster.deliver_all();
    assert_eq!(cluster.node(3).log().len(), 1);

    // The leader has moved on and assumes node 3 holds them. The next
    // heartbeat is refused, the leader backs up, and node 3 catches up.
    cluster.isolated.clear();
    let heartbeat = cluster.node(1).config().heartbeat_interval;
    cluster.tick(1, heartbeat);
    cluster.deliver_all();
    assert_eq!(
        cluster.node(3).log().as_slice(),
        cluster.node(1).log().as_slice()
    );
    assert_eq!(cluster.node(3).commit_index(), i(3));
}

#[test]
fn a_long_backlog_is_sent_in_several_messages() {
    let mut cluster = Cluster::with(3, |config| config.max_entries_per_message = 2);
    cluster.elect(1);
    cluster.isolated.insert(n(3));
    for index in 0..7 {
        cluster.propose(1, &format!("e{index}"));
    }
    cluster.deliver_all();
    cluster.isolated.clear();
    cluster.delivered.clear();

    let heartbeat = cluster.node(1).config().heartbeat_interval;
    cluster.tick(1, heartbeat);
    cluster.deliver_all();
    assert_eq!(cluster.node(3).log().len(), 8);
    assert_eq!(cluster.node(3).commit_index(), i(8));
    let largest = cluster
        .delivered
        .iter()
        .filter_map(|message| match &message.body {
            Body::AppendEntries { entries, .. } => Some(entries.len()),
            _ => None,
        })
        .max()
        .unwrap();
    assert_eq!(largest, 2);
}

#[test]
fn the_byte_limit_still_lets_one_large_entry_through() {
    let mut cluster = Cluster::with(3, |config| config.max_bytes_per_message = 1);
    cluster.elect(1);
    cluster.propose(1, "a");
    cluster.propose(1, "b");
    cluster.deliver_all();
    assert_eq!(cluster.node(2).commit_index(), i(3));
    assert_eq!(cluster.node(3).commit_index(), i(3));
}

#[test]
fn a_follower_commits_only_what_the_message_vouched_for() {
    // The follower holds three entries. The third is left over from an old
    // leader and the current leader has something else there.
    let config = Config::new(n(2), voters(3));
    let hard = HardState {
        term: t(3),
        voted_for: None,
    };
    let mut node = Node::new(config, hard, log_with_terms(&[1, 1, 2])).unwrap();

    // A heartbeat that checks only up to index 2, from a leader that has
    // committed up to 3.
    node.step(Message {
        from: n(1),
        to: n(2),
        term: t(3),
        body: Body::AppendEntries {
            prev_log: LogId::new(t(1), i(2)),
            entries: vec![],
            leader_commit: i(3),
        },
    })
    .unwrap();
    let ready = node.ready().unwrap();
    assert_eq!(ready.commit_index, i(2));
    assert_eq!(
        ready.messages[0].body,
        Body::AppendEntriesResponse {
            success: true,
            match_index: i(2),
            conflict_index: i(0),
        }
    );
}

#[test]
fn a_repeated_or_late_append_never_shortens_the_log() {
    let config = Config::new(n(2), voters(3));
    let hard = HardState {
        term: t(1),
        voted_for: None,
    };
    let mut node = Node::new(config, hard, log_with_terms(&[1, 1, 1, 1])).unwrap();
    let before = node.log().clone();

    // An old message that carried only the first two entries.
    let entries = before.as_slice()[..2].to_vec();
    node.step(Message {
        from: n(1),
        to: n(2),
        term: t(1),
        body: Body::AppendEntries {
            prev_log: LogId::ZERO,
            entries,
            leader_commit: i(0),
        },
    })
    .unwrap();
    let ready = node.ready().unwrap();
    assert_eq!(node.log(), &before);
    // Nothing was written, so there is nothing to sync.
    assert!(!ready.must_sync);
}

#[test]
fn entries_that_skip_an_index_are_ignored() {
    let mut cluster = Cluster::new(3);
    cluster
        .node_mut(2)
        .step(Message {
            from: n(1),
            to: n(2),
            term: t(1),
            body: Body::AppendEntries {
                prev_log: LogId::ZERO,
                entries: vec![LogEntry::new(t(1), i(2), Command::Noop)],
                leader_commit: i(0),
            },
        })
        .unwrap();
    assert!(cluster.node(2).log().is_empty());
}

// ---- repairing diverged logs ---------------------------------------------

/// Builds a three-node cluster from hand-written logs. Each node's term is
/// the term of its last entry.
fn cluster_from_logs(logs: [&[u64]; 3]) -> Cluster {
    let mut cluster = Cluster::with(0, |_| {});
    for (terms, id) in logs.into_iter().zip(1u64..) {
        let hard = HardState {
            term: t(terms.last().copied().unwrap_or(0)),
            voted_for: None,
        };
        cluster.add(Config::new(n(id), voters(3)), hard, log_with_terms(terms));
    }
    cluster
}

#[test]
fn a_new_leader_overwrites_a_followers_uncommitted_tail() {
    // Node 1 will lead. Node 2 has extra entries from a leader of term 2
    // that never committed them. Node 3 is simply behind.
    let mut cluster = cluster_from_logs([&[1, 1, 3], &[1, 1, 2, 2, 2], &[1]]);
    cluster.elect(1);

    let leader_log = cluster.node(1).log().clone();
    // Three old entries and the no-op of the new term.
    assert_eq!(terms_of(cluster.node(1)), vec![1, 1, 3, 4]);
    for id in 2..=3 {
        assert_eq!(cluster.node(id).log(), &leader_log, "node {id}");
        assert_eq!(cluster.node(id).commit_index(), i(4));
    }
}

#[test]
fn a_rejection_skips_back_over_a_whole_term() {
    let mut cluster = cluster_from_logs([&[1, 4, 4, 4, 4, 4], &[1, 2, 2, 2, 2], &[1]]);
    cluster.isolated.insert(n(3));
    // Node 3 cannot vote, so node 2 has to. It does, because node 1's log
    // ends in a later term.
    cluster.campaign(1);
    cluster.delivered.clear();
    cluster.deliver_all();
    assert!(cluster.node(1).is_leader());
    assert_eq!(cluster.node(2).log(), cluster.node(1).log());

    // Node 2 pointed the leader at the start of its term 2 entries in one
    // answer, instead of being walked back one entry at a time.
    let rejections: Vec<LogIndex> = cluster
        .delivered
        .iter()
        .filter_map(|message| match message.body {
            Body::AppendEntriesResponse {
                success: false,
                conflict_index,
                ..
            } => Some(conflict_index),
            _ => None,
        })
        .collect();
    assert_eq!(rejections, vec![i(6), i(2)]);
}

#[test]
fn an_entry_from_an_old_term_is_not_committed_by_counting_replicas() {
    // This is the situation in figure 8 of the Raft paper. Node 1 holds an
    // entry from term 2 that the others lack, and becomes leader in term 4.
    let mut cluster = Cluster::with(0, |_| {});
    let logs: [&[u64]; 3] = [&[1, 2], &[1], &[1]];
    for (terms, id) in logs.into_iter().zip(1u64..) {
        let hard = HardState {
            term: t(3),
            voted_for: None,
        };
        let mut config = Config::new(n(id), voters(3));
        // One entry per message, so the old entry and the new no-op are
        // acknowledged separately.
        config.max_entries_per_message = 1;
        cluster.add(config, hard, log_with_terms(terms));
    }
    cluster.campaign(1);

    // Deliver messages one at a time and watch the commit index. It must
    // never rest on the term 2 entry alone.
    let mut seen = BTreeSet::new();
    while let Some(message) = cluster.queue.pop_front() {
        let to = message.to;
        let node = cluster.nodes.get_mut(&to).unwrap();
        node.step(message).unwrap();
        let ready = node.ready().unwrap();
        cluster.queue.extend(ready.messages);
        seen.insert(cluster.node(1).commit_index().get());
    }
    assert!(cluster.node(1).is_leader());
    assert_eq!(terms_of(cluster.node(1)), vec![1, 2, 4]);
    // Index 2 was on a majority before index 3 was, but the commit index
    // went from 0 straight to 3.
    assert_eq!(seen, BTreeSet::from([0, 3]));
}

// ---- stale and foreign messages ------------------------------------------

#[test]
fn a_stale_leader_steps_down_when_it_hears_a_newer_term() {
    let mut cluster = Cluster::new(3);
    cluster.elect(1);

    // Node 1 is cut off and the others move on.
    cluster.isolated.insert(n(1));
    cluster.elect(2);
    assert_eq!(cluster.node(2).term(), t(2));
    assert_eq!(cluster.leaders(), vec![1, 2]);

    // Back in touch, the old leader's heartbeat is answered with the new
    // term and it steps down.
    cluster.isolated.clear();
    let heartbeat = cluster.node(1).config().heartbeat_interval;
    cluster.tick(1, heartbeat);
    cluster.deliver_all();
    assert_eq!(cluster.leaders(), vec![2]);
    assert_eq!(cluster.node(1).term(), t(2));

    let heartbeat = cluster.node(2).config().heartbeat_interval;
    cluster.tick(2, heartbeat);
    cluster.deliver_all();
    assert_eq!(cluster.node(1).leader(), Some(n(2)));
    assert_eq!(cluster.node(1).log(), cluster.node(2).log());
}

#[test]
fn an_isolated_leaders_uncommitted_entries_are_replaced() {
    let mut cluster = Cluster::new(3);
    cluster.elect(1);

    cluster.isolated.insert(n(1));
    let lost = cluster.propose(1, "lost");
    cluster.deliver_all();

    cluster.elect(2);
    let kept = cluster.propose(2, "kept");
    cluster.deliver_all();
    // The new leader's no-op took the index the lost entry was using.
    assert_eq!(lost, LogId::new(t(1), i(2)));
    assert_eq!(kept, LogId::new(t(2), i(3)));
    assert_eq!(
        cluster.node(1).log().get(i(2)).unwrap().command,
        cmd("lost")
    );

    cluster.isolated.clear();
    let heartbeat = cluster.node(2).config().heartbeat_interval;
    cluster.tick(2, heartbeat);
    cluster.deliver_all();
    assert_eq!(cluster.node(1).log(), cluster.node(2).log());
    assert_eq!(
        cluster.node(1).log().get(i(2)).unwrap().command,
        Command::Noop
    );
    assert_eq!(
        cluster.node(1).log().get(i(3)).unwrap().command,
        cmd("kept")
    );
    assert_eq!(cluster.node(1).commit_index(), i(3));
}

#[test]
fn a_leader_without_a_majority_steps_down() {
    let mut cluster = Cluster::new(3);
    cluster.elect(1);
    let min = cluster.node(1).config().election_timeout_min;

    // While followers answer, it stays leader for as long as it likes.
    for _ in 0..3 * min {
        cluster.tick(1, 1);
        cluster.deliver_all();
    }
    assert!(cluster.node(1).is_leader());

    // Cut off, it gives up within two election timeouts.
    cluster.isolated.insert(n(1));
    for _ in 0..2 * min {
        cluster.tick(1, 1);
        cluster.deliver_all();
    }
    assert_eq!(cluster.node(1).role(), Role::Follower);
    assert_eq!(cluster.node(1).leader(), None);
    // It keeps its term. Stepping down is not an election.
    assert_eq!(cluster.node(1).term(), t(1));
}

#[test]
fn late_and_repeated_answers_do_not_move_the_leader_backwards() {
    let mut cluster = Cluster::new(3);
    cluster.elect(1);
    for name in ["a", "b", "c"] {
        cluster.propose(1, name);
    }
    cluster.deliver_all();
    assert_eq!(cluster.node(1).commit_index(), i(4));

    let answer = |success: bool, match_index: u64, conflict_index: u64| Message {
        from: n(2),
        to: n(1),
        term: t(1),
        body: Body::AppendEntriesResponse {
            success,
            match_index: i(match_index),
            conflict_index: i(conflict_index),
        },
    };
    // An old success and an old rejection turn up again.
    cluster.node_mut(1).step(answer(true, 2, 0)).unwrap();
    cluster.node_mut(1).step(answer(false, 0, 2)).unwrap();
    cluster.collect(1);

    // The rejection made the leader send once more, from just after what
    // node 2 has confirmed and not from index 2.
    let resent: Vec<&Message> = cluster.queue.iter().filter(|m| m.to == n(2)).collect();
    assert_eq!(resent.len(), 1);
    match &resent[0].body {
        Body::AppendEntries {
            prev_log, entries, ..
        } => {
            assert_eq!(prev_log.index, i(4));
            assert!(entries.is_empty());
        }
        other => panic!("unexpected {other:?}"),
    }
    cluster.deliver_all();
    assert_eq!(cluster.node(1).commit_index(), i(4));
}

#[test]
fn messages_from_strangers_or_for_someone_else_are_dropped() {
    let mut cluster = Cluster::new(3);
    let vote = |from: u64, to: u64| Message {
        from: n(from),
        to: n(to),
        term: t(9),
        body: Body::RequestVote {
            last_log: LogId::new(t(9), i(9)),
        },
    };
    // Not a voter.
    cluster.node_mut(1).step(vote(7, 1)).unwrap();
    // Addressed to another node.
    cluster.node_mut(1).step(vote(2, 3)).unwrap();
    // Claims to be from the node itself.
    cluster.node_mut(1).step(vote(1, 1)).unwrap();

    assert_eq!(cluster.node(1).term(), t(0));
    assert!(cluster.node_mut(1).ready().unwrap().is_idle());
}

#[test]
fn a_request_from_an_older_term_is_answered_with_the_current_one() {
    let config = Config::new(n(2), voters(3));
    let hard = HardState {
        term: t(5),
        voted_for: None,
    };
    let mut node = Node::new(config, hard, MemLog::new()).unwrap();
    node.step(Message {
        from: n(1),
        to: n(2),
        term: t(3),
        body: Body::AppendEntries {
            prev_log: LogId::ZERO,
            entries: vec![LogEntry::new(t(3), i(1), Command::Noop)],
            leader_commit: i(1),
        },
    })
    .unwrap();
    node.step(Message {
        from: n(3),
        to: n(2),
        term: t(4),
        body: Body::RequestVote {
            last_log: LogId::new(t(4), i(10)),
        },
    })
    .unwrap();
    let ready = node.ready().unwrap();

    assert!(node.log().is_empty());
    assert_eq!(node.commit_index(), i(0));
    assert_eq!(ready.messages.len(), 2);
    assert!(ready.messages.iter().all(|message| message.term == t(5)));
    assert!(matches!(
        ready.messages[0].body,
        Body::AppendEntriesResponse { success: false, .. }
    ));
    assert_eq!(
        ready.messages[1].body,
        Body::RequestVoteResponse { granted: false }
    );
    assert_eq!(ready.messages[0].body.kind(), "append_entries_response");
}

#[test]
fn mem_log_reads_within_limits() {
    let log = log_with_terms(&[1, 1, 2, 3]);
    assert_eq!(log.last_id(), LogId::new(t(3), i(4)));
    assert_eq!(log.term(i(0)).unwrap(), Some(t(0)));
    assert_eq!(log.term(i(3)).unwrap(), Some(t(2)));
    assert_eq!(log.term(i(5)).unwrap(), None);
    assert_eq!(log.entries(i(2), 2, usize::MAX).unwrap().len(), 2);
    assert_eq!(log.entries(i(2), 9, usize::MAX).unwrap().len(), 3);
    assert_eq!(log.entries(i(2), 9, 1).unwrap().len(), 1);
    assert!(log.entries(i(5), 9, usize::MAX).unwrap().is_empty());

    let mut log = log;
    log.truncate_suffix(i(3)).unwrap();
    assert_eq!(log.len(), 2);
    assert_eq!(MemLog::new().last_id(), LogId::ZERO);
}
