//! The pieces of this crate against a real disk and real sockets.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fl_net::{
    hard_state_from_disk, hard_state_to_disk, PeerMap, RaftService, ReplySink, RequestSink,
    SharedLog, Transport, TransportOptions, MAX_MESSAGE_BYTES,
};
use fl_proto::RaftServer;
use fl_raft::{Body, Config, HardState, LogStore, MemLog, Message, Node};
use fl_storage::{HardStateStore, Log, LogConfig, SyncPolicy};
use fl_types::{Command, LogEntry, LogId, LogIndex, NodeId, Record, RecordBatch, Term, TopicId};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tonic::transport::server::TcpIncoming;

struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("ferrumlog-net-{tag}-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        TempDir(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn n(id: u64) -> NodeId {
    NodeId::new(id)
}

fn entry(term: u64, index: u64) -> LogEntry {
    LogEntry::new(Term::new(term), LogIndex::new(index), Command::Noop)
}

fn open(dir: &Path) -> SharedLog {
    let config = LogConfig {
        sync: SyncPolicy::Manual,
        ..LogConfig::default()
    };
    SharedLog::new(Log::open(dir, config).unwrap())
}

// ---- storage -------------------------------------------------------------

#[test]
fn the_disk_log_behaves_like_the_memory_log() {
    let dir = TempDir::new("logstore");
    let mut disk = open(dir.path());
    let mut memory = MemLog::new();

    // The same operations on both, compared after each one.
    let same = |disk: &SharedLog, memory: &MemLog| {
        assert_eq!(disk.last_id(), memory.last_id());
        for index in 0..=6 {
            let index = LogIndex::new(index);
            assert_eq!(disk.term(index).unwrap(), memory.term(index).unwrap());
        }
        for from in 1..=6 {
            let from = LogIndex::new(from);
            assert_eq!(
                disk.entries(from, 3, usize::MAX).unwrap(),
                memory.entries(from, 3, usize::MAX).unwrap()
            );
        }
    };
    same(&disk, &memory);

    let first = [entry(1, 1), entry(1, 2), entry(2, 3), entry(2, 4)];
    disk.append(&first).unwrap();
    memory.append(&first).unwrap();
    same(&disk, &memory);

    // A new leader replaces the tail.
    disk.truncate_suffix(LogIndex::new(3)).unwrap();
    memory.truncate_suffix(LogIndex::new(3)).unwrap();
    same(&disk, &memory);

    let second = [entry(3, 3), entry(3, 4), entry(3, 5)];
    disk.append(&second).unwrap();
    memory.append(&second).unwrap();
    same(&disk, &memory);
    disk.sync().unwrap();

    // Index zero is asked for as "everything", and is not an error.
    assert_eq!(
        disk.entries(LogIndex::ZERO, 9, usize::MAX).unwrap().len(),
        5
    );
}

#[test]
fn clones_share_one_log_and_it_survives_a_reopen() {
    let dir = TempDir::new("shared");
    {
        let mut writer = open(dir.path());
        let reader = writer.clone();
        writer.append(&[entry(1, 1), entry(1, 2)]).unwrap();
        assert_eq!(reader.read().last_index(), LogIndex::new(2));
        writer.sync().unwrap();
    }
    let reopened = open(dir.path());
    assert_eq!(
        reopened.last_id(),
        LogId::new(Term::new(1), LogIndex::new(2))
    );
}

#[test]
fn a_node_runs_on_the_disk_log() {
    let dir = TempDir::new("node");
    let peers = PeerMap::single(n(1), "unused");
    let hard = {
        let mut node = Node::new(
            Config::new(n(1), peers.membership()),
            HardState::default(),
            open(dir.path()),
        )
        .unwrap();
        node.campaign().unwrap();
        let batch = RecordBatch::new(TopicId::new(1), vec![Record::new("v")]);
        node.propose(Command::Append(batch)).unwrap();
        let ready = node.ready().unwrap();
        assert!(ready.must_sync);
        assert_eq!(ready.commit_index, LogIndex::new(2));
        node.log().sync().unwrap();
        ready.hard_state.unwrap()
    };

    // The term and vote go through the hard state file and back.
    let mut store = HardStateStore::open(dir.path()).unwrap();
    store.save(hard_state_to_disk(hard)).unwrap();
    drop(store);
    let store = HardStateStore::open(dir.path()).unwrap();
    let restored = hard_state_from_disk(store.get());
    assert_eq!(restored, hard);
    assert_eq!(restored.voted_for, Some(n(1)));

    // The hard state file sits beside the segments without confusing the
    // log, and a restarted node finds what the first one wrote.
    let node = Node::new(
        Config::new(n(1), peers.membership()),
        restored,
        open(dir.path()),
    )
    .unwrap();
    assert_eq!(node.term(), Term::new(1));
    assert_eq!(node.log().last_id().index, LogIndex::new(2));
}

// ---- transport and service -----------------------------------------------

/// Serves the Raft service for a real node on a loopback port, and returns
/// the address.
async fn serve(node: Node<MemLog>) -> String {
    let me = node.id();
    let node = Arc::new(Mutex::new(node));
    let sink: RequestSink = Arc::new(move |message, answer| {
        let mut node = node.lock().unwrap();
        let from = message.from;
        node.step(message).unwrap();
        let ready = node.ready().unwrap();
        if let Some(reply) = ready.messages.into_iter().find(|m| m.to == from) {
            let _ = answer.send(reply);
        }
        true
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let service = RaftServer::new(RaftService::new(me, sink))
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES);
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpIncoming::from(listener)),
    );
    addr
}

/// A transport for node 1 whose replies land in a channel.
fn transport(peers: &PeerMap) -> (Transport, mpsc::UnboundedReceiver<Message>) {
    let (replies, received) = mpsc::unbounded_channel();
    let sink: ReplySink = Arc::new(move |message| {
        let _ = replies.send(message);
    });
    let options = TransportOptions {
        connect_timeout: Duration::from_millis(300),
        request_timeout: Duration::from_secs(2),
        queue: 4,
    };
    (Transport::start(n(1), peers, options, sink), received)
}

async fn next(received: &mut mpsc::UnboundedReceiver<Message>) -> Message {
    tokio::time::timeout(Duration::from_secs(5), received.recv())
        .await
        .expect("no reply arrived")
        .expect("the transport went away")
}

#[tokio::test]
async fn a_vote_and_an_append_cross_a_real_socket() {
    let voters = PeerMap::parse(["1=unused", "2=unused"])
        .unwrap()
        .membership();
    let follower = Node::new(
        Config::new(n(2), voters),
        HardState::default(),
        MemLog::new(),
    );
    let addr = serve(follower.unwrap()).await;
    let peers = PeerMap::parse(["1=unused".to_owned(), format!("2={addr}")]).unwrap();
    let (transport, mut received) = transport(&peers);

    transport.send(Message {
        from: n(1),
        to: n(2),
        term: Term::new(1),
        body: Body::RequestVote {
            last_log: LogId::ZERO,
        },
    });
    let reply = next(&mut received).await;
    assert_eq!(
        (reply.from, reply.to, reply.term),
        (n(2), n(1), Term::new(1))
    );
    assert_eq!(reply.body, Body::RequestVoteResponse { granted: true });

    // A large entry, to exercise the raised message size limit.
    let big = Record::new(vec![7u8; 1 << 20]);
    let entries = vec![
        entry(1, 1),
        LogEntry::new(
            Term::new(1),
            LogIndex::new(2),
            Command::Append(RecordBatch::new(TopicId::new(1), vec![big])),
        ),
    ];
    transport.send(Message {
        from: n(1),
        to: n(2),
        term: Term::new(1),
        body: Body::AppendEntries {
            prev_log: LogId::ZERO,
            entries,
            leader_commit: LogIndex::new(1),
        },
    });
    let reply = next(&mut received).await;
    assert_eq!(
        reply.body,
        Body::AppendEntriesResponse {
            success: true,
            match_index: LogIndex::new(2),
            conflict_index: LogIndex::ZERO,
        }
    );
    assert_eq!(transport.dropped(), 0);
}

#[tokio::test]
async fn messages_for_a_dead_peer_are_dropped_without_blocking() {
    // Nothing listens on this port.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let peers = PeerMap::parse(["1=unused".to_owned(), format!("2={addr}")]).unwrap();
    let (transport, mut received) = transport(&peers);

    let heartbeat = |to: u64| Message {
        from: n(1),
        to: n(to),
        term: Term::new(1),
        body: Body::AppendEntries {
            prev_log: LogId::ZERO,
            entries: vec![],
            leader_commit: LogIndex::ZERO,
        },
    };
    // Far more than the queue holds. None of these calls may wait.
    let started = std::time::Instant::now();
    for _ in 0..1_000 {
        transport.send(heartbeat(2));
    }
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(transport.dropped() > 900, "{} dropped", transport.dropped());

    // A message for a node that is not in the peer map is dropped too.
    let before = transport.dropped();
    transport.send(heartbeat(9));
    assert_eq!(transport.dropped(), before + 1);

    // And nothing ever comes back.
    let quiet = tokio::time::timeout(Duration::from_millis(600), received.recv()).await;
    assert!(quiet.is_err());
}

#[tokio::test]
async fn a_request_the_node_cannot_take_is_an_error_not_a_hang() {
    use fl_proto::{pb, RaftClient};

    // A sink that refuses everything, as a node does when its queue is full.
    let sink: RequestSink = Arc::new(|_, _| false);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(RaftServer::new(RaftService::new(n(2), sink)))
            .serve_with_incoming(TcpIncoming::from(listener)),
    );

    let mut client = RaftClient::connect(format!("http://{addr}")).await.unwrap();
    let vote = pb::RequestVoteRequest {
        term: 1,
        candidate_id: 1,
        last_log: None,
    };
    let status = client.request_vote(vote).await.unwrap_err();
    assert_eq!(status.code(), tonic::Code::Unavailable);

    // An entry with no command is refused before it reaches the node.
    let append = pb::AppendEntriesRequest {
        term: 1,
        leader_id: 1,
        prev_log: None,
        entries: vec![pb::Entry {
            term: 1,
            index: 1,
            command: None,
        }],
        leader_commit: 0,
    };
    let status = client.append_entries(append).await.unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}
