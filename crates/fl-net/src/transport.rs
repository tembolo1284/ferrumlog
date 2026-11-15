//! The sending side: one connection and one queue per peer.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fl_proto::RaftClient;
use fl_raft::Message;
use fl_types::NodeId;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Endpoint};
use tonic::Status;
use tracing::{debug, warn};

use crate::peers::PeerMap;
use crate::wire::{self, Request};
use crate::MAX_MESSAGE_BYTES;

/// Where a peer's responses are handed back to the node. Must not block.
pub type ReplySink = Arc<dyn Fn(Message) + Send + Sync>;

/// Timeouts and queue sizes for the transport.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TransportOptions {
    /// How long to wait for a connection to a peer.
    pub connect_timeout: Duration,
    /// How long to wait for a peer to answer one request.
    pub request_timeout: Duration,
    /// Messages that may wait for one peer before new ones are dropped.
    pub queue: usize,
}

impl Default for TransportOptions {
    fn default() -> Self {
        TransportOptions {
            connect_timeout: Duration::from_millis(500),
            request_timeout: Duration::from_secs(2),
            queue: 64,
        }
    }
}

/// Sends a node's outgoing Raft messages to its peers.
///
/// Each peer has a bounded queue and a task that works through it one
/// request at a time, over a connection that is opened on first use and
/// reopened after a failure. When a queue is full the message is dropped.
/// Raft recovers from lost messages by design, and a queue that grows
/// without limit behind a dead peer would be worse than a dropped
/// heartbeat.
///
/// Dropping the transport stops its tasks.
pub struct Transport {
    queues: BTreeMap<NodeId, mpsc::Sender<Message>>,
    tasks: Vec<JoinHandle<()>>,
    dropped: Arc<AtomicU64>,
}

impl Transport {
    /// Starts one sender task for every member of `peers` other than `me`.
    ///
    /// Must be called from inside a tokio runtime. No connection is opened
    /// until there is something to send.
    pub fn start(me: NodeId, peers: &PeerMap, options: TransportOptions, sink: ReplySink) -> Self {
        let mut queues = BTreeMap::new();
        let mut tasks = Vec::new();
        for (peer, addr) in peers.iter() {
            if peer == me {
                continue;
            }
            let (queue, messages) = mpsc::channel(options.queue.max(1));
            queues.insert(peer, queue);
            let link = Link {
                me,
                peer,
                addr: addr.to_owned(),
                options,
                sink: Arc::clone(&sink),
            };
            tasks.push(tokio::spawn(link.run(messages)));
        }
        Transport {
            queues,
            tasks,
            dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Queues a message for the peer it is addressed to. Never blocks.
    ///
    /// A message for an unknown node, or for a peer whose queue is full,
    /// is dropped.
    pub fn send(&self, message: Message) {
        let delivered = match self.queues.get(&message.to) {
            Some(queue) => queue.try_send(message).is_ok(),
            None => false,
        };
        if !delivered {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// How many messages [`Transport::send`] has dropped so far.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl Drop for Transport {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// The connection to one peer.
struct Link {
    me: NodeId,
    peer: NodeId,
    addr: String,
    options: TransportOptions,
    sink: ReplySink,
}

impl Link {
    async fn run(self, mut messages: mpsc::Receiver<Message>) {
        let Some(channel) = self.channel() else {
            // Keep draining so the sender never sees a closed queue, but
            // there is nowhere to send to.
            while messages.recv().await.is_some() {}
            return;
        };
        let mut client = RaftClient::new(channel)
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES);
        // Log the first failure of a run of failures, not every one.
        let mut healthy = true;

        while let Some(message) = messages.recv().await {
            let Some(request) = wire::request(message) else {
                continue;
            };
            let reply = match request {
                Request::Vote(request) => client
                    .request_vote(request)
                    .await
                    .map(|response| wire::vote_reply(self.peer, self.me, response.get_ref())),
                Request::Append(request) => client
                    .append_entries(request)
                    .await
                    .map(|response| wire::append_reply(self.peer, self.me, response.get_ref())),
            };
            match reply {
                Ok(reply) => {
                    if !healthy {
                        debug!(peer = %self.peer, "peer is reachable again");
                        healthy = true;
                    }
                    (self.sink)(reply);
                }
                Err(status) => self.failed(&status, &mut healthy),
            }
        }
    }

    /// A lazily connected channel. Connecting happens on the first
    /// request, and again after any failure.
    fn channel(&self) -> Option<Channel> {
        let url = if self.addr.contains("://") {
            self.addr.clone()
        } else {
            format!("http://{}", self.addr)
        };
        match Endpoint::from_shared(url) {
            Ok(endpoint) => Some(
                endpoint
                    .connect_timeout(self.options.connect_timeout)
                    .timeout(self.options.request_timeout)
                    .tcp_nodelay(true)
                    .connect_lazy(),
            ),
            Err(e) => {
                warn!(peer = %self.peer, addr = %self.addr, error = %e, "unusable peer address");
                None
            }
        }
    }

    fn failed(&self, status: &Status, healthy: &mut bool) {
        if *healthy {
            debug!(
                peer = %self.peer,
                addr = %self.addr,
                code = ?status.code(),
                "cannot reach peer, will keep trying"
            );
            *healthy = false;
        }
    }
}
