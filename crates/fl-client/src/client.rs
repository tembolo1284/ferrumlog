//! The client: endpoint selection, redirects, and retries.

use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use fl_proto::pb;
use fl_proto::redirect;
use fl_proto::{AdminClient, LogClient};
use fl_types::{Offset, Record, TopicId};
use tonic::transport::{Channel, Endpoint};
use tonic::{Status, Streaming};

use crate::config::{normalize, ClientConfig};
use crate::error::{is_transient, never_sent, Error, Result};
use crate::subscribe::Subscription;
use crate::types::{
    batch, Batch, FetchOptions, NodeStatus, Produced, StartAt, TopicCreated, TopicInfo,
};

/// How often an idle connection is pinged. Keeps long-lived subscriptions
/// alive through NAT gateways and load balancers that drop quiet
/// connections.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(20);

/// How long to wait for a ping reply before the connection is considered
/// dead.
const KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// Whether a call can be repeated when its outcome is unknown.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Repeat {
    /// Doing it twice is the same as doing it once.
    Safe,
    /// Doing it twice writes twice.
    Unsafe,
}

struct Node {
    url: String,
    channel: Channel,
}

struct Nodes {
    list: Vec<Node>,
    /// Position in `list` of the node calls go to first. After a redirect
    /// this is the leader.
    current: usize,
}

struct Inner {
    config: ClientConfig,
    nodes: Mutex<Nodes>,
}

/// A handle to a ferrumlog cluster.
///
/// Cloning is cheap and clones share connections, so make one client and
/// hand copies to every task that needs it.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

impl Client {
    /// Creates a client without touching the network. Connections are
    /// opened on first use.
    ///
    /// Must be called from inside a tokio runtime.
    pub fn new(config: ClientConfig) -> Result<Client> {
        if config.endpoints.is_empty() {
            return Err(Error::NoEndpoints);
        }
        let mut list: Vec<Node> = Vec::with_capacity(config.endpoints.len());
        for endpoint in &config.endpoints {
            let url = normalize(endpoint)?;
            if list.iter().all(|node| node.url != url) {
                let channel = open_channel(&config, &url)?;
                list.push(Node { url, channel });
            }
        }
        Ok(Client {
            inner: Arc::new(Inner {
                config,
                nodes: Mutex::new(Nodes { list, current: 0 }),
            }),
        })
    }

    /// Creates a client and checks that a node answers.
    pub async fn connect(config: ClientConfig) -> Result<Client> {
        let client = Client::new(config)?;
        client.status().await?;
        Ok(client)
    }

    /// The endpoints the client knows, in the order it would try them. The
    /// list grows when a redirect names a node that was not configured.
    pub fn endpoints(&self) -> Vec<String> {
        let nodes = self.nodes();
        let count = nodes.list.len();
        (0..count)
            .map(|step| nodes.list[(nodes.current + step) % count].url.clone())
            .collect()
    }

    /// Creates a topic. Succeeds without change if it already exists.
    pub async fn create_topic(&self, name: &str) -> Result<TopicCreated> {
        let request = pb::CreateTopicRequest { name: name.into() };
        let reply = self
            .call(Repeat::Safe, |channel| {
                let request = request.clone();
                async move { LogClient::new(channel).create_topic(request).await }
            })
            .await?
            .into_inner();
        Ok(TopicCreated {
            id: TopicId::new(reply.topic_id),
            created: reply.created,
        })
    }

    /// Lists every topic.
    pub async fn list_topics(&self) -> Result<Vec<TopicInfo>> {
        let reply = self
            .call(Repeat::Safe, |channel| async move {
                LogClient::new(channel)
                    .list_topics(pb::ListTopicsRequest {})
                    .await
            })
            .await?
            .into_inner();
        Ok(reply.topics.into_iter().map(Into::into).collect())
    }

    /// Appends records to a topic. Returns once they are committed.
    ///
    /// See the crate documentation for what happens when this fails with
    /// [`Error::Unavailable`].
    pub async fn produce(&self, topic: &str, records: Vec<Record>) -> Result<Produced> {
        let request = pb::ProduceRequest {
            topic: topic.into(),
            records: records.into_iter().map(Into::into).collect(),
        };
        let reply = self
            .call(Repeat::Unsafe, |channel| {
                let request = request.clone();
                async move { LogClient::new(channel).produce(request).await }
            })
            .await?
            .into_inner();
        Ok(Produced {
            base_offset: Offset::new(reply.base_offset),
            count: reply.count,
        })
    }

    /// Reads one batch of records starting at `offset`. The batch is empty
    /// if `offset` is at or past the high watermark.
    pub async fn fetch(&self, topic: &str, offset: Offset, options: FetchOptions) -> Result<Batch> {
        let request = pb::FetchRequest {
            topic: topic.into(),
            offset: offset.get(),
            max_records: options.max_records,
            max_bytes: options.max_bytes,
        };
        let reply = self
            .call(Repeat::Safe, |channel| {
                let request = request.clone();
                async move { LogClient::new(channel).fetch(request).await }
            })
            .await?
            .into_inner();
        batch(reply.records, reply.high_watermark)
    }

    /// Opens a subscription that delivers records as they are committed.
    ///
    /// Fails straight away if the topic does not exist.
    pub async fn subscribe(&self, topic: &str, start: StartAt) -> Result<Subscription> {
        let stream = self.open_stream(topic, start).await?;
        Ok(Subscription::new(self.clone(), topic.into(), start, stream))
    }

    /// Asks the node the client is currently using for its status.
    pub async fn status(&self) -> Result<NodeStatus> {
        let (url, reply) = self
            .call_with_url(Repeat::Safe, |channel| async move {
                AdminClient::new(channel).status(pb::StatusRequest {}).await
            })
            .await?;
        Ok(NodeStatus::from_wire(url, reply.into_inner()))
    }

    /// Asks one specific node for its status, with no retries and no
    /// failover. Use it to look at every node in turn.
    pub async fn status_of(&self, endpoint: &str) -> Result<NodeStatus> {
        let url = normalize(endpoint)?;
        let known = {
            let nodes = self.nodes();
            nodes
                .list
                .iter()
                .find(|node| node.url == url)
                .map(|node| node.channel.clone())
        };
        let channel = match known {
            Some(channel) => channel,
            None => open_channel(&self.inner.config, &url)?,
        };
        let mut admin = AdminClient::new(channel);
        let call = admin.status(pb::StatusRequest {});
        let timeout = self.inner.config.request_timeout;
        let result = match tokio::time::timeout(timeout, call).await {
            Ok(result) => result,
            Err(_) => Err(timed_out(&url, timeout)),
        };
        match result {
            Ok(reply) => Ok(NodeStatus::from_wire(url, reply.into_inner())),
            Err(status) if is_transient(status.code()) => Err(Error::Unavailable {
                attempts: 1,
                status,
            }),
            Err(status) => Err(Error::Rejected(status)),
        }
    }

    pub(crate) fn config(&self) -> &ClientConfig {
        &self.inner.config
    }

    /// Opens the server stream behind a subscription.
    pub(crate) async fn open_stream(
        &self,
        topic: &str,
        start: StartAt,
    ) -> Result<Streaming<pb::SubscribeResponse>> {
        let request = pb::SubscribeRequest {
            topic: topic.into(),
            start: Some(start.to_wire()),
        };
        let reply = self
            .call(Repeat::Safe, |channel| {
                let request = request.clone();
                async move { LogClient::new(channel).subscribe(request).await }
            })
            .await?;
        Ok(reply.into_inner())
    }

    async fn call<T, F, Fut>(&self, repeat: Repeat, f: F) -> Result<T>
    where
        F: Fn(Channel) -> Fut,
        Fut: Future<Output = std::result::Result<T, Status>>,
    {
        self.call_with_url(repeat, f).await.map(|(_, value)| value)
    }

    /// Runs one logical call, following redirects and retrying transient
    /// failures. Returns the URL of the node that answered along with the
    /// reply.
    async fn call_with_url<T, F, Fut>(&self, repeat: Repeat, f: F) -> Result<(String, T)>
    where
        F: Fn(Channel) -> Fut,
        Fut: Future<Output = std::result::Result<T, Status>>,
    {
        let config = &self.inner.config;
        let mut attempts = 0u32;
        // The node that most recently could not be reached at all.
        let mut unreachable: Option<String> = None;
        loop {
            let (url, channel) = self.current();
            attempts += 1;
            let status = match tokio::time::timeout(config.request_timeout, f(channel)).await {
                Ok(Ok(value)) => return Ok((url, value)),
                Ok(Err(status)) => status,
                Err(_) => timed_out(&url, config.request_timeout),
            };

            // Decide whether another try makes sense, and against whom.
            let mut wait = config.backoff(attempts);
            if redirect::is_not_leader(&status) {
                // The node did nothing with the request, so trying again
                // elsewhere is always safe.
                // Right after a leader dies, the other nodes still name it
                // until they elect a new one. Going back to a node that
                // was just unreachable would only burn retries, so such a
                // hint is treated like no hint at all.
                let leader = redirect::leader_hint(&status)
                    .and_then(|hint| hint.addr)
                    .filter(|addr| !same_node(addr, unreachable.as_deref()));
                match leader {
                    Some(addr) if self.prefer(&addr) => wait = Duration::ZERO,
                    // No leader known yet, which happens mid-election, or
                    // an address we cannot use. Ask someone else.
                    _ => self.rotate(&url),
                }
            } else if is_transient(status.code()) {
                unreachable = Some(url.clone());
                self.rotate(&url);
                // A write whose outcome is unknown is not sent twice
                // unless the caller asked for that. A refused connection
                // is not such a case: the write never left this machine.
                let ambiguous = !never_sent(&status);
                if repeat == Repeat::Unsafe && ambiguous && !config.retry_writes {
                    return Err(Error::Unavailable { attempts, status });
                }
            } else {
                return Err(Error::Rejected(status));
            }

            if attempts > config.max_retries {
                return Err(Error::Unavailable { attempts, status });
            }
            tokio::time::sleep(wait).await;
        }
    }

    fn nodes(&self) -> MutexGuard<'_, Nodes> {
        // Nothing panics while holding this lock, so poisoning carries no
        // information worth acting on.
        self.inner
            .nodes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn current(&self) -> (String, Channel) {
        let nodes = self.nodes();
        let node = &nodes.list[nodes.current];
        (node.url.clone(), node.channel.clone())
    }

    /// Moves on to the next node, unless another task already moved away
    /// from `failed`.
    fn rotate(&self, failed: &str) {
        let mut nodes = self.nodes();
        if nodes.list[nodes.current].url == failed {
            nodes.current = (nodes.current + 1) % nodes.list.len();
        }
    }

    /// Makes `addr` the node calls go to first, adding it to the list if it
    /// is new. Returns false if the address cannot be used.
    fn prefer(&self, addr: &str) -> bool {
        let Ok(url) = normalize(addr) else {
            return false;
        };
        let mut nodes = self.nodes();
        if let Some(position) = nodes.list.iter().position(|node| node.url == url) {
            nodes.current = position;
            return true;
        }
        let Ok(channel) = open_channel(&self.inner.config, &url) else {
            return false;
        };
        nodes.list.push(Node { url, channel });
        nodes.current = nodes.list.len() - 1;
        true
    }
}

/// Builds a channel that connects on first use and reconnects by itself
/// after a failure.
fn open_channel(config: &ClientConfig, url: &str) -> Result<Channel> {
    let endpoint = Endpoint::from_shared(url.to_owned()).map_err(|e| Error::InvalidEndpoint {
        endpoint: url.to_owned(),
        reason: e.to_string(),
    })?;
    Ok(endpoint
        .connect_timeout(config.connect_timeout)
        .tcp_nodelay(true)
        .http2_keep_alive_interval(KEEP_ALIVE_INTERVAL)
        .keep_alive_timeout(KEEP_ALIVE_TIMEOUT)
        .keep_alive_while_idle(true)
        .connect_lazy())
}

/// True if `addr` names the node at `url`. A hint carries a bare
/// `host:port` and the node list holds URLs.
fn same_node(addr: &str, url: Option<&str>) -> bool {
    let bare = |text: &str| {
        text.split_once("://")
            .map_or(text, |(_, rest)| rest)
            .trim_end_matches('/')
            .to_owned()
    };
    url.is_some_and(|url| bare(url) == bare(addr))
}

fn timed_out(url: &str, timeout: Duration) -> Status {
    Status::deadline_exceeded(format!("no reply from {url} within {timeout:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_hint_is_matched_to_a_node_url() {
        assert!(same_node("10.0.0.1:7001", Some("http://10.0.0.1:7001")));
        assert!(same_node(
            "http://10.0.0.1:7001/",
            Some("http://10.0.0.1:7001")
        ));
        assert!(!same_node("10.0.0.2:7001", Some("http://10.0.0.1:7001")));
        assert!(!same_node("10.0.0.1:7001", None));
    }

    fn client(endpoints: &[&str]) -> Client {
        Client::new(ClientConfig::new(endpoints.iter().copied())).unwrap()
    }

    #[tokio::test]
    async fn construction_checks_the_endpoints() {
        let none: [&str; 0] = [];
        assert!(matches!(
            Client::new(ClientConfig::new(none)),
            Err(Error::NoEndpoints)
        ));
        assert!(matches!(
            Client::new(ClientConfig::new(["ok:1", "not ok:2"])),
            Err(Error::InvalidEndpoint { .. })
        ));
        // Duplicates collapse, including ones that differ only in form.
        let client = client(&["a:1", "http://a:1", "b:2", "a:1/"]);
        assert_eq!(client.endpoints(), ["http://a:1", "http://b:2"]);
    }

    #[tokio::test]
    async fn rotate_moves_on_only_from_the_node_that_failed() {
        let client = client(&["a:1", "b:2", "c:3"]);
        assert_eq!(client.current().0, "http://a:1");

        client.rotate("http://a:1");
        assert_eq!(client.current().0, "http://b:2");
        // A second task reporting the same failure must not skip a node.
        client.rotate("http://a:1");
        assert_eq!(client.current().0, "http://b:2");

        client.rotate("http://b:2");
        client.rotate("http://c:3");
        assert_eq!(client.current().0, "http://a:1");
        assert_eq!(
            client.endpoints(),
            ["http://a:1", "http://b:2", "http://c:3"]
        );
    }

    #[tokio::test]
    async fn prefer_selects_a_known_node_or_learns_a_new_one() {
        let client = client(&["a:1", "b:2"]);
        assert!(client.prefer("b:2"));
        assert_eq!(client.current().0, "http://b:2");
        assert_eq!(client.endpoints(), ["http://b:2", "http://a:1"]);

        assert!(client.prefer("c:3"));
        assert_eq!(client.current().0, "http://c:3");
        assert_eq!(
            client.endpoints(),
            ["http://c:3", "http://a:1", "http://b:2"]
        );

        assert!(!client.prefer("not an address"));
        assert_eq!(client.current().0, "http://c:3");
    }
}
