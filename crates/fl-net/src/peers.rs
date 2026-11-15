//! The members of a cluster and where to reach them.

use std::collections::BTreeMap;
use std::fmt;

use fl_types::{Membership, NodeId};

/// Every node in the cluster with its address, keyed by node id.
///
/// The address is the node's gRPC endpoint, as `host:port`. Peers use it
/// for Raft and clients are redirected to it, so it has to be reachable by
/// both.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PeerMap {
    addrs: BTreeMap<NodeId, String>,
}

impl PeerMap {
    /// A cluster of one.
    pub fn single(id: NodeId, addr: impl Into<String>) -> Self {
        PeerMap {
            addrs: BTreeMap::from([(id, addr.into())]),
        }
    }

    /// Parses entries of the form `id=host:port`, one per node.
    ///
    /// ```
    /// use fl_net::PeerMap;
    ///
    /// let peers = PeerMap::parse(["1=10.0.0.1:7001", "2=10.0.0.2:7001"]).unwrap();
    /// assert_eq!(peers.len(), 2);
    /// ```
    pub fn parse<I, S>(entries: I) -> Result<Self, PeerError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut addrs = BTreeMap::new();
        for entry in entries {
            let entry = entry.as_ref().trim();
            if entry.is_empty() {
                continue;
            }
            let malformed = || PeerError::Malformed(entry.to_owned());
            let (id, addr) = entry.split_once('=').ok_or_else(malformed)?;
            let id: u64 = id.trim().parse().map_err(|_| malformed())?;
            let addr = addr.trim();
            if addr.is_empty() || addr.contains(char::is_whitespace) {
                return Err(malformed());
            }
            let id = NodeId::new(id);
            if addrs.insert(id, addr.to_owned()).is_some() {
                return Err(PeerError::Duplicate(id));
            }
        }
        if addrs.is_empty() {
            return Err(PeerError::Empty);
        }
        Ok(PeerMap { addrs })
    }

    /// The address of `id`, if it is a member.
    pub fn addr(&self, id: NodeId) -> Option<&str> {
        self.addrs.get(&id).map(String::as_str)
    }

    /// True if `id` is a member.
    pub fn contains(&self, id: NodeId) -> bool {
        self.addrs.contains_key(&id)
    }

    /// Number of members.
    pub fn len(&self) -> usize {
        self.addrs.len()
    }

    /// Always false. A peer map cannot be built empty.
    pub fn is_empty(&self) -> bool {
        self.addrs.is_empty()
    }

    /// Every member and its address, in id order.
    pub fn iter(&self) -> impl Iterator<Item = (NodeId, &str)> {
        self.addrs.iter().map(|(id, addr)| (*id, addr.as_str()))
    }

    /// The members as a Raft voter set.
    pub fn membership(&self) -> Membership {
        Membership::new(self.addrs.keys().copied()).expect("a peer map is never empty")
    }
}

/// A peer list that cannot be used.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum PeerError {
    /// An entry is not of the form `id=host:port`.
    Malformed(String),
    /// The same node id appears twice.
    Duplicate(NodeId),
    /// The list has no entries.
    Empty,
}

impl fmt::Display for PeerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PeerError::Malformed(entry) => {
                write!(f, "peer {entry:?} is not of the form id=host:port")
            }
            PeerError::Duplicate(id) => write!(f, "node {id} is listed twice"),
            PeerError::Empty => f.write_str("the peer list is empty"),
        }
    }
}

impl std::error::Error for PeerError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_cluster() {
        let peers = PeerMap::parse(["2=b:7001", " 1 = a:7001 ", "", "3=c:7001"]).unwrap();
        assert_eq!(peers.len(), 3);
        assert!(!peers.is_empty());
        assert_eq!(peers.addr(NodeId::new(1)), Some("a:7001"));
        assert_eq!(peers.addr(NodeId::new(9)), None);
        assert!(peers.contains(NodeId::new(3)));
        let order: Vec<u64> = peers.iter().map(|(id, _)| id.get()).collect();
        assert_eq!(order, [1, 2, 3]);
        assert_eq!(peers.membership().quorum(), 2);
    }

    #[test]
    fn rejects_bad_lists() {
        let malformed = |entry: &str| Err(PeerError::Malformed(entry.to_owned()));
        assert_eq!(PeerMap::parse(["a:7001"]), malformed("a:7001"));
        assert_eq!(PeerMap::parse(["x=a:7001"]), malformed("x=a:7001"));
        assert_eq!(PeerMap::parse(["1="]), malformed("1="));
        assert_eq!(PeerMap::parse(["1=a b"]), malformed("1=a b"));
        assert_eq!(
            PeerMap::parse(["1=a:1", "1=b:1"]),
            Err(PeerError::Duplicate(NodeId::new(1)))
        );
        assert_eq!(PeerMap::parse([""; 0]), Err(PeerError::Empty));
        assert!(PeerError::Empty.to_string().contains("empty"));
    }

    #[test]
    fn a_single_node_is_its_own_cluster() {
        let peers = PeerMap::single(NodeId::new(4), "here:7001");
        assert_eq!(peers.len(), 1);
        assert_eq!(peers.membership().quorum(), 1);
    }
}
