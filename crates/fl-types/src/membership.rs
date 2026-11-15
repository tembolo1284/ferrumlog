//! Cluster membership and quorum arithmetic.

use std::collections::BTreeSet;

use crate::error::{Error, Result};
use crate::ids::NodeId;

/// The set of voting nodes in the cluster.
///
/// Stored in a `BTreeSet` so iteration order is the same on every node and
/// in every simulator run.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Membership {
    voters: BTreeSet<NodeId>,
}

impl Membership {
    /// Builds a membership from a set of voters. Duplicates are merged. An
    /// empty set is an error because it could never elect a leader.
    pub fn new(voters: impl IntoIterator<Item = NodeId>) -> Result<Self> {
        let voters: BTreeSet<NodeId> = voters.into_iter().collect();
        if voters.is_empty() {
            return Err(Error::EmptyMembership);
        }
        Ok(Membership { voters })
    }

    /// The voters in ascending id order.
    pub fn voters(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.voters.iter().copied()
    }

    /// The voters other than `me`, in ascending id order.
    pub fn peers(&self, me: NodeId) -> impl Iterator<Item = NodeId> + '_ {
        self.voters().filter(move |id| *id != me)
    }

    /// Number of voters.
    pub fn len(&self) -> usize {
        self.voters.len()
    }

    /// Always false. A membership cannot be constructed empty.
    pub fn is_empty(&self) -> bool {
        self.voters.is_empty()
    }

    /// True if `node` is a voter.
    pub fn contains(&self, node: NodeId) -> bool {
        self.voters.contains(&node)
    }

    /// The smallest number of voters that forms a majority.
    pub fn quorum(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    /// True if `nodes` includes a majority of the voters. Nodes that are not
    /// voters, and repeated nodes, do not count.
    pub fn has_quorum(&self, nodes: impl IntoIterator<Item = NodeId>) -> bool {
        let agreeing: BTreeSet<NodeId> = nodes
            .into_iter()
            .filter(|id| self.voters.contains(id))
            .collect();
        agreeing.len() >= self.quorum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(raw: &[u64]) -> Vec<NodeId> {
        raw.iter().copied().map(NodeId::new).collect()
    }

    fn cluster(raw: &[u64]) -> Membership {
        Membership::new(ids(raw)).unwrap()
    }

    #[test]
    fn empty_is_rejected() {
        assert_eq!(Membership::new(ids(&[])), Err(Error::EmptyMembership));
    }

    #[test]
    fn duplicates_merge_and_order_is_stable() {
        let m = cluster(&[3, 1, 2, 3, 1]);
        assert_eq!(m.len(), 3);
        assert!(!m.is_empty());
        assert_eq!(m.voters().collect::<Vec<_>>(), ids(&[1, 2, 3]));
        assert_eq!(m.peers(NodeId::new(2)).collect::<Vec<_>>(), ids(&[1, 3]));
    }

    #[test]
    fn quorum_sizes() {
        let sizes = [(1, 1), (2, 2), (3, 2), (4, 3), (5, 3), (7, 4)];
        for (n, want) in sizes {
            let m = Membership::new((1..=n).map(NodeId::new)).unwrap();
            assert_eq!(m.quorum(), want, "cluster of {n}");
        }
    }

    #[test]
    fn has_quorum_counts_distinct_voters_only() {
        let m = cluster(&[1, 2, 3]);
        assert!(m.has_quorum(ids(&[1, 2])));
        assert!(m.has_quorum(ids(&[1, 2, 3])));
        assert!(!m.has_quorum(ids(&[1])));
        // a repeated vote counts once
        assert!(!m.has_quorum(ids(&[1, 1, 1])));
        // a node outside the membership does not count
        assert!(!m.has_quorum(ids(&[1, 9])));
        assert!(m.contains(NodeId::new(3)));
        assert!(!m.contains(NodeId::new(9)));
    }

    #[test]
    fn single_node_is_its_own_quorum() {
        let m = cluster(&[1]);
        assert!(m.has_quorum(ids(&[1])));
        assert_eq!(m.peers(NodeId::new(1)).count(), 0);
    }
}
