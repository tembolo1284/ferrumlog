//! The "not the leader" signal.
//!
//! Only the leader can accept writes. When a client sends a write to any
//! other node, that node fails the call with `FAILED_PRECONDITION` and, if
//! it knows who the leader is, names it in the response metadata. The client
//! reads the hint and retries against the leader.
//!
//! The server side builds the status with [`not_leader`]. The client side
//! reads it back with [`leader_hint`].

use fl_types::NodeId;
use tonic::metadata::MetadataValue;
use tonic::{Code, Status};

/// Metadata key that marks a status as a not-leader rejection. Always
/// present on one, with the value `"1"`.
pub const NOT_LEADER_KEY: &str = "ferrumlog-not-leader";

/// Metadata key holding the leader's node id as a decimal number.
pub const LEADER_ID_KEY: &str = "ferrumlog-leader-id";

/// Metadata key holding the address clients should use to reach the leader.
pub const LEADER_ADDR_KEY: &str = "ferrumlog-leader-addr";

/// What a node knows about the current leader.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct LeaderHint {
    /// The leader's node id.
    pub node_id: NodeId,
    /// The leader's client-facing address, if the rejecting node knew it.
    pub addr: Option<String>,
}

/// Builds the status a non-leader returns for a write.
///
/// Pass `None` when no leader is known, which happens during an election.
/// An address that cannot be carried in metadata is dropped, and the client
/// falls back to looking the node id up itself.
pub fn not_leader(leader: Option<&LeaderHint>) -> Status {
    let mut status = Status::new(Code::FailedPrecondition, "not the leader");
    let metadata = status.metadata_mut();
    metadata.insert(NOT_LEADER_KEY, MetadataValue::from_static("1"));
    if let Some(hint) = leader {
        metadata.insert(LEADER_ID_KEY, MetadataValue::from(hint.node_id.get()));
        if let Some(Ok(addr)) = hint.addr.as_deref().map(MetadataValue::try_from) {
            metadata.insert(LEADER_ADDR_KEY, addr);
        }
    }
    status
}

/// True if `status` is a not-leader rejection built by [`not_leader`].
pub fn is_not_leader(status: &Status) -> bool {
    status.code() == Code::FailedPrecondition && status.metadata().contains_key(NOT_LEADER_KEY)
}

/// Extracts the leader hint from a not-leader rejection.
///
/// Returns `None` if `status` is not such a rejection, or if the rejecting
/// node did not know the leader.
pub fn leader_hint(status: &Status) -> Option<LeaderHint> {
    if !is_not_leader(status) {
        return None;
    }
    let metadata = status.metadata();
    let node_id = metadata
        .get(LEADER_ID_KEY)?
        .to_str()
        .ok()?
        .parse::<u64>()
        .ok()?;
    let addr = metadata
        .get(LEADER_ADDR_KEY)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    Some(LeaderHint {
        node_id: NodeId::new(node_id),
        addr,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hint(id: u64, addr: Option<&str>) -> LeaderHint {
        LeaderHint {
            node_id: NodeId::new(id),
            addr: addr.map(str::to_owned),
        }
    }

    #[test]
    fn hint_round_trips_with_an_address() {
        let sent = hint(2, Some("10.0.1.12:7001"));
        let status = not_leader(Some(&sent));
        assert_eq!(status.code(), Code::FailedPrecondition);
        assert!(is_not_leader(&status));
        assert_eq!(leader_hint(&status), Some(sent));
    }

    #[test]
    fn hint_round_trips_without_an_address() {
        let sent = hint(u64::MAX, None);
        let status = not_leader(Some(&sent));
        assert_eq!(leader_hint(&status), Some(sent));
    }

    #[test]
    fn unknown_leader_is_still_a_not_leader_rejection() {
        let status = not_leader(None);
        assert!(is_not_leader(&status));
        assert_eq!(leader_hint(&status), None);
    }

    #[test]
    fn address_that_metadata_cannot_carry_is_dropped() {
        let status = not_leader(Some(&hint(3, Some("bad\naddr"))));
        assert_eq!(leader_hint(&status), Some(hint(3, None)));
    }

    #[test]
    fn other_statuses_are_not_mistaken_for_redirects() {
        let plain = Status::failed_precondition("topic does not exist");
        assert!(!is_not_leader(&plain));
        assert_eq!(leader_hint(&plain), None);

        // The marker alone is not enough. The code must match too.
        let mut wrong_code = Status::unavailable("try later");
        wrong_code
            .metadata_mut()
            .insert(NOT_LEADER_KEY, MetadataValue::from_static("1"));
        assert!(!is_not_leader(&wrong_code));
    }

    #[test]
    fn garbled_leader_id_yields_no_hint() {
        let mut status = not_leader(None);
        status
            .metadata_mut()
            .insert(LEADER_ID_KEY, MetadataValue::from_static("not-a-number"));
        assert_eq!(leader_hint(&status), None);
    }
}
