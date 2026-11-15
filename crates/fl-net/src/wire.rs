//! Conversions between Raft messages and the gRPC types.

use fl_proto::{pb, ConvertError};
use fl_raft::{Body, Message};
use fl_types::{LogEntry, LogIndex, NodeId, Term};

/// A Raft message in the form it is sent to a peer.
#[derive(Clone, PartialEq, Debug)]
pub enum Request {
    /// Goes out through the `RequestVote` RPC.
    Vote(pb::RequestVoteRequest),
    /// Goes out through the `AppendEntries` RPC.
    Append(pb::AppendEntriesRequest),
}

/// Turns an outgoing message into a request.
///
/// Returns `None` for the two response kinds. Those never travel as
/// requests. They go back as the response of the RPC that asked.
pub fn request(message: Message) -> Option<Request> {
    let Message {
        from, term, body, ..
    } = message;
    match body {
        Body::RequestVote { last_log } => Some(Request::Vote(pb::RequestVoteRequest {
            term: term.get(),
            candidate_id: from.get(),
            last_log: Some(last_log.into()),
        })),
        Body::AppendEntries {
            prev_log,
            entries,
            leader_commit,
        } => Some(Request::Append(pb::AppendEntriesRequest {
            term: term.get(),
            leader_id: from.get(),
            prev_log: Some(prev_log.into()),
            entries: entries.into_iter().map(Into::into).collect(),
            leader_commit: leader_commit.get(),
        })),
        Body::RequestVoteResponse { .. } | Body::AppendEntriesResponse { .. } => None,
    }
}

/// Turns a vote request received by node `me` into a message.
pub fn vote_request(me: NodeId, request: pb::RequestVoteRequest) -> Message {
    Message {
        from: NodeId::new(request.candidate_id),
        to: me,
        term: Term::new(request.term),
        body: Body::RequestVote {
            last_log: request.last_log_id(),
        },
    }
}

/// Turns an append request received by node `me` into a message.
///
/// Fails if an entry carries no command or an invalid one.
pub fn append_request(
    me: NodeId,
    request: pb::AppendEntriesRequest,
) -> Result<Message, ConvertError> {
    let prev_log = request.prev_log_id();
    let entries = request
        .entries
        .into_iter()
        .map(LogEntry::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Message {
        from: NodeId::new(request.leader_id),
        to: me,
        term: Term::new(request.term),
        body: Body::AppendEntries {
            prev_log,
            entries,
            leader_commit: LogIndex::new(request.leader_commit),
        },
    })
}

/// Turns a node's answer to a vote request into the RPC response.
///
/// Returns `None` if the message is some other kind.
pub fn vote_response(message: &Message) -> Option<pb::RequestVoteResponse> {
    match message.body {
        Body::RequestVoteResponse { granted } => Some(pb::RequestVoteResponse {
            term: message.term.get(),
            vote_granted: granted,
        }),
        _ => None,
    }
}

/// Turns a node's answer to an append request into the RPC response.
///
/// Returns `None` if the message is some other kind.
pub fn append_response(message: &Message) -> Option<pb::AppendEntriesResponse> {
    match message.body {
        Body::AppendEntriesResponse {
            success,
            match_index,
            conflict_index,
        } => Some(pb::AppendEntriesResponse {
            term: message.term.get(),
            success,
            match_index: match_index.get(),
            conflict_index: conflict_index.get(),
        }),
        _ => None,
    }
}

/// Turns the vote response that `peer` gave to node `me` into a message.
pub fn vote_reply(peer: NodeId, me: NodeId, response: &pb::RequestVoteResponse) -> Message {
    Message {
        from: peer,
        to: me,
        term: Term::new(response.term),
        body: Body::RequestVoteResponse {
            granted: response.vote_granted,
        },
    }
}

/// Turns the append response that `peer` gave to node `me` into a message.
pub fn append_reply(peer: NodeId, me: NodeId, response: &pb::AppendEntriesResponse) -> Message {
    Message {
        from: peer,
        to: me,
        term: Term::new(response.term),
        body: Body::AppendEntriesResponse {
            success: response.success,
            match_index: LogIndex::new(response.match_index),
            conflict_index: LogIndex::new(response.conflict_index),
        },
    }
}

/// True for the two message kinds that answer a request.
pub fn is_response(message: &Message) -> bool {
    matches!(
        message.body,
        Body::RequestVoteResponse { .. } | Body::AppendEntriesResponse { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use fl_types::{Command, LogId, Record, RecordBatch, TopicId};

    fn n(id: u64) -> NodeId {
        NodeId::new(id)
    }

    fn id(term: u64, index: u64) -> LogId {
        LogId::new(Term::new(term), LogIndex::new(index))
    }

    #[test]
    fn a_vote_request_survives_the_wire() {
        let sent = Message {
            from: n(1),
            to: n(2),
            term: Term::new(7),
            body: Body::RequestVote {
                last_log: id(6, 40),
            },
        };
        let Some(Request::Vote(on_wire)) = request(sent.clone()) else {
            panic!("expected a vote request");
        };
        assert_eq!(vote_request(n(2), on_wire), sent);
    }

    #[test]
    fn an_append_request_survives_the_wire() {
        let batch = RecordBatch::new(TopicId::new(1), vec![Record::new("v").with_key("k")]);
        let sent = Message {
            from: n(3),
            to: n(1),
            term: Term::new(4),
            body: Body::AppendEntries {
                prev_log: id(3, 9),
                entries: vec![
                    LogEntry::new(Term::new(4), LogIndex::new(10), Command::Noop),
                    LogEntry::new(Term::new(4), LogIndex::new(11), Command::Append(batch)),
                ],
                leader_commit: LogIndex::new(9),
            },
        };
        let Some(Request::Append(on_wire)) = request(sent.clone()) else {
            panic!("expected an append request");
        };
        assert_eq!(append_request(n(1), on_wire).unwrap(), sent);
    }

    #[test]
    fn an_entry_without_a_command_is_refused() {
        let request = pb::AppendEntriesRequest {
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
        assert!(append_request(n(2), request).is_err());
    }

    #[test]
    fn responses_survive_the_wire() {
        let vote = Message {
            from: n(2),
            to: n(1),
            term: Term::new(7),
            body: Body::RequestVoteResponse { granted: true },
        };
        let on_wire = vote_response(&vote).unwrap();
        assert_eq!(vote_reply(n(2), n(1), &on_wire), vote);

        let append = Message {
            from: n(2),
            to: n(1),
            term: Term::new(7),
            body: Body::AppendEntriesResponse {
                success: false,
                match_index: LogIndex::new(0),
                conflict_index: LogIndex::new(12),
            },
        };
        let on_wire = append_response(&append).unwrap();
        assert_eq!(append_reply(n(2), n(1), &on_wire), append);

        // Each converter accepts only its own kind.
        assert!(vote_response(&append).is_none());
        assert!(append_response(&vote).is_none());
        assert!(is_response(&vote) && is_response(&append));
        // A response never becomes a request.
        assert!(request(vote).is_none());
    }
}
