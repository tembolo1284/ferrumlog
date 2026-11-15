//! The receiving side: the `Raft` gRPC service.

use std::sync::Arc;

use fl_proto::pb;
use fl_raft::Message;
use fl_types::NodeId;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

use crate::wire;

/// Where incoming requests are handed to the node.
///
/// The function receives the request as a message, and a channel for the
/// node's answer. It must not block. It returns `false` if the node cannot
/// take the request right now, for example because its queue is full.
///
/// Dropping the channel without sending means there is no answer, and the
/// caller gets an error.
pub type RequestSink = Arc<dyn Fn(Message, oneshot::Sender<Message>) -> bool + Send + Sync>;

/// Implements the `Raft` gRPC service for one node.
pub struct RaftService {
    me: NodeId,
    sink: RequestSink,
}

impl RaftService {
    /// A service for node `me` that hands requests to `sink`.
    pub fn new(me: NodeId, sink: RequestSink) -> Self {
        RaftService { me, sink }
    }

    async fn ask(&self, message: Message) -> Result<Message, Status> {
        let (answer, answered) = oneshot::channel();
        if !(self.sink)(message, answer) {
            return Err(Status::unavailable("the node is busy or shutting down"));
        }
        answered
            .await
            .map_err(|_| Status::unavailable("the node gave no answer"))
    }
}

fn wrong_kind() -> Status {
    Status::internal("the node answered with the wrong kind of message")
}

#[tonic::async_trait]
impl fl_proto::Raft for RaftService {
    async fn request_vote(
        &self,
        request: Request<pb::RequestVoteRequest>,
    ) -> Result<Response<pb::RequestVoteResponse>, Status> {
        let message = wire::vote_request(self.me, request.into_inner());
        let answer = self.ask(message).await?;
        wire::vote_response(&answer)
            .map(Response::new)
            .ok_or_else(wrong_kind)
    }

    async fn append_entries(
        &self,
        request: Request<pb::AppendEntriesRequest>,
    ) -> Result<Response<pb::AppendEntriesResponse>, Status> {
        let message = wire::append_request(self.me, request.into_inner())?;
        let answer = self.ask(message).await?;
        wire::append_response(&answer)
            .map(Response::new)
            .ok_or_else(wrong_kind)
    }
}
