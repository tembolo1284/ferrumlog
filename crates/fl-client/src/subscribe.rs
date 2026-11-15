//! Subscriptions that survive a dropped connection.

use fl_proto::pb;
use tonic::{Status, Streaming};

use crate::client::Client;
use crate::error::{is_transient, Error, Result};
use crate::types::{batch, Batch, StartAt};

/// A live feed of one topic.
///
/// Call [`Subscription::next`] in a loop. Each call returns the next batch
/// of records, waiting if there is nothing new yet.
///
/// If the connection breaks, the next call reconnects, possibly to a
/// different node, and resumes at the offset right after the last record
/// it returned. Records are neither skipped nor repeated.
pub struct Subscription {
    client: Client,
    topic: String,
    /// Where the stream should start if it has to be reopened.
    position: StartAt,
    /// The open stream, or `None` after a failure.
    stream: Option<Streaming<pb::SubscribeResponse>>,
    /// Reconnects since the last batch arrived.
    failures: u32,
}

impl Subscription {
    pub(crate) fn new(
        client: Client,
        topic: String,
        position: StartAt,
        stream: Streaming<pb::SubscribeResponse>,
    ) -> Self {
        Subscription {
            client,
            topic,
            position,
            stream: Some(stream),
            failures: 0,
        }
    }

    /// The topic this subscription reads.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Waits for the next batch of records.
    ///
    /// The batch is never empty. This returns an error only when the server
    /// rejects the subscription or no node can be reached after the
    /// configured retries. After an error the subscription can still be
    /// used: the next call tries to reconnect again.
    pub async fn next(&mut self) -> Result<Batch> {
        loop {
            let mut stream = match self.stream.take() {
                Some(stream) => stream,
                None => self.client.open_stream(&self.topic, self.position).await?,
            };

            let failure = match stream.message().await {
                Ok(Some(message)) => {
                    let batch = batch(message.records, message.high_watermark)?;
                    self.stream = Some(stream);
                    // An empty message carries nothing to hand back, so
                    // keep waiting on the same stream.
                    let Some(next) = batch.next_offset() else {
                        continue;
                    };
                    self.position = StartAt::Offset(next);
                    self.failures = 0;
                    return Ok(batch);
                }
                // The server ended the stream, which it does when it shuts
                // down.
                Ok(None) => Status::unavailable("the server closed the stream"),
                Err(status) if is_transient(status.code()) => status,
                Err(status) => return Err(Error::Rejected(status)),
            };

            self.failures += 1;
            let config = self.client.config();
            if self.failures > config.max_retries {
                let attempts = self.failures;
                // Start counting afresh so the caller can try again later.
                self.failures = 0;
                return Err(Error::Unavailable {
                    attempts,
                    status: failure,
                });
            }
            tokio::time::sleep(config.backoff(self.failures)).await;
        }
    }
}
