//! The ferrumlog client library.
//!
//! [`Client`] wraps the generated gRPC stubs with the behaviour every
//! caller needs and nobody wants to write twice:
//!
//! * **Several endpoints.** Give it every node you know about. It talks to
//!   one at a time and moves to the next when that one stops answering.
//! * **Leader redirects.** A node that is not the leader says who is. The
//!   client follows the hint and remembers it for later calls.
//! * **Retries with backoff.** Reads and other idempotent calls are retried
//!   on transient failures. See below for writes.
//! * **Resumable subscriptions.** A [`Subscription`] remembers the next
//!   offset it needs. If the connection drops it reconnects and carries on
//!   from there, with no gaps and no repeats.
//!
//! # Writes and retries
//!
//! When a produce call fails with a transient error, the client cannot
//! tell whether the records were written. The request may have been lost
//! on the way out, or the reply on the way back.
//!
//! By default the client does not retry in that case. It returns
//! [`Error::Unavailable`] and the caller decides. Setting
//! [`ClientConfig::retry_writes`] makes it retry instead, which gives
//! at-least-once delivery: nothing acknowledged is lost, but a record can
//! appear twice.
//!
//! A redirect is different. A node that answers "not the leader" did not
//! write anything, so following a redirect is always safe.
//!
//! # Example
//!
//! ```no_run
//! use fl_client::{Client, ClientConfig, StartAt};
//! use fl_types::Record;
//!
//! # async fn example() -> Result<(), fl_client::Error> {
//! let client = Client::connect(ClientConfig::new(["127.0.0.1:7001"])).await?;
//! client.create_topic("quotes").await?;
//! client.produce("quotes", vec![Record::new("99.5")]).await?;
//!
//! let mut subscription = client.subscribe("quotes", StartAt::Earliest).await?;
//! let batch = subscription.next().await?;
//! println!("{} records", batch.records.len());
//! # Ok(())
//! # }
//! ```

mod client;
mod config;
mod error;
mod subscribe;
mod types;

pub use client::Client;
pub use config::ClientConfig;
pub use error::{Error, Result};
pub use subscribe::Subscription;
pub use types::{
    Batch, ConsumedRecord, FetchOptions, NodeStatus, Peer, Produced, Role, StartAt, TopicCreated,
    TopicInfo,
};
