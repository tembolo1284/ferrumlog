//! The consume benchmark.

use std::sync::Arc;

use fl_client::{Client, ClientConfig, Error, FetchOptions};
use fl_types::Offset;
use tokio::time::Instant;

use crate::produce::micros;
use crate::report::Summary;

/// Settings for [`consume`].
#[derive(Clone, Debug)]
pub struct ConsumeOptions {
    /// How to reach the cluster. Each worker makes its own client.
    pub client: ClientConfig,
    /// The topic to read.
    pub topic: String,
    /// Concurrent readers. Each one reads the whole topic.
    pub workers: usize,
    /// Most records per fetch. Zero leaves it to the server.
    pub max_records: u32,
    /// Most record bytes per fetch. Zero leaves it to the server.
    pub max_bytes: u32,
}

impl ConsumeOptions {
    /// One reader with the server's default fetch size.
    pub fn new(client: ClientConfig, topic: impl Into<String>) -> Self {
        ConsumeOptions {
            client,
            topic: topic.into(),
            workers: 1,
            max_records: 0,
            max_bytes: 0,
        }
    }
}

/// Runs the consume benchmark and returns what it measured.
///
/// Every worker reads from the first record up to where the topic ended
/// when the run began, so records written during the run do not move the
/// finish line. Fails if the topic cannot be read at all.
pub async fn consume(options: ConsumeOptions) -> Result<Summary, Error> {
    let workers = options.workers.max(1);
    let options = Arc::new(ConsumeOptions { workers, ..options });
    let fetch = FetchOptions {
        max_records: options.max_records,
        max_bytes: options.max_bytes,
    };

    // A fetch far past the end returns no records and the high watermark,
    // which is the finish line. It also fails early for a missing topic.
    let probe = Client::new(options.client.clone())?;
    let end = probe
        .fetch(&options.topic, Offset::new(u64::MAX), fetch)
        .await?
        .high_watermark;

    let mut clients = Vec::with_capacity(workers);
    for _ in 0..workers {
        clients.push(Client::new(options.client.clone())?);
    }
    let started = Instant::now();
    let mut tasks = Vec::with_capacity(workers);
    for client in clients {
        let options = Arc::clone(&options);
        tasks.push(tokio::spawn(async move {
            read_all(&client, &options.topic, end, fetch).await
        }));
    }

    let mut total = Summary::default();
    for task in tasks {
        if let Ok(summary) = task.await {
            total.merge(&summary);
        }
    }
    total.elapsed = started.elapsed();
    Ok(total)
}

async fn read_all(client: &Client, topic: &str, end: Offset, fetch: FetchOptions) -> Summary {
    let mut summary = Summary::default();
    let mut next = Offset::ZERO;
    // A reader that keeps failing gives up instead of spinning forever.
    let mut failures_in_a_row = 0;

    while next < end && failures_in_a_row < 5 {
        let asked = Instant::now();
        match client.fetch(topic, next, fetch).await {
            Ok(batch) => {
                failures_in_a_row = 0;
                let Some(after) = batch.next_offset() else {
                    // Nothing came back short of the end. Do not loop on it.
                    break;
                };
                summary.latency.record(micros(asked.elapsed()));
                summary.requests += 1;
                summary.records += batch.records.len() as u64;
                summary.bytes += batch
                    .records
                    .iter()
                    .map(|consumed| consumed.record.payload_len() as u64)
                    .sum::<u64>();
                next = after;
            }
            Err(_) => {
                summary.errors += 1;
                failures_in_a_row += 1;
            }
        }
    }
    summary
}
