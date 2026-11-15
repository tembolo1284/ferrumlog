//! The produce benchmark.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fl_client::{Client, ClientConfig, Error};
use fl_types::Record;
use tokio::time::Instant;

use crate::report::Summary;

/// When a produce run stops.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Limit {
    /// After this many requests in total, across all workers.
    Requests(u64),
    /// After this much time.
    Time(Duration),
}

/// Settings for [`produce`].
#[derive(Clone, Debug)]
pub struct ProduceOptions {
    /// How to reach the cluster. Each worker makes its own client from
    /// this, so each has its own connections.
    pub client: ClientConfig,
    /// The topic to write to. Created if it does not exist.
    pub topic: String,
    /// Concurrent writers.
    pub workers: usize,
    /// Size of each record's value in bytes. At least 8, because every
    /// record starts with a number that makes it unique.
    pub record_bytes: usize,
    /// Records per request.
    pub batch: usize,
    /// When to stop.
    pub limit: Limit,
    /// Requests per second to aim for, across all workers. `None` sends
    /// as fast as the cluster answers.
    pub rate: Option<f64>,
}

impl ProduceOptions {
    /// Eight workers writing single 100-byte records for ten seconds.
    pub fn new(client: ClientConfig, topic: impl Into<String>) -> Self {
        ProduceOptions {
            client,
            topic: topic.into(),
            workers: 8,
            record_bytes: 100,
            batch: 1,
            limit: Limit::Time(Duration::from_secs(10)),
            rate: None,
        }
    }
}

/// Runs the produce benchmark and returns what it measured.
///
/// Fails only if the cluster cannot be reached to create the topic. Once
/// the run is under way, a failed request is counted and the run goes on.
pub async fn produce(options: ProduceOptions) -> Result<Summary, Error> {
    let workers = options.workers.max(1);
    let options = Arc::new(ProduceOptions {
        workers,
        record_bytes: options.record_bytes.max(8),
        batch: options.batch.max(1),
        ..options
    });
    Client::new(options.client.clone())?
        .create_topic(&options.topic)
        .await?;

    // Each client is built up front, so the measured time holds no setup.
    let mut clients = Vec::with_capacity(workers);
    for _ in 0..workers {
        clients.push(Client::new(options.client.clone())?);
    }

    let budget = Arc::new(AtomicU64::new(match options.limit {
        Limit::Requests(requests) => requests,
        Limit::Time(_) => u64::MAX,
    }));
    let sequence = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let deadline = match options.limit {
        Limit::Time(time) => Some(started + time),
        Limit::Requests(_) => None,
    };

    let mut tasks = Vec::with_capacity(workers);
    for client in clients {
        let worker = Worker {
            client,
            options: Arc::clone(&options),
            budget: Arc::clone(&budget),
            sequence: Arc::clone(&sequence),
            started,
            deadline,
        };
        tasks.push(tokio::spawn(worker.run()));
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

struct Worker {
    client: Client,
    options: Arc<ProduceOptions>,
    /// Requests left to send, shared by all workers.
    budget: Arc<AtomicU64>,
    /// Numbers the records, so no two are the same.
    sequence: Arc<AtomicU64>,
    started: Instant,
    deadline: Option<Instant>,
}

impl Worker {
    async fn run(self) -> Summary {
        let options = &self.options;
        let mut summary = Summary::default();
        // With a target rate, this worker's share of it sets a schedule.
        let interval = options
            .rate
            .filter(|rate| *rate > 0.0)
            .map(|rate| Duration::from_secs_f64(options.workers as f64 / rate));
        let mut sent = 0u32;

        loop {
            // On a schedule, a request is due at a fixed time whether or
            // not the one before it has finished.
            let due = interval.map(|interval| self.started + interval * sent);
            if let Some(due) = due {
                tokio::time::sleep_until(due).await;
            }
            if self
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
            {
                break;
            }
            if !take_one(&self.budget) {
                break;
            }
            sent = sent.saturating_add(1);

            let records = self.batch();
            let bytes: usize = records.iter().map(Record::payload_len).sum();
            // Measured from when the request was due, if there is a
            // schedule. A request that had to wait behind a slow one then
            // counts the wait as well.
            let from = due.unwrap_or_else(Instant::now);
            match self.client.produce(&options.topic, records).await {
                Ok(produced) => {
                    summary.latency.record(micros(from.elapsed()));
                    summary.requests += 1;
                    summary.records += u64::from(produced.count);
                    summary.bytes += bytes as u64;
                }
                Err(_) => summary.errors += 1,
            }
        }
        summary
    }

    fn batch(&self) -> Vec<Record> {
        let options = &self.options;
        let first = self
            .sequence
            .fetch_add(options.batch as u64, Ordering::Relaxed);
        (0..options.batch as u64)
            .map(|offset| {
                let mut value = vec![b'x'; options.record_bytes];
                value[..8].copy_from_slice(&(first + offset).to_be_bytes());
                Record::new(value)
            })
            .collect()
    }
}

/// Takes one request from the shared budget. False when none are left.
fn take_one(budget: &AtomicU64) -> bool {
    budget
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
            left.checked_sub(1)
        })
        .is_ok()
}

pub(crate) fn micros(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_budget_is_spent_exactly() {
        let budget = AtomicU64::new(3);
        assert!(take_one(&budget));
        assert!(take_one(&budget));
        assert!(take_one(&budget));
        assert!(!take_one(&budget));
        assert!(!take_one(&budget));
        assert_eq!(budget.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn defaults_are_sensible() {
        let options = ProduceOptions::new(ClientConfig::new(["127.0.0.1:7001"]), "bench");
        assert_eq!(options.workers, 8);
        assert_eq!(options.record_bytes, 100);
        assert_eq!(options.batch, 1);
        assert_eq!(options.limit, Limit::Time(Duration::from_secs(10)));
        assert_eq!(options.rate, None);
        assert_eq!(micros(Duration::from_millis(3)), 3_000);
    }
}
