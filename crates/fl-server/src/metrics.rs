//! Process metrics in the Prometheus text exposition format.
//!
//! Every metric is a handful of atomics, so recording one never takes a
//! lock and never allocates. [`Metrics::render`] walks them and writes the
//! text a Prometheus server scrapes from `/metrics`.

use std::fmt::Write;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

/// A value that only goes up.
#[derive(Default)]
pub struct Counter(AtomicU64);

impl Counter {
    /// Adds one.
    pub fn inc(&self) {
        self.add(1);
    }

    /// Adds `n`.
    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    /// The current value.
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// A value that can go up and down.
#[derive(Default)]
pub struct Gauge(AtomicI64);

impl Gauge {
    /// Replaces the value.
    pub fn set(&self, value: u64) {
        self.0
            .store(i64::try_from(value).unwrap_or(i64::MAX), Ordering::Relaxed);
    }

    /// Adds one.
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    /// Subtracts one.
    pub fn dec(&self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }

    /// The current value.
    pub fn get(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// A distribution of observed values over fixed buckets.
pub struct Histogram {
    /// Upper bound of each bucket, ascending.
    bounds: &'static [f64],
    /// Observations that fell in each bucket, not cumulative. One extra
    /// slot at the end counts values above the last bound.
    counts: Vec<AtomicU64>,
    /// Sum of all observations, in millionths.
    sum_micros: AtomicU64,
}

/// Bucket bounds in seconds, from 100 microseconds to 5 seconds.
const SECONDS: &[f64] = &[
    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

/// Bucket bounds for small counts.
const SIZES: &[f64] = &[1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0];

impl Histogram {
    fn new(bounds: &'static [f64]) -> Self {
        Histogram {
            bounds,
            counts: (0..=bounds.len()).map(|_| AtomicU64::new(0)).collect(),
            sum_micros: AtomicU64::new(0),
        }
    }

    /// Records one value.
    pub fn observe(&self, value: f64) {
        let slot = self.bounds.partition_point(|bound| *bound < value);
        self.counts[slot].fetch_add(1, Ordering::Relaxed);
        let micros = (value.max(0.0) * 1_000_000.0).round() as u64;
        self.sum_micros.fetch_add(micros, Ordering::Relaxed);
    }

    /// Records a duration in seconds.
    pub fn observe_duration(&self, elapsed: Duration) {
        self.observe(elapsed.as_secs_f64());
    }

    /// Number of values recorded.
    pub fn count(&self) -> u64 {
        self.counts.iter().map(|c| c.load(Ordering::Relaxed)).sum()
    }
}

/// Every metric the node exports.
pub struct Metrics {
    /// Produce calls that were committed.
    pub produce_ok: Counter,
    /// Produce calls that were rejected or failed.
    pub produce_err: Counter,
    /// Records committed by produce calls.
    pub records_produced: Counter,
    /// Key and value bytes committed by produce calls.
    pub bytes_produced: Counter,
    /// Time from a produce call arriving to its reply, in seconds.
    pub produce_seconds: Histogram,

    /// Fetch calls and subscription reads served.
    pub fetches: Counter,
    /// Records returned by fetches and subscriptions.
    pub records_fetched: Counter,
    /// Subscribe streams currently open.
    pub subscriptions: Gauge,

    /// Group commits performed. Each one is a single fsync.
    pub commits: Counter,
    /// Write requests per group commit.
    pub commit_batch_size: Histogram,
    /// Time to append and fsync one group commit, in seconds.
    pub commit_seconds: Histogram,

    /// Index of the last entry in the log.
    pub log_last_index: Gauge,
    /// Total size of the segment files in bytes.
    pub log_bytes: Gauge,
    /// Number of segment files.
    pub log_segments: Gauge,
    /// Number of topics.
    pub topics: Gauge,

    /// This node's current Raft term.
    pub raft_term: Gauge,
    /// 1 while this node is the leader, otherwise 0.
    pub raft_leader: Gauge,
    /// Highest log index this node knows to be committed.
    pub commit_index: Gauge,
}

impl Default for Metrics {
    fn default() -> Self {
        Metrics {
            produce_ok: Counter::default(),
            produce_err: Counter::default(),
            records_produced: Counter::default(),
            bytes_produced: Counter::default(),
            produce_seconds: Histogram::new(SECONDS),
            fetches: Counter::default(),
            records_fetched: Counter::default(),
            subscriptions: Gauge::default(),
            commits: Counter::default(),
            commit_batch_size: Histogram::new(SIZES),
            commit_seconds: Histogram::new(SECONDS),
            log_last_index: Gauge::default(),
            log_bytes: Gauge::default(),
            log_segments: Gauge::default(),
            topics: Gauge::default(),
            raft_term: Gauge::default(),
            raft_leader: Gauge::default(),
            commit_index: Gauge::default(),
        }
    }
}

impl Metrics {
    /// Renders every metric in the Prometheus text format.
    pub fn render(&self) -> String {
        let mut out = String::with_capacity(4096);

        header(
            &mut out,
            "ferrumlog_produce_requests_total",
            "counter",
            "Produce calls by outcome.",
        );
        line(
            &mut out,
            "ferrumlog_produce_requests_total{result=\"ok\"}",
            self.produce_ok.get(),
        );
        line(
            &mut out,
            "ferrumlog_produce_requests_total{result=\"error\"}",
            self.produce_err.get(),
        );

        counter(
            &mut out,
            "ferrumlog_records_produced_total",
            "Records committed by produce calls.",
            &self.records_produced,
        );
        counter(
            &mut out,
            "ferrumlog_bytes_produced_total",
            "Key and value bytes committed by produce calls.",
            &self.bytes_produced,
        );
        histogram(
            &mut out,
            "ferrumlog_produce_duration_seconds",
            "Time from a produce call arriving to its reply.",
            &self.produce_seconds,
        );

        counter(
            &mut out,
            "ferrumlog_fetch_requests_total",
            "Fetch calls and subscription reads served.",
            &self.fetches,
        );
        counter(
            &mut out,
            "ferrumlog_records_fetched_total",
            "Records returned by fetches and subscriptions.",
            &self.records_fetched,
        );
        gauge(
            &mut out,
            "ferrumlog_subscriptions",
            "Subscribe streams currently open.",
            &self.subscriptions,
        );

        counter(
            &mut out,
            "ferrumlog_commits_total",
            "Group commits performed, one fsync each.",
            &self.commits,
        );
        histogram(
            &mut out,
            "ferrumlog_commit_batch_size",
            "Write requests per group commit.",
            &self.commit_batch_size,
        );
        histogram(
            &mut out,
            "ferrumlog_commit_duration_seconds",
            "Time to append and fsync one group commit.",
            &self.commit_seconds,
        );

        gauge(
            &mut out,
            "ferrumlog_log_last_index",
            "Index of the last entry in the log.",
            &self.log_last_index,
        );
        gauge(
            &mut out,
            "ferrumlog_log_bytes",
            "Total size of the segment files in bytes.",
            &self.log_bytes,
        );
        gauge(
            &mut out,
            "ferrumlog_log_segments",
            "Number of segment files.",
            &self.log_segments,
        );
        gauge(
            &mut out,
            "ferrumlog_topics",
            "Number of topics.",
            &self.topics,
        );

        gauge(
            &mut out,
            "ferrumlog_raft_term",
            "This node's current Raft term.",
            &self.raft_term,
        );
        gauge(
            &mut out,
            "ferrumlog_raft_leader",
            "1 while this node is the leader, otherwise 0.",
            &self.raft_leader,
        );
        gauge(
            &mut out,
            "ferrumlog_commit_index",
            "Highest log index known to be committed.",
            &self.commit_index,
        );
        out
    }
}

fn header(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

fn line(out: &mut String, series: &str, value: impl std::fmt::Display) {
    let _ = writeln!(out, "{series} {value}");
}

fn counter(out: &mut String, name: &str, help: &str, value: &Counter) {
    header(out, name, "counter", help);
    line(out, name, value.get());
}

fn gauge(out: &mut String, name: &str, help: &str, value: &Gauge) {
    header(out, name, "gauge", help);
    line(out, name, value.get());
}

fn histogram(out: &mut String, name: &str, help: &str, value: &Histogram) {
    header(out, name, "histogram", help);
    // Prometheus buckets are cumulative: each one counts every observation
    // at or below its bound.
    let mut running = 0u64;
    for (bound, count) in value.bounds.iter().zip(&value.counts) {
        running += count.load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_bucket{{le=\"{bound}\"}} {running}");
    }
    let total = value.count();
    let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {total}");
    let sum = value.sum_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0;
    let _ = writeln!(out, "{name}_sum {sum}");
    let _ = writeln!(out, "{name}_count {total}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_and_gauges_move() {
        let counter = Counter::default();
        counter.inc();
        counter.add(4);
        assert_eq!(counter.get(), 5);

        let gauge = Gauge::default();
        gauge.inc();
        gauge.inc();
        gauge.dec();
        assert_eq!(gauge.get(), 1);
        gauge.set(42);
        assert_eq!(gauge.get(), 42);
    }

    #[test]
    fn histogram_buckets_are_cumulative_and_inclusive() {
        let h = Histogram::new(SIZES);
        // 1 lands in le=1, 2 in le=2, 3 in le=4, 600 above every bound.
        for value in [1.0, 2.0, 3.0, 600.0] {
            h.observe(value);
        }
        assert_eq!(h.count(), 4);

        let mut out = String::new();
        histogram(&mut out, "m", "help", &h);
        assert!(out.contains("m_bucket{le=\"1\"} 1\n"), "{out}");
        assert!(out.contains("m_bucket{le=\"2\"} 2\n"), "{out}");
        assert!(out.contains("m_bucket{le=\"4\"} 3\n"), "{out}");
        assert!(out.contains("m_bucket{le=\"512\"} 3\n"), "{out}");
        assert!(out.contains("m_bucket{le=\"+Inf\"} 4\n"), "{out}");
        assert!(out.contains("m_sum 606\n"), "{out}");
        assert!(out.contains("m_count 4\n"), "{out}");
    }

    #[test]
    fn durations_are_recorded_in_seconds() {
        let h = Histogram::new(SECONDS);
        h.observe_duration(Duration::from_millis(3));
        let mut out = String::new();
        histogram(&mut out, "d", "help", &h);
        assert!(out.contains("d_bucket{le=\"0.0025\"} 0\n"), "{out}");
        assert!(out.contains("d_bucket{le=\"0.005\"} 1\n"), "{out}");
        assert!(out.contains("d_sum 0.003\n"), "{out}");
    }

    #[test]
    fn render_is_well_formed() {
        let metrics = Metrics::default();
        metrics.produce_ok.add(7);
        metrics.produce_err.inc();
        metrics.topics.set(2);
        metrics.commit_batch_size.observe(3.0);
        let text = metrics.render();

        assert!(text.contains("ferrumlog_produce_requests_total{result=\"ok\"} 7\n"));
        assert!(text.contains("ferrumlog_produce_requests_total{result=\"error\"} 1\n"));
        assert!(text.contains("# TYPE ferrumlog_topics gauge\nferrumlog_topics 2\n"));
        assert!(text.contains("ferrumlog_commit_batch_size_count 1\n"));

        // Every line is a comment or `series value`, and every series has a
        // TYPE line before it.
        let mut typed = std::collections::BTreeSet::new();
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix("# TYPE ") {
                typed.insert(rest.split(' ').next().unwrap().to_owned());
                continue;
            }
            if line.starts_with("# HELP ") {
                continue;
            }
            let (series, value) = line.rsplit_once(' ').expect(line);
            assert!(value.parse::<f64>().is_ok(), "{line}");
            let name = series.split('{').next().unwrap();
            let base = name
                .trim_end_matches("_bucket")
                .trim_end_matches("_sum")
                .trim_end_matches("_count");
            assert!(
                typed.contains(name) || typed.contains(base),
                "no TYPE for {line}"
            );
        }
    }
}
