//! The result of a benchmark, and how it is printed.

use std::fmt::Write as _;
use std::time::Duration;

use crate::histogram::Histogram;

/// What one benchmark run measured.
#[derive(Clone, Debug, Default)]
pub struct Summary {
    /// Requests that succeeded.
    pub requests: u64,
    /// Requests that failed. Their time is not in the histogram.
    pub errors: u64,
    /// Records written or read by the successful requests.
    pub records: u64,
    /// Key and value bytes in those records.
    pub bytes: u64,
    /// Wall-clock time from the first request to the last answer.
    pub elapsed: Duration,
    /// Latency of the successful requests, in microseconds.
    pub latency: Histogram,
}

impl Summary {
    /// Adds another worker's results. The elapsed time is not summed. The
    /// caller sets it for the run as a whole.
    pub fn merge(&mut self, other: &Summary) {
        self.requests += other.requests;
        self.errors += other.errors;
        self.records += other.records;
        self.bytes += other.bytes;
        self.latency.merge(&other.latency);
    }

    /// Successful requests per second.
    pub fn requests_per_second(&self) -> f64 {
        per_second(self.requests, self.elapsed)
    }

    /// Records per second.
    pub fn records_per_second(&self) -> f64 {
        per_second(self.records, self.elapsed)
    }

    /// Megabytes of record data per second, with a megabyte as 10^6 bytes.
    pub fn megabytes_per_second(&self) -> f64 {
        per_second(self.bytes, self.elapsed) / 1e6
    }

    /// A few lines for a person to read.
    pub fn to_text(&self, title: &str) -> String {
        let ms = |micros: u64| micros as f64 / 1_000.0;
        let latency = &self.latency;
        let mut out = String::new();
        let _ = writeln!(out, "{title}");
        let _ = writeln!(
            out,
            "  elapsed     {:.2} s",
            self.elapsed.as_secs_f64()
        );
        let _ = writeln!(
            out,
            "  requests    {} ok, {} failed, {:.0}/s",
            self.requests,
            self.errors,
            self.requests_per_second()
        );
        let _ = writeln!(
            out,
            "  records     {}, {:.0}/s",
            self.records,
            self.records_per_second()
        );
        let _ = writeln!(
            out,
            "  data        {:.2} MB, {:.2} MB/s",
            self.bytes as f64 / 1e6,
            self.megabytes_per_second()
        );
        let _ = writeln!(
            out,
            "  latency ms  p50 {:.2}  p90 {:.2}  p99 {:.2}  p99.9 {:.2}  max {:.2}  mean {:.2}",
            ms(latency.percentile(50.0)),
            ms(latency.percentile(90.0)),
            ms(latency.percentile(99.0)),
            ms(latency.percentile(99.9)),
            ms(latency.max()),
            latency.mean() / 1_000.0,
        );
        out
    }

    /// One line of JSON, for scripts. Latencies are in microseconds.
    pub fn to_json(&self, benchmark: &str) -> String {
        let latency = &self.latency;
        format!(
            "{{\"benchmark\":\"{benchmark}\",\"elapsed_seconds\":{:.6},\"requests\":{},\
             \"errors\":{},\"records\":{},\"bytes\":{},\"requests_per_second\":{:.1},\
             \"records_per_second\":{:.1},\"megabytes_per_second\":{:.3},\
             \"latency_us\":{{\"min\":{},\"mean\":{:.1},\"p50\":{},\"p90\":{},\"p99\":{},\
             \"p999\":{},\"max\":{}}}}}",
            self.elapsed.as_secs_f64(),
            self.requests,
            self.errors,
            self.records,
            self.bytes,
            self.requests_per_second(),
            self.records_per_second(),
            self.megabytes_per_second(),
            latency.min(),
            latency.mean(),
            latency.percentile(50.0),
            latency.percentile(90.0),
            latency.percentile(99.0),
            latency.percentile(99.9),
            latency.max(),
        )
    }
}

fn per_second(count: u64, elapsed: Duration) -> f64 {
    let seconds = elapsed.as_secs_f64();
    if seconds <= 0.0 {
        return 0.0;
    }
    count as f64 / seconds
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Summary {
        let mut summary = Summary {
            requests: 1_000,
            errors: 2,
            records: 10_000,
            bytes: 2_000_000,
            elapsed: Duration::from_secs(4),
            latency: Histogram::new(),
        };
        for micros in [500, 1_000, 1_500, 20_000] {
            summary.latency.record(micros);
        }
        summary
    }

    #[test]
    fn rates_divide_by_the_elapsed_time() {
        let summary = sample();
        assert_eq!(summary.requests_per_second(), 250.0);
        assert_eq!(summary.records_per_second(), 2_500.0);
        assert_eq!(summary.megabytes_per_second(), 0.5);
        // No time, no rate, and no division by zero.
        assert_eq!(Summary::default().records_per_second(), 0.0);
    }

    #[test]
    fn merging_adds_counts_and_keeps_the_elapsed_time() {
        let mut total = sample();
        total.merge(&sample());
        assert_eq!(total.requests, 2_000);
        assert_eq!(total.errors, 4);
        assert_eq!(total.records, 20_000);
        assert_eq!(total.latency.count(), 8);
        assert_eq!(total.elapsed, Duration::from_secs(4));
    }

    #[test]
    fn text_and_json_carry_the_numbers() {
        let summary = sample();
        let text = summary.to_text("produce");
        assert!(text.starts_with("produce\n"));
        assert!(text.contains("1000 ok, 2 failed, 250/s"));
        assert!(text.contains("max 20.00"));

        let json = summary.to_json("produce");
        assert!(json.starts_with("{\"benchmark\":\"produce\""));
        assert!(json.contains("\"records\":10000"));
        assert!(json.contains("\"max\":20000"));
        assert!(json.ends_with("}}"));
        assert!(!json.contains('\n'));
    }
}
