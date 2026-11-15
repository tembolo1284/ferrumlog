//! A latency histogram with bounded memory and bounded error.

/// Sub-buckets per power of two. With 32, a recorded value is off by at
/// most about 3 percent.
const SUB_BUCKETS: u64 = 32;
const SUB_BITS: u32 = 5;
/// Enough buckets for any `u64`.
const BUCKETS: usize = ((64 - SUB_BITS as usize) + 1) * SUB_BUCKETS as usize;

/// Counts values in buckets whose width grows with the value.
///
/// Values below 32 each have their own bucket. Above that, every power of
/// two is split into 32 equal buckets, so the relative error stays about
/// the same from microseconds to minutes. The unit is whatever the caller
/// records. The benchmarks record microseconds.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Histogram {
    counts: Vec<u64>,
    count: u64,
    sum: u128,
    min: u64,
    max: u64,
}

impl Default for Histogram {
    fn default() -> Self {
        Histogram::new()
    }
}

impl Histogram {
    /// An empty histogram.
    pub fn new() -> Self {
        Histogram {
            counts: vec![0; BUCKETS],
            count: 0,
            sum: 0,
            min: u64::MAX,
            max: 0,
        }
    }

    /// Records one value.
    pub fn record(&mut self, value: u64) {
        self.counts[bucket_of(value)] += 1;
        self.count += 1;
        self.sum += u128::from(value);
        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }

    /// Adds everything `other` has recorded.
    pub fn merge(&mut self, other: &Histogram) {
        for (mine, theirs) in self.counts.iter_mut().zip(&other.counts) {
            *mine += theirs;
        }
        self.count += other.count;
        self.sum += other.sum;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
    }

    /// Number of values recorded.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// The smallest value recorded, or zero if there are none.
    pub fn min(&self) -> u64 {
        if self.count == 0 {
            0
        } else {
            self.min
        }
    }

    /// The largest value recorded, exactly.
    pub fn max(&self) -> u64 {
        self.max
    }

    /// The mean of the recorded values, or zero if there are none.
    pub fn mean(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        self.sum as f64 / self.count as f64
    }

    /// The value at or below which `percent` of the recorded values fall.
    ///
    /// The answer is the upper edge of the bucket that holds that value,
    /// so it can be up to about 3 percent high, and is never above the
    /// true maximum. Returns zero for an empty histogram.
    pub fn percentile(&self, percent: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let percent = percent.clamp(0.0, 100.0);
        let rank = ((percent / 100.0) * self.count as f64).ceil() as u64;
        let rank = rank.clamp(1, self.count);
        let mut seen = 0u64;
        for (bucket, count) in self.counts.iter().enumerate() {
            seen += count;
            if seen >= rank {
                return upper_edge(bucket).min(self.max);
            }
        }
        self.max
    }
}

/// The bucket a value falls in.
fn bucket_of(value: u64) -> usize {
    if value < SUB_BUCKETS {
        return value as usize;
    }
    // The position of the highest set bit decides the power of two, and
    // the five bits below it decide the sub-bucket.
    let exponent = 63 - value.leading_zeros();
    let shift = exponent - SUB_BITS;
    let mantissa = value >> shift;
    ((u64::from(shift) + 1) * SUB_BUCKETS + (mantissa - SUB_BUCKETS)) as usize
}

/// The largest value that falls in a bucket.
fn upper_edge(bucket: usize) -> u64 {
    let bucket = bucket as u64;
    if bucket < SUB_BUCKETS {
        return bucket;
    }
    let shift = bucket / SUB_BUCKETS - 1;
    let mantissa = SUB_BUCKETS + bucket % SUB_BUCKETS;
    // The last bucket's edge would overflow, so saturate.
    match (mantissa + 1).checked_shl(shift as u32) {
        Some(next) if next >> shift == mantissa + 1 => next - 1,
        _ => u64::MAX,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_histogram_reports_zeros() {
        let histogram = Histogram::new();
        assert_eq!(histogram.count(), 0);
        assert_eq!(histogram.min(), 0);
        assert_eq!(histogram.max(), 0);
        assert_eq!(histogram.mean(), 0.0);
        assert_eq!(histogram.percentile(99.0), 0);
    }

    #[test]
    fn small_values_are_exact() {
        let mut histogram = Histogram::new();
        for value in 1..=20 {
            histogram.record(value);
        }
        assert_eq!(histogram.count(), 20);
        assert_eq!(histogram.min(), 1);
        assert_eq!(histogram.max(), 20);
        assert_eq!(histogram.mean(), 10.5);
        assert_eq!(histogram.percentile(50.0), 10);
        assert_eq!(histogram.percentile(95.0), 19);
        assert_eq!(histogram.percentile(100.0), 20);
        assert_eq!(histogram.percentile(0.0), 1);
    }

    #[test]
    fn every_value_lands_in_a_bucket_that_contains_it() {
        let mut previous = 0usize;
        let samples = (0..64u32).flat_map(|bit| {
            let base = 1u64 << bit;
            [base.saturating_sub(1), base, base + base / 3, base + base / 2]
        });
        for value in samples.chain([u64::MAX]) {
            let bucket = bucket_of(value);
            assert!(bucket < BUCKETS, "{value} is out of range");
            assert!(upper_edge(bucket) >= value, "{value} is above its bucket");
            if bucket > 0 {
                assert!(upper_edge(bucket - 1) < value, "{value} is in too high a bucket");
            }
            // Larger values never land in lower buckets.
            assert!(bucket >= previous || value < 2);
            previous = previous.max(bucket);
        }
    }

    #[test]
    fn large_values_are_within_a_few_percent() {
        let mut histogram = Histogram::new();
        for value in (1_000..=1_000_000u64).step_by(1_000) {
            histogram.record(value);
        }
        for (percent, exact) in [(50.0, 500_000.0), (90.0, 900_000.0), (99.0, 990_000.0)] {
            let got = histogram.percentile(percent) as f64;
            let error = (got - exact).abs() / exact;
            assert!(error < 0.035, "p{percent}: {got} against {exact}");
            // A percentile is never reported below the true value.
            assert!(got >= exact);
        }
        // The maximum is tracked exactly, and no percentile exceeds it.
        assert_eq!(histogram.max(), 1_000_000);
        assert_eq!(histogram.percentile(100.0), 1_000_000);
    }

    #[test]
    fn merging_is_the_same_as_recording_everything_in_one() {
        let mut all = Histogram::new();
        let mut left = Histogram::new();
        let mut right = Histogram::new();
        for value in 0..10_000u64 {
            let value = value * value % 77_777;
            all.record(value);
            if value % 2 == 0 {
                left.record(value);
            } else {
                right.record(value);
            }
        }
        left.merge(&right);
        assert_eq!(left, all);
        // Merging an empty histogram changes nothing.
        left.merge(&Histogram::new());
        assert_eq!(left, all);
    }
}
