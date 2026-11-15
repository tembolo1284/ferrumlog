//! The simulator's only source of randomness.

/// xorshift64*, seeded through splitmix64.
#[derive(Clone, Debug)]
pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) fn new(seed: u64) -> Self {
        let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        Rng(if z == 0 { 0x9E37_79B9_7F4A_7C15 } else { z })
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// A value in `low..=high`.
    pub(crate) fn range(&mut self, low: u64, high: u64) -> u64 {
        debug_assert!(low <= high);
        low + self.next_u64() % (high - low + 1)
    }

    /// True with the given probability. Always draws a number, so turning
    /// a fault rate down to zero does not shift every later decision.
    pub(crate) fn chance(&mut self, probability: f64) -> bool {
        // 53 random bits make a uniform value in [0, 1).
        let unit = (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
        unit < probability
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_sequence() {
        let mut a = Rng::new(9);
        let mut b = Rng::new(9);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
        assert_ne!(Rng::new(1).next_u64(), Rng::new(2).next_u64());
    }

    #[test]
    fn range_and_chance_behave() {
        let mut rng = Rng::new(3);
        assert!((0..1000).all(|_| (5..=9).contains(&rng.range(5, 9))));
        assert_eq!(rng.range(4, 4), 4);
        assert!((0..1000).all(|_| !rng.chance(0.0)));
        assert!((0..1000).all(|_| rng.chance(1.0)));
        let hits = (0..10_000).filter(|_| rng.chance(0.25)).count();
        assert!((2_200..2_800).contains(&hits), "{hits}");
    }
}
