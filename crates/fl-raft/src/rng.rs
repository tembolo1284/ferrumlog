//! A tiny deterministic random number generator.
//!
//! Raft needs randomness in exactly one place, the election timeout. Using
//! a seeded generator instead of the operating system keeps a node's
//! behavior a pure function of its inputs.

/// xorshift64*. Small, fast, and good enough to spread election timeouts.
#[derive(Clone, Debug)]
pub(crate) struct Rng(u64);

impl Rng {
    /// Builds a generator from a seed. Any seed is fine, including zero.
    pub(crate) fn new(seed: u64) -> Self {
        // Run the seed through splitmix64 so that nearby seeds, such as
        // consecutive node ids, give unrelated sequences. The state must
        // also never be zero, because xorshift would then stay at zero.
        let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        Rng(if z == 0 { 0x9E37_79B9_7F4A_7C15 } else { z })
    }

    /// The next 64 random bits.
    pub(crate) fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// A value in `low..=high`.
    pub(crate) fn range(&mut self, low: u32, high: u32) -> u32 {
        debug_assert!(low <= high);
        let span = u64::from(high - low) + 1;
        low + (self.next_u64() % span) as u32
    }
}
