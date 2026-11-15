//! The index file written beside each sealed segment.
//!
//! ```text
//! u32  magic "FLIX"
//! u32  version
//! u64  base index of the segment
//! u64  length of the segment file in bytes
//! u32  entry count
//! u32  term run count
//! u32  file position of each entry's frame      (entry count times)
//! u64  term, u64 first index                    (term run count times)
//! u32  CRC-32C of everything above
//! ```
//!
//! The index is a cache. It can always be rebuilt by scanning the segment,
//! so any index that fails a check is treated as missing instead of as an
//! error.

use std::fs;
use std::path::Path;

use fl_types::{LogIndex, Term};

use crate::crc::crc32c;
use crate::segment::HEADER_LEN;

const MAGIC: &[u8; 4] = b"FLIX";
const VERSION: u32 = 1;
const FIXED_LEN: usize = 32;
const CRC_LEN: usize = 4;

/// A maximal stretch of consecutive entries that share a term.
///
/// Terms never decrease along the log, so a short list of runs answers
/// "what is the term at index i" without touching the disk.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct TermRun {
    /// The term shared by the run.
    pub term: Term,
    /// Index of the first entry in the run.
    pub first: LogIndex,
}

/// The contents of a valid index file.
#[derive(PartialEq, Eq, Debug)]
pub(crate) struct Loaded {
    pub positions: Vec<u32>,
    pub runs: Vec<TermRun>,
}

/// Serializes an index.
pub(crate) fn encode(base: LogIndex, log_len: u64, positions: &[u32], runs: &[TermRun]) -> Vec<u8> {
    let mut out = Vec::with_capacity(FIXED_LEN + positions.len() * 4 + runs.len() * 16 + CRC_LEN);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&base.get().to_le_bytes());
    out.extend_from_slice(&log_len.to_le_bytes());
    out.extend_from_slice(&(positions.len() as u32).to_le_bytes());
    out.extend_from_slice(&(runs.len() as u32).to_le_bytes());
    for position in positions {
        out.extend_from_slice(&position.to_le_bytes());
    }
    for run in runs {
        out.extend_from_slice(&run.term.get().to_le_bytes());
        out.extend_from_slice(&run.first.get().to_le_bytes());
    }
    let crc = crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// Reads and validates an index file. Returns `None` if the file is missing,
/// unreadable, damaged, or does not describe the given segment.
pub(crate) fn load(path: &Path, base: LogIndex, log_len: u64) -> Option<Loaded> {
    let bytes = fs::read(path).ok()?;
    decode(&bytes, base, log_len)
}

fn decode(bytes: &[u8], base: LogIndex, log_len: u64) -> Option<Loaded> {
    if bytes.len() < FIXED_LEN + CRC_LEN {
        return None;
    }
    let (body, crc) = bytes.split_at(bytes.len() - CRC_LEN);
    if crc32c(body) != u32_at(crc, 0)? {
        return None;
    }
    if &body[0..4] != MAGIC || u32_at(body, 4)? != VERSION {
        return None;
    }
    if u64_at(body, 8)? != base.get() || u64_at(body, 16)? != log_len {
        return None;
    }
    let count = u32_at(body, 24)? as usize;
    let run_count = u32_at(body, 28)? as usize;
    let expected = FIXED_LEN
        .checked_add(count.checked_mul(4)?)?
        .checked_add(run_count.checked_mul(16)?)?;
    if body.len() != expected {
        return None;
    }

    let mut positions = Vec::with_capacity(count);
    let mut at = FIXED_LEN;
    for _ in 0..count {
        positions.push(u32_at(body, at)?);
        at += 4;
    }
    let mut runs = Vec::with_capacity(run_count);
    for _ in 0..run_count {
        runs.push(TermRun {
            term: Term::new(u64_at(body, at)?),
            first: LogIndex::new(u64_at(body, at + 8)?),
        });
        at += 16;
    }

    if !positions_are_sane(&positions, log_len) || !runs_are_sane(&runs, base, count) {
        return None;
    }
    Some(Loaded { positions, runs })
}

/// Positions must start right after the segment header, strictly increase,
/// and stay inside the file.
fn positions_are_sane(positions: &[u32], log_len: u64) -> bool {
    match positions.first() {
        None => return log_len == HEADER_LEN,
        Some(&first) if u64::from(first) != HEADER_LEN => return false,
        Some(_) => {}
    }
    positions.windows(2).all(|w| w[0] < w[1])
        && positions.last().is_some_and(|&p| u64::from(p) < log_len)
}

/// Runs must start at the segment's first entry, strictly increase in both
/// term and index, and stay inside the segment.
fn runs_are_sane(runs: &[TermRun], base: LogIndex, count: usize) -> bool {
    if count == 0 {
        return runs.is_empty();
    }
    let Some(first) = runs.first() else {
        return false;
    };
    let end = base.get().saturating_add(count as u64);
    first.first == base
        && runs
            .windows(2)
            .all(|w| w[0].term < w[1].term && w[0].first < w[1].first)
        && runs.last().is_some_and(|r| r.first.get() < end)
}

fn u32_at(bytes: &[u8], at: usize) -> Option<u32> {
    let raw = bytes.get(at..at + 4)?;
    Some(u32::from_le_bytes(raw.try_into().ok()?))
}

fn u64_at(bytes: &[u8], at: usize) -> Option<u64> {
    let raw = bytes.get(at..at + 8)?;
    Some(u64::from_le_bytes(raw.try_into().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(term: u64, first: u64) -> TermRun {
        TermRun {
            term: Term::new(term),
            first: LogIndex::new(first),
        }
    }

    fn sample() -> (LogIndex, u64, Vec<u32>, Vec<TermRun>) {
        (
            LogIndex::new(100),
            400,
            vec![16, 60, 150, 320],
            vec![run(3, 100), run(5, 102)],
        )
    }

    #[test]
    fn round_trips() {
        let (base, len, positions, runs) = sample();
        let bytes = encode(base, len, &positions, &runs);
        let loaded = decode(&bytes, base, len).unwrap();
        assert_eq!(loaded.positions, positions);
        assert_eq!(loaded.runs, runs);
    }

    #[test]
    fn empty_segment_round_trips() {
        let bytes = encode(LogIndex::new(1), HEADER_LEN, &[], &[]);
        let loaded = decode(&bytes, LogIndex::new(1), HEADER_LEN).unwrap();
        assert!(loaded.positions.is_empty());
        assert!(loaded.runs.is_empty());
    }

    #[test]
    fn rejects_an_index_for_a_different_segment() {
        let (base, len, positions, runs) = sample();
        let bytes = encode(base, len, &positions, &runs);
        assert!(decode(&bytes, LogIndex::new(101), len).is_none());
        assert!(decode(&bytes, base, len + 1).is_none());
    }

    #[test]
    fn rejects_any_damage() {
        let (base, len, positions, runs) = sample();
        let bytes = encode(base, len, &positions, &runs);
        for i in 0..bytes.len() {
            let mut bad = bytes.clone();
            bad[i] ^= 0x10;
            assert!(decode(&bad, base, len).is_none(), "flip at {i}");
        }
        for cut in 0..bytes.len() {
            assert!(decode(&bytes[..cut], base, len).is_none(), "cut at {cut}");
        }
    }

    #[test]
    fn rejects_inconsistent_contents_with_a_valid_checksum() {
        let (base, len, _, runs) = sample();
        // positions not increasing
        let bytes = encode(base, len, &[16, 150, 60, 320], &runs);
        assert!(decode(&bytes, base, len).is_none());
        // first position not right after the header
        let bytes = encode(base, len, &[20, 60, 150, 320], &runs);
        assert!(decode(&bytes, base, len).is_none());
        // a position beyond the end of the file
        let bytes = encode(base, len, &[16, 60, 150, 400], &runs);
        assert!(decode(&bytes, base, len).is_none());
        // runs that do not start at the base
        let bytes = encode(base, len, &[16, 60, 150, 320], &[run(3, 101)]);
        assert!(decode(&bytes, base, len).is_none());
        // a run that starts past the last entry
        let bytes = encode(base, len, &[16, 60, 150, 320], &[run(3, 100), run(4, 104)]);
        assert!(decode(&bytes, base, len).is_none());
        // terms that go backwards
        let bytes = encode(base, len, &[16, 60, 150, 320], &[run(3, 100), run(2, 102)]);
        assert!(decode(&bytes, base, len).is_none());
    }
}
