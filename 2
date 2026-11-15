//! Durable storage for a ferrumlog node.
//!
//! Two things must survive a crash for Raft to be correct, and this crate
//! stores both:
//!
//! * [`Log`] holds the replicated log as a directory of segment files.
//! * [`HardStateStore`] holds the current term and vote.
//!
//! # On-disk layout
//!
//! ```text
//! data/
//!   hardstate                    term and vote, replaced atomically
//!   00000000000000000001.log     segment, named by its first log index
//!   00000000000000000001.idx     index for a sealed segment
//!   00000000000000048213.log     active segment, no index yet
//! ```
//!
//! A segment is a 16-byte header followed by frames. Each frame is a 4-byte
//! payload length, a 4-byte CRC-32C of the payload, and the payload. See
//! [`codec`] for the payload format. All integers are little-endian.
//!
//! Only the last segment is ever written. When it reaches the configured
//! size it is synced, an index file is written beside it, and a new segment
//! is started. The index holds every frame's file position and the term
//! boundaries, so reopening a sealed segment does not require reading it.
//!
//! # Crash recovery
//!
//! On open the active segment is scanned frame by frame. The scan stops at
//! the first frame that is incomplete, fails its checksum, or is out of
//! sequence, and the file is cut back to that point. That is the torn tail
//! of a write that was in flight when the process died.
//!
//! Sealed segments were synced before they were sealed, so damage there is
//! not a torn write. It is reported as [`Error::Corrupt`] and never repaired
//! silently.
//!
//! # Platform
//!
//! Unix only. Reads and writes use positional I/O from
//! `std::os::unix::fs::FileExt`.

pub mod codec;
mod crc;
mod error;
mod fsutil;
mod index;
mod log;
mod meta;
mod sealed;
mod segment;

#[cfg(test)]
mod testutil;

pub use crc::crc32c;
pub use error::{Error, Result};
pub use log::{Log, LogConfig, RecoveryReport, SyncPolicy};
pub use meta::{HardState, HardStateStore};
pub use sealed::{sealed_segments, segment_file_names, SealedSegment};
