//! Copies a node's log to object storage, and brings it back.
//!
//! A ferrumlog log is a row of segment files. All but the last are sealed
//! and never change again. This crate copies sealed segments to a
//! [`Store`], which is a local directory or an S3 bucket, and can rebuild
//! a data directory from what it stored.
//!
//! * [`archive`] uploads every sealed segment that is fully committed and
//!   not yet in the store.
//! * [`verify`] downloads every archived segment and checks it.
//! * [`restore`] writes the archived segments into an empty data
//!   directory, ready for a node to start on.
//!
//! # What is safe to upload
//!
//! Only committed entries are final. A follower can hold entries past its
//! commit index that a new leader will replace, and a sealed segment can
//! contain them. So the archiver is told the node's commit index and
//! skips any segment that reaches past it. Such a segment is picked up on
//! a later pass.
//!
//! # The manifest
//!
//! The store holds one small text file that lists every archived segment
//! with its size and CRC-32C. A segment's files are uploaded first and the
//! manifest is replaced last, so the manifest never names a file that is
//! not there. A pass that is interrupted leaves at worst an unlisted file,
//! which the next pass overwrites.
//!
//! # What this is not, yet
//!
//! The name says tiering, and the plan is for old segments to live only
//! in object storage. That needs the log to start somewhere other than
//! index 1, which in turn needs Raft snapshots. Until those exist nothing
//! is deleted locally, and this crate is a continuous, verified backup.
//!
//! An archive is a copy of one node's log up to its last archived
//! segment. Entries in the segment still being written are not in it.
//!
//! # Security
//!
//! The archive holds every record in the clear. Restrict who can read the
//! directory or bucket, and turn on encryption at rest for the bucket.

mod archive;
mod error;
mod manifest;
mod restore;
#[cfg(feature = "s3")]
mod s3;
mod store;

pub use archive::{archive, ArchiveReport};
pub use error::{Error, Result};
pub use manifest::{Manifest, SegmentRecord};
pub use restore::{restore, verify, RestoreReport};
pub use store::{FsStore, Store};
