//! Uploading sealed segments.

use std::fs;
use std::path::Path;

use fl_storage::{crc32c, sealed_segments};
use fl_types::{LogIndex, NodeId};

use crate::error::{Error, Result};
use crate::manifest::{Manifest, SegmentRecord, MANIFEST_KEY};
use crate::store::Store;

/// What one archive pass did.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ArchiveReport {
    /// The segments uploaded in this pass, oldest first.
    pub uploaded: Vec<SegmentRecord>,
    /// Bytes uploaded in this pass.
    pub bytes: u64,
    /// The last log index now in the archive.
    pub archived_through: LogIndex,
    /// Sealed segments left for a later pass because they reach past the
    /// commit index.
    pub held_back: usize,
}

/// Uploads every sealed segment in `data_dir` that is fully committed and
/// not yet in `store`.
///
/// `node` is the id of the node the directory belongs to, and
/// `commit_index` is its current commit index. Ask the node for both, with
/// its Admin service. Passing a commit index that is too high would upload
/// entries that can still be replaced.
///
/// The pass can be run again at any time, by hand or on a timer. It picks
/// up where the manifest in the store ends.
pub async fn archive(
    data_dir: &Path,
    node: NodeId,
    commit_index: LogIndex,
    store: &Store,
) -> Result<ArchiveReport> {
    let mut manifest = load_manifest(store).await?;
    match manifest.node {
        Some(archived) if archived != node => {
            return Err(Error::WrongNode {
                archived,
                local: node,
            })
        }
        _ => manifest.node = Some(node),
    }

    let mut report = ArchiveReport {
        uploaded: Vec::new(),
        bytes: 0,
        archived_through: manifest.archived_through(),
        held_back: 0,
    };
    for segment in sealed_segments(data_dir)? {
        if segment.last_index <= manifest.archived_through() {
            continue;
        }
        if segment.last_index > commit_index {
            // This one and every later one can still change.
            report.held_back += 1;
            continue;
        }

        let log = fs::read(&segment.log_path).map_err(Error::io(&segment.log_path))?;
        let index = fs::read(&segment.index_path).map_err(Error::io(&segment.index_path))?;
        let record = SegmentRecord {
            first_index: segment.first_index,
            last_index: segment.last_index,
            log_bytes: log.len() as u64,
            log_crc: crc32c(&log),
            index_bytes: index.len() as u64,
            index_crc: crc32c(&index),
        };
        // Checked before anything is uploaded, so a segment that does not
        // continue the archive leaves the store untouched.
        manifest.push(record)?;

        // The files first and the manifest last. The manifest then never
        // names a file that is not in the store.
        let (log_key, index_key) = record.keys();
        store.put(&log_key, log).await?;
        store.put(&index_key, index).await?;
        store
            .put(MANIFEST_KEY, manifest.encode().into_bytes())
            .await?;

        report.bytes += record.log_bytes + record.index_bytes;
        report.archived_through = record.last_index;
        report.uploaded.push(record);
    }
    Ok(report)
}

/// Reads the manifest from the store. A store with no manifest is an
/// empty archive.
pub(crate) async fn load_manifest(store: &Store) -> Result<Manifest> {
    let Some(bytes) = store.get(MANIFEST_KEY).await? else {
        return Ok(Manifest::default());
    };
    let text = String::from_utf8(bytes)
        .map_err(|_| Error::BadManifest("it is not valid UTF-8".into()))?;
    Manifest::decode(&text)
}
