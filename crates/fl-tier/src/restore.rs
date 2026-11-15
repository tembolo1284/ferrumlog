//! Checking an archive and rebuilding a data directory from it.

use std::fs;
use std::path::Path;

use fl_storage::{crc32c, segment_file_names};
use fl_types::{LogIndex, NodeId};

use crate::archive::load_manifest;
use crate::error::{Error, Result};
use crate::manifest::SegmentRecord;
use crate::store::{write_atomic, Store};

/// What a restore or a verification covered.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RestoreReport {
    /// The node whose log the archive holds, if it holds anything.
    pub node: Option<NodeId>,
    /// Number of segments.
    pub segments: usize,
    /// Total size of their files in bytes.
    pub bytes: u64,
    /// The last log index they contain, or zero for an empty archive.
    pub through: LogIndex,
}

/// Downloads every archived file and checks its size and CRC-32C against
/// the manifest. Writes nothing.
pub async fn verify(store: &Store) -> Result<RestoreReport> {
    let manifest = load_manifest(store).await?;
    for segment in &manifest.segments {
        fetch(store, segment).await?;
    }
    Ok(RestoreReport {
        node: manifest.node,
        segments: manifest.segments.len(),
        bytes: manifest.total_bytes(),
        through: manifest.archived_through(),
    })
}

/// Writes every archived segment into `data_dir`, checking each one on
/// the way.
///
/// The directory must not hold a log already. Afterwards a node can be
/// started on it. It will hold every entry up to the returned index.
/// Entries that were written after the last archived segment are not in
/// the archive and are not restored.
///
/// The node's term and vote are not archived. A restored node starts with
/// none, which is safe for a node that comes up alone or with a whole
/// restored cluster. Do not restore one node into a cluster whose other
/// members kept running, unless that node's old data is truly gone.
pub async fn restore(store: &Store, data_dir: &Path) -> Result<RestoreReport> {
    fs::create_dir_all(data_dir).map_err(Error::io(data_dir))?;
    for item in fs::read_dir(data_dir).map_err(Error::io(data_dir))? {
        let name = item.map_err(Error::io(data_dir))?.file_name();
        let name = name.to_string_lossy();
        if name.ends_with(".log") || name.ends_with(".idx") || name == "hardstate" {
            return Err(Error::NotEmpty(data_dir.to_path_buf()));
        }
    }

    let manifest = load_manifest(store).await?;
    for segment in &manifest.segments {
        let (log, index) = fetch(store, segment).await?;
        let (log_name, index_name) = segment_file_names(segment.first_index);
        // The index first. A log file without its index is what a crash
        // half way through would leave, and the log can rebuild that.
        write_atomic(&data_dir.join(index_name), &index)?;
        write_atomic(&data_dir.join(log_name), &log)?;
    }
    Ok(RestoreReport {
        node: manifest.node,
        segments: manifest.segments.len(),
        bytes: manifest.total_bytes(),
        through: manifest.archived_through(),
    })
}

/// Downloads one segment's two files and checks them.
async fn fetch(store: &Store, segment: &SegmentRecord) -> Result<(Vec<u8>, Vec<u8>)> {
    let (log_key, index_key) = segment.keys();
    let log = checked(store, &log_key, segment.log_bytes, segment.log_crc).await?;
    let index = checked(store, &index_key, segment.index_bytes, segment.index_crc).await?;
    Ok((log, index))
}

async fn checked(store: &Store, key: &str, bytes: u64, crc: u32) -> Result<Vec<u8>> {
    let damaged = |reason: String| Error::Damaged {
        key: key.to_owned(),
        reason,
    };
    let Some(contents) = store.get(key).await? else {
        return Err(damaged("it is listed in the manifest but missing".into()));
    };
    if contents.len() as u64 != bytes {
        return Err(damaged(format!(
            "it is {} bytes, the manifest says {bytes}",
            contents.len()
        )));
    }
    let actual = crc32c(&contents);
    if actual != crc {
        return Err(damaged(format!(
            "its checksum is {actual:08x}, the manifest says {crc:08x}"
        )));
    }
    Ok(contents)
}
