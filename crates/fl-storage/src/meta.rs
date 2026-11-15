//! The Raft hard state: the current term and the vote cast in it.
//!
//! ```text
//! u32  magic "FLHS"
//! u32  version
//! u64  term
//! u8   1 if a vote was cast, else 0
//! u64  node voted for (0 if none)
//! u32  CRC-32C of everything above
//! ```

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use fl_types::{NodeId, Term};

use crate::crc::crc32c;
use crate::error::{Error, Result};
use crate::fsutil;

const FILE_NAME: &str = "hardstate";
const MAGIC: &[u8; 4] = b"FLHS";
const VERSION: u32 = 1;
const FILE_LEN: usize = 29;

/// The part of a Raft node's state that must be durable before the node
/// answers any message that depends on it.
///
/// Losing it could let a node vote twice in one term, which could elect two
/// leaders.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct HardState {
    /// The latest term this node has seen.
    pub term: Term,
    /// The candidate this node voted for in `term`, if any.
    pub voted_for: Option<NodeId>,
}

/// Stores the [`HardState`] in a single small file that is replaced
/// atomically on every change.
pub struct HardStateStore {
    path: PathBuf,
    state: HardState,
}

impl HardStateStore {
    /// Opens the store in `dir`, creating the directory if needed.
    ///
    /// A missing file means a brand-new node and yields the default state.
    /// A damaged file is an error. Guessing a default there would risk a
    /// double vote.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir).map_err(Error::io(dir))?;
        fsutil::remove_stale_tmp(dir)?;

        let path = dir.join(FILE_NAME);
        let state = match fs::read(&path) {
            Ok(bytes) => decode(&bytes).map_err(|reason| Error::corrupt(&path, 0, reason))?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => HardState::default(),
            Err(e) => return Err(Error::io(&path)(e)),
        };
        Ok(HardStateStore { path, state })
    }

    /// The current state.
    pub fn get(&self) -> HardState {
        self.state
    }

    /// Makes `state` durable. Returns only after it is on disk.
    pub fn save(&mut self, state: HardState) -> Result<()> {
        if state == self.state && self.path.exists() {
            return Ok(());
        }
        fsutil::write_atomic(&self.path, &encode(state))?;
        self.state = state;
        Ok(())
    }
}

fn encode(state: HardState) -> Vec<u8> {
    let mut out = Vec::with_capacity(FILE_LEN);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&state.term.get().to_le_bytes());
    out.push(u8::from(state.voted_for.is_some()));
    out.extend_from_slice(&state.voted_for.map_or(0, NodeId::get).to_le_bytes());
    let crc = crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

fn decode(bytes: &[u8]) -> std::result::Result<HardState, &'static str> {
    if bytes.len() != FILE_LEN {
        return Err("hard state file has the wrong length");
    }
    let (body, crc) = bytes.split_at(FILE_LEN - 4);
    if crc32c(body).to_le_bytes() != crc {
        return Err("hard state checksum mismatch");
    }
    if &body[0..4] != MAGIC {
        return Err("bad hard state magic");
    }
    if body[4..8] != VERSION.to_le_bytes() {
        return Err("unsupported hard state version");
    }
    let term = Term::new(u64::from_le_bytes(body[8..16].try_into().unwrap()));
    let node = NodeId::new(u64::from_le_bytes(body[17..25].try_into().unwrap()));
    let voted_for = match body[16] {
        0 => None,
        1 => Some(node),
        _ => return Err("bad vote flag in hard state"),
    };
    Ok(HardState { term, voted_for })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::TempDir;

    fn state(term: u64, vote: Option<u64>) -> HardState {
        HardState {
            term: Term::new(term),
            voted_for: vote.map(NodeId::new),
        }
    }

    #[test]
    fn new_node_starts_with_the_default() {
        let dir = TempDir::new("meta-default");
        let store = HardStateStore::open(dir.path()).unwrap();
        assert_eq!(store.get(), HardState::default());
        assert_eq!(store.get().term, Term::ZERO);
        assert_eq!(store.get().voted_for, None);
    }

    #[test]
    fn saved_state_survives_reopen() {
        let dir = TempDir::new("meta-reopen");
        let mut store = HardStateStore::open(dir.path()).unwrap();
        store.save(state(5, Some(2))).unwrap();
        store.save(state(6, None)).unwrap();
        store.save(state(6, Some(0))).unwrap();
        drop(store);

        let store = HardStateStore::open(dir.path()).unwrap();
        // A vote for node 0 is a real vote, distinct from no vote.
        assert_eq!(store.get(), state(6, Some(0)));
    }

    #[test]
    fn saving_the_default_on_a_new_node_writes_the_file() {
        let dir = TempDir::new("meta-first-save");
        let mut store = HardStateStore::open(dir.path()).unwrap();
        store.save(HardState::default()).unwrap();
        assert!(dir.path().join(FILE_NAME).exists());
    }

    #[test]
    fn damaged_file_is_an_error_not_a_default() {
        let dir = TempDir::new("meta-corrupt");
        let mut store = HardStateStore::open(dir.path()).unwrap();
        store.save(state(9, Some(3))).unwrap();
        drop(store);

        let path = dir.path().join(FILE_NAME);
        let good = fs::read(&path).unwrap();

        for i in 0..good.len() {
            let mut bad = good.clone();
            bad[i] ^= 0x01;
            fs::write(&path, &bad).unwrap();
            let result = HardStateStore::open(dir.path());
            assert!(
                matches!(result, Err(Error::Corrupt { .. })),
                "flip at byte {i}"
            );
        }

        fs::write(&path, &good[..10]).unwrap();
        assert!(matches!(
            HardStateStore::open(dir.path()),
            Err(Error::Corrupt { .. })
        ));
    }

    #[test]
    fn crash_before_rename_keeps_the_old_state() {
        let dir = TempDir::new("meta-tmp");
        let mut store = HardStateStore::open(dir.path()).unwrap();
        store.save(state(4, Some(1))).unwrap();
        drop(store);

        // A half-written replacement left behind by a crash.
        let tmp = dir.path().join("hardstate.tmp");
        fs::write(&tmp, b"partial").unwrap();

        let store = HardStateStore::open(dir.path()).unwrap();
        assert_eq!(store.get(), state(4, Some(1)));
        assert!(!tmp.exists());
    }
}
