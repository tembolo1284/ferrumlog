//! Small filesystem helpers shared by the log and the hard-state store.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

/// Flushes a directory's entry table to disk.
///
/// Creating, renaming, or deleting a file changes the directory, not the
/// file. Without this a crash can leave a fully synced file that the
/// directory no longer points to.
pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(Error::io(dir))
}

/// The temporary path used while replacing `path`.
fn tmp_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    path.with_file_name(name)
}

/// Replaces `path` with `bytes` so that a crash leaves either the old
/// contents or the new contents, never a mixture.
///
/// The bytes go to a temporary file that is synced, then renamed over the
/// target, and then the directory is synced so the rename itself is durable.
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = tmp_path(path);
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)
        .map_err(Error::io(&tmp))?;
    file.write_all(bytes).map_err(Error::io(&tmp))?;
    file.sync_all().map_err(Error::io(&tmp))?;
    drop(file);

    fs::rename(&tmp, path).map_err(Error::io(path))?;
    match path.parent() {
        Some(dir) => sync_dir(dir),
        None => Ok(()),
    }
}

/// Deletes a file, treating "already gone" as success.
pub(crate) fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(Error::io(path)(e)),
    }
}

/// Deletes `*.tmp` files left behind by a crash during [`write_atomic`].
pub(crate) fn remove_stale_tmp(dir: &Path) -> Result<()> {
    for item in fs::read_dir(dir).map_err(Error::io(dir))? {
        let path = item.map_err(Error::io(dir))?.path();
        if path.extension().is_some_and(|ext| ext == "tmp") {
            remove_if_exists(&path)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::TempDir;

    #[test]
    fn write_atomic_replaces_and_leaves_no_tmp() {
        let dir = TempDir::new("fsutil-atomic");
        let path = dir.path().join("state");

        write_atomic(&path, b"one").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"one");
        write_atomic(&path, b"two").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"two");

        assert!(!tmp_path(&path).exists());
    }

    #[test]
    fn stale_tmp_files_are_removed() {
        let dir = TempDir::new("fsutil-tmp");
        fs::write(dir.path().join("state.tmp"), b"junk").unwrap();
        fs::write(dir.path().join("keep.log"), b"data").unwrap();

        remove_stale_tmp(dir.path()).unwrap();
        assert!(!dir.path().join("state.tmp").exists());
        assert!(dir.path().join("keep.log").exists());
    }

    #[test]
    fn remove_if_exists_tolerates_missing_files() {
        let dir = TempDir::new("fsutil-remove");
        remove_if_exists(&dir.path().join("nope")).unwrap();
    }
}
