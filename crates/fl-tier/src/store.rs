//! Where archived files are kept.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};

use crate::error::{Error, Result};

/// A place to keep files by name: a directory, or an S3 bucket.
///
/// Keys are relative paths with `/` between the parts, such as
/// `segments/00000000000000000001.log`. Storing a key replaces whatever
/// was there, completely or not at all.
pub enum Store {
    /// A directory on a local or mounted file system.
    Fs(FsStore),
    /// An S3 bucket. Only in builds with the `s3` feature.
    #[cfg(feature = "s3")]
    S3(crate::s3::S3Store),
}

impl Store {
    /// Opens a store from its location.
    ///
    /// * `s3://bucket/prefix` is an S3 bucket, with credentials and region
    ///   taken from the environment the way the AWS CLI does.
    /// * `file:///some/dir`, or any plain path, is a directory. It is
    ///   created if missing.
    pub async fn open(location: &str) -> Result<Store> {
        if let Some(rest) = location.strip_prefix("s3://") {
            return open_s3(rest).await;
        }
        if let Some((scheme, _)) = location.split_once("://") {
            if scheme != "file" {
                return Err(Error::Unsupported(format!(
                    "unknown store location {location:?}, expected a path, file:// or s3://"
                )));
            }
        }
        let path = location.strip_prefix("file://").unwrap_or(location);
        if path.is_empty() {
            return Err(Error::Unsupported("the store location is empty".into()));
        }
        Ok(Store::Fs(FsStore::open(path)?))
    }

    /// Stores `bytes` under `key`, replacing any earlier contents.
    pub async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()> {
        match self {
            Store::Fs(store) => store.put(key, &bytes),
            #[cfg(feature = "s3")]
            Store::S3(store) => store.put(key, bytes).await,
        }
    }

    /// Reads the file stored under `key`, or `None` if there is none.
    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        match self {
            Store::Fs(store) => store.get(key),
            #[cfg(feature = "s3")]
            Store::S3(store) => store.get(key).await,
        }
    }

    /// The location, for messages.
    pub fn describe(&self) -> String {
        match self {
            Store::Fs(store) => store.root.display().to_string(),
            #[cfg(feature = "s3")]
            Store::S3(store) => store.describe(),
        }
    }
}

#[cfg(feature = "s3")]
async fn open_s3(rest: &str) -> Result<Store> {
    Ok(Store::S3(crate::s3::S3Store::open(rest).await?))
}

#[cfg(not(feature = "s3"))]
async fn open_s3(_rest: &str) -> Result<Store> {
    Err(Error::Unsupported(
        "this build has no S3 support, rebuild fl-tier with `--features s3`".into(),
    ))
}

/// A [`Store`] in a directory.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FsStore {
    root: PathBuf,
}

impl FsStore {
    /// Opens the store in `root`, creating the directory if needed.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(Error::io(&root))?;
        Ok(FsStore { root })
    }

    /// The directory the store keeps its files in.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn path(&self, key: &str) -> Result<PathBuf> {
        let relative = Path::new(key);
        // A key must stay inside the store. No absolute paths and no
        // stepping out through "..".
        let inside = !key.is_empty()
            && relative
                .components()
                .all(|part| matches!(part, Component::Normal(_)));
        if !inside {
            return Err(Error::Store(format!("{key:?} is not a valid key")));
        }
        Ok(self.root.join(relative))
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let path = self.path(key)?;
        let dir = path.parent().unwrap_or(&self.root);
        fs::create_dir_all(dir).map_err(Error::io(dir))?;
        write_atomic(&path, bytes)
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let path = self.path(key)?;
        match fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::io(&path)(e)),
        }
    }
}

/// Replaces `path` with `bytes` so that a crash leaves the old contents or
/// the new, never a mixture: write a temporary file, sync it, rename it
/// over the target, and sync the directory.
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);

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
    if let Some(dir) = path.parent() {
        File::open(dir)
            .and_then(|dir| dir.sync_all())
            .map_err(Error::io(dir))?;
    }
    Ok(())
}
