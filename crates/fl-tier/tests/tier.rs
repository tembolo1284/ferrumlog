//! Archive, verify, and restore against a real log and a real node.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use fl_client::{Client, ClientConfig, FetchOptions};
use fl_server::{Config, Server};
use fl_storage::{sealed_segments, Log, LogConfig};
use fl_tier::{archive, restore, verify, Error, Manifest, Store};
use fl_types::{Command, LogEntry, LogIndex, NodeId, Offset, Record, RecordBatch, Term, TopicId};

struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("ferrumlog-tier-{tag}-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        TempDir(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn n(id: u64) -> NodeId {
    NodeId::new(id)
}

fn i(index: u64) -> LogIndex {
    LogIndex::new(index)
}

/// Segments this small fill up after a few entries.
fn small_segments() -> LogConfig {
    LogConfig {
        segment_bytes: 512,
        ..LogConfig::default()
    }
}

fn entry(index: u64) -> LogEntry {
    let record = Record::new(format!("value-{index:04}-{}", "x".repeat(40)));
    let batch = RecordBatch::new(TopicId::new(1), vec![record]);
    LogEntry::new(Term::new(1), i(index), Command::Append(batch))
}

/// Appends entries `from..=to` to the log in `dir`.
fn append(dir: &Path, from: u64, to: u64) {
    let mut log = Log::open(dir, small_segments()).unwrap();
    let entries: Vec<LogEntry> = (from..=to).map(entry).collect();
    log.append(&entries).unwrap();
    log.sync().unwrap();
}

async fn store_in(dir: &TempDir) -> Store {
    Store::open(dir.path().to_str().unwrap()).await.unwrap()
}

#[tokio::test]
async fn sealed_committed_segments_are_archived_once() {
    let data = TempDir::new("data");
    let bucket = TempDir::new("bucket");
    let store = store_in(&bucket).await;
    append(data.path(), 1, 30);
    let sealed = sealed_segments(data.path()).unwrap();
    assert!(sealed.len() >= 4, "{} sealed segments", sealed.len());

    // Nothing is committed yet, so nothing may be uploaded.
    let report = archive(data.path(), n(1), i(0), &store).await.unwrap();
    assert!(report.uploaded.is_empty());
    assert_eq!(report.held_back, sealed.len());
    assert_eq!(report.archived_through, i(0));

    // Committed up to the end of the second segment. A commit index in
    // the middle of the third does not make the third eligible.
    let through = sealed[1].last_index;
    let middle = i(through.get() + 1);
    let report = archive(data.path(), n(1), middle, &store).await.unwrap();
    assert_eq!(report.uploaded.len(), 2);
    assert_eq!(report.archived_through, through);
    assert_eq!(report.held_back, sealed.len() - 2);
    assert!(report.bytes > 0);

    // The same pass again finds nothing new.
    let again = archive(data.path(), n(1), middle, &store).await.unwrap();
    assert!(again.uploaded.is_empty());
    assert_eq!(again.archived_through, through);

    // Everything committed: the rest of the sealed segments follow, and
    // the segment still being written stays behind.
    let report = archive(data.path(), n(1), i(30), &store).await.unwrap();
    assert_eq!(report.uploaded.len(), sealed.len() - 2);
    assert_eq!(report.held_back, 0);
    let last_sealed = sealed.last().unwrap().last_index;
    assert_eq!(report.archived_through, last_sealed);
    assert!(last_sealed < i(30));

    // The manifest in the store lists them all, in order.
    let text = std::fs::read_to_string(bucket.path().join("manifest")).unwrap();
    let manifest = Manifest::decode(&text).unwrap();
    assert_eq!(manifest.node, Some(n(1)));
    assert_eq!(manifest.segments.len(), sealed.len());
    assert_eq!(manifest.archived_through(), last_sealed);

    let checked = verify(&store).await.unwrap();
    assert_eq!(checked.segments, sealed.len());
    assert_eq!(checked.through, last_sealed);
    assert_eq!(checked.bytes, manifest.total_bytes());
}

#[tokio::test]
async fn a_restored_directory_holds_the_same_entries() {
    let data = TempDir::new("data");
    let bucket = TempDir::new("bucket");
    let fresh = TempDir::new("fresh");
    let store = store_in(&bucket).await;
    append(data.path(), 1, 30);
    let archived = archive(data.path(), n(1), i(30), &store).await.unwrap();
    let through = archived.archived_through;

    let report = restore(&store, fresh.path()).await.unwrap();
    assert_eq!(report.through, through);
    assert_eq!(report.segments, archived.uploaded.len());
    assert_eq!(report.node, Some(n(1)));

    // The restored log opens cleanly and holds exactly the archived
    // entries, byte for byte the ones that were written.
    let original = Log::open(data.path(), small_segments()).unwrap();
    let restored = Log::open(fresh.path(), small_segments()).unwrap();
    assert_eq!(restored.last_index(), through);
    assert_eq!(restored.recovery().truncated_bytes, 0);
    let all = |log: &Log| log.read(i(1), usize::MAX, usize::MAX).unwrap();
    let want: Vec<LogEntry> = all(&original)
        .into_iter()
        .filter(|entry| entry.index() <= through)
        .collect();
    assert_eq!(all(&restored), want);
    drop(restored);

    // It can be written to again, continuing where the archive ended.
    append(fresh.path(), through.get() + 1, through.get() + 5);
    let reopened = Log::open(fresh.path(), small_segments()).unwrap();
    assert_eq!(reopened.last_index(), i(through.get() + 5));
}

#[tokio::test]
async fn restore_refuses_a_directory_that_holds_a_log() {
    let data = TempDir::new("data");
    let bucket = TempDir::new("bucket");
    let store = store_in(&bucket).await;
    append(data.path(), 1, 30);
    archive(data.path(), n(1), i(30), &store).await.unwrap();

    let error = restore(&store, data.path()).await.unwrap_err();
    assert!(matches!(error, Error::NotEmpty(_)), "{error}");
    assert!(error.to_string().contains("refusing"));
}

#[tokio::test]
async fn damage_in_the_archive_is_found() {
    let data = TempDir::new("data");
    let bucket = TempDir::new("bucket");
    let fresh = TempDir::new("fresh");
    let store = store_in(&bucket).await;
    append(data.path(), 1, 30);
    let report = archive(data.path(), n(1), i(30), &store).await.unwrap();
    let (log_key, index_key) = report.uploaded[1].keys();

    // One flipped byte in a segment.
    let path = bucket.path().join(&log_key);
    let mut bytes = std::fs::read(&path).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0x01;
    std::fs::write(&path, &bytes).unwrap();
    let error = verify(&store).await.unwrap_err();
    assert!(
        matches!(&error, Error::Damaged { key, .. } if *key == log_key),
        "{error}"
    );
    assert!(error.to_string().contains("checksum"));
    // A restore stops at the damaged segment too.
    assert!(matches!(
        restore(&store, fresh.path()).await,
        Err(Error::Damaged { .. })
    ));

    // A truncated file.
    bytes[middle] ^= 0x01;
    bytes.pop();
    std::fs::write(&path, &bytes).unwrap();
    let error = verify(&store).await.unwrap_err();
    assert!(error.to_string().contains("bytes"), "{error}");

    // A missing file.
    std::fs::remove_file(bucket.path().join(&index_key)).unwrap();
    std::fs::remove_file(&path).unwrap();
    let error = verify(&store).await.unwrap_err();
    assert!(error.to_string().contains("missing"), "{error}");
}

#[tokio::test]
async fn one_archive_belongs_to_one_node() {
    let data = TempDir::new("data");
    let other = TempDir::new("other");
    let bucket = TempDir::new("bucket");
    let store = store_in(&bucket).await;
    append(data.path(), 1, 30);
    append(other.path(), 1, 30);
    archive(data.path(), n(1), i(30), &store).await.unwrap();

    let error = archive(other.path(), n(2), i(30), &store).await.unwrap_err();
    assert!(
        matches!(error, Error::WrongNode { archived, local } if archived == n(1) && local == n(2)),
        "{error}"
    );
}

#[tokio::test]
async fn a_log_that_does_not_continue_the_archive_is_refused() {
    let data = TempDir::new("data");
    let bucket = TempDir::new("bucket");
    let store = store_in(&bucket).await;
    append(data.path(), 1, 30);
    let sealed = sealed_segments(data.path()).unwrap();
    archive(data.path(), n(1), sealed[0].last_index, &store)
        .await
        .unwrap();

    // The first unarchived segment disappears from the data directory.
    std::fs::remove_file(&sealed[1].log_path).unwrap();
    let before = std::fs::read_to_string(bucket.path().join("manifest")).unwrap();
    let error = archive(data.path(), n(1), i(30), &store).await.unwrap_err();
    assert!(matches!(error, Error::Gap { .. }), "{error}");
    // Nothing was added to the store on the way to finding that out.
    let after = std::fs::read_to_string(bucket.path().join("manifest")).unwrap();
    assert_eq!(before, after);
}

#[tokio::test]
async fn store_locations_are_checked() {
    let dir = TempDir::new("locations");
    let path = dir.path().join("nested/archive");
    let location = format!("file://{}", path.display());
    let store = Store::open(&location).await.unwrap();
    assert!(path.is_dir());
    assert_eq!(store.describe(), path.display().to_string());

    // Files round-trip, a missing key is not an error, and a later put
    // replaces an earlier one.
    assert_eq!(store.get("a/b").await.unwrap(), None);
    store.put("a/b", b"one".to_vec()).await.unwrap();
    store.put("a/b", b"two".to_vec()).await.unwrap();
    assert_eq!(store.get("a/b").await.unwrap(), Some(b"two".to_vec()));
    // No temporary file is left behind.
    assert_eq!(std::fs::read_dir(path.join("a")).unwrap().count(), 1);

    // A key cannot reach outside the store.
    for key in ["../escape", "/etc/passwd", "a/../../b", ""] {
        assert!(store.put(key, vec![1]).await.is_err(), "{key:?}");
        assert!(store.get(key).await.is_err(), "{key:?}");
    }

    assert!(matches!(
        Store::open("ftp://example/archive").await,
        Err(Error::Unsupported(_))
    ));
    assert!(matches!(Store::open("").await, Err(Error::Unsupported(_))));
    // Without the s3 feature the location is understood and refused with
    // a message that says what to do.
    #[cfg(not(feature = "s3"))]
    match Store::open("s3://bucket/prefix").await {
        Err(Error::Unsupported(reason)) => assert!(reason.contains("--features s3"), "{reason}"),
        _ => panic!("expected the s3 location to be refused"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_is_archived_while_running_and_restored_elsewhere() {
    let data = TempDir::new("node");
    let bucket = TempDir::new("bucket");
    let fresh = TempDir::new("restored");
    let config = |dir: &Path| Config {
        node_id: 1,
        data_dir: dir.to_path_buf(),
        listen: "127.0.0.1:0".parse().unwrap(),
        advertise: None,
        metrics_listen: "127.0.0.1:0".parse().unwrap(),
        no_metrics: true,
        // Small segments, so a few hundred records seal several.
        segment_bytes: 4096,
        max_commit_batch: 256,
        unsafe_no_fsync: true,
        peers: Vec::new(),
        tick_ms: 20,
    };

    let server = Server::start(config(data.path())).await.unwrap();
    let endpoint = server.grpc_addr().to_string();
    let client = Client::new(ClientConfig::new([endpoint.clone()])).unwrap();
    client.create_topic("quotes").await.unwrap();
    for number in 0..300u32 {
        let record = Record::new(format!("quote-{number:04}-{}", "x".repeat(60)));
        client.produce("quotes", vec![record]).await.unwrap();
    }

    // Archive the way the binary does: ask the running node for its
    // commit index, then upload.
    let status = client.status_of(&endpoint).await.unwrap();
    let store = store_in(&bucket).await;
    let report = archive(data.path(), status.node_id, status.commit_index, &store)
        .await
        .unwrap();
    assert!(report.uploaded.len() >= 3, "{} segments", report.uploaded.len());
    assert_eq!(report.held_back, 0);
    verify(&store).await.unwrap();
    server.stop().await.unwrap();

    // A new node on the restored directory serves the archived records.
    let restored = restore(&store, fresh.path()).await.unwrap();
    assert_eq!(restored.through, report.archived_through);
    let server = Server::start(config(fresh.path())).await.unwrap();
    let client = Client::new(ClientConfig::new([server.grpc_addr().to_string()])).unwrap();
    let topics = client.list_topics().await.unwrap();
    assert_eq!(topics.len(), 1);
    let recovered = topics[0].high_watermark.get();
    // Everything up to the last sealed segment, which is most of it but
    // not the tail that was still being written.
    assert!((150..300).contains(&recovered), "{recovered} records");

    let options = FetchOptions {
        max_records: 5,
        max_bytes: 0,
    };
    let first = client.fetch("quotes", Offset::ZERO, options).await.unwrap();
    assert!(first.records[0].record.value.starts_with(b"quote-0000-"));
    let last = client
        .fetch("quotes", Offset::new(recovered - 1), options)
        .await
        .unwrap();
    let expected = format!("quote-{:04}-", recovered - 1);
    assert!(last.records[0].record.value.starts_with(expected.as_bytes()));

    // And it takes new writes, continuing the offsets.
    let produced = client
        .produce("quotes", vec![Record::new("after the restore")])
        .await
        .unwrap();
    assert_eq!(produced.base_offset.get(), recovered);
    server.stop().await.unwrap();
}
