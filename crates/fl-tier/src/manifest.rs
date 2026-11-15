//! The list of what is in an archive.
//!
//! A small text file, so that a person can read it with `cat` or
//! `aws s3 cp`:
//!
//! ```text
//! ferrumlog-archive 1
//! node 1
//! segment 1 4821 67108800 9f3a11c2 19320 0b77e4d1
//! segment 4822 9630 67108712 51c08e3f 19276 a90f2e77
//! ```
//!
//! Each segment line holds the first and last log index, then the size in
//! bytes and the CRC-32C in hex of the log file, then the same two for
//! the index file.

use std::fmt::Write as _;

use fl_types::{LogIndex, NodeId};

use crate::error::{Error, Result};

/// The key the manifest is stored under.
pub(crate) const MANIFEST_KEY: &str = "manifest";

const HEADER: &str = "ferrumlog-archive 1";

/// One archived segment.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SegmentRecord {
    /// Index of the first entry in the segment.
    pub first_index: LogIndex,
    /// Index of the last entry in the segment.
    pub last_index: LogIndex,
    /// Size of the log file in bytes.
    pub log_bytes: u64,
    /// CRC-32C of the log file.
    pub log_crc: u32,
    /// Size of the index file in bytes.
    pub index_bytes: u64,
    /// CRC-32C of the index file.
    pub index_crc: u32,
}

impl SegmentRecord {
    /// The keys of this segment's log file and index file in the store.
    pub fn keys(&self) -> (String, String) {
        let (log, index) = fl_storage::segment_file_names(self.first_index);
        (format!("segments/{log}"), format!("segments/{index}"))
    }
}

/// Everything an archive holds.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Manifest {
    /// The node whose log this is. `None` for an archive with nothing in
    /// it yet.
    pub node: Option<NodeId>,
    /// The archived segments, oldest first, with no gaps.
    pub segments: Vec<SegmentRecord>,
}

impl Manifest {
    /// The last log index in the archive, or zero if it is empty.
    pub fn archived_through(&self) -> LogIndex {
        self.segments
            .last()
            .map_or(LogIndex::ZERO, |segment| segment.last_index)
    }

    /// Total size of the archived files in bytes.
    pub fn total_bytes(&self) -> u64 {
        self.segments
            .iter()
            .map(|segment| segment.log_bytes + segment.index_bytes)
            .sum()
    }

    /// Adds a segment at the end. It must start right after the last one.
    pub fn push(&mut self, segment: SegmentRecord) -> Result<()> {
        let expected = self.archived_through().next();
        if segment.first_index != expected {
            return Err(Error::Gap {
                expected,
                found: segment.first_index,
            });
        }
        self.segments.push(segment);
        Ok(())
    }

    /// The manifest as text.
    pub fn encode(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "{HEADER}");
        if let Some(node) = self.node {
            let _ = writeln!(out, "node {}", node.get());
        }
        for segment in &self.segments {
            let _ = writeln!(
                out,
                "segment {} {} {} {:08x} {} {:08x}",
                segment.first_index.get(),
                segment.last_index.get(),
                segment.log_bytes,
                segment.log_crc,
                segment.index_bytes,
                segment.index_crc,
            );
        }
        out
    }

    /// Parses a manifest, checking that its segments are in order with no
    /// gaps.
    pub fn decode(text: &str) -> Result<Manifest> {
        let bad = |reason: String| Error::BadManifest(reason);
        let mut lines = text.lines().map(str::trim).filter(|line| !line.is_empty());
        match lines.next() {
            Some(HEADER) => {}
            other => return Err(bad(format!("unexpected first line {other:?}"))),
        }

        let mut manifest = Manifest::default();
        for line in lines {
            let fields: Vec<&str> = line.split_whitespace().collect();
            let number = |field: &str| {
                field
                    .parse::<u64>()
                    .map_err(|_| bad(format!("{field:?} is not a number in {line:?}")))
            };
            let crc = |field: &str| {
                u32::from_str_radix(field, 16)
                    .map_err(|_| bad(format!("{field:?} is not a checksum in {line:?}")))
            };
            match fields[..] {
                ["node", id] => manifest.node = Some(NodeId::new(number(id)?)),
                ["segment", first, last, log_bytes, log_crc, index_bytes, index_crc] => {
                    let segment = SegmentRecord {
                        first_index: LogIndex::new(number(first)?),
                        last_index: LogIndex::new(number(last)?),
                        log_bytes: number(log_bytes)?,
                        log_crc: crc(log_crc)?,
                        index_bytes: number(index_bytes)?,
                        index_crc: crc(index_crc)?,
                    };
                    if segment.last_index < segment.first_index {
                        return Err(bad(format!("a segment ends before it starts in {line:?}")));
                    }
                    manifest
                        .push(segment)
                        .map_err(|e| bad(format!("{e} in {line:?}")))?;
                }
                _ => return Err(bad(format!("unrecognized line {line:?}"))),
            }
        }
        Ok(manifest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn segment(first: u64, last: u64) -> SegmentRecord {
        SegmentRecord {
            first_index: LogIndex::new(first),
            last_index: LogIndex::new(last),
            log_bytes: 1_000 + first,
            log_crc: 0x9F3A_11C2,
            index_bytes: 80,
            index_crc: 0x0000_00AB,
        }
    }

    #[test]
    fn a_manifest_survives_encoding() {
        let mut manifest = Manifest {
            node: Some(NodeId::new(3)),
            segments: Vec::new(),
        };
        manifest.push(segment(1, 40)).unwrap();
        manifest.push(segment(41, 97)).unwrap();

        let text = manifest.encode();
        assert!(text.starts_with("ferrumlog-archive 1\nnode 3\n"));
        assert!(text.contains("segment 41 97 1041 9f3a11c2 80 000000ab\n"));
        assert_eq!(Manifest::decode(&text).unwrap(), manifest);

        assert_eq!(manifest.archived_through(), LogIndex::new(97));
        assert_eq!(manifest.total_bytes(), 1_001 + 80 + 1_041 + 80);
    }

    #[test]
    fn an_empty_manifest_is_valid() {
        let empty = Manifest::default();
        assert_eq!(empty.archived_through(), LogIndex::ZERO);
        assert_eq!(empty.encode(), "ferrumlog-archive 1\n");
        assert_eq!(Manifest::decode(&empty.encode()).unwrap(), empty);
    }

    #[test]
    fn segments_must_follow_one_another() {
        let mut manifest = Manifest::default();
        // The first segment has to start at index 1.
        assert!(matches!(
            manifest.push(segment(5, 9)),
            Err(Error::Gap { .. })
        ));
        manifest.push(segment(1, 9)).unwrap();
        assert!(matches!(manifest.push(segment(11, 20)), Err(Error::Gap { .. })));
        assert!(matches!(manifest.push(segment(9, 20)), Err(Error::Gap { .. })));
        manifest.push(segment(10, 20)).unwrap();
    }

    #[test]
    fn keys_use_the_segment_file_names() {
        let (log, index) = segment(41, 97).keys();
        assert_eq!(log, "segments/00000000000000000041.log");
        assert_eq!(index, "segments/00000000000000000041.idx");
    }

    #[test]
    fn nonsense_is_rejected() {
        let cases = [
            "",
            "something else\n",
            "ferrumlog-archive 2\n",
            "ferrumlog-archive 1\nnode x\n",
            "ferrumlog-archive 1\nsegment 1 2 3\n",
            "ferrumlog-archive 1\nsegment 1 9 10 zz 5 00\n",
            "ferrumlog-archive 1\nsegment 9 1 10 00 5 00\n",
            "ferrumlog-archive 1\nsegment 1 9 10 00 5 00\nsegment 11 12 10 00 5 00\n",
            "ferrumlog-archive 1\nwhat is this\n",
        ];
        for text in cases {
            assert!(
                matches!(Manifest::decode(text), Err(Error::BadManifest(_))),
                "{text:?} was accepted"
            );
        }
    }
}
