//! Binary encoding of log entries.
//!
//! A frame on disk looks like this, with every integer little-endian:
//!
//! ```text
//! u32  payload length
//! u32  CRC-32C of the payload
//! payload:
//!   u64  term
//!   u64  index
//!   u8   command kind
//!   ...  command body
//! ```
//!
//! Command bodies:
//!
//! ```text
//! Noop         (empty)
//! Append       u32 topic id, u32 record count, then per record:
//!                u8 flags (bit 0 = has key), u64 timestamp,
//!                [u32 key length, key bytes] if has key,
//!                u32 value length, value bytes
//! CreateTopic  u16 name length, name bytes
//! Reconfigure  u32 voter count, then a u64 node id per voter
//! ```

use std::fmt;

use fl_types::{
    Command, LogEntry, LogIndex, Membership, NodeId, Record, RecordBatch, Term, Timestamp, TopicId,
    TopicName,
};

use crate::crc::crc32c;
use crate::error::{Error, Result};

/// Bytes in a frame before the payload: the length and the checksum.
pub const FRAME_HEADER_LEN: usize = 8;

/// Largest payload a frame may carry.
pub const MAX_PAYLOAD_BYTES: usize = 8 << 20;

/// Smallest possible payload: term, index, and kind.
const MIN_PAYLOAD_BYTES: usize = 17;

const KIND_NOOP: u8 = 0;
const KIND_APPEND: u8 = 1;
const KIND_CREATE_TOPIC: u8 = 2;
const KIND_RECONFIGURE: u8 = 3;

const FLAG_HAS_KEY: u8 = 0x01;

/// Why a frame could not be decoded.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct DecodeError(pub &'static str);

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for DecodeError {}

/// Appends one framed entry to `out` and returns the frame's total length.
///
/// On error `out` is left exactly as it was.
pub fn encode_frame(entry: &LogEntry, out: &mut Vec<u8>) -> Result<usize> {
    let start = out.len();
    out.extend_from_slice(&[0u8; FRAME_HEADER_LEN]);
    encode_payload(entry, out);

    let payload_start = start + FRAME_HEADER_LEN;
    let payload_len = out.len() - payload_start;
    if payload_len > MAX_PAYLOAD_BYTES {
        out.truncate(start);
        return Err(Error::EntryTooLarge {
            size: payload_len,
            max: MAX_PAYLOAD_BYTES,
        });
    }

    let crc = crc32c(&out[payload_start..]);
    out[start..start + 4].copy_from_slice(&(payload_len as u32).to_le_bytes());
    out[start + 4..payload_start].copy_from_slice(&crc.to_le_bytes());
    Ok(out.len() - start)
}

/// Decodes the frame at the start of `buf`.
///
/// Returns the entry and the number of bytes the frame occupied. Any failure,
/// including a buffer that ends partway through the frame, is an error.
pub fn decode_frame(buf: &[u8]) -> std::result::Result<(LogEntry, usize), DecodeError> {
    if buf.len() < FRAME_HEADER_LEN {
        return Err(DecodeError("incomplete frame header"));
    }
    let payload_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    let crc = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);

    if !(MIN_PAYLOAD_BYTES..=MAX_PAYLOAD_BYTES).contains(&payload_len) {
        return Err(DecodeError("frame length out of range"));
    }
    let frame_len = FRAME_HEADER_LEN + payload_len;
    if buf.len() < frame_len {
        return Err(DecodeError("incomplete frame"));
    }
    let payload = &buf[FRAME_HEADER_LEN..frame_len];
    if crc32c(payload) != crc {
        return Err(DecodeError("checksum mismatch"));
    }
    Ok((decode_payload(payload)?, frame_len))
}

fn encode_payload(entry: &LogEntry, out: &mut Vec<u8>) {
    out.extend_from_slice(&entry.term().get().to_le_bytes());
    out.extend_from_slice(&entry.index().get().to_le_bytes());
    match &entry.command {
        Command::Noop => out.push(KIND_NOOP),
        Command::Append(batch) => {
            out.push(KIND_APPEND);
            out.extend_from_slice(&batch.topic.get().to_le_bytes());
            put_len(out, batch.records.len());
            for record in &batch.records {
                let flags = if record.key.is_some() {
                    FLAG_HAS_KEY
                } else {
                    0
                };
                out.push(flags);
                out.extend_from_slice(&record.timestamp.as_millis().to_le_bytes());
                if let Some(key) = &record.key {
                    put_len(out, key.len());
                    out.extend_from_slice(key);
                }
                put_len(out, record.value.len());
                out.extend_from_slice(&record.value);
            }
        }
        Command::CreateTopic { name } => {
            out.push(KIND_CREATE_TOPIC);
            // TopicName caps the length at 255, so it always fits in a u16.
            out.extend_from_slice(&(name.as_str().len() as u16).to_le_bytes());
            out.extend_from_slice(name.as_str().as_bytes());
        }
        Command::Reconfigure(membership) => {
            out.push(KIND_RECONFIGURE);
            put_len(out, membership.len());
            for voter in membership.voters() {
                out.extend_from_slice(&voter.get().to_le_bytes());
            }
        }
    }
}

/// Writes a length as a u32. A length that does not fit is written as
/// `u32::MAX`, which makes the payload exceed the frame limit and fail in
/// `encode_frame` instead of wrapping silently.
fn put_len(out: &mut Vec<u8>, len: usize) {
    let len = u32::try_from(len).unwrap_or(u32::MAX);
    out.extend_from_slice(&len.to_le_bytes());
}

fn decode_payload(payload: &[u8]) -> std::result::Result<LogEntry, DecodeError> {
    let mut r = Reader::new(payload);
    let term = Term::new(r.u64()?);
    let index = LogIndex::new(r.u64()?);
    let command = match r.u8()? {
        KIND_NOOP => Command::Noop,
        KIND_APPEND => {
            let topic = TopicId::new(r.u32()?);
            let count = r.u32()? as usize;
            // Every record takes at least 13 bytes, so a count larger than
            // the remaining bytes allow is malformed. Checking here stops a
            // bad count from driving a huge allocation.
            if count > r.remaining() / 13 {
                return Err(DecodeError("record count exceeds payload"));
            }
            let mut records = Vec::with_capacity(count);
            for _ in 0..count {
                let flags = r.u8()?;
                if flags & !FLAG_HAS_KEY != 0 {
                    return Err(DecodeError("unknown record flags"));
                }
                let timestamp = Timestamp::from_millis(r.u64()?);
                let key = if flags & FLAG_HAS_KEY != 0 {
                    let len = r.u32()? as usize;
                    Some(r.bytes(len)?.to_vec())
                } else {
                    None
                };
                let len = r.u32()? as usize;
                let value = r.bytes(len)?.to_vec();
                records.push(Record {
                    key,
                    value,
                    timestamp,
                });
            }
            Command::Append(RecordBatch::new(topic, records))
        }
        KIND_CREATE_TOPIC => {
            let len = r.u16()? as usize;
            let raw = r.bytes(len)?;
            let name = std::str::from_utf8(raw)
                .ok()
                .and_then(|s| TopicName::new(s).ok())
                .ok_or(DecodeError("invalid topic name"))?;
            Command::CreateTopic { name }
        }
        KIND_RECONFIGURE => {
            let count = r.u32()? as usize;
            if count > r.remaining() / 8 {
                return Err(DecodeError("voter count exceeds payload"));
            }
            let mut voters = Vec::with_capacity(count);
            for _ in 0..count {
                voters.push(NodeId::new(r.u64()?));
            }
            let membership =
                Membership::new(voters).map_err(|_| DecodeError("empty membership"))?;
            Command::Reconfigure(membership)
        }
        _ => return Err(DecodeError("unknown command kind")),
    };
    if r.remaining() != 0 {
        return Err(DecodeError("trailing bytes after command"));
    }
    Ok(LogEntry::new(term, index, command))
}

/// A bounds-checked cursor over a payload.
struct Reader<'a> {
    buf: &'a [u8],
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Reader { buf }
    }

    fn remaining(&self) -> usize {
        self.buf.len()
    }

    fn bytes(&mut self, len: usize) -> std::result::Result<&'a [u8], DecodeError> {
        if self.buf.len() < len {
            return Err(DecodeError("payload ends early"));
        }
        let (head, tail) = self.buf.split_at(len);
        self.buf = tail;
        Ok(head)
    }

    fn u8(&mut self) -> std::result::Result<u8, DecodeError> {
        Ok(self.bytes(1)?[0])
    }

    fn u16(&mut self) -> std::result::Result<u16, DecodeError> {
        let b = self.bytes(2)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }

    fn u32(&mut self) -> std::result::Result<u32, DecodeError> {
        let b = self.bytes(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn u64(&mut self) -> std::result::Result<u64, DecodeError> {
        let b = self.bytes(8)?;
        Ok(u64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_entries() -> Vec<LogEntry> {
        let batch = RecordBatch::new(
            TopicId::new(7),
            vec![
                Record::new("px=99.5")
                    .with_key("UST10Y")
                    .with_timestamp(Timestamp::from_millis(1_700_000_000_000)),
                Record::new(""),
                Record::new(vec![0xAB; 300]).with_key(""),
            ],
        );
        let members = Membership::new([1, 2, 3].map(NodeId::new)).unwrap();
        vec![
            LogEntry::new(Term::new(1), LogIndex::new(1), Command::Noop),
            LogEntry::new(
                Term::new(1),
                LogIndex::new(2),
                Command::CreateTopic {
                    name: TopicName::new("ust.quotes").unwrap(),
                },
            ),
            LogEntry::new(Term::new(2), LogIndex::new(3), Command::Append(batch)),
            LogEntry::new(
                Term::new(u64::MAX),
                LogIndex::new(u64::MAX),
                Command::Reconfigure(members),
            ),
        ]
    }

    #[test]
    fn round_trips_every_command() {
        for entry in sample_entries() {
            let mut buf = Vec::new();
            let len = encode_frame(&entry, &mut buf).unwrap();
            assert_eq!(len, buf.len());
            let (decoded, used) = decode_frame(&buf).unwrap();
            assert_eq!(used, len);
            assert_eq!(decoded, entry);
        }
    }

    #[test]
    fn frames_decode_back_to_back() {
        let entries = sample_entries();
        let mut buf = Vec::new();
        for entry in &entries {
            encode_frame(entry, &mut buf).unwrap();
        }
        let mut pos = 0;
        for entry in &entries {
            let (decoded, used) = decode_frame(&buf[pos..]).unwrap();
            assert_eq!(&decoded, entry);
            pos += used;
        }
        assert_eq!(pos, buf.len());
    }

    #[test]
    fn noop_frame_has_the_documented_size() {
        let mut buf = Vec::new();
        let entry = LogEntry::new(Term::new(1), LogIndex::new(1), Command::Noop);
        assert_eq!(encode_frame(&entry, &mut buf).unwrap(), 8 + 17);
    }

    #[test]
    fn every_truncation_is_rejected() {
        let mut buf = Vec::new();
        encode_frame(&sample_entries()[2], &mut buf).unwrap();
        for cut in 0..buf.len() {
            assert!(decode_frame(&buf[..cut]).is_err(), "cut at {cut}");
        }
    }

    #[test]
    fn every_single_byte_flip_is_rejected() {
        let entry = &sample_entries()[1];
        let mut buf = Vec::new();
        encode_frame(entry, &mut buf).unwrap();
        for i in 0..buf.len() {
            let mut bad = buf.clone();
            bad[i] ^= 0x40;
            match decode_frame(&bad) {
                Err(_) => {}
                Ok((decoded, _)) => panic!("flip at {i} decoded as {decoded:?}"),
            }
        }
    }

    #[test]
    fn zeroed_bytes_are_not_a_frame() {
        assert_eq!(
            decode_frame(&[0u8; 64]),
            Err(DecodeError("frame length out of range"))
        );
    }

    #[test]
    fn oversized_entry_leaves_the_buffer_untouched() {
        let records = vec![Record::new(vec![0u8; 1 << 20]); 9];
        let entry = LogEntry::new(
            Term::new(1),
            LogIndex::new(1),
            Command::Append(RecordBatch::new(TopicId::new(1), records)),
        );
        let mut buf = vec![1, 2, 3];
        let err = encode_frame(&entry, &mut buf).unwrap_err();
        assert!(matches!(err, Error::EntryTooLarge { .. }));
        assert_eq!(buf, vec![1, 2, 3]);
    }
}
