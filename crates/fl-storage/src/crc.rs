//! CRC-32C (Castagnoli), the checksum used on every frame and index file.
//!
//! This is a plain table-driven implementation, one byte per step. It keeps
//! the crate free of dependencies. If profiling ever shows it on the hot
//! path, the `crc32c` crate is a drop-in replacement that uses the CPU's
//! CRC instructions.

/// The Castagnoli polynomial, bit-reflected.
const POLY: u32 = 0x82F6_3B78;

const TABLE: [u32; 256] = make_table();

const fn make_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ POLY
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

/// Computes the CRC-32C of `data`.
pub fn crc32c(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in data {
        crc = TABLE[((crc ^ u32::from(byte)) & 0xFF) as usize] ^ (crc >> 8);
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_standard_check_values() {
        // The check value from the CRC catalogue, and the RFC 3720 vectors.
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(&[0u8; 32]), 0x8A91_36AA);
        assert_eq!(crc32c(&[0xFFu8; 32]), 0x62A8_AB43);
        assert_eq!(crc32c(b""), 0);
    }

    #[test]
    fn detects_a_single_bit_flip() {
        let mut data = b"ferrumlog segment frame".to_vec();
        let before = crc32c(&data);
        data[5] ^= 0x01;
        assert_ne!(crc32c(&data), before);
    }
}
