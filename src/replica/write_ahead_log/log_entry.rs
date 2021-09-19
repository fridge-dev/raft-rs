use crate::replica::Term;
use crate::commitlog;

/// Byte representation:
///
/// ```text
/// |                                         1                           |
/// | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 0 | 1 | 2 | 3 | 4 | 5 | ... |
/// +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+-...-+
/// |Vrs|       Term (8 bytes)          |   Data (variable size)      ... |
/// +---+-------------------------------+-----------------------------...-+
/// ```
///
/// * `Vrs` - version of the serialized payload
/// * `Term` - raft leadership term when this entry was created
/// * `Data` - app specific data payload
///
/// Not needed:
///
/// * Checksum is not needed, it's guaranteed by underlying commitlog.
/// * Size/length of `Data` is not needed; the underlying commitlog will give us the correctly allocated array.
#[derive(Clone)]
pub(crate) struct WriteAheadLogEntry {
    pub term: Term,
    pub data: Vec<u8>,
}

const RAFT_LOG_ENTRY_FORMAT_VERSION: u8 = 1;

impl commitlog::Entry for WriteAheadLogEntry {}

impl From<Vec<u8>> for WriteAheadLogEntry {
    fn from(bytes: Vec<u8>) -> Self {
        // TODO:2.5 research how to do this correctly, safely, and efficiently.
        // TODO:2.5 use TryFrom so we can return error
        assert!(bytes.len() >= 9);
        assert_eq!(bytes[0], RAFT_LOG_ENTRY_FORMAT_VERSION);

        let term: u64 = bytes[1] as u64
            | (bytes[2] as u64) << 1 * 8
            | (bytes[3] as u64) << 2 * 8
            | (bytes[4] as u64) << 3 * 8
            | (bytes[5] as u64) << 4 * 8
            | (bytes[6] as u64) << 5 * 8
            | (bytes[7] as u64) << 6 * 8
            | (bytes[8] as u64) << 7 * 8;
        WriteAheadLogEntry {
            term: Term::new(term),
            data: bytes[9..].to_vec(),
        }
    }
}

impl Into<Vec<u8>> for WriteAheadLogEntry {
    fn into(self) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::with_capacity(1 + 8 + self.data.len());

        let term = self.term.as_u64();
        bytes.push(RAFT_LOG_ENTRY_FORMAT_VERSION);
        bytes.push(term as u8);
        bytes.push((term >> 1 * 8) as u8);
        bytes.push((term >> 2 * 8) as u8);
        bytes.push((term >> 3 * 8) as u8);
        bytes.push((term >> 4 * 8) as u8);
        bytes.push((term >> 5 * 8) as u8);
        bytes.push((term >> 6 * 8) as u8);
        bytes.push((term >> 7 * 8) as u8);

        for b in self.data {
            bytes.push(b);
        }

        bytes
    }
}
