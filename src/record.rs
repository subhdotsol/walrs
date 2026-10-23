// Define records, encode them into bytes, decode them, and validate malformed input.
//
// Defining binary format for encoding and decoding to agree
//
// Field         Size        Description
// ------------  ----------  ------------------------------------------------------------
// Record length 4 bytes     Total record size, including header and checksum
// Sequence      8 bytes     Record sequence number
// Operation     1 byte      1 = PUT, 2 = DELETE
// Key length    4 bytes     Length of key in bytes
// Value length  4 bytes     Length of PUT value; zero for DELETE
// Key           Variable    Raw key bytes
// Value         Variable    Raw value bytes for PUT
// Checksum      4 bytes     CRC-32 of all preceding record bytes
//
// Fixed overhead: 25 bytes.
// Maximum record size: 16 MiB (16 * 1024 * 1024 bytes).

use std::error::Error;
use std::fmt;

const HEADER_SIZE: usize = 21;
const CHECKSUM_SIZE: usize = 4;
pub const MAX_RECORD_SIZE: usize = 16 * 1024 * 1024;

const PUT: u8 = 1;
const DELETE: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub sequence: u64,
    pub operation: Operation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalError {
    RecordTooSmall,
    RecordTooLarge,
    TruncatedRecord,
    InvalidLength,
    UnknownOperation(u8),
    InvalidOperationData,
    ChecksumMismatch,
    Io(String),
    FileNotEmpty,
    WriterPoisoned,
    SequenceOverflow,
}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalError::RecordTooSmall => write!(f, "record is too small"),
            WalError::RecordTooLarge => write!(f, "record exceeds maximum size"),
            WalError::TruncatedRecord => write!(f, "record is truncated"),
            WalError::InvalidLength => write!(f, "invalid record length"),
            WalError::UnknownOperation(op) => {
                write!(f, "unknown operation type: {op}")
            }
            WalError::InvalidOperationData => {
                write!(f, "invalid data for operation")
            }
            WalError::ChecksumMismatch => write!(f, "checksum mismatch"),
            WalError::Io(message) => write!(f, "WAL I/O error: {message}"),
            WalError::FileNotEmpty => {
                write!(f, "WAL file is nonempty; recovery is not implemented")
            }
            WalError::WriterPoisoned => {
                write!(f, "WAL writer cannot append after a write failure")
            }
            WalError::SequenceOverflow => {
                write!(f, "WAL sequence number overflow")
            }
        }
    }
}

impl Error for WalError {}

impl Record {
    /// Encodes one complete record.
    ///
    /// Format:
    /// length:u32 | sequence:u64 | operation:u8 |
    /// key_len:u32 | value_len:u32 | key | value | crc32:u32
    ///
    /// All integers are little-endian.
    pub fn encode(&self) -> Result<Vec<u8>, WalError> {
        let (op_type, key, value): (u8, &[u8], &[u8]) = match &self.operation {
            Operation::Put { key, value } => (PUT, key, value),
            Operation::Delete { key } => (DELETE, key, &[]),
        };

        let key_len = u32::try_from(key.len()).map_err(|_| WalError::RecordTooLarge)?;
        let value_len = u32::try_from(value.len()).map_err(|_| WalError::RecordTooLarge)?;

        let total_len = HEADER_SIZE
            .checked_add(key.len())
            .and_then(|n| n.checked_add(value.len()))
            .and_then(|n| n.checked_add(CHECKSUM_SIZE))
            .ok_or(WalError::RecordTooLarge)?;

        if total_len > MAX_RECORD_SIZE {
            return Err(WalError::RecordTooLarge);
        }

        let total_len_u32 = u32::try_from(total_len).map_err(|_| WalError::RecordTooLarge)?;

        let mut bytes = Vec::with_capacity(total_len);

        bytes.extend_from_slice(&total_len_u32.to_le_bytes());
        bytes.extend_from_slice(&self.sequence.to_le_bytes());
        bytes.push(op_type);
        bytes.extend_from_slice(&key_len.to_le_bytes());
        bytes.extend_from_slice(&value_len.to_le_bytes());
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(value);

        let checksum = crc32fast::hash(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());

        debug_assert_eq!(bytes.len(), total_len);
        Ok(bytes)
    }

    /// Decodes and validates one complete record.
    pub fn decode(bytes: &[u8]) -> Result<Record, WalError> {
        if bytes.len() < HEADER_SIZE + CHECKSUM_SIZE {
            return Err(WalError::RecordTooSmall);
        }

        if bytes.len() > MAX_RECORD_SIZE {
            return Err(WalError::RecordTooLarge);
        }

        let declared_len = read_u32(bytes, 0)? as usize;

        if declared_len != bytes.len() {
            return Err(WalError::InvalidLength);
        }

        if declared_len > MAX_RECORD_SIZE {
            return Err(WalError::RecordTooLarge);
        }

        let sequence = read_u64(bytes, 4)?;
        let op_type = bytes[12];
        let key_len = read_u32(bytes, 13)? as usize;
        let value_len = read_u32(bytes, 17)? as usize;

        let payload_len = key_len
            .checked_add(value_len)
            .ok_or(WalError::InvalidLength)?;

        let expected_len = HEADER_SIZE
            .checked_add(payload_len)
            .and_then(|n| n.checked_add(CHECKSUM_SIZE))
            .ok_or(WalError::InvalidLength)?;

        if expected_len != declared_len {
            return Err(WalError::InvalidLength);
        }

        // Validate the checksum before constructing payload vectors.
        let checksum_offset = declared_len - CHECKSUM_SIZE;
        let stored_checksum = read_u32(bytes, checksum_offset)?;
        let actual_checksum = crc32fast::hash(&bytes[..checksum_offset]);

        if stored_checksum != actual_checksum {
            return Err(WalError::ChecksumMismatch);
        }

        let key_start = HEADER_SIZE;
        let value_start = key_start + key_len;
        let payload_end = value_start + value_len;

        let key = bytes[key_start..value_start].to_vec();
        let value = bytes[value_start..payload_end].to_vec();

        let operation = match op_type {
            PUT => Operation::Put { key, value },
            DELETE => {
                if value_len != 0 {
                    return Err(WalError::InvalidOperationData);
                }

                Operation::Delete { key }
            }
            other => return Err(WalError::UnknownOperation(other)),
        };

        Ok(Record {
            sequence,
            operation,
        })
    }
}

/// Reads a little-endian `u32` from `bytes` starting at `offset`.
///
/// # Parameters
/// - `bytes`: The byte slice to read from.
/// - `offset`: The index in `bytes` to start reading from.
///
/// # Returns
/// - `Ok(u32)`: The decoded little-endian value if 4 bytes are available starting at `offset`.
/// - `Err(WalError::TruncatedRecord)`: If fewer than 4 bytes remain from `offset` (e.g., array ends too early).
///
/// # Notes
/// - Byte order is little-endian (least significant byte first). For example, `[42, 0, 0, 0]` -> `42`.
/// - This does not advance any cursor; the caller is responsible for tracking `offset`.
fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, WalError> {
    let slice = bytes
        .get(offset..offset + 4)
        .ok_or(WalError::TruncatedRecord)?;

    let array: [u8; 4] = slice.try_into().map_err(|_| WalError::TruncatedRecord)?;

    Ok(u32::from_le_bytes(array))
}

/// Reads a little-endian `u64` from `bytes` starting at `offset`.
///
/// # Parameters
/// - `bytes`: The byte slice to read from.
/// - `offset`: The index in `bytes` to start reading from.
///
/// # Returns
/// - `Ok(u64)`: The decoded little-endian value if 8 bytes are available starting at `offset`.
/// - `Err(WalError::TruncatedRecord)`: If fewer than 8 bytes remain from `offset` (e.g., array ends too early).
///
/// # Notes
/// - Byte order is little-endian (least significant byte first). For example, `[42, 0, 0, 0, 0, 0, 0, 0]` -> `42`.
/// - For multi-byte values, byte significance increases to the right (e.g., `[0x2A, 0x01, 0, 0, 0, 0, 0, 0]` -> `0x01*256 + 0x2A = 298`).
/// - This does not advance any cursor; the caller is responsible for tracking `offset`.
fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, WalError> {
    let slice = bytes
        .get(offset..offset + 8)
        .ok_or(WalError::TruncatedRecord)?;

    let array: [u8; 8] = slice.try_into().map_err(|_| WalError::TruncatedRecord)?;

    Ok(u64::from_le_bytes(array))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_put() -> Record {
        Record {
            sequence: 42,
            operation: Operation::Put {
                key: b"name".to_vec(),
                value: b"Alice".to_vec(),
            },
        }
    }

    fn round_trip(record: Record) {
        let encoded = record.encode().unwrap();
        let decoded = Record::decode(&encoded).unwrap();
        assert_eq!(decoded, record);
    }

    #[test]
    fn put_round_trip() {
        round_trip(sample_put());
    }

    #[test]
    fn delete_round_trip() {
        round_trip(Record {
            sequence: 7,
            operation: Operation::Delete {
                key: b"name".to_vec(),
            },
        });
    }

    #[test]
    fn empty_key_and_value() {
        round_trip(Record {
            sequence: 0,
            operation: Operation::Put {
                key: vec![],
                value: vec![],
            },
        });

        round_trip(Record {
            sequence: 1,
            operation: Operation::Delete { key: vec![] },
        });
    }

    #[test]
    fn binary_key_and_value() {
        round_trip(Record {
            sequence: 99,
            operation: Operation::Put {
                key: vec![0, 1, 0, 255],
                value: vec![255, 0, 128, 0, 42],
            },
        });
    }

    #[test]
    fn truncated_header() {
        assert!(Record::decode(&[0; 10]).is_err());
    }

    #[test]
    fn truncated_payload() {
        let mut bytes = sample_put().encode().unwrap();
        bytes.pop();

        assert!(Record::decode(&bytes).is_err());
    }

    #[test]
    fn unknown_operation() {
        let mut bytes = sample_put().encode().unwrap();
        bytes[12] = 99;

        // Recalculate checksum so the operation validation is reached.
        let checksum_offset = bytes.len() - CHECKSUM_SIZE;
        let checksum = crc32fast::hash(&bytes[..checksum_offset]);
        bytes[checksum_offset..].copy_from_slice(&checksum.to_le_bytes());

        assert_eq!(Record::decode(&bytes), Err(WalError::UnknownOperation(99)));
    }

    #[test]
    fn invalid_lengths() {
        let mut bytes = sample_put().encode().unwrap();

        // Make key_len inconsistent with the record's actual size.
        bytes[13..17].copy_from_slice(&u32::MAX.to_le_bytes());

        assert_eq!(Record::decode(&bytes), Err(WalError::InvalidLength));
    }

    #[test]
    fn checksum_mismatch() {
        let mut bytes = sample_put().encode().unwrap();
        bytes[HEADER_SIZE] ^= 0x01;

        assert_eq!(Record::decode(&bytes), Err(WalError::ChecksumMismatch));
    }

    #[test]
    fn oversized_record_rejected() {
        let record = Record {
            sequence: 1,
            operation: Operation::Put {
                key: vec![],
                value: vec![0; MAX_RECORD_SIZE],
            },
        };

        assert_eq!(record.encode(), Err(WalError::RecordTooLarge));
    }

    #[test]
    fn declared_length_mismatch() {
        let mut bytes = sample_put().encode().unwrap();
        bytes[0..4].copy_from_slice(&1u32.to_le_bytes());

        assert_eq!(Record::decode(&bytes), Err(WalError::InvalidLength));
    }

    #[test]
    fn delete_with_value_is_rejected() {
        let mut bytes = Record {
            sequence: 5,
            operation: Operation::Delete { key: b"k".to_vec() },
        }
        .encode()
        .unwrap();

        // Change the operation to PUT's opposite scenario: DELETE with
        // a non-zero value length. Adjust length and checksum consistently.
        bytes[17..21].copy_from_slice(&1u32.to_le_bytes());
        bytes.insert(bytes.len() - CHECKSUM_SIZE, b'x');

        let new_len = bytes.len() as u32;
        bytes[0..4].copy_from_slice(&new_len.to_le_bytes());

        let checksum_offset = bytes.len() - CHECKSUM_SIZE;
        let checksum = crc32fast::hash(&bytes[..checksum_offset]);
        bytes[checksum_offset..].copy_from_slice(&checksum.to_le_bytes());

        assert_eq!(Record::decode(&bytes), Err(WalError::InvalidOperationData));
    }
}
