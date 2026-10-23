// Read records sequentially, validate them, and handle incomplete trailing records.

use crate::{MAX_RECORD_SIZE, Record, WalError};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

const LENGTH_PREFIX_SIZE: usize = 4;
const MIN_RECORD_SIZE: usize = 25;

/// Recovers and validates records from a WAL file.
///
/// Recovery policy:
/// - A trailing fragment shorter than the length prefix is truncated.
/// - A plausible record whose declared length extends beyond EOF is truncated.
/// - Invalid lengths, invalid checksums, malformed records, and invalid
///   sequence numbers are errors. They are never silently skipped.
/// - Sequence numbers must start at 1 and increase by exactly 1.
pub struct WalRecovery {
    file: File,
}

impl WalRecovery {
    /// Opens an existing WAL for reading and possible tail truncation.
    ///
    /// The file must already exist. Recovery does not create a missing WAL.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, WalError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|error| WalError::Io(error.to_string()))?;

        Ok(Self { file })
    }

    /// Returns all valid records in log order.
    ///
    /// An incomplete trailing record is removed by truncating the file to
    /// the last valid boundary. The truncation is synchronized before return.
    pub fn recover(&mut self) -> Result<Vec<Record>, WalError> {
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(|error| WalError::Io(error.to_string()))?;

        let mut bytes = Vec::new();
        self.file
            .read_to_end(&mut bytes)
            .map_err(|error| WalError::Io(error.to_string()))?;

        let mut records = Vec::new();
        let mut offset = 0usize;
        let mut expected_sequence = 1u64;
        let mut truncate_tail = false;

        while offset < bytes.len() {
            let remaining = bytes.len() - offset;

            // Not enough bytes to read the complete length prefix.
            if remaining < LENGTH_PREFIX_SIZE {
                truncate_tail = true;
                break;
            }

            let length_bytes: [u8; 4] = bytes[offset..offset + 4]
                .try_into()
                .map_err(|_| WalError::CorruptRecord("unable to read record length".to_owned()))?;

            let record_len = u32::from_le_bytes(length_bytes) as usize;

            // An impossible length is corruption, not a torn tail.
            if record_len < MIN_RECORD_SIZE {
                return Err(WalError::CorruptRecord(format!(
                    "record at offset {offset} declares invalid length {record_len}"
                )));
            }

            if record_len > MAX_RECORD_SIZE {
                return Err(WalError::CorruptRecord(format!(
                    "record at offset {offset} exceeds maximum size: {record_len}"
                )));
            }

            // The prefix is plausible, but the declared record extends
            // beyond EOF. Treat it as an incomplete final record.
            if record_len > remaining {
                truncate_tail = true;
                break;
            }

            let end = offset
                .checked_add(record_len)
                .ok_or_else(|| WalError::CorruptRecord("record offset overflow".to_owned()))?;

            let record = Record::decode(&bytes[offset..end]).map_err(|error| {
                WalError::CorruptRecord(format!("invalid record at offset {offset}: {error}"))
            })?;

            if record.sequence != expected_sequence {
                return Err(WalError::SequenceMismatch {
                    expected: expected_sequence,
                    actual: record.sequence,
                });
            }

            records.push(record);
            expected_sequence = expected_sequence
                .checked_add(1)
                .ok_or(WalError::SequenceOverflow)?;

            offset = end;
        }

        if truncate_tail {
            let valid_len = u64::try_from(offset)
                .map_err(|_| WalError::CorruptRecord("valid offset exceeds u64".to_owned()))?;

            self.file
                .set_len(valid_len)
                .map_err(|error| WalError::Io(error.to_string()))?;

            self.file
                .sync_data()
                .map_err(|error| WalError::Io(error.to_string()))?;
        }

        // Leave the file cursor at the end of the valid log.
        self.file
            .seek(SeekFrom::End(0))
            .map_err(|error| WalError::Io(error.to_string()))?;

        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Operation;
    use std::fs;

    fn put(key: &[u8], value: &[u8]) -> Operation {
        Operation::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    fn record(sequence: u64, operation: Operation) -> Record {
        Record {
            sequence,
            operation,
        }
    }

    fn append_encoded(bytes: &mut Vec<u8>, record: &Record) {
        bytes.extend(record.encode().unwrap());
    }

    fn write_records(path: &Path, records: &[Record]) {
        let mut bytes = Vec::new();

        for record in records {
            append_encoded(&mut bytes, record);
        }

        fs::write(path, bytes).unwrap();
    }

    #[test]
    fn empty_wal_recovers_no_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        fs::write(&path, []).unwrap();

        let mut recovery = WalRecovery::open(&path).unwrap();

        assert!(recovery.recover().unwrap().is_empty());
    }

    #[test]
    fn recovers_valid_records_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let expected = vec![
            record(1, put(b"a", b"1")),
            record(2, put(b"b", b"2")),
            record(3, Operation::Delete { key: b"a".to_vec() }),
        ];

        write_records(&path, &expected);

        let mut recovery = WalRecovery::open(&path).unwrap();

        assert_eq!(recovery.recover().unwrap(), expected);
    }

    #[test]
    fn truncated_final_header_is_removed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let first = record(1, put(b"a", b"1"));
        let mut bytes = first.encode().unwrap();
        let valid_len = bytes.len();

        // An incomplete four-byte length prefix.
        bytes.extend_from_slice(&[0x20, 0x00]);

        fs::write(&path, bytes).unwrap();

        let mut recovery = WalRecovery::open(&path).unwrap();

        assert_eq!(recovery.recover().unwrap(), vec![first]);
        assert_eq!(fs::metadata(&path).unwrap().len(), valid_len as u64);
    }

    #[test]
    fn truncated_final_payload_is_removed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let first = record(1, put(b"a", b"1"));
        let second = record(2, put(b"long-key", b"long-value"));

        let mut bytes = first.encode().unwrap();
        let valid_len = bytes.len();

        let mut incomplete = second.encode().unwrap();
        incomplete.pop();
        bytes.extend(incomplete);

        fs::write(&path, bytes).unwrap();

        let mut recovery = WalRecovery::open(&path).unwrap();

        assert_eq!(recovery.recover().unwrap(), vec![first]);
        assert_eq!(fs::metadata(&path).unwrap().len(), valid_len as u64);
    }

    #[test]
    fn complete_record_with_invalid_checksum_is_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let mut bytes = record(1, put(b"a", b"1")).encode().unwrap();
        let checksum_offset = bytes.len() - 1;
        bytes[checksum_offset] ^= 0xFF;

        fs::write(&path, bytes).unwrap();

        let mut recovery = WalRecovery::open(&path).unwrap();

        assert!(matches!(
            recovery.recover(),
            Err(WalError::CorruptRecord(_))
        ));
    }

    #[test]
    fn corrupted_middle_record_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let first = record(1, put(b"a", b"1")).encode().unwrap();
        let mut middle = record(2, put(b"b", b"2")).encode().unwrap();
        let last = record(3, put(b"c", b"3")).encode().unwrap();

        let last_index = middle.len() - 1;
        middle[last_index] ^= 0xFF;

        let mut bytes = first;
        bytes.extend(middle);
        bytes.extend(last);

        fs::write(&path, bytes).unwrap();

        let mut recovery = WalRecovery::open(&path).unwrap();

        assert!(matches!(
            recovery.recover(),
            Err(WalError::CorruptRecord(_))
        ));
    }

    #[test]
    fn missing_sequence_number_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let records = vec![record(1, put(b"a", b"1")), record(3, put(b"b", b"2"))];

        write_records(&path, &records);

        let mut recovery = WalRecovery::open(&path).unwrap();

        assert_eq!(
            recovery.recover().unwrap_err(),
            WalError::SequenceMismatch {
                expected: 2,
                actual: 3,
            }
        );
    }

    #[test]
    fn repeated_sequence_number_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let records = vec![record(1, put(b"a", b"1")), record(1, put(b"b", b"2"))];

        write_records(&path, &records);

        let mut recovery = WalRecovery::open(&path).unwrap();

        assert_eq!(
            recovery.recover().unwrap_err(),
            WalError::SequenceMismatch {
                expected: 2,
                actual: 1,
            }
        );
    }

    #[test]
    fn recovery_succeeds_again_after_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let first = record(1, put(b"a", b"1"));
        let mut bytes = first.encode().unwrap();

        bytes.extend_from_slice(&[0x40, 0x00, 0x00]);
        fs::write(&path, bytes).unwrap();

        let mut recovery = WalRecovery::open(&path).unwrap();

        assert_eq!(recovery.recover().unwrap(), vec![first.clone()]);
        assert_eq!(recovery.recover().unwrap(), vec![first]);
    }

    #[test]
    fn valid_log_recovers_identically_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let expected = vec![record(1, put(b"a", b"1")), record(2, put(b"b", b"2"))];

        write_records(&path, &expected);

        {
            let mut recovery = WalRecovery::open(&path).unwrap();
            assert_eq!(recovery.recover().unwrap(), expected);
        }

        let mut recovery = WalRecovery::open(&path).unwrap();

        assert_eq!(recovery.recover().unwrap(), expected);
    }

    #[test]
    fn implausible_length_is_corruption_not_a_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

        // A complete length prefix declaring a record smaller than the
        // format's 25-byte minimum.
        fs::write(&path, 10u32.to_le_bytes()).unwrap();

        let mut recovery = WalRecovery::open(&path).unwrap();

        assert!(matches!(
            recovery.recover(),
            Err(WalError::CorruptRecord(_))
        ));
    }

    #[test]
    fn missing_file_returns_io_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing.log");

        assert!(matches!(WalRecovery::open(&path), Err(WalError::Io(_))));
    }
}
