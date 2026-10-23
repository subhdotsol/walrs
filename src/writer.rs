// Open a file, append encoded records, track offsets, and implement syncing.

use crate::{Operation, Record, WalError};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

/// Append-only WAL writer.
///
/// This implementation supports only new or empty files. Opening a nonempty
/// file returns `WalError::FileNotEmpty` because recovery and sequence-number
/// discovery are not implemented yet.
///
/// A successful append means the complete encoded record was accepted by
/// the file write operation. It does not guarantee durability; call `sync()`.
pub struct WalWriter {
    file: File,
    next_sequence: u64,
    offset: u64,
    failed: bool,
}

impl WalWriter {
    /// Opens a WAL file without truncating existing data.
    ///
    /// Creates the file if it does not exist. Rejects nonempty files.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, WalError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|error| WalError::Io(error.to_string()))?;

        let offset = file
            .metadata()
            .map_err(|error| WalError::Io(error.to_string()))?
            .len();

        if offset != 0 {
            return Err(WalError::FileNotEmpty);
        }

        Ok(Self {
            file,
            next_sequence: 1,
            offset: 0,
            failed: false,
        })
    }

    /// Encodes and appends one operation.
    ///
    /// Returns the sequence number assigned to the record after the entire
    /// encoded record has been written successfully.
    ///
    /// If a write fails, some bytes may already have reached the file.
    /// In that case, this writer is marked failed and further appends are
    /// rejected to avoid compounding a potentially partial record.
    pub fn append(&mut self, operation: Operation) -> Result<u64, WalError> {
        if self.failed {
            return Err(WalError::WriterPoisoned);
        }

        let sequence = self.next_sequence;
        let following_sequence = sequence.checked_add(1).ok_or(WalError::SequenceOverflow)?;

        let record = Record {
            sequence,
            operation,
        };

        let encoded = record.encode()?;

        let encoded_len = u64::try_from(encoded.len()).map_err(|_| WalError::RecordTooLarge)?;

        let new_offset = self
            .offset
            .checked_add(encoded_len)
            .ok_or(WalError::RecordTooLarge)?;

        if let Err(error) = self.file.write_all(&encoded) {
            self.failed = true;
            return Err(WalError::Io(error.to_string()));
        }

        // Update state only after the complete record has been written.
        self.offset = new_offset;
        self.next_sequence = following_sequence;

        Ok(sequence)
    }

    /// Requests synchronization of file data to the underlying storage.
    ///
    /// A successful return indicates that `sync_data()` succeeded; it is
    /// not an absolute guarantee against every hardware or filesystem failure.
    pub fn sync(&mut self) -> Result<(), WalError> {
        self.file
            .sync_data()
            .map_err(|error| WalError::Io(error.to_string()))
    }

    /// Returns the number of bytes successfully appended by this writer.
    pub fn offset(&self) -> u64 {
        self.offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn put(key: &[u8], value: &[u8]) -> Operation {
        Operation::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    #[test]
    fn append_one_put_writes_encoded_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let mut writer = WalWriter::open(&path).unwrap();
        let sequence = writer.append(put(b"key", b"value")).unwrap();

        assert_eq!(sequence, 1);

        let actual = fs::read(&path).unwrap();
        let expected = Record {
            sequence: 1,
            operation: put(b"key", b"value"),
        }
        .encode()
        .unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn multiple_records_are_written_in_append_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let mut writer = WalWriter::open(&path).unwrap();

        assert_eq!(writer.append(put(b"a", b"1")).unwrap(), 1);
        assert_eq!(writer.append(put(b"b", b"2")).unwrap(), 2);
        assert_eq!(
            writer
                .append(Operation::Delete { key: b"a".to_vec() })
                .unwrap(),
            3
        );

        let mut expected = Vec::new();

        for record in [
            Record {
                sequence: 1,
                operation: put(b"a", b"1"),
            },
            Record {
                sequence: 2,
                operation: put(b"b", b"2"),
            },
            Record {
                sequence: 3,
                operation: Operation::Delete { key: b"a".to_vec() },
            },
        ] {
            expected.extend(record.encode().unwrap());
        }

        assert_eq!(fs::read(&path).unwrap(), expected);
    }

    #[test]
    fn sequence_numbers_increase_monotonically() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let mut writer = WalWriter::open(&path).unwrap();

        let first = writer.append(put(b"a", b"1")).unwrap();
        let second = writer.append(put(b"b", b"2")).unwrap();
        let third = writer.append(put(b"c", b"3")).unwrap();

        assert!(first < second);
        assert!(second < third);
        assert_eq!((first, second, third), (1, 2, 3));
    }

    #[test]
    fn offset_tracks_written_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let mut writer = WalWriter::open(&path).unwrap();

        assert_eq!(writer.offset(), 0);

        writer.append(put(b"key", b"value")).unwrap();

        assert_eq!(writer.offset(), fs::metadata(&path).unwrap().len());
    }

    #[test]
    fn sync_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let mut writer = WalWriter::open(&path).unwrap();
        writer.append(put(b"key", b"value")).unwrap();

        writer.sync().unwrap();
    }

    #[test]
    fn reopening_nonempty_file_is_rejected_without_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let original = b"existing WAL data";

        fs::write(&path, original).unwrap();

        assert!(matches!(
            WalWriter::open(&path),
            Err(WalError::FileNotEmpty)
        ));

        assert_eq!(fs::read(&path).unwrap(), original);
    }

    #[test]
    fn opening_an_empty_existing_file_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

        fs::write(&path, []).unwrap();

        let mut writer = WalWriter::open(&path).unwrap();

        assert_eq!(writer.append(put(b"k", b"v")).unwrap(), 1);
    }

    #[test]
    fn nonexistent_parent_returns_io_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing").join("wal.log");

        assert!(matches!(WalWriter::open(&path), Err(WalError::Io(_))));
    }

    #[test]
    fn opening_a_directory_returns_io_error() {
        let dir = tempfile::tempdir().unwrap();

        assert!(matches!(WalWriter::open(dir.path()), Err(WalError::Io(_))));
    }
}
