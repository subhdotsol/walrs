
use crate::{Operation, Record, WalError, WalRecovery, WalWriter, MAX_RECORD_SIZE};
use std::fs;
use std::path::{Path, PathBuf};

const MIN_RECORD_SIZE: u64 = 25;
const SEGMENT_EXTENSION: &str = "log";

/// Append-only WAL split across numbered segment files.
///
/// Segment size includes all encoded record bytes. Records are never split.
/// A record larger than `max_segment_size` is rejected.
///
/// Existing segments are recovered in ascending filename order. Segment
/// numbering must start at 1 and be contiguous.
pub struct SegmentedWal {
    directory: PathBuf,
    max_segment_size: u64,
    segment_number: u64,
    writer: WalWriter,
    next_sequence: u64,
}

impl SegmentedWal {
    /// Opens or initializes a segmented WAL directory.
    pub fn open(
        directory: impl AsRef<Path>,
        max_segment_size: u64,
    ) -> Result<Self, WalError> {
        if max_segment_size < MIN_RECORD_SIZE {
            return Err(WalError::CorruptRecord(
                "maximum segment size is smaller than the minimum record size".into(),
            ));
        }

        fs::create_dir_all(directory.as_ref())
            .map_err(|error| WalError::Io(error.to_string()))?;

        let directory = directory.as_ref().to_path_buf();
        let mut segments = discover_segments(&directory)?;

        if segments.is_empty() {
            let segment_number = 1;
            let path = segment_path(&directory, segment_number);
            let writer = WalWriter::open_segment(&path, 1)?;

            return Ok(Self {
                directory,
                max_segment_size,
                segment_number,
                writer,
                next_sequence: 1,
            });
        }

        segments.sort_unstable_by_key(|(number, _)| *number);

        for (index, (number, _)) in segments.iter().enumerate() {
            let expected = u64::try_from(index + 1)
                .map_err(|_| WalError::SequenceOverflow)?;

            if *number != expected {
                return Err(WalError::CorruptRecord(format!(
                    "segment numbering gap: expected {expected}, found {number}"
                )));
            }
        }

        let mut next_sequence = 1u64;

        for (_, path) in &segments {
            let mut recovery = WalRecovery::open(path)?;
            let records = recovery.recover_from(next_sequence)?;

            if let Some(last) = records.last() {
                next_sequence = last
                    .sequence
                    .checked_add(1)
                    .ok_or(WalError::SequenceOverflow)?;
            }
        }

        let (segment_number, last_path) =
            segments.last().ok_or_else(|| {
                WalError::CorruptRecord("no segments found".into())
            })?;

        let last_size = fs::metadata(last_path)
            .map_err(|error| WalError::Io(error.to_string()))?
            .len();

        if last_size != 0 {
            let new_number = segment_number
                .checked_add(1)
                .ok_or(WalError::SequenceOverflow)?;

            let writer =
                WalWriter::open_segment(segment_path(&directory, new_number), next_sequence)?;

            return Ok(Self {
                directory,
                max_segment_size,
                segment_number: new_number,
                writer,
                next_sequence,
            });
        }

        let writer = WalWriter::open_segment_existing_empty(
            last_path,
            next_sequence,
        )?;

        Ok(Self {
            directory,
            max_segment_size,
            segment_number: *segment_number,
            writer,
            next_sequence,
        })
    }

    /// Appends a record, rotating first if the current segment would overflow.
    pub fn append(&mut self, operation: Operation) -> Result<u64, WalError> {
        let record = Record {
            sequence: self.next_sequence,
            operation: operation.clone(),
        };

        let encoded = record.encode()?;
        let record_size = u64::try_from(encoded.len())
            .map_err(|_| WalError::RecordTooLarge)?;

        if record_size > self.max_segment_size {
            return Err(WalError::RecordTooLarge);
        }

        if self
            .writer
            .offset()
            .checked_add(record_size)
            .ok_or(WalError::RecordTooLarge)?
            > self.max_segment_size
        {
            self.rotate()?;
        }

        let sequence = self.writer.append(operation)?;
        self.next_sequence = sequence
            .checked_add(1)
            .ok_or(WalError::SequenceOverflow)?;

        Ok(sequence)
    }

    /// Synchronizes the current active segment.
    pub fn sync(&mut self) -> Result<(), WalError> {
        self.writer.sync()
    }

    fn rotate(&mut self) -> Result<(), WalError> {
        self.writer.sync()?;

        let next_segment = self
            .segment_number
            .checked_add(1)
            .ok_or(WalError::SequenceOverflow)?;

        let path = segment_path(&self.directory, next_segment);

        let writer = WalWriter::open_segment(&path, self.next_sequence)?;

        self.segment_number = next_segment;
        self.writer = writer;

        Ok(())
    }
}

fn segment_path(directory: &Path, number: u64) -> PathBuf {
    directory.join(format!("{number:06}.{SEGMENT_EXTENSION}"))
}

/// Discovers numbered WAL files and rejects unexpected files in the directory.
fn discover_segments(directory: &Path) -> Result<Vec<(u64, PathBuf)>, WalError> {
    let mut segments = Vec::new();

    for entry in fs::read_dir(directory)
        .map_err(|error| WalError::Io(error.to_string()))?
    {
        let entry = entry.map_err(|error| WalError::Io(error.to_string()))?;
        let path = entry.path();

        if !entry
            .file_type()
            .map_err(|error| WalError::Io(error.to_string()))?
            .is_file()
        {
            return Err(WalError::CorruptRecord(format!(
                "unexpected non-file entry in WAL directory: {}",
                path.display()
            )));
        }

        let name = entry.file_name();
        let name = name.to_string_lossy();

        let Some(stem) = name.strip_suffix(".log") else {
            return Err(WalError::CorruptRecord(format!(
                "invalid segment filename: {name}"
            )));
        };

        if stem.len() != 6 || !stem.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(WalError::CorruptRecord(format!(
                "invalid segment filename: {name}"
            )));
        }

        let number = stem.parse::<u64>().map_err(|_| {
            WalError::CorruptRecord(format!("invalid segment number: {name}"))
        })?;

        if number == 0 {
            return Err(WalError::CorruptRecord(
                "segment number zero is invalid".into(),
            ));
        }

        segments.push((number, path));
    }

    Ok(segments)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(key: &[u8], value: &[u8]) -> Operation {
        Operation::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    #[test]
    fn empty_directory_creates_initial_segment() {
        let dir = tempfile::tempdir().unwrap();
        let wal = SegmentedWal::open(dir.path(), 128).unwrap();

        assert!(dir.path().join("000001.log").exists());
        assert_eq!(wal.segment_number, 1);
    }

    #[test]
    fn appends_below_limit_without_rotating() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = SegmentedWal::open(dir.path(), 128).unwrap();

        wal.append(put(b"a", b"1")).unwrap();
        wal.append(put(b"b", b"2")).unwrap();

        assert!(dir.path().join("000001.log").exists());
        assert!(!dir.path().join("000002.log").exists());
    }

    #[test]
    fn rotates_when_next_record_would_exceed_limit() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = SegmentedWal::open(dir.path(), 30).unwrap();

        wal.append(put(b"a", b"1")).unwrap();
        wal.append(put(b"b", b"2")).unwrap();

        assert!(dir.path().join("000001.log").exists());
        assert!(dir.path().join("000002.log").exists());
    }

    #[test]
    fn records_are_never_split_across_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = SegmentedWal::open(dir.path(), 48).unwrap();

        wal.append(put(b"a", b"1")).unwrap();
        wal.append(put(b"long-key", b"long-value")).unwrap();

        for number in 1..=2 {
            let path = segment_path(dir.path(), number);
            if path.exists() {
                let bytes = fs::read(path).unwrap();
                assert!(bytes.len() <= 48);
            }
        }
    }

    #[test]
    fn sequence_numbers_continue_across_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = SegmentedWal::open(dir.path(), 30).unwrap();

        assert_eq!(wal.append(put(b"a", b"1")).unwrap(), 1);
        assert_eq!(wal.append(put(b"b", b"2")).unwrap(), 2);
        assert_eq!(wal.append(put(b"c", b"3")).unwrap(), 3);
    }

    #[test]
    fn oversized_record_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = SegmentedWal::open(dir.path(), 30).unwrap();

        let result = wal.append(put(b"key", b"value"));

        assert!(matches!(result, Err(WalError::RecordTooLarge)));
    }

    #[test]
    fn sync_succeeds_after_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = SegmentedWal::open(dir.path(), 30).unwrap();

        wal.append(put(b"a", b"1")).unwrap();
        wal.append(put(b"b", b"2")).unwrap();

        wal.sync().unwrap();
    }

    #[test]
    fn rejects_invalid_segment_names() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("backup.log"), []).unwrap();

        assert!(matches!(
            SegmentedWal::open(dir.path(), 128),
            Err(WalError::CorruptRecord(_))
        ));
    }

    #[test]
    fn reopens_existing_segments_without_overwriting() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mut wal = SegmentedWal::open(dir.path(), 30).unwrap();
            wal.append(put(b"a", b"1")).unwrap();
            wal.append(put(b"b", b"2")).unwrap();
            wal.sync().unwrap();
        }

        let first_segment = fs::read(dir.path().join("000001.log")).unwrap();

        let mut wal = SegmentedWal::open(dir.path(), 30).unwrap();
        assert_eq!(wal.append(put(b"c", b"3")).unwrap(), 3);

        assert_eq!(
            fs::read(dir.path().join("000001.log")).unwrap(),
            first_segment
        );
    }
}
