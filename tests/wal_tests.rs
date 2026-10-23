
use rust_wal::{Operation, Record, SegmentedWal};
use std::fs;

fn put(key: &[u8], value: &[u8]) -> Operation {
    Operation::Put {
        key: key.to_vec(),
        value: value.to_vec(),
    }
}

fn delete(key: &[u8]) -> Operation {
    Operation::Delete { key: key.to_vec() }
}

#[test]
fn end_to_end_recovery_across_segments() {
    let dir = tempfile::tempdir().unwrap();

    let expected = vec![
        Record {
            sequence: 1,
            operation: put(b"alpha", b"one"),
        },
        Record {
            sequence: 2,
            operation: put(b"beta", b"two"),
        },
        Record {
            sequence: 3,
            operation: delete(b"alpha"),
        },
        Record {
            sequence: 4,
            operation: put(b"gamma", b"three"),
        },
    ];

    {
        let mut wal = SegmentedWal::open(dir.path(), 40).unwrap();

        for record in &expected {
            assert_eq!(
                wal.append(record.operation.clone()).unwrap(),
                record.sequence
            );
        }

        wal.sync().unwrap();
    }

    let recovered = SegmentedWal::recover_all(dir.path()).unwrap();
    assert_eq!(recovered, expected);

    let recovered_again = SegmentedWal::recover_all(dir.path()).unwrap();
    assert_eq!(recovered_again, expected);
}

#[test]
fn partial_final_record_is_not_recovered() {
    let dir = tempfile::tempdir().unwrap();

    let first = Record {
        sequence: 1,
        operation: put(b"key", b"value"),
    };

    {
        let mut wal = SegmentedWal::open(dir.path(), 128).unwrap();
        assert_eq!(wal.append(first.operation.clone()).unwrap(), 1);
        wal.sync().unwrap();
    }

    let path = dir.path().join("000001.log");
    let original = fs::read(&path).unwrap();

    let mut damaged = original.clone();
    damaged.extend_from_slice(&[0x20, 0x00]);
    fs::write(&path, damaged).unwrap();

    let recovered = SegmentedWal::recover_all(dir.path()).unwrap();
    assert_eq!(recovered, vec![first.clone()]);

    assert_eq!(fs::metadata(&path).unwrap().len(), original.len() as u64);
    assert_eq!(SegmentedWal::recover_all(dir.path()).unwrap(), vec![first]);
}

#[test]
fn corrupted_checksum_is_reported() {
    let dir = tempfile::tempdir().unwrap();

    {
        let mut wal = SegmentedWal::open(dir.path(), 128).unwrap();
        wal.append(put(b"key", b"value")).unwrap();
        wal.sync().unwrap();
    }

    let path = dir.path().join("000001.log");
    let mut bytes = fs::read(&path).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    fs::write(&path, bytes).unwrap();

    assert!(SegmentedWal::recover_all(dir.path()).is_err());
}

#[test]
fn restart_after_sync_preserves_acknowledged_records() {
    let dir = tempfile::tempdir().unwrap();

    {
        let mut wal = SegmentedWal::open(dir.path(), 128).unwrap();
        assert_eq!(wal.append(put(b"key", b"value")).unwrap(), 1);
        wal.sync().unwrap();
    }

    let recovered = SegmentedWal::recover_all(dir.path()).unwrap();

    assert_eq!(
        recovered,
        vec![Record {
            sequence: 1,
            operation: put(b"key", b"value"),
        }]
    );
}
