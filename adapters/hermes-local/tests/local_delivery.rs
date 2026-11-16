#![cfg(unix)]
use hermes_host::{DurableLedger, HostError};
use hermes_ledger::{
    AppendOutcomeV1, ConsumerId, CorrelationId, EventEnvelopeV1, EventSequence, IdempotencyKey,
    Limits, ProducerId, SchemaId, StreamId, persistence::CheckpointBudgetV1,
};
use keystone_storage::{Checkpoint, CheckpointStore, CommitOutcome, Revision};
use keystone_storage_directory::{DirectoryCheckpointStore, DirectoryErrorV1, LimitsV1};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "hermes-local-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> PathBuf {
        self.0.join("ledger")
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn event() -> EventEnvelopeV1 {
    EventEnvelopeV1::new(
        ProducerId::new([1; 16]),
        SchemaId::new([2; 16]),
        IdempotencyKey::new([3; 16]),
        CorrelationId::new([4; 16]),
        None,
        b"private-event".to_vec(),
    )
}

#[test]
fn local_restart_preserves_replay_conflict_and_independent_cursors() {
    let fixture = Fixture::new();
    let stream = StreamId::new([5; 16]);
    let first = ConsumerId::new([6; 16]);
    let second = ConsumerId::new([7; 16]);
    let mut ledger = hermes_local::create_new(
        fixture.path(),
        Limits::default(),
        CheckpointBudgetV1::default(),
    )
    .unwrap();
    assert!(
        hermes_local::open_existing(
            fixture.path(),
            Limits::default(),
            CheckpointBudgetV1::default()
        )
        .is_err()
    );
    assert_eq!(
        ledger.append(stream, None, event()).unwrap(),
        AppendOutcomeV1::Appended(EventSequence(0))
    );
    ledger.acknowledge(first, stream, EventSequence(0)).unwrap();
    let revision = ledger.revision();
    drop(ledger);
    let mut ledger = hermes_local::open_existing(
        fixture.path(),
        Limits::default(),
        CheckpointBudgetV1::default(),
    )
    .unwrap();
    assert_eq!(
        ledger.append(stream, None, event()).unwrap(),
        AppendOutcomeV1::Replayed(EventSequence(0))
    );
    assert_eq!(
        ledger
            .append(StreamId::new([8; 16]), None, event())
            .unwrap(),
        AppendOutcomeV1::Conflict
    );
    assert_eq!(
        ledger
            .consumer_cursor(first, stream)
            .unwrap()
            .unwrap()
            .next_sequence,
        EventSequence(1)
    );
    assert_eq!(ledger.consumer_cursor(second, stream).unwrap(), None);
    assert_eq!(
        ledger.read(stream, EventSequence(0), 1).unwrap().events[0]
            .envelope
            .payload(),
        b"private-event"
    );
    assert_eq!(ledger.revision(), revision);
    assert_eq!(ledger.signals().commits, 0);
}

struct LoseReceipt {
    inner: DirectoryCheckpointStore,
    lose: bool,
}
impl CheckpointStore for LoseReceipt {
    type Error = DirectoryErrorV1;
    fn load(&self, key: &str) -> Result<Option<Checkpoint>, Self::Error> {
        self.inner.load(key)
    }
    fn compare_exchange(
        &mut self,
        key: &str,
        expected: Option<Revision>,
        bytes: &[u8],
    ) -> Result<CommitOutcome, Self::Error> {
        let result = self.inner.compare_exchange(key, expected, bytes)?;
        if self.lose && matches!(result, CommitOutcome::Committed(_)) {
            Ok(CommitOutcome::Indeterminate)
        } else {
            Ok(result)
        }
    }
}

#[test]
fn real_commit_with_lost_receipt_fences_then_reopen_resolves_as_replay() {
    let fixture = Fixture::new();
    drop(
        hermes_local::create_new(
            fixture.path(),
            Limits::default(),
            CheckpointBudgetV1::default(),
        )
        .unwrap(),
    );
    let store =
        DirectoryCheckpointStore::open_existing(fixture.path(), LimitsV1::default()).unwrap();
    let mut ledger = DurableLedger::open(
        LoseReceipt {
            inner: store,
            lose: true,
        },
        Limits::default(),
        CheckpointBudgetV1::default(),
    )
    .unwrap();
    let stream = StreamId::new([5; 16]);
    assert!(matches!(
        ledger.append(stream, None, event()),
        Err(HostError::Indeterminate)
    ));
    assert!(matches!(
        ledger.read(stream, EventSequence(0), 1),
        Err(HostError::Fenced)
    ));
    drop(ledger);
    let mut recovered = hermes_local::open_existing(
        fixture.path(),
        Limits::default(),
        CheckpointBudgetV1::default(),
    )
    .unwrap();
    assert_eq!(
        recovered.append(stream, None, event()).unwrap(),
        AppendOutcomeV1::Replayed(EventSequence(0))
    );
}

#[test]
fn missing_or_corrupt_files_do_not_reset_the_ledger() {
    let fixture = Fixture::new();
    drop(
        hermes_local::create_new(
            fixture.path(),
            Limits::default(),
            CheckpointBudgetV1::default(),
        )
        .unwrap(),
    );
    let head = fixture.path().join("checkpoint.bin");
    std::fs::write(&head, b"corrupt").unwrap();
    assert!(
        hermes_local::open_existing(
            fixture.path(),
            Limits::default(),
            CheckpointBudgetV1::default()
        )
        .is_err()
    );
    std::fs::remove_file(head).unwrap();
    assert!(
        hermes_local::open_existing(
            fixture.path(),
            Limits::default(),
            CheckpointBudgetV1::default()
        )
        .is_err()
    );
    assert!(
        hermes_local::create_new(
            fixture.path(),
            Limits::default(),
            CheckpointBudgetV1::default()
        )
        .is_err()
    );
}

#[test]
fn known_invalid_configuration_creates_no_directory() {
    let fixture = Fixture::new();
    assert!(
        hermes_local::create_new(
            fixture.path(),
            Limits::default(),
            CheckpointBudgetV1 {
                max_bytes: 1,
                max_fields: 1
            }
        )
        .is_err()
    );
    assert!(!fixture.path().exists());
}
