use hermes_host::{DurableLedger, HostError};
use hermes_ledger::{
    AppendOutcomeV1, ConsumerId, CorrelationId, EventEnvelopeV1, EventSequence, IdempotencyKey,
    Limits, ProducerId, ReadGapV1, SchemaId, StreamId, persistence::CheckpointBudgetV1,
};
use keystone_storage::{
    AcknowledgementStrength, Checkpoint, CheckpointStore, CommitOutcome, CommitReceipt, Revision,
};
use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
};

#[derive(Clone, Copy)]
enum Fault {
    None,
    Before,
    After,
    Weak,
    WrongRevision,
}
#[derive(Clone)]
struct Store {
    state: Arc<Mutex<Option<Checkpoint>>>,
    calls: Arc<Mutex<usize>>,
    fault: Fault,
}
impl Default for Store {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(None)),
            calls: Arc::new(Mutex::new(0)),
            fault: Fault::None,
        }
    }
}
impl CheckpointStore for Store {
    type Error = Infallible;
    fn load(&self, _: &str) -> Result<Option<Checkpoint>, Infallible> {
        Ok(self.state.lock().unwrap().clone())
    }
    fn compare_exchange(
        &mut self,
        _: &str,
        expected: Option<Revision>,
        bytes: &[u8],
    ) -> Result<CommitOutcome, Infallible> {
        let call = {
            let mut count = self.calls.lock().unwrap();
            *count += 1;
            *count
        };
        let fault = if call == 2 { self.fault } else { Fault::None };
        let mut state = self.state.lock().unwrap();
        assert_eq!(
            expected,
            state.as_ref().map(|checkpoint| checkpoint.revision)
        );
        if matches!(fault, Fault::Before) {
            return Ok(CommitOutcome::Indeterminate);
        }
        let revision = Revision(expected.map_or(0, |revision| revision.0 + 1));
        *state = Some(Checkpoint {
            revision,
            bytes: bytes.to_vec(),
            strength: if matches!(fault, Fault::Weak) {
                AcknowledgementStrength::Volatile
            } else {
                AcknowledgementStrength::Durable
            },
        });
        if matches!(fault, Fault::After) {
            return Ok(CommitOutcome::Indeterminate);
        }
        Ok(CommitOutcome::Committed(CommitReceipt {
            revision: if matches!(fault, Fault::WrongRevision) {
                Revision(revision.0 + 1)
            } else {
                revision
            },
            mechanism: "synthetic-qualified-store",
            strength: if matches!(fault, Fault::Weak) {
                AcknowledgementStrength::Volatile
            } else {
                AcknowledgementStrength::Durable
            },
        }))
    }
}
fn event(key: u8) -> EventEnvelopeV1 {
    EventEnvelopeV1::new(
        ProducerId::new([1; 16]),
        SchemaId::new([2; 16]),
        IdempotencyKey::new([key; 16]),
        CorrelationId::new([3; 16]),
        None,
        b"private-body".to_vec(),
    )
}
fn open(store: Store) -> DurableLedger<Store> {
    DurableLedger::create_new(store, Limits::default(), CheckpointBudgetV1::default()).unwrap()
}

#[test]
fn commit_gates_publication_and_restart_retains_replay_and_consumer_progress() {
    let store = Store::default();
    let stream = StreamId::new([5; 16]);
    let consumer = ConsumerId::new([6; 16]);
    let mut ledger = open(store.clone());
    assert_eq!(
        ledger.append(stream, None, event(7)).unwrap(),
        AppendOutcomeV1::Appended(EventSequence(0))
    );
    ledger
        .acknowledge(consumer, stream, EventSequence(0))
        .unwrap();
    assert_eq!(*store.calls.lock().unwrap(), 3);
    drop(ledger);
    let mut recovered = DurableLedger::open(
        store.clone(),
        Limits::default(),
        CheckpointBudgetV1::default(),
    )
    .unwrap();
    assert_eq!(
        recovered.append(stream, None, event(7)).unwrap(),
        AppendOutcomeV1::Replayed(EventSequence(0))
    );
    assert_eq!(
        recovered
            .consumer_cursor(consumer, stream)
            .unwrap()
            .unwrap()
            .next_sequence,
        EventSequence(1)
    );
    recovered
        .acknowledge(consumer, stream, EventSequence(0))
        .unwrap();
    assert_eq!(
        *store.calls.lock().unwrap(),
        3,
        "replay and identity acknowledgment must not commit"
    );
}

#[test]
fn compaction_commit_survives_restart_with_gap_and_key_tombstone() {
    let store = Store::default();
    let stream = StreamId::new([5; 16]);
    let consumer = ConsumerId::new([6; 16]);
    let mut ledger = open(store.clone());
    ledger.append(stream, None, event(7)).unwrap();
    ledger
        .acknowledge(consumer, stream, EventSequence(0))
        .unwrap();
    ledger.compact_through(stream, EventSequence(0)).unwrap();
    drop(ledger);

    let mut recovered =
        DurableLedger::open(store, Limits::default(), CheckpointBudgetV1::default()).unwrap();
    let batch = recovered.read(stream, EventSequence(0), 1).unwrap();
    assert!(batch.events.is_empty());
    assert!(matches!(batch.gap, Some(ReadGapV1::BeforeRetention { .. })));
    assert_eq!(
        recovered
            .append(stream, Some(EventSequence(0)), event(7))
            .unwrap(),
        AppendOutcomeV1::ReplayHistoryLost(EventSequence(0))
    );
}

#[test]
fn unknown_commit_fences_reads_and_recovery_observes_the_actual_winner() {
    for fault in [Fault::Before, Fault::After] {
        let store = Store {
            fault,
            ..Store::default()
        };
        let stream = StreamId::new([5; 16]);
        let mut ledger = open(store.clone());
        assert!(matches!(
            ledger.append(stream, None, event(7)),
            Err(HostError::Indeterminate)
        ));
        assert!(matches!(
            ledger.read(stream, EventSequence(0), 1),
            Err(HostError::Fenced)
        ));
        assert!(ledger.signals().fenced);
        let mut recovered = DurableLedger::open(
            Store {
                fault: Fault::None,
                ..store.clone()
            },
            Limits::default(),
            CheckpointBudgetV1::default(),
        )
        .unwrap();
        let result = recovered.append(stream, None, event(7)).unwrap();
        assert_eq!(
            result,
            if matches!(fault, Fault::After) {
                AppendOutcomeV1::Replayed(EventSequence(0))
            } else {
                AppendOutcomeV1::Appended(EventSequence(0))
            }
        );
    }
}

#[test]
fn weak_or_misattributed_receipts_never_release_success() {
    for fault in [Fault::Weak, Fault::WrongRevision] {
        let mut ledger = open(Store {
            fault,
            ..Store::default()
        });
        assert!(matches!(
            ledger.append(StreamId::new([5; 16]), None, event(7)),
            Err(HostError::InvalidReceipt)
        ));
        assert!(matches!(
            ledger.read(StreamId::new([5; 16]), EventSequence(0), 1),
            Err(HostError::Fenced)
        ));
    }
}

#[test]
fn encoding_refusal_does_not_publish_candidate_or_offer_a_store_commit() {
    let store = Store::default();
    let stream = StreamId::new([5; 16]);
    let mut ledger = DurableLedger::create_new(
        store.clone(),
        Limits::default(),
        CheckpointBudgetV1 {
            max_bytes: 64,
            ..CheckpointBudgetV1::default()
        },
    )
    .unwrap();
    assert!(matches!(
        ledger.append(stream, None, event(7)),
        Err(HostError::Checkpoint(_))
    ));
    assert_eq!(*store.calls.lock().unwrap(), 1);
    assert!(matches!(
        ledger.read(stream, EventSequence(0), 1),
        Err(HostError::Domain(_))
    ));
    assert!(!ledger.signals().fenced);
}

#[test]
fn storage_revision_must_match_the_domain_envelope_before_recovery() {
    let store = Store::default();
    let stream = StreamId::new([5; 16]);
    open(store.clone()).append(stream, None, event(7)).unwrap();
    store.state.lock().unwrap().as_mut().unwrap().revision = Revision(99);
    assert!(matches!(
        DurableLedger::open(store, Limits::default(), CheckpointBudgetV1::default()),
        Err(HostError::InvalidRecovery)
    ));
}

#[test]
fn creation_and_recovery_never_silently_reset_missing_or_existing_state() {
    let store = Store::default();
    assert!(matches!(
        DurableLedger::open(
            store.clone(),
            Limits::default(),
            CheckpointBudgetV1::default()
        ),
        Err(HostError::MissingState)
    ));
    let _ledger = open(store.clone());
    assert!(matches!(
        DurableLedger::create_new(store, Limits::default(), CheckpointBudgetV1::default()),
        Err(HostError::AlreadyInitialized)
    ));
}

#[test]
fn weak_recovery_cannot_turn_a_volatile_commit_into_a_durable_replay() {
    let store = Store {
        fault: Fault::Weak,
        ..Store::default()
    };
    let mut ledger = open(store.clone());
    assert!(
        ledger
            .append(StreamId::new([5; 16]), None, event(7))
            .is_err()
    );
    assert!(matches!(
        DurableLedger::open(store, Limits::default(), CheckpointBudgetV1::default()),
        Err(HostError::InvalidRecovery)
    ));
}

#[test]
fn disabled_observation_changes_no_domain_or_persistence_result() {
    let first = Store::default();
    let second = Store::default();
    let mut observed = open(first.clone());
    let mut disabled = open(second.clone());
    disabled.set_observation(false);
    let before = disabled.signals();
    let stream = StreamId::new([5; 16]);
    let consumer = ConsumerId::new([6; 16]);
    assert_eq!(
        observed.append(stream, None, event(7)).unwrap(),
        disabled.append(stream, None, event(7)).unwrap()
    );
    assert_eq!(
        observed
            .acknowledge(consumer, stream, EventSequence(0))
            .unwrap(),
        disabled
            .acknowledge(consumer, stream, EventSequence(0))
            .unwrap()
    );
    assert_eq!(
        observed.read(stream, EventSequence(0), 1).unwrap(),
        disabled.read(stream, EventSequence(0), 1).unwrap()
    );
    assert_eq!(
        observed.consumer_cursor(consumer, stream).unwrap(),
        disabled.consumer_cursor(consumer, stream).unwrap()
    );
    assert_eq!(*first.state.lock().unwrap(), *second.state.lock().unwrap());
    assert_eq!(observed.signals().offers, [1, 1, 1, 1, 0]);
    assert_eq!(disabled.signals().offers, [0; 5]);
    assert_eq!(disabled.signals().commits, before.commits);
    assert_eq!(disabled.signals().commit_time, before.commit_time);
    assert_eq!(disabled.signals().usage, observed.signals().usage);
}
