//! Trusted in-process persistence boundary. No credentials or network listener.
#![forbid(unsafe_code)]

use hermes_ledger::{
    AppendOutcomeV1, CompactionOutcomeV1, ConsumerCursorV1, ConsumerId, Engine, EventEnvelopeV1,
    EventSequence, Limits, ReadBatchV1, RefusalV1, StreamId, StreamObservationV1, UsageV1,
    persistence::{CheckpointBudgetV1, CheckpointErrorV1},
};
use keystone_observe::SaturatingCounter;
use keystone_storage::{
    AcknowledgementStrength, CheckpointStore, CommitGate, CommitGateFailure, CommitGateRefusal,
    Revision, StoreRefusal,
};
use std::time::{Duration, Instant};

const KEY: &str = "hermes-ledger-v1";
const ENVELOPE: &[u8] = b"HERMESST\x01";
const HEADER_BYTES: usize = 17;

/// Maximum opaque value offered to storage, including the host envelope.
pub fn storage_value_limit(budget: CheckpointBudgetV1) -> Option<usize> {
    budget.max_bytes.checked_add(HEADER_BYTES)
}

pub enum HostError<E> {
    Domain(RefusalV1),
    Checkpoint(CheckpointErrorV1),
    Store(E),
    StoreRefused(StoreRefusal),
    Indeterminate,
    InvalidReceipt,
    InvalidRecovery,
    MissingState,
    AlreadyInitialized,
    RevisionExhausted,
    Fenced,
}

impl<E> std::fmt::Debug for HostError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Domain(error) => f.debug_tuple("Domain").field(error).finish(),
            Self::Checkpoint(error) => f.debug_tuple("Checkpoint").field(error).finish(),
            Self::Store(_) => f.write_str("Store(<redacted>)"),
            Self::StoreRefused(error) => f.debug_tuple("StoreRefused").field(error).finish(),
            Self::Indeterminate => f.write_str("Indeterminate"),
            Self::InvalidReceipt => f.write_str("InvalidReceipt"),
            Self::InvalidRecovery => f.write_str("InvalidRecovery"),
            Self::MissingState => f.write_str("MissingState"),
            Self::AlreadyInitialized => f.write_str("AlreadyInitialized"),
            Self::RevisionExhausted => f.write_str("RevisionExhausted"),
            Self::Fenced => f.write_str("Fenced"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum OperationV1 {
    Append = 0,
    Acknowledge = 1,
    Read = 2,
    Cursor = 3,
    Compact = 4,
}
impl OperationV1 {
    pub const fn index(self) -> usize {
        self as usize
    }
}

/// Fixed cardinality, content-free, non-authoritative measurements. Commit
/// counts/time include initial creation. Usage is the last confirmed state;
/// it is not a live-state or recovery claim after the instance becomes fenced.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SignalsV1 {
    pub offers: [u64; 5],
    pub residence: [Duration; 5],
    pub domain_refusals: u64,
    pub admission_refusals: u64,
    pub replays: u64,
    pub conflicts: u64,
    pub commits: u64,
    pub commit_failures: u64,
    pub commit_time: Duration,
    pub usage: UsageV1,
    pub last_checkpoint_bytes: usize,
    pub fenced: bool,
    pub saturated: bool,
}

fn increment(value: &mut u64, saturated: &mut bool) {
    let mut counter = SaturatingCounter::from_value(*value);
    let _ = counter.increment();
    *value = counter.value();
    *saturated |= counter.is_saturated();
}
// Hermes preserves the last exact duration when the next sample cannot be
// represented. Keystone's DurationAccumulator deliberately clamps to MAX, so
// this distinct consumer law remains local rather than being normalized away.
fn add_time(value: &mut Duration, elapsed: Duration, saturated: &mut bool) {
    if let Some(next) = value.checked_add(elapsed) {
        *value = next;
    } else {
        *saturated = true;
    }
}

pub struct DurableLedger<S: CheckpointStore> {
    engine: Option<Engine>,
    store: S,
    commit_gate: CommitGate,
    budget: CheckpointBudgetV1,
    signals: SignalsV1,
    observing: bool,
}

impl<S: CheckpointStore> DurableLedger<S> {
    /// Explicit privileged initialization. An empty checkpoint is durably
    /// published before returning. Existing state is never replaced here.
    /// The host selects the store and establishes its exclusive ownership;
    /// identifiers or store access alone are not authenticated caller authority.
    pub fn create_new(
        store: S,
        limits: Limits,
        budget: CheckpointBudgetV1,
    ) -> Result<Self, HostError<S::Error>> {
        if store.load(KEY).map_err(HostError::Store)?.is_some() {
            return Err(HostError::AlreadyInitialized);
        }
        let engine =
            Engine::try_new(limits).map_err(|_| HostError::Checkpoint(CheckpointErrorV1::Limit))?;
        let mut result = Self {
            engine: Some(engine.clone()),
            store,
            commit_gate: CommitGate::new(None),
            budget,
            signals: SignalsV1::default(),
            observing: true,
        };
        result.publish(engine)?;
        Ok(result)
    }

    /// Recovery only: absence, weak load evidence or mismatched storage/domain
    /// revisions refuse. Missing state never becomes a fresh empty ledger.
    pub fn open(
        store: S,
        limits: Limits,
        budget: CheckpointBudgetV1,
    ) -> Result<Self, HostError<S::Error>> {
        let checkpoint = store
            .load(KEY)
            .map_err(HostError::Store)?
            .ok_or(HostError::MissingState)?;
        if checkpoint.strength != AcknowledgementStrength::Durable
            || checkpoint.bytes.len()
                > budget
                    .max_bytes
                    .checked_add(HEADER_BYTES)
                    .ok_or(HostError::InvalidRecovery)?
            || !checkpoint.bytes.starts_with(ENVELOPE)
        {
            return Err(HostError::InvalidRecovery);
        }
        let encoded_revision = checkpoint
            .bytes
            .get(ENVELOPE.len()..HEADER_BYTES)
            .ok_or(HostError::InvalidRecovery)?;
        let revision = Revision(u64::from_be_bytes(
            encoded_revision
                .try_into()
                .map_err(|_| HostError::InvalidRecovery)?,
        ));
        if revision != checkpoint.revision {
            return Err(HostError::InvalidRecovery);
        }
        let bytes = checkpoint
            .bytes
            .get(HEADER_BYTES..)
            .ok_or(HostError::InvalidRecovery)?;
        let engine =
            Engine::recover_checkpoint(bytes, limits, budget).map_err(HostError::Checkpoint)?;
        let signals = SignalsV1 {
            usage: engine.usage(),
            last_checkpoint_bytes: checkpoint.bytes.len(),
            ..SignalsV1::default()
        };
        Ok(Self {
            engine: Some(engine),
            store,
            commit_gate: CommitGate::new(Some(revision)),
            budget,
            signals,
            observing: true,
        })
    }

    pub fn signals(&self) -> SignalsV1 {
        self.signals
    }
    /// Last confirmed storage position, not current-state evidence after fencing.
    pub fn revision(&self) -> Option<Revision> {
        self.commit_gate.confirmed_revision()
    }
    pub fn set_observation(&mut self, enabled: bool) {
        self.observing = enabled;
    }

    /// Transfers the exclusively owned persistence adapter to a new host. This
    /// does not export live engine state; callers must reopen from the last
    /// durably acknowledged checkpoint.
    pub fn into_store(self) -> S {
        self.store
    }

    fn begin(&mut self, operation: OperationV1) -> Option<Instant> {
        if !self.observing {
            return None;
        }
        increment(
            &mut self.signals.offers[operation.index()],
            &mut self.signals.saturated,
        );
        Some(Instant::now())
    }
    fn finish<T>(
        &mut self,
        operation: OperationV1,
        started: Option<Instant>,
        result: &Result<T, HostError<S::Error>>,
    ) {
        if let Some(started) = started {
            add_time(
                &mut self.signals.residence[operation.index()],
                started.elapsed(),
                &mut self.signals.saturated,
            );
            match result {
                Err(HostError::Domain(_)) => increment(
                    &mut self.signals.domain_refusals,
                    &mut self.signals.saturated,
                ),
                Err(HostError::Checkpoint(_) | HostError::RevisionExhausted) => increment(
                    &mut self.signals.admission_refusals,
                    &mut self.signals.saturated,
                ),
                _ => (),
            }
        }
    }

    fn publish(&mut self, candidate: Engine) -> Result<(), HostError<S::Error>> {
        let revision = self
            .commit_gate
            .next_revision()
            .map_err(|error| match error {
                CommitGateRefusal::Fenced => HostError::Fenced,
                CommitGateRefusal::RevisionExhausted => HostError::RevisionExhausted,
            })?;
        // Pure preparation: encoding refusal cannot publish or fence live state.
        let checkpoint = candidate
            .checkpoint(self.budget)
            .map_err(HostError::Checkpoint)?;
        let length = checkpoint
            .len()
            .checked_add(HEADER_BYTES)
            .ok_or(HostError::Checkpoint(CheckpointErrorV1::Limit))?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| HostError::Checkpoint(CheckpointErrorV1::Capacity))?;
        bytes.extend_from_slice(ENVELOPE);
        bytes.extend_from_slice(&revision.0.to_be_bytes());
        bytes.extend_from_slice(&checkpoint);
        let started = self.observing.then(Instant::now);
        let result =
            self.store
                .compare_exchange(KEY, self.commit_gate.confirmed_revision(), &bytes);
        if let Some(started) = started {
            add_time(
                &mut self.signals.commit_time,
                started.elapsed(),
                &mut self.signals.saturated,
            );
        }
        let error = match self.commit_gate.resolve(revision, result) {
            Ok(()) => {
                self.signals.usage = candidate.usage();
                self.signals.last_checkpoint_bytes = bytes.len();
                self.engine = Some(candidate);
                if self.observing {
                    increment(&mut self.signals.commits, &mut self.signals.saturated);
                }
                return Ok(());
            }
            Err(CommitGateFailure::Store(error)) => HostError::Store(error),
            Err(CommitGateFailure::Refused(error)) => HostError::StoreRefused(error),
            Err(CommitGateFailure::Indeterminate) => HostError::Indeterminate,
            Err(CommitGateFailure::InvalidReceipt) => HostError::InvalidReceipt,
        };
        // Once offered to storage, failure/weak evidence cannot justify either
        // the old or candidate state. No retry, success or further read escapes.
        self.engine = None;
        self.signals.fenced = self.commit_gate.is_fenced();
        if self.observing {
            increment(
                &mut self.signals.commit_failures,
                &mut self.signals.saturated,
            );
        }
        Err(error)
    }

    pub fn append(
        &mut self,
        stream: StreamId,
        predecessor: Option<EventSequence>,
        envelope: EventEnvelopeV1,
    ) -> Result<AppendOutcomeV1, HostError<S::Error>> {
        let started = self.begin(OperationV1::Append);
        let result = (|| {
            let mut candidate = self.engine.as_ref().ok_or(HostError::Fenced)?.clone();
            let outcome = candidate
                .append(stream, predecessor, envelope)
                .map_err(HostError::Domain)?;
            match outcome {
                AppendOutcomeV1::Appended(_) => self.publish(candidate)?,
                AppendOutcomeV1::Replayed(_) | AppendOutcomeV1::ReplayHistoryLost(_)
                    if self.observing =>
                {
                    increment(&mut self.signals.replays, &mut self.signals.saturated)
                }
                AppendOutcomeV1::Conflict if self.observing => {
                    increment(&mut self.signals.conflicts, &mut self.signals.saturated)
                }
                _ => (),
            }
            Ok(outcome)
        })();
        self.finish(OperationV1::Append, started, &result);
        result
    }

    pub fn acknowledge(
        &mut self,
        consumer: ConsumerId,
        stream: StreamId,
        through: EventSequence,
    ) -> Result<ConsumerCursorV1, HostError<S::Error>> {
        let started = self.begin(OperationV1::Acknowledge);
        let result = (|| {
            let engine = self.engine.as_ref().ok_or(HostError::Fenced)?;
            let before = engine.consumer_cursor(consumer, stream);
            let mut candidate = engine.clone();
            let cursor = candidate
                .acknowledge(consumer, stream, through)
                .map_err(HostError::Domain)?;
            if before != Some(cursor) {
                self.publish(candidate)?;
            }
            Ok(cursor)
        })();
        self.finish(OperationV1::Acknowledge, started, &result);
        result
    }

    pub fn compact_through(
        &mut self,
        stream: StreamId,
        through: EventSequence,
    ) -> Result<CompactionOutcomeV1, HostError<S::Error>> {
        let started = self.begin(OperationV1::Compact);
        let result = (|| {
            let mut candidate = self.engine.as_ref().ok_or(HostError::Fenced)?.clone();
            let outcome = candidate
                .compact_through(stream, through)
                .map_err(HostError::Domain)?;
            if matches!(outcome, CompactionOutcomeV1::Compacted { .. }) {
                self.publish(candidate)?;
            }
            Ok(outcome)
        })();
        self.finish(OperationV1::Compact, started, &result);
        result
    }

    pub fn read(
        &mut self,
        stream: StreamId,
        start: EventSequence,
        limit: usize,
    ) -> Result<ReadBatchV1, HostError<S::Error>> {
        let started = self.begin(OperationV1::Read);
        let result = self
            .engine
            .as_ref()
            .ok_or(HostError::Fenced)
            .and_then(|engine| engine.read(stream, start, limit).map_err(HostError::Domain));
        self.finish(OperationV1::Read, started, &result);
        result
    }

    pub fn consumer_cursor(
        &mut self,
        consumer: ConsumerId,
        stream: StreamId,
    ) -> Result<Option<ConsumerCursorV1>, HostError<S::Error>> {
        let started = self.begin(OperationV1::Cursor);
        let result = self
            .engine
            .as_ref()
            .ok_or(HostError::Fenced)
            .map(|engine| engine.consumer_cursor(consumer, stream));
        self.finish(OperationV1::Cursor, started, &result);
        result
    }

    /// Content-free stream health. This is deliberately not counted as a
    /// ledger operation so observation cannot change the snapshot it reads.
    pub fn stream_observation(
        &self,
        stream: StreamId,
    ) -> Result<Option<StreamObservationV1>, HostError<S::Error>> {
        self.engine
            .as_ref()
            .ok_or(HostError::Fenced)
            .map(|engine| engine.stream_observation(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn observation_overflow_saturates_without_wrapping_or_changing_control_flow() {
        let mut counter = u64::MAX;
        let mut saturated = false;
        increment(&mut counter, &mut saturated);
        assert_eq!(counter, u64::MAX);
        assert!(saturated);
        let mut elapsed = Duration::MAX;
        add_time(&mut elapsed, Duration::from_nanos(1), &mut saturated);
        assert_eq!(elapsed, Duration::MAX);
        let last_exact = Duration::MAX - Duration::from_nanos(1);
        let mut elapsed = last_exact;
        saturated = false;
        add_time(&mut elapsed, Duration::from_nanos(2), &mut saturated);
        assert_eq!(elapsed, last_exact);
        assert!(saturated);
        assert!(
            !format!("{:?}", HostError::Store("private-store-token"))
                .contains("private-store-token")
        );
    }
}
