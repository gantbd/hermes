//! Authenticated, stream-authorized application boundary for Hermes.
//!
//! Authentication adapters provide volatile subjects. Authorization adapters
//! decide actions against service/stream resources. Producer and consumer IDs
//! are never consulted as caller identity or authority.

use hermes_host::{DurableLedger, HostError};
use hermes_ledger as core;
use hermes_protocol as protocol;
use keystone_storage::CheckpointStore;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct AuthenticatedSessionV1 {
    pub subject: [u8; 16],
}

impl std::fmt::Debug for AuthenticatedSessionV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthenticatedSessionV1")
            .field("subject", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthorizationActionV1 {
    Append,
    Read,
    ObserveCursor,
    Acknowledge,
    Compact,
    Observe,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthorizationResourceV1 {
    Service,
    Stream(protocol::StreamIdV1),
}

pub trait Authorizer {
    type Error;
    fn authorize(
        &mut self,
        subject: [u8; 16],
        action: AuthorizationActionV1,
        resource: AuthorizationResourceV1,
    ) -> Result<bool, Self::Error>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServiceLimitsV1 {
    pub max_sessions: usize,
    pub max_read_batch: usize,
}

impl Default for ServiceLimitsV1 {
    fn default() -> Self {
        Self {
            max_sessions: 1_024,
            max_read_batch: 1_024,
        }
    }
}

#[derive(Debug)]
pub enum ServiceErrorV1<S, A> {
    Host(HostError<S>),
    Authorization(A),
    Representation,
}

pub struct Service<S: CheckpointStore, A: Authorizer> {
    ledger: DurableLedger<S>,
    authorizer: A,
    limits: ServiceLimitsV1,
    sessions: BTreeMap<protocol::SessionHandleV1, AuthenticatedSessionV1>,
    next_session: u64,
}

impl<S: CheckpointStore, A: Authorizer> Service<S, A> {
    pub fn new(ledger: DurableLedger<S>, authorizer: A, limits: ServiceLimitsV1) -> Option<Self> {
        if limits.max_sessions == 0 || limits.max_read_batch == 0 {
            return None;
        }
        Some(Self {
            ledger,
            authorizer,
            limits,
            sessions: BTreeMap::new(),
            next_session: 0,
        })
    }

    pub fn open_session(
        &mut self,
        facts: AuthenticatedSessionV1,
    ) -> Option<protocol::SessionHandleV1> {
        if self.sessions.len() == self.limits.max_sessions || self.next_session == u64::MAX {
            return None;
        }
        let handle = protocol::SessionHandleV1(self.next_session);
        self.next_session += 1;
        self.sessions.insert(handle, facts);
        Some(handle)
    }

    pub fn close_session(&mut self, session: protocol::SessionHandleV1) -> bool {
        self.sessions.remove(&session).is_some()
    }

    pub fn execute(
        &mut self,
        session: protocol::SessionHandleV1,
        command: protocol::CommandV1,
    ) -> Result<protocol::ReplyV1, ServiceErrorV1<S::Error, A::Error>> {
        let Some(facts) = self.sessions.get(&session).copied() else {
            return Ok(refused(protocol::RefusalV1::UnknownSession));
        };
        let (action, resource) = authorization_scope(&command);
        if !self
            .authorizer
            .authorize(facts.subject, action, resource)
            .map_err(ServiceErrorV1::Authorization)?
        {
            return Ok(refused(protocol::RefusalV1::NotAuthorized));
        }
        match command {
            protocol::CommandV1::Append {
                stream,
                expected_predecessor,
                envelope,
            } => match self.ledger.append(
                stream_core(stream),
                expected_predecessor.map(sequence_core),
                envelope_core(envelope),
            ) {
                Ok(value) => Ok(accepted(protocol::OutcomeV1::Append(append(value)))),
                Err(HostError::Domain(value)) => Ok(refused(domain(value))),
                Err(value) => Err(ServiceErrorV1::Host(value)),
            },
            protocol::CommandV1::Read {
                stream,
                start,
                limit,
            } => {
                let Ok(limit) = usize::try_from(limit) else {
                    return Ok(refused(protocol::RefusalV1::InvalidReadLimit));
                };
                if limit == 0 || limit > self.limits.max_read_batch {
                    return Ok(refused(protocol::RefusalV1::InvalidReadLimit));
                }
                match self
                    .ledger
                    .read(stream_core(stream), sequence_core(start), limit)
                {
                    Ok(value) => Ok(accepted(protocol::OutcomeV1::Read(read_batch(value)))),
                    Err(HostError::Domain(value)) => Ok(refused(domain(value))),
                    Err(value) => Err(ServiceErrorV1::Host(value)),
                }
            }
            protocol::CommandV1::Acknowledge {
                consumer,
                stream,
                through,
            } => match self.ledger.acknowledge(
                consumer_core(consumer),
                stream_core(stream),
                sequence_core(through),
            ) {
                Ok(value) => Ok(accepted(protocol::OutcomeV1::Acknowledged(cursor(value)))),
                Err(HostError::Domain(value)) => Ok(refused(domain(value))),
                Err(value) => Err(ServiceErrorV1::Host(value)),
            },
            protocol::CommandV1::Cursor { consumer, stream } => {
                match self
                    .ledger
                    .consumer_cursor(consumer_core(consumer), stream_core(stream))
                {
                    Ok(value) => Ok(accepted(protocol::OutcomeV1::Cursor(value.map(cursor)))),
                    Err(HostError::Domain(value)) => Ok(refused(domain(value))),
                    Err(value) => Err(ServiceErrorV1::Host(value)),
                }
            }
            protocol::CommandV1::Compact { stream, through } => {
                match self
                    .ledger
                    .compact_through(stream_core(stream), sequence_core(through))
                {
                    Ok(value) => Ok(accepted(protocol::OutcomeV1::Compacted(compaction(value)?))),
                    Err(HostError::Domain(value)) => Ok(refused(domain(value))),
                    Err(value) => Err(ServiceErrorV1::Host(value)),
                }
            }
            protocol::CommandV1::Status => {
                let signals = self.ledger.signals();
                Ok(accepted(protocol::OutcomeV1::Status(protocol::StatusV1 {
                    storage_revision: self.ledger.revision().map(|value| value.0),
                    fenced: signals.fenced,
                })))
            }
            protocol::CommandV1::Usage => {
                let signals = self.ledger.signals();
                Ok(accepted(protocol::OutcomeV1::Usage(usage(
                    signals.usage,
                    signals.saturated,
                )?)))
            }
            protocol::CommandV1::StreamObservation { stream } => {
                let value = self
                    .ledger
                    .stream_observation(stream_core(stream))
                    .map_err(ServiceErrorV1::Host)?;
                Ok(accepted(protocol::OutcomeV1::StreamObservation(
                    value.map(stream_observation).transpose()?,
                )))
            }
            protocol::CommandV1::Signals => {
                let value = operational_signals(self.ledger.signals())?;
                Ok(accepted(protocol::OutcomeV1::Signals(value)))
            }
        }
    }

    pub fn into_parts(self) -> (DurableLedger<S>, A) {
        (self.ledger, self.authorizer)
    }
}

fn authorization_scope(
    command: &protocol::CommandV1,
) -> (AuthorizationActionV1, AuthorizationResourceV1) {
    match command {
        protocol::CommandV1::Append { stream, .. } => (
            AuthorizationActionV1::Append,
            AuthorizationResourceV1::Stream(*stream),
        ),
        protocol::CommandV1::Read { stream, .. } => (
            AuthorizationActionV1::Read,
            AuthorizationResourceV1::Stream(*stream),
        ),
        protocol::CommandV1::Cursor { stream, .. } => (
            AuthorizationActionV1::ObserveCursor,
            AuthorizationResourceV1::Stream(*stream),
        ),
        protocol::CommandV1::Acknowledge { stream, .. } => (
            AuthorizationActionV1::Acknowledge,
            AuthorizationResourceV1::Stream(*stream),
        ),
        protocol::CommandV1::Compact { stream, .. } => (
            AuthorizationActionV1::Compact,
            AuthorizationResourceV1::Stream(*stream),
        ),
        protocol::CommandV1::StreamObservation { stream } => (
            AuthorizationActionV1::Observe,
            AuthorizationResourceV1::Stream(*stream),
        ),
        protocol::CommandV1::Status | protocol::CommandV1::Usage | protocol::CommandV1::Signals => {
            (
                AuthorizationActionV1::Observe,
                AuthorizationResourceV1::Service,
            )
        }
    }
}

fn stream_core(value: protocol::StreamIdV1) -> core::StreamId {
    core::StreamId::new(value.0)
}
fn consumer_core(value: protocol::ConsumerIdV1) -> core::ConsumerId {
    core::ConsumerId::new(value.0)
}
fn sequence_core(value: protocol::EventSequenceV1) -> core::EventSequence {
    core::EventSequence(value.0)
}
fn envelope_core(value: protocol::EventEnvelopeV1) -> core::EventEnvelopeV1 {
    core::EventEnvelopeV1::new(
        core::ProducerId::new(value.producer.0),
        core::SchemaId::new(value.schema.0),
        core::IdempotencyKey::new(value.idempotency_key.0),
        core::CorrelationId::new(value.correlation.0),
        value.causation.map(|value| core::CausationId::new(value.0)),
        value.payload,
    )
}
fn envelope(value: core::EventEnvelopeV1) -> protocol::EventEnvelopeV1 {
    protocol::EventEnvelopeV1 {
        producer: protocol::ProducerIdV1(value.producer.into_bytes()),
        schema: protocol::SchemaIdV1(value.schema.into_bytes()),
        idempotency_key: protocol::IdempotencyKeyV1(value.idempotency_key.into_bytes()),
        correlation: protocol::CorrelationIdV1(value.correlation.into_bytes()),
        causation: value
            .causation
            .map(|value| protocol::CausationIdV1(value.into_bytes())),
        payload: value.payload().to_vec(),
    }
}
fn append(value: core::AppendOutcomeV1) -> protocol::AppendOutcomeV1 {
    match value {
        core::AppendOutcomeV1::Appended(value) => {
            protocol::AppendOutcomeV1::Appended(protocol::EventSequenceV1(value.0))
        }
        core::AppendOutcomeV1::Replayed(value) => {
            protocol::AppendOutcomeV1::Replayed(protocol::EventSequenceV1(value.0))
        }
        core::AppendOutcomeV1::ReplayHistoryLost(value) => {
            protocol::AppendOutcomeV1::ReplayHistoryLost(protocol::EventSequenceV1(value.0))
        }
        core::AppendOutcomeV1::Conflict => protocol::AppendOutcomeV1::Conflict,
        core::AppendOutcomeV1::Indeterminate => protocol::AppendOutcomeV1::Indeterminate,
    }
}
fn read_batch(value: core::ReadBatchV1) -> protocol::ReadBatchV1 {
    protocol::ReadBatchV1 {
        events: value
            .events
            .into_iter()
            .map(|value| protocol::StoredEventV1 {
                sequence: protocol::EventSequenceV1(value.sequence.0),
                envelope: envelope(value.envelope),
            })
            .collect(),
        retained_from: protocol::EventSequenceV1(value.retained_from.0),
        published_edge: value
            .published_edge
            .map(|value| protocol::EventSequenceV1(value.0)),
        gap: value.gap.map(|value| match value {
            core::ReadGapV1::BeforeRetention {
                requested,
                retained_from,
            } => protocol::ReadGapV1::BeforeRetention {
                requested: protocol::EventSequenceV1(requested.0),
                retained_from: protocol::EventSequenceV1(retained_from.0),
            },
            core::ReadGapV1::AheadOfPublication {
                requested,
                next_sequence,
            } => protocol::ReadGapV1::AheadOfPublication {
                requested: protocol::EventSequenceV1(requested.0),
                next_sequence: protocol::EventSequenceV1(next_sequence.0),
            },
        }),
    }
}
fn cursor(value: core::ConsumerCursorV1) -> protocol::ConsumerCursorV1 {
    protocol::ConsumerCursorV1 {
        consumer: protocol::ConsumerIdV1(value.consumer.into_bytes()),
        stream: protocol::StreamIdV1(value.stream.into_bytes()),
        next_sequence: protocol::EventSequenceV1(value.next_sequence.0),
    }
}
fn compaction<S, A>(
    value: core::CompactionOutcomeV1,
) -> Result<protocol::CompactionOutcomeV1, ServiceErrorV1<S, A>> {
    Ok(match value {
        core::CompactionOutcomeV1::Compacted {
            removed,
            retained_from,
        } => protocol::CompactionOutcomeV1::Compacted {
            removed: number(removed)?,
            retained_from: protocol::EventSequenceV1(retained_from.0),
        },
        core::CompactionOutcomeV1::Identity { retained_from } => {
            protocol::CompactionOutcomeV1::Identity {
                retained_from: protocol::EventSequenceV1(retained_from.0),
            }
        }
    })
}
fn number<S, A>(value: usize) -> Result<u64, ServiceErrorV1<S, A>> {
    u64::try_from(value).map_err(|_| ServiceErrorV1::Representation)
}
fn usage<S, A>(
    value: core::UsageV1,
    saturated: bool,
) -> Result<protocol::UsageV1, ServiceErrorV1<S, A>> {
    Ok(protocol::UsageV1 {
        streams: number(value.streams)?,
        events: number(value.events)?,
        idempotency_records: number(value.idempotency_records)?,
        compacted_records: number(value.compacted_records)?,
        cursors: number(value.cursors)?,
        saturated,
    })
}
fn stream_observation<S, A>(
    value: core::StreamObservationV1,
) -> Result<protocol::StreamObservationV1, ServiceErrorV1<S, A>> {
    Ok(protocol::StreamObservationV1 {
        retained_from: protocol::EventSequenceV1(value.retained_from.0),
        published_edge: value
            .published_edge
            .map(|value| protocol::EventSequenceV1(value.0)),
        retained_events: number(value.retained_events)?,
        consumer_count: number(value.consumer_count)?,
        stalled_consumers: number(value.stalled_consumers)?,
        max_consumer_backlog: value.max_consumer_backlog,
    })
}
fn operational_signals<S, A>(
    value: hermes_host::SignalsV1,
) -> Result<protocol::OperationalSignalsV1, ServiceErrorV1<S, A>> {
    let mut saturated = value.saturated;
    let residence_nanos = value.residence.map(|duration| {
        u64::try_from(duration.as_nanos()).unwrap_or_else(|_| {
            saturated = true;
            u64::MAX
        })
    });
    let commit_time_nanos = u64::try_from(value.commit_time.as_nanos()).unwrap_or_else(|_| {
        saturated = true;
        u64::MAX
    });
    let last_checkpoint_bytes = u64::try_from(value.last_checkpoint_bytes).unwrap_or_else(|_| {
        saturated = true;
        u64::MAX
    });
    Ok(protocol::OperationalSignalsV1 {
        offers: value.offers,
        residence_nanos,
        domain_refusals: value.domain_refusals,
        admission_refusals: value.admission_refusals,
        replays: value.replays,
        conflicts: value.conflicts,
        commits: value.commits,
        commit_failures: value.commit_failures,
        commit_time_nanos,
        confirmed_usage: usage(value.usage, saturated)?,
        last_checkpoint_bytes,
        fenced: value.fenced,
        saturated,
    })
}
fn domain(value: core::RefusalV1) -> protocol::RefusalV1 {
    match value {
        core::RefusalV1::PayloadLimit => protocol::RefusalV1::PayloadLimit,
        core::RefusalV1::StreamLimit => protocol::RefusalV1::StreamLimit,
        core::RefusalV1::StreamEventLimit => protocol::RefusalV1::StreamEventLimit,
        core::RefusalV1::IdempotencyLimit => protocol::RefusalV1::IdempotencyLimit,
        core::RefusalV1::SequenceExhausted => protocol::RefusalV1::SequenceExhausted,
        core::RefusalV1::PredecessorMismatch { expected, actual } => {
            protocol::RefusalV1::PredecessorMismatch {
                expected: expected.map(|value| protocol::EventSequenceV1(value.0)),
                actual: actual.map(|value| protocol::EventSequenceV1(value.0)),
            }
        }
        core::RefusalV1::UnknownStream => protocol::RefusalV1::UnknownStream,
        core::RefusalV1::InvalidReadLimit => protocol::RefusalV1::InvalidReadLimit,
        core::RefusalV1::ConsumerLimit => protocol::RefusalV1::ConsumerLimit,
        core::RefusalV1::CursorBeyondPublished => protocol::RefusalV1::CursorBeyondPublished,
        core::RefusalV1::CursorRegression => protocol::RefusalV1::CursorRegression,
        core::RefusalV1::CursorBehindRetention { retained_from } => {
            protocol::RefusalV1::CursorBehindRetention {
                retained_from: protocol::EventSequenceV1(retained_from.0),
            }
        }
        core::RefusalV1::CompactionBeyondPublished => {
            protocol::RefusalV1::CompactionBeyondPublished
        }
        core::RefusalV1::CompactionBlocked => protocol::RefusalV1::CompactionBlocked,
    }
}
fn accepted(value: protocol::OutcomeV1) -> protocol::ReplyV1 {
    protocol::ReplyV1::Accepted(value)
}
fn refused(value: protocol::RefusalV1) -> protocol::ReplyV1 {
    protocol::ReplyV1::Refused(value)
}
