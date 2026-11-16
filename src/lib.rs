//! Deterministic bounded volatile reference ledger.
//!
//! Hermes performs no I/O. Durable stores must implement the same append and
//! cursor laws and return attributed commit evidence.

use std::collections::BTreeMap;

pub mod persistence;

pub const PROJECT_STAGE: &str = "volatile-reference-ledger";

macro_rules! id {
    ($name:ident) => {
        #[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name([u8; 16]);

        impl $name {
            pub const fn new(value: [u8; 16]) -> Self {
                Self(value)
            }

            pub const fn into_bytes(self) -> [u8; 16] {
                self.0
            }
        }

        impl std::fmt::Debug for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(concat!(stringify!($name), "(<opaque>)"))
            }
        }
    };
}

id!(StreamId);
id!(ProducerId);
id!(ConsumerId);
id!(SchemaId);
id!(IdempotencyKey);
id!(CorrelationId);
id!(CausationId);

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct EventSequence(pub u64);

#[derive(Clone, Eq, PartialEq)]
pub struct EventEnvelopeV1 {
    pub producer: ProducerId,
    pub schema: SchemaId,
    pub idempotency_key: IdempotencyKey,
    pub correlation: CorrelationId,
    pub causation: Option<CausationId>,
    payload: Vec<u8>,
}

impl EventEnvelopeV1 {
    pub fn new(
        producer: ProducerId,
        schema: SchemaId,
        idempotency_key: IdempotencyKey,
        correlation: CorrelationId,
        causation: Option<CausationId>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            producer,
            schema,
            idempotency_key,
            correlation,
            causation,
            payload,
        }
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

impl std::fmt::Debug for EventEnvelopeV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EventEnvelopeV1")
            .field("producer", &self.producer)
            .field("schema", &self.schema)
            .field("idempotency_key", &self.idempotency_key)
            .field("correlation", &self.correlation)
            .field("causation", &self.causation)
            .field("payload_bytes", &self.payload.len())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Limits {
    pub max_streams: usize,
    pub max_events_per_stream: usize,
    pub max_idempotency_records: usize,
    pub max_consumers: usize,
    pub max_payload_bytes: usize,
    pub max_read_batch: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_streams: 1_024,
            max_events_per_stream: 65_536,
            max_idempotency_records: 65_536,
            max_consumers: 4_096,
            max_payload_bytes: 1_024 * 1_024,
            max_read_batch: 1_024,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LimitsError {
    Disabled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AppendOutcomeV1 {
    Appended(EventSequence),
    Replayed(EventSequence),
    /// The producer key is retained, but its compacted canonical envelope is
    /// no longer available to prove replay versus conflict.
    ReplayHistoryLost(EventSequence),
    Conflict,
    Indeterminate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RefusalV1 {
    PayloadLimit,
    StreamLimit,
    StreamEventLimit,
    IdempotencyLimit,
    SequenceExhausted,
    PredecessorMismatch {
        expected: Option<EventSequence>,
        actual: Option<EventSequence>,
    },
    UnknownStream,
    InvalidReadLimit,
    ConsumerLimit,
    CursorBeyondPublished,
    CursorRegression,
    CursorBehindRetention {
        retained_from: EventSequence,
    },
    CompactionBeyondPublished,
    CompactionBlocked,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredEventV1 {
    pub sequence: EventSequence,
    pub envelope: EventEnvelopeV1,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadBatchV1 {
    pub events: Vec<StoredEventV1>,
    pub retained_from: EventSequence,
    pub published_edge: Option<EventSequence>,
    pub gap: Option<ReadGapV1>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadGapV1 {
    BeforeRetention {
        requested: EventSequence,
        retained_from: EventSequence,
    },
    AheadOfPublication {
        requested: EventSequence,
        next_sequence: EventSequence,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompactionOutcomeV1 {
    Compacted {
        removed: usize,
        retained_from: EventSequence,
    },
    Identity {
        retained_from: EventSequence,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConsumerCursorV1 {
    pub consumer: ConsumerId,
    pub stream: StreamId,
    pub next_sequence: EventSequence,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UsageV1 {
    pub streams: usize,
    pub events: usize,
    pub idempotency_records: usize,
    pub compacted_records: usize,
    pub cursors: usize,
}

/// Fixed-cardinality, content-free stream health. Consumer identities and
/// event contents never cross this observation boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamObservationV1 {
    pub retained_from: EventSequence,
    pub published_edge: Option<EventSequence>,
    pub retained_events: usize,
    pub consumer_count: usize,
    pub stalled_consumers: usize,
    pub max_consumer_backlog: u64,
}

#[derive(Clone)]
struct AppendRecord {
    stream: StreamId,
    envelope: Option<EventEnvelopeV1>,
    sequence: EventSequence,
}

#[derive(Clone)]
struct StreamState {
    retained_from: EventSequence,
    next_sequence: EventSequence,
    events: Vec<StoredEventV1>,
}

#[derive(Clone)]
pub struct Engine {
    limits: Limits,
    streams: BTreeMap<StreamId, StreamState>,
    appends: BTreeMap<(ProducerId, IdempotencyKey), AppendRecord>,
    cursors: BTreeMap<(ConsumerId, StreamId), ConsumerCursorV1>,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Engine")
            .field("stream_count", &self.streams.len())
            .field("append_count", &self.appends.len())
            .field("cursor_count", &self.cursors.len())
            .finish()
    }
}

impl Engine {
    /// Fixed-cardinality observations, never authority or commit evidence.
    pub fn usage(&self) -> UsageV1 {
        UsageV1 {
            streams: self.streams.len(),
            events: self
                .streams
                .values()
                .map(|stream| stream.events.len())
                .sum(),
            idempotency_records: self.appends.len(),
            compacted_records: self
                .appends
                .values()
                .filter(|record| record.envelope.is_none())
                .count(),
            cursors: self.cursors.len(),
        }
    }

    /// Observation only; consumer IDs are not authentication or authority.
    pub fn consumer_cursor(
        &self,
        consumer: ConsumerId,
        stream: StreamId,
    ) -> Option<ConsumerCursorV1> {
        self.cursors.get(&(consumer, stream)).copied()
    }

    /// Observation only; no consumer identity or event content is returned.
    pub fn stream_observation(&self, stream: StreamId) -> Option<StreamObservationV1> {
        let state = self.streams.get(&stream)?;
        let published_edge = state.next_sequence.0.checked_sub(1).map(EventSequence);
        let mut consumer_count = 0usize;
        let mut stalled_consumers = 0usize;
        let mut max_consumer_backlog = 0u64;
        for cursor in self
            .cursors
            .values()
            .filter(|cursor| cursor.stream == stream)
        {
            consumer_count = consumer_count.saturating_add(1);
            let backlog = state.next_sequence.0.saturating_sub(cursor.next_sequence.0);
            if backlog > 0 {
                stalled_consumers = stalled_consumers.saturating_add(1);
            }
            max_consumer_backlog = max_consumer_backlog.max(backlog);
        }
        Some(StreamObservationV1 {
            retained_from: state.retained_from,
            published_edge,
            retained_events: state.events.len(),
            consumer_count,
            stalled_consumers,
            max_consumer_backlog,
        })
    }

    pub fn try_new(limits: Limits) -> Result<Self, LimitsError> {
        if limits.max_streams == 0
            || limits.max_events_per_stream == 0
            || limits.max_idempotency_records == 0
            || limits.max_consumers == 0
            || limits.max_payload_bytes == 0
            || limits.max_read_batch == 0
        {
            return Err(LimitsError::Disabled);
        }
        Ok(Self {
            limits,
            streams: BTreeMap::new(),
            appends: BTreeMap::new(),
            cursors: BTreeMap::new(),
        })
    }

    pub fn append(
        &mut self,
        stream: StreamId,
        expected_predecessor: Option<EventSequence>,
        envelope: EventEnvelopeV1,
    ) -> Result<AppendOutcomeV1, RefusalV1> {
        if envelope.payload.len() > self.limits.max_payload_bytes {
            return Err(RefusalV1::PayloadLimit);
        }
        let key = (envelope.producer, envelope.idempotency_key);
        if let Some(existing) = self.appends.get(&key) {
            return match &existing.envelope {
                Some(retained) if existing.stream == stream && *retained == envelope => {
                    Ok(AppendOutcomeV1::Replayed(existing.sequence))
                }
                Some(_) => Ok(AppendOutcomeV1::Conflict),
                None => Ok(AppendOutcomeV1::ReplayHistoryLost(existing.sequence)),
            };
        }
        let actual = self
            .streams
            .get(&stream)
            .and_then(|state| state.next_sequence.0.checked_sub(1))
            .map(EventSequence);
        if actual != expected_predecessor {
            return Err(RefusalV1::PredecessorMismatch {
                expected: expected_predecessor,
                actual,
            });
        }
        if !self.streams.contains_key(&stream) && self.streams.len() >= self.limits.max_streams {
            return Err(RefusalV1::StreamLimit);
        }
        let event_count = self
            .streams
            .get(&stream)
            .map_or(0, |state| state.events.len());
        if event_count >= self.limits.max_events_per_stream {
            return Err(RefusalV1::StreamEventLimit);
        }
        if self.appends.len() >= self.limits.max_idempotency_records {
            return Err(RefusalV1::IdempotencyLimit);
        }
        let sequence = self
            .streams
            .get(&stream)
            .map_or(EventSequence(0), |state| state.next_sequence);
        let next_sequence = EventSequence(
            sequence
                .0
                .checked_add(1)
                .ok_or(RefusalV1::SequenceExhausted)?,
        );
        let state = self.streams.entry(stream).or_insert_with(|| StreamState {
            retained_from: EventSequence(0),
            next_sequence: EventSequence(0),
            events: Vec::new(),
        });
        state.events.push(StoredEventV1 {
            sequence,
            envelope: envelope.clone(),
        });
        state.next_sequence = next_sequence;
        self.appends.insert(
            key,
            AppendRecord {
                stream,
                envelope: Some(envelope),
                sequence,
            },
        );
        Ok(AppendOutcomeV1::Appended(sequence))
    }

    pub fn read(
        &self,
        stream: StreamId,
        start: EventSequence,
        limit: usize,
    ) -> Result<ReadBatchV1, RefusalV1> {
        if limit == 0 || limit > self.limits.max_read_batch {
            return Err(RefusalV1::InvalidReadLimit);
        }
        let state = self.streams.get(&stream).ok_or(RefusalV1::UnknownStream)?;
        let published_edge = state.next_sequence.0.checked_sub(1).map(EventSequence);
        let (effective_start, gap) = if start < state.retained_from {
            (
                state.retained_from,
                Some(ReadGapV1::BeforeRetention {
                    requested: start,
                    retained_from: state.retained_from,
                }),
            )
        } else if start > state.next_sequence {
            (
                start,
                Some(ReadGapV1::AheadOfPublication {
                    requested: start,
                    next_sequence: state.next_sequence,
                }),
            )
        } else {
            (start, None)
        };
        let events = state
            .events
            .iter()
            .filter(|event| event.sequence >= effective_start)
            .take(limit)
            .cloned()
            .collect();
        Ok(ReadBatchV1 {
            events,
            retained_from: state.retained_from,
            published_edge,
            gap,
        })
    }

    pub fn compact_through(
        &mut self,
        stream: StreamId,
        through: EventSequence,
    ) -> Result<CompactionOutcomeV1, RefusalV1> {
        let state = self.streams.get(&stream).ok_or(RefusalV1::UnknownStream)?;
        let edge = state
            .next_sequence
            .0
            .checked_sub(1)
            .map(EventSequence)
            .ok_or(RefusalV1::UnknownStream)?;
        if through > edge {
            return Err(RefusalV1::CompactionBeyondPublished);
        }
        let retained_from = EventSequence(
            through
                .0
                .checked_add(1)
                .ok_or(RefusalV1::SequenceExhausted)?,
        );
        if retained_from <= state.retained_from {
            return Ok(CompactionOutcomeV1::Identity {
                retained_from: state.retained_from,
            });
        }
        if self
            .cursors
            .values()
            .any(|cursor| cursor.stream == stream && cursor.next_sequence < retained_from)
        {
            return Err(RefusalV1::CompactionBlocked);
        }
        let state = self.streams.get_mut(&stream).expect("checked above");
        let removed = state
            .events
            .iter()
            .take_while(|event| event.sequence < retained_from)
            .count();
        state.events.drain(..removed);
        state.retained_from = retained_from;
        for record in self.appends.values_mut() {
            if record.stream == stream && record.sequence < retained_from {
                record.envelope = None;
            }
        }
        Ok(CompactionOutcomeV1::Compacted {
            removed,
            retained_from,
        })
    }

    pub fn acknowledge(
        &mut self,
        consumer: ConsumerId,
        stream: StreamId,
        through: EventSequence,
    ) -> Result<ConsumerCursorV1, RefusalV1> {
        let edge = self
            .streams
            .get(&stream)
            .and_then(|state| state.next_sequence.0.checked_sub(1))
            .map(EventSequence)
            .ok_or(RefusalV1::UnknownStream)?;
        if through > edge {
            return Err(RefusalV1::CursorBeyondPublished);
        }
        let next = EventSequence(
            through
                .0
                .checked_add(1)
                .ok_or(RefusalV1::SequenceExhausted)?,
        );
        let key = (consumer, stream);
        if let Some(existing) = self.cursors.get(&key) {
            if next < existing.next_sequence {
                return Err(RefusalV1::CursorRegression);
            }
            if next == existing.next_sequence {
                return Ok(*existing);
            }
        } else {
            let retained_from = self.streams[&stream].retained_from;
            if through < retained_from {
                return Err(RefusalV1::CursorBehindRetention { retained_from });
            }
            if self.cursors.len() >= self.limits.max_consumers {
                return Err(RefusalV1::ConsumerLimit);
            }
        }
        let cursor = ConsumerCursorV1 {
            consumer,
            stream,
            next_sequence: next,
        };
        self.cursors.insert(key, cursor);
        Ok(cursor)
    }
}
