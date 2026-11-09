//! Deterministic bounded volatile reference ledger.
//!
//! Hermes performs no I/O. Durable stores must implement the same append and
//! cursor laws and return attributed commit evidence.

use std::collections::BTreeMap;

pub const PROJECT_STAGE: &str = "volatile-reference-ledger";

macro_rules! id {
    ($name:ident) => {
        #[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name([u8; 16]);

        impl $name {
            pub const fn new(value: [u8; 16]) -> Self {
                Self(value)
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredEventV1 {
    pub sequence: EventSequence,
    pub envelope: EventEnvelopeV1,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadBatchV1 {
    pub events: Vec<StoredEventV1>,
    pub published_edge: Option<EventSequence>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConsumerCursorV1 {
    pub consumer: ConsumerId,
    pub stream: StreamId,
    pub next_sequence: EventSequence,
}

#[derive(Clone)]
struct AppendRecord {
    stream: StreamId,
    envelope: EventEnvelopeV1,
    sequence: EventSequence,
}

pub struct Engine {
    limits: Limits,
    streams: BTreeMap<StreamId, Vec<StoredEventV1>>,
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
            return if existing.stream == stream && existing.envelope == envelope {
                Ok(AppendOutcomeV1::Replayed(existing.sequence))
            } else {
                Ok(AppendOutcomeV1::Conflict)
            };
        }
        let actual = self
            .streams
            .get(&stream)
            .and_then(|events| events.last())
            .map(|event| event.sequence);
        if actual != expected_predecessor {
            return Err(RefusalV1::PredecessorMismatch {
                expected: expected_predecessor,
                actual,
            });
        }
        if !self.streams.contains_key(&stream) && self.streams.len() >= self.limits.max_streams {
            return Err(RefusalV1::StreamLimit);
        }
        let event_count = self.streams.get(&stream).map_or(0, Vec::len);
        if event_count >= self.limits.max_events_per_stream {
            return Err(RefusalV1::StreamEventLimit);
        }
        if self.appends.len() >= self.limits.max_idempotency_records {
            return Err(RefusalV1::IdempotencyLimit);
        }
        let sequence =
            EventSequence(u64::try_from(event_count).map_err(|_| RefusalV1::SequenceExhausted)?);
        self.streams.entry(stream).or_default().push(StoredEventV1 {
            sequence,
            envelope: envelope.clone(),
        });
        self.appends.insert(
            key,
            AppendRecord {
                stream,
                envelope,
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
        let events = self.streams.get(&stream).ok_or(RefusalV1::UnknownStream)?;
        let published_edge = events.last().map(|event| event.sequence);
        let events = events
            .iter()
            .filter(|event| event.sequence >= start)
            .take(limit)
            .cloned()
            .collect();
        Ok(ReadBatchV1 {
            events,
            published_edge,
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
            .and_then(|events| events.last())
            .map(|event| event.sequence)
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
        } else if self.cursors.len() >= self.limits.max_consumers {
            return Err(RefusalV1::ConsumerLimit);
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
