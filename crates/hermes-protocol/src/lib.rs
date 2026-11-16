//! Representation-independent values for the authenticated Hermes service.
//!
//! Producer and consumer identifiers are correlation only. They never identify
//! the authenticated caller or confer stream authority.

macro_rules! opaque_id {
    ($name:ident) => {
        #[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
        pub struct $name(pub [u8; 16]);

        impl std::fmt::Debug for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(concat!(stringify!($name), "(<opaque>)"))
            }
        }
    };
}

opaque_id!(StreamIdV1);
opaque_id!(ProducerIdV1);
opaque_id!(ConsumerIdV1);
opaque_id!(SchemaIdV1);
opaque_id!(IdempotencyKeyV1);
opaque_id!(CorrelationIdV1);
opaque_id!(CausationIdV1);

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SessionHandleV1(pub u64);

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct EventSequenceV1(pub u64);

#[derive(Clone, Eq, PartialEq)]
pub struct EventEnvelopeV1 {
    pub producer: ProducerIdV1,
    pub schema: SchemaIdV1,
    pub idempotency_key: IdempotencyKeyV1,
    pub correlation: CorrelationIdV1,
    pub causation: Option<CausationIdV1>,
    pub payload: Vec<u8>,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandV1 {
    Append {
        stream: StreamIdV1,
        expected_predecessor: Option<EventSequenceV1>,
        envelope: EventEnvelopeV1,
    },
    Read {
        stream: StreamIdV1,
        start: EventSequenceV1,
        limit: u64,
    },
    Acknowledge {
        consumer: ConsumerIdV1,
        stream: StreamIdV1,
        through: EventSequenceV1,
    },
    Cursor {
        consumer: ConsumerIdV1,
        stream: StreamIdV1,
    },
    Compact {
        stream: StreamIdV1,
        through: EventSequenceV1,
    },
    Status,
    Usage,
    StreamObservation {
        stream: StreamIdV1,
    },
    Signals,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AppendOutcomeV1 {
    Appended(EventSequenceV1),
    Replayed(EventSequenceV1),
    ReplayHistoryLost(EventSequenceV1),
    Conflict,
    Indeterminate,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredEventV1 {
    pub sequence: EventSequenceV1,
    pub envelope: EventEnvelopeV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadGapV1 {
    BeforeRetention {
        requested: EventSequenceV1,
        retained_from: EventSequenceV1,
    },
    AheadOfPublication {
        requested: EventSequenceV1,
        next_sequence: EventSequenceV1,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadBatchV1 {
    pub events: Vec<StoredEventV1>,
    pub retained_from: EventSequenceV1,
    pub published_edge: Option<EventSequenceV1>,
    pub gap: Option<ReadGapV1>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConsumerCursorV1 {
    pub consumer: ConsumerIdV1,
    pub stream: StreamIdV1,
    pub next_sequence: EventSequenceV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompactionOutcomeV1 {
    Compacted {
        removed: u64,
        retained_from: EventSequenceV1,
    },
    Identity {
        retained_from: EventSequenceV1,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StatusV1 {
    pub storage_revision: Option<u64>,
    pub fenced: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UsageV1 {
    pub streams: u64,
    pub events: u64,
    pub idempotency_records: u64,
    pub compacted_records: u64,
    pub cursors: u64,
    pub saturated: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamObservationV1 {
    pub retained_from: EventSequenceV1,
    pub published_edge: Option<EventSequenceV1>,
    pub retained_events: u64,
    pub consumer_count: u64,
    pub stalled_consumers: u64,
    pub max_consumer_backlog: u64,
}

/// Content-free service measurements. Operation slots are append,
/// acknowledge, read, cursor and compact, in that order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationalSignalsV1 {
    pub offers: [u64; 5],
    pub residence_nanos: [u64; 5],
    pub domain_refusals: u64,
    pub admission_refusals: u64,
    pub replays: u64,
    pub conflicts: u64,
    pub commits: u64,
    pub commit_failures: u64,
    pub commit_time_nanos: u64,
    pub confirmed_usage: UsageV1,
    pub last_checkpoint_bytes: u64,
    pub fenced: bool,
    pub saturated: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutcomeV1 {
    Append(AppendOutcomeV1),
    Read(ReadBatchV1),
    Acknowledged(ConsumerCursorV1),
    Cursor(Option<ConsumerCursorV1>),
    Compacted(CompactionOutcomeV1),
    Status(StatusV1),
    Usage(UsageV1),
    StreamObservation(Option<StreamObservationV1>),
    Signals(OperationalSignalsV1),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RefusalV1 {
    UnknownSession,
    NotAuthorized,
    InvalidReadLimit,
    PayloadLimit,
    StreamLimit,
    StreamEventLimit,
    IdempotencyLimit,
    SequenceExhausted,
    PredecessorMismatch {
        expected: Option<EventSequenceV1>,
        actual: Option<EventSequenceV1>,
    },
    UnknownStream,
    ConsumerLimit,
    CursorBeyondPublished,
    CursorRegression,
    CursorBehindRetention {
        retained_from: EventSequenceV1,
    },
    CompactionBeyondPublished,
    CompactionBlocked,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplyV1 {
    Accepted(OutcomeV1),
    Refused(RefusalV1),
}
