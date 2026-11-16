//! Canonical ledger checkpoints. Pure bytes, no store or durability claim.
//! Host limits are inputs, never relaxed by values decoded from the checkpoint.
use crate::*;

const MAGIC: &[u8] = b"HERMESCP";
const VERSION_V1: u8 = 1;
const VERSION_V2: u8 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckpointBudgetV1 {
    pub max_bytes: usize,
    pub max_fields: usize,
}

impl Default for CheckpointBudgetV1 {
    fn default() -> Self {
        Self {
            max_bytes: 8 * 1024 * 1024,
            max_fields: 1_000_000,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointErrorV1 {
    Limit,
    Capacity,
    Malformed,
    UnsupportedVersion,
    InvalidState,
}

// This schema-local cursor keeps existing engine consumers dependency-free.
// Effects and shared store mechanisms belong in independently selected adapters.
struct Decoder<'a> {
    input: &'a [u8],
    position: usize,
    fields: usize,
    budget: CheckpointBudgetV1,
    max_field_bytes: usize,
}

impl<'a> Decoder<'a> {
    fn new(
        input: &'a [u8],
        budget: CheckpointBudgetV1,
        limits: Limits,
    ) -> Result<Self, CheckpointErrorV1> {
        if input.len() > budget.max_bytes {
            return Err(CheckpointErrorV1::Limit);
        }
        Ok(Self {
            input,
            position: 0,
            fields: 0,
            budget,
            max_field_bytes: limits.max_payload_bytes.max(16),
        })
    }
    fn field(&mut self) -> Result<(), CheckpointErrorV1> {
        if self.fields >= self.budget.max_fields {
            return Err(CheckpointErrorV1::Limit);
        }
        self.fields += 1;
        Ok(())
    }
    fn raw(&mut self, count: usize) -> Result<&'a [u8], CheckpointErrorV1> {
        let end = self
            .position
            .checked_add(count)
            .ok_or(CheckpointErrorV1::Malformed)?;
        let bytes = self
            .input
            .get(self.position..end)
            .ok_or(CheckpointErrorV1::Malformed)?;
        self.position = end;
        Ok(bytes)
    }
    fn read_u8(&mut self) -> Result<u8, CheckpointErrorV1> {
        self.field()?;
        Ok(self.raw(1)?[0])
    }
    fn read_u64_be(&mut self) -> Result<u64, CheckpointErrorV1> {
        self.field()?;
        Ok(u64::from_be_bytes(
            self.raw(8)?
                .try_into()
                .map_err(|_| CheckpointErrorV1::Malformed)?,
        ))
    }
    fn read_bytes(&mut self) -> Result<&'a [u8], CheckpointErrorV1> {
        self.field()?;
        let count = u32::from_be_bytes(
            self.raw(4)?
                .try_into()
                .map_err(|_| CheckpointErrorV1::Malformed)?,
        );
        let count = usize::try_from(count).map_err(|_| CheckpointErrorV1::Limit)?;
        if count > self.max_field_bytes {
            return Err(CheckpointErrorV1::Limit);
        }
        self.raw(count)
    }
    fn is_finished(&self) -> bool {
        self.position == self.input.len()
    }
}

struct Encoder {
    output: Vec<u8>,
    fields: usize,
    budget: CheckpointBudgetV1,
}

impl Encoder {
    fn reserve(&mut self, bytes: usize) -> Result<(), CheckpointErrorV1> {
        if self.fields >= self.budget.max_fields
            || bytes > self.budget.max_bytes.saturating_sub(self.output.len())
        {
            return Err(CheckpointErrorV1::Limit);
        }
        self.output
            .try_reserve(bytes)
            .map_err(|_| CheckpointErrorV1::Capacity)?;
        self.fields += 1;
        Ok(())
    }
    fn byte(&mut self, value: u8) -> Result<(), CheckpointErrorV1> {
        self.reserve(1)?;
        self.output.push(value);
        Ok(())
    }
    fn number(&mut self, value: u64) -> Result<(), CheckpointErrorV1> {
        self.reserve(8)?;
        self.output.extend_from_slice(&value.to_be_bytes());
        Ok(())
    }
    fn count(&mut self, value: usize) -> Result<(), CheckpointErrorV1> {
        self.number(u64::try_from(value).map_err(|_| CheckpointErrorV1::Limit)?)
    }
    fn bytes(&mut self, value: &[u8]) -> Result<(), CheckpointErrorV1> {
        let length = u32::try_from(value.len()).map_err(|_| CheckpointErrorV1::Limit)?;
        self.reserve(value.len().checked_add(4).ok_or(CheckpointErrorV1::Limit)?)?;
        self.output.extend_from_slice(&length.to_be_bytes());
        self.output.extend_from_slice(value);
        Ok(())
    }
    fn envelope(&mut self, value: &EventEnvelopeV1) -> Result<(), CheckpointErrorV1> {
        self.bytes(&value.producer.0)?;
        self.bytes(&value.schema.0)?;
        self.bytes(&value.idempotency_key.0)?;
        self.bytes(&value.correlation.0)?;
        match value.causation {
            None => self.byte(0)?,
            Some(causation) => {
                self.byte(1)?;
                self.bytes(&causation.0)?;
            }
        }
        self.bytes(&value.payload)
    }
}

fn count(reader: &mut Decoder<'_>, ceiling: usize) -> Result<usize, CheckpointErrorV1> {
    let value = usize::try_from(reader.read_u64_be()?).map_err(|_| CheckpointErrorV1::Limit)?;
    if value > ceiling {
        return Err(CheckpointErrorV1::Limit);
    }
    Ok(value)
}

fn identity(reader: &mut Decoder<'_>) -> Result<[u8; 16], CheckpointErrorV1> {
    reader
        .read_bytes()?
        .try_into()
        .map_err(|_| CheckpointErrorV1::Malformed)
}

fn envelope(
    reader: &mut Decoder<'_>,
    limits: Limits,
) -> Result<EventEnvelopeV1, CheckpointErrorV1> {
    let producer = ProducerId::new(identity(reader)?);
    let schema = SchemaId::new(identity(reader)?);
    let key = IdempotencyKey::new(identity(reader)?);
    let correlation = CorrelationId::new(identity(reader)?);
    let causation = match reader.read_u8()? {
        0 => None,
        1 => Some(CausationId::new(identity(reader)?)),
        _ => return Err(CheckpointErrorV1::Malformed),
    };
    let bytes = reader.read_bytes()?;
    if bytes.len() > limits.max_payload_bytes {
        return Err(CheckpointErrorV1::Limit);
    }
    let mut payload = Vec::new();
    payload
        .try_reserve_exact(bytes.len())
        .map_err(|_| CheckpointErrorV1::Capacity)?;
    payload.extend_from_slice(bytes);
    Ok(EventEnvelopeV1::new(
        producer,
        schema,
        key,
        correlation,
        causation,
        payload,
    ))
}

impl Engine {
    /// Encodes one whole bounded state. V2 records retained/published edges and
    /// producer-key tombstones separately from retained event envelopes.
    pub fn checkpoint(&self, budget: CheckpointBudgetV1) -> Result<Vec<u8>, CheckpointErrorV1> {
        let mut writer = Encoder {
            output: Vec::new(),
            fields: 0,
            budget,
        };
        writer.bytes(MAGIC)?;
        writer.byte(VERSION_V2)?;
        writer.count(self.streams.len())?;
        let mut total = 0_usize;
        for (stream, state) in &self.streams {
            if state.retained_from > state.next_sequence
                || state.events.len()
                    != usize::try_from(state.next_sequence.0 - state.retained_from.0)
                        .map_err(|_| CheckpointErrorV1::Limit)?
            {
                return Err(CheckpointErrorV1::InvalidState);
            }
            writer.bytes(&stream.0)?;
            writer.number(state.retained_from.0)?;
            writer.number(state.next_sequence.0)?;
            writer.count(state.events.len())?;
            total = total
                .checked_add(state.events.len())
                .ok_or(CheckpointErrorV1::Limit)?;
            for (index, event) in state.events.iter().enumerate() {
                let expected = state
                    .retained_from
                    .0
                    .checked_add(u64::try_from(index).map_err(|_| CheckpointErrorV1::Limit)?)
                    .ok_or(CheckpointErrorV1::Limit)?;
                if event.sequence.0 != expected {
                    return Err(CheckpointErrorV1::InvalidState);
                }
                writer.number(event.sequence.0)?;
                writer.envelope(&event.envelope)?;
                let key = (event.envelope.producer, event.envelope.idempotency_key);
                let indexed = self
                    .appends
                    .get(&key)
                    .ok_or(CheckpointErrorV1::InvalidState)?;
                if indexed.stream != *stream
                    || indexed.sequence != event.sequence
                    || indexed.envelope.as_ref() != Some(&event.envelope)
                {
                    return Err(CheckpointErrorV1::InvalidState);
                }
            }
        }
        let tombstones = self
            .appends
            .iter()
            .filter(|(_, record)| record.envelope.is_none())
            .collect::<Vec<_>>();
        total = total
            .checked_add(tombstones.len())
            .ok_or(CheckpointErrorV1::Limit)?;
        if total != self.appends.len() {
            return Err(CheckpointErrorV1::InvalidState);
        }
        writer.count(tombstones.len())?;
        for ((producer, key), record) in tombstones {
            let state = self
                .streams
                .get(&record.stream)
                .ok_or(CheckpointErrorV1::InvalidState)?;
            if record.sequence >= state.retained_from {
                return Err(CheckpointErrorV1::InvalidState);
            }
            writer.bytes(&producer.0)?;
            writer.bytes(&key.0)?;
            writer.bytes(&record.stream.0)?;
            writer.number(record.sequence.0)?;
        }
        writer.count(self.cursors.len())?;
        for ((consumer, stream), cursor) in &self.cursors {
            let state = self
                .streams
                .get(stream)
                .ok_or(CheckpointErrorV1::InvalidState)?;
            if cursor.consumer != *consumer
                || cursor.stream != *stream
                || cursor.next_sequence.0 == 0
                || cursor.next_sequence < state.retained_from
                || cursor.next_sequence > state.next_sequence
            {
                return Err(CheckpointErrorV1::InvalidState);
            }
            writer.bytes(&consumer.0)?;
            writer.bytes(&stream.0)?;
            writer.number(cursor.next_sequence.0)?;
        }
        Ok(writer.output)
    }

    /// Recovers only a canonical, complete state under independently supplied
    /// byte/field and semantic limits. A failed decode publishes no candidate.
    /// Collection allocations are bounded by those limits, not an RSS contract;
    /// standard-library map allocation failure is not a recoverable refusal.
    pub fn recover_checkpoint(
        bytes: &[u8],
        limits: Limits,
        budget: CheckpointBudgetV1,
    ) -> Result<Self, CheckpointErrorV1> {
        let mut reader = Decoder::new(bytes, budget, limits)?;
        if reader.read_bytes()? != MAGIC {
            return Err(CheckpointErrorV1::Malformed);
        }
        let version = reader.read_u8()?;
        if !matches!(version, VERSION_V1 | VERSION_V2) {
            return Err(CheckpointErrorV1::UnsupportedVersion);
        }
        let mut engine = Self::try_new(limits).map_err(|_| CheckpointErrorV1::Limit)?;
        let streams = count(&mut reader, limits.max_streams)?;
        let mut previous = None;
        let mut total = 0_usize;
        for _ in 0..streams {
            let stream = StreamId::new(identity(&mut reader)?);
            if previous.is_some_and(|previous| stream <= previous) {
                return Err(CheckpointErrorV1::InvalidState);
            }
            previous = Some(stream);
            let (retained_from, next_sequence, events) = match version {
                VERSION_V1 => {
                    let events = count(&mut reader, limits.max_events_per_stream)?;
                    if events == 0 {
                        return Err(CheckpointErrorV1::InvalidState);
                    }
                    (
                        0,
                        u64::try_from(events).map_err(|_| CheckpointErrorV1::Limit)?,
                        events,
                    )
                }
                VERSION_V2 => {
                    let retained_from = reader.read_u64_be()?;
                    let next_sequence = reader.read_u64_be()?;
                    let events = count(&mut reader, limits.max_events_per_stream)?;
                    if retained_from > next_sequence
                        || next_sequence - retained_from
                            != u64::try_from(events).map_err(|_| CheckpointErrorV1::Limit)?
                    {
                        return Err(CheckpointErrorV1::InvalidState);
                    }
                    (retained_from, next_sequence, events)
                }
                _ => return Err(CheckpointErrorV1::UnsupportedVersion),
            };
            total = total
                .checked_add(events)
                .filter(|total| *total <= limits.max_idempotency_records)
                .ok_or(CheckpointErrorV1::Limit)?;
            for index in 0..events {
                let sequence = reader.read_u64_be()?;
                let expected = retained_from
                    .checked_add(u64::try_from(index).map_err(|_| CheckpointErrorV1::Limit)?)
                    .ok_or(CheckpointErrorV1::Limit)?;
                if sequence != expected {
                    return Err(CheckpointErrorV1::InvalidState);
                }
                let event = envelope(&mut reader, limits)?;
                let key = (event.producer, event.idempotency_key);
                if engine
                    .appends
                    .insert(
                        key,
                        AppendRecord {
                            stream,
                            envelope: Some(event.clone()),
                            sequence: EventSequence(sequence),
                        },
                    )
                    .is_some()
                {
                    return Err(CheckpointErrorV1::InvalidState);
                }
                engine
                    .streams
                    .entry(stream)
                    .or_insert_with(|| StreamState {
                        retained_from: EventSequence(retained_from),
                        next_sequence: EventSequence(next_sequence),
                        events: Vec::new(),
                    })
                    .events
                    .push(StoredEventV1 {
                        sequence: EventSequence(sequence),
                        envelope: event,
                    });
            }
            engine.streams.entry(stream).or_insert_with(|| StreamState {
                retained_from: EventSequence(retained_from),
                next_sequence: EventSequence(next_sequence),
                events: Vec::new(),
            });
        }
        if version == VERSION_V2 {
            let tombstones = count(&mut reader, limits.max_idempotency_records)?;
            total = total
                .checked_add(tombstones)
                .filter(|total| *total <= limits.max_idempotency_records)
                .ok_or(CheckpointErrorV1::Limit)?;
            let mut previous = None;
            for _ in 0..tombstones {
                let producer = ProducerId::new(identity(&mut reader)?);
                let key = IdempotencyKey::new(identity(&mut reader)?);
                if previous.is_some_and(|previous| (producer, key) <= previous) {
                    return Err(CheckpointErrorV1::InvalidState);
                }
                previous = Some((producer, key));
                let stream = StreamId::new(identity(&mut reader)?);
                let sequence = EventSequence(reader.read_u64_be()?);
                let state = engine
                    .streams
                    .get(&stream)
                    .ok_or(CheckpointErrorV1::InvalidState)?;
                if sequence >= state.retained_from
                    || engine
                        .appends
                        .insert(
                            (producer, key),
                            AppendRecord {
                                stream,
                                envelope: None,
                                sequence,
                            },
                        )
                        .is_some()
                {
                    return Err(CheckpointErrorV1::InvalidState);
                }
            }
        }
        if total != engine.appends.len() {
            return Err(CheckpointErrorV1::InvalidState);
        }
        let consumers = count(&mut reader, limits.max_consumers)?;
        let mut previous = None;
        for _ in 0..consumers {
            let consumer = ConsumerId::new(identity(&mut reader)?);
            let stream = StreamId::new(identity(&mut reader)?);
            let key = (consumer, stream);
            if previous.is_some_and(|previous| key <= previous) {
                return Err(CheckpointErrorV1::InvalidState);
            }
            previous = Some(key);
            let next = reader.read_u64_be()?;
            let state = engine
                .streams
                .get(&stream)
                .ok_or(CheckpointErrorV1::InvalidState)?;
            let next = EventSequence(next);
            if next.0 == 0 || next < state.retained_from || next > state.next_sequence {
                return Err(CheckpointErrorV1::InvalidState);
            }
            engine.cursors.insert(
                key,
                ConsumerCursorV1 {
                    consumer,
                    stream,
                    next_sequence: next,
                },
            );
        }
        if !reader.is_finished() {
            return Err(CheckpointErrorV1::Malformed);
        }
        Ok(engine)
    }
}
