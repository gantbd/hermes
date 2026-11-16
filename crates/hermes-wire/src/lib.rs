//! Canonical bounded V1 request, reply and readiness frames for Hermes.

use hermes_protocol::*;

const MAGIC: &[u8; 8] = b"HERMESW1";
const REQUEST: u8 = 1;
const REPLY: u8 = 2;
const READY: u8 = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LimitsV1 {
    pub max_frame_bytes: usize,
}
impl Default for LimitsV1 {
    fn default() -> Self {
        Self {
            max_frame_bytes: 2 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CodecErrorV1 {
    Limit,
    Capacity,
    Malformed,
    Unsupported,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadyV1 {
    pub protocol_version: u8,
    pub max_frame_bytes: u32,
    pub max_requests: u32,
}

struct Writer {
    bytes: Vec<u8>,
    limit: usize,
}
impl Writer {
    fn new(kind: u8, limits: LimitsV1) -> Result<Self, CodecErrorV1> {
        let mut value = Self {
            bytes: Vec::new(),
            limit: limits.max_frame_bytes,
        };
        value.raw(MAGIC)?;
        value.byte(kind)?;
        Ok(value)
    }
    fn raw(&mut self, value: &[u8]) -> Result<(), CodecErrorV1> {
        if value.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(CodecErrorV1::Limit);
        }
        self.bytes
            .try_reserve(value.len())
            .map_err(|_| CodecErrorV1::Capacity)?;
        self.bytes.extend_from_slice(value);
        Ok(())
    }
    fn byte(&mut self, value: u8) -> Result<(), CodecErrorV1> {
        self.raw(&[value])
    }
    fn u32(&mut self, value: u32) -> Result<(), CodecErrorV1> {
        self.raw(&value.to_be_bytes())
    }
    fn u64(&mut self, value: u64) -> Result<(), CodecErrorV1> {
        self.raw(&value.to_be_bytes())
    }
    fn option<T>(
        &mut self,
        value: Option<T>,
        encode: impl FnOnce(&mut Self, T) -> Result<(), CodecErrorV1>,
    ) -> Result<(), CodecErrorV1> {
        match value {
            None => self.byte(0),
            Some(value) => {
                self.byte(1)?;
                encode(self, value)
            }
        }
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8], kind: u8, limits: LimitsV1) -> Result<Self, CodecErrorV1> {
        if bytes.len() > limits.max_frame_bytes {
            return Err(CodecErrorV1::Limit);
        }
        if bytes.get(..8) != Some(MAGIC.as_slice()) || bytes.get(8) != Some(&kind) {
            return Err(CodecErrorV1::Unsupported);
        }
        Ok(Self { bytes, position: 9 })
    }
    fn raw<const N: usize>(&mut self) -> Result<[u8; N], CodecErrorV1> {
        let end = self
            .position
            .checked_add(N)
            .ok_or(CodecErrorV1::Malformed)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(CodecErrorV1::Malformed)?;
        self.position = end;
        value.try_into().map_err(|_| CodecErrorV1::Malformed)
    }
    fn bytes(&mut self, length: usize) -> Result<Vec<u8>, CodecErrorV1> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(CodecErrorV1::Malformed)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(CodecErrorV1::Malformed)?;
        let mut result = Vec::new();
        result
            .try_reserve_exact(length)
            .map_err(|_| CodecErrorV1::Capacity)?;
        result.extend_from_slice(value);
        self.position = end;
        Ok(result)
    }
    fn byte(&mut self) -> Result<u8, CodecErrorV1> {
        Ok(self.raw::<1>()?[0])
    }
    fn u32(&mut self) -> Result<u32, CodecErrorV1> {
        Ok(u32::from_be_bytes(self.raw()?))
    }
    fn u64(&mut self) -> Result<u64, CodecErrorV1> {
        Ok(u64::from_be_bytes(self.raw()?))
    }
    fn option<T>(
        &mut self,
        decode: impl FnOnce(&mut Self) -> Result<T, CodecErrorV1>,
    ) -> Result<Option<T>, CodecErrorV1> {
        match self.byte()? {
            0 => Ok(None),
            1 => decode(self).map(Some),
            _ => Err(CodecErrorV1::Malformed),
        }
    }
    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }
    fn finish(self) -> Result<(), CodecErrorV1> {
        (self.position == self.bytes.len())
            .then_some(())
            .ok_or(CodecErrorV1::Malformed)
    }
}

pub fn encode_request(value: &CommandV1, limits: LimitsV1) -> Result<Vec<u8>, CodecErrorV1> {
    let mut writer = Writer::new(REQUEST, limits)?;
    command(&mut writer, value)?;
    Ok(writer.bytes)
}
pub fn decode_request(bytes: &[u8], limits: LimitsV1) -> Result<CommandV1, CodecErrorV1> {
    let mut reader = Reader::new(bytes, REQUEST, limits)?;
    let value = read_command(&mut reader)?;
    reader.finish()?;
    Ok(value)
}
pub fn encode_reply(value: &ReplyV1, limits: LimitsV1) -> Result<Vec<u8>, CodecErrorV1> {
    let mut writer = Writer::new(REPLY, limits)?;
    match value {
        ReplyV1::Accepted(value) => {
            writer.byte(0)?;
            outcome(&mut writer, value)?;
        }
        ReplyV1::Refused(value) => {
            writer.byte(1)?;
            refusal(&mut writer, *value)?;
        }
    }
    Ok(writer.bytes)
}
pub fn decode_reply(bytes: &[u8], limits: LimitsV1) -> Result<ReplyV1, CodecErrorV1> {
    let mut reader = Reader::new(bytes, REPLY, limits)?;
    let value = match reader.byte()? {
        0 => ReplyV1::Accepted(read_outcome(&mut reader)?),
        1 => ReplyV1::Refused(read_refusal(&mut reader)?),
        _ => return Err(CodecErrorV1::Unsupported),
    };
    reader.finish()?;
    Ok(value)
}
pub fn encode_ready(value: ReadyV1, limits: LimitsV1) -> Result<Vec<u8>, CodecErrorV1> {
    if value.protocol_version != 1 || value.max_frame_bytes == 0 || value.max_requests == 0 {
        return Err(CodecErrorV1::Malformed);
    }
    let mut writer = Writer::new(READY, limits)?;
    writer.byte(value.protocol_version)?;
    writer.u32(value.max_frame_bytes)?;
    writer.u32(value.max_requests)?;
    Ok(writer.bytes)
}
pub fn decode_ready(bytes: &[u8], limits: LimitsV1) -> Result<ReadyV1, CodecErrorV1> {
    let mut reader = Reader::new(bytes, READY, limits)?;
    let value = ReadyV1 {
        protocol_version: reader.byte()?,
        max_frame_bytes: reader.u32()?,
        max_requests: reader.u32()?,
    };
    if value.protocol_version != 1 || value.max_frame_bytes == 0 || value.max_requests == 0 {
        return Err(CodecErrorV1::Malformed);
    }
    reader.finish()?;
    Ok(value)
}

fn command(w: &mut Writer, value: &CommandV1) -> Result<(), CodecErrorV1> {
    match value {
        CommandV1::Append {
            stream,
            expected_predecessor,
            envelope: value,
        } => {
            w.byte(0)?;
            w.raw(&stream.0)?;
            w.option(*expected_predecessor, |w, value| w.u64(value.0))?;
            envelope(w, value)
        }
        CommandV1::Read {
            stream,
            start,
            limit,
        } => {
            w.byte(1)?;
            w.raw(&stream.0)?;
            w.u64(start.0)?;
            w.u64(*limit)
        }
        CommandV1::Acknowledge {
            consumer,
            stream,
            through,
        } => {
            w.byte(2)?;
            w.raw(&consumer.0)?;
            w.raw(&stream.0)?;
            w.u64(through.0)
        }
        CommandV1::Cursor { consumer, stream } => {
            w.byte(3)?;
            w.raw(&consumer.0)?;
            w.raw(&stream.0)
        }
        CommandV1::Compact { stream, through } => {
            w.byte(4)?;
            w.raw(&stream.0)?;
            w.u64(through.0)
        }
        CommandV1::Status => w.byte(5),
        CommandV1::Usage => w.byte(6),
        CommandV1::StreamObservation { stream } => {
            w.byte(7)?;
            w.raw(&stream.0)
        }
        CommandV1::Signals => w.byte(8),
    }
}
fn read_command(r: &mut Reader<'_>) -> Result<CommandV1, CodecErrorV1> {
    Ok(match r.byte()? {
        0 => CommandV1::Append {
            stream: StreamIdV1(r.raw()?),
            expected_predecessor: r.option(|r| r.u64().map(EventSequenceV1))?,
            envelope: read_envelope(r)?,
        },
        1 => CommandV1::Read {
            stream: StreamIdV1(r.raw()?),
            start: EventSequenceV1(r.u64()?),
            limit: r.u64()?,
        },
        2 => CommandV1::Acknowledge {
            consumer: ConsumerIdV1(r.raw()?),
            stream: StreamIdV1(r.raw()?),
            through: EventSequenceV1(r.u64()?),
        },
        3 => CommandV1::Cursor {
            consumer: ConsumerIdV1(r.raw()?),
            stream: StreamIdV1(r.raw()?),
        },
        4 => CommandV1::Compact {
            stream: StreamIdV1(r.raw()?),
            through: EventSequenceV1(r.u64()?),
        },
        5 => CommandV1::Status,
        6 => CommandV1::Usage,
        7 => CommandV1::StreamObservation {
            stream: StreamIdV1(r.raw()?),
        },
        8 => CommandV1::Signals,
        _ => return Err(CodecErrorV1::Unsupported),
    })
}

fn envelope(w: &mut Writer, value: &EventEnvelopeV1) -> Result<(), CodecErrorV1> {
    w.raw(&value.producer.0)?;
    w.raw(&value.schema.0)?;
    w.raw(&value.idempotency_key.0)?;
    w.raw(&value.correlation.0)?;
    w.option(value.causation, |w, value| w.raw(&value.0))?;
    w.u32(u32::try_from(value.payload.len()).map_err(|_| CodecErrorV1::Limit)?)?;
    w.raw(&value.payload)
}
fn read_envelope(r: &mut Reader<'_>) -> Result<EventEnvelopeV1, CodecErrorV1> {
    let producer = ProducerIdV1(r.raw()?);
    let schema = SchemaIdV1(r.raw()?);
    let idempotency_key = IdempotencyKeyV1(r.raw()?);
    let correlation = CorrelationIdV1(r.raw()?);
    let causation = r.option(|r| r.raw().map(CausationIdV1))?;
    let length = usize::try_from(r.u32()?).map_err(|_| CodecErrorV1::Limit)?;
    if length > r.remaining() {
        return Err(CodecErrorV1::Limit);
    }
    Ok(EventEnvelopeV1 {
        producer,
        schema,
        idempotency_key,
        correlation,
        causation,
        payload: r.bytes(length)?,
    })
}

fn outcome(w: &mut Writer, value: &OutcomeV1) -> Result<(), CodecErrorV1> {
    match value {
        OutcomeV1::Append(value) => {
            w.byte(0)?;
            append(w, *value)
        }
        OutcomeV1::Read(value) => {
            w.byte(1)?;
            write_len(w, value.events.len())?;
            for event in &value.events {
                w.u64(event.sequence.0)?;
                envelope(w, &event.envelope)?;
            }
            w.u64(value.retained_from.0)?;
            w.option(value.published_edge, |w, value| w.u64(value.0))?;
            w.option(value.gap, gap)
        }
        OutcomeV1::Acknowledged(value) => {
            w.byte(2)?;
            cursor(w, *value)
        }
        OutcomeV1::Cursor(value) => {
            w.byte(3)?;
            w.option(*value, cursor)
        }
        OutcomeV1::Compacted(value) => {
            w.byte(4)?;
            compaction(w, *value)
        }
        OutcomeV1::Status(value) => {
            w.byte(5)?;
            w.option(value.storage_revision, Writer::u64)?;
            w.byte(u8::from(value.fenced))
        }
        OutcomeV1::Usage(value) => {
            w.byte(6)?;
            usage(w, *value)
        }
        OutcomeV1::StreamObservation(value) => {
            w.byte(7)?;
            w.option(*value, stream_observation)
        }
        OutcomeV1::Signals(value) => {
            w.byte(8)?;
            for item in value.offers {
                w.u64(item)?;
            }
            for item in value.residence_nanos {
                w.u64(item)?;
            }
            w.u64(value.domain_refusals)?;
            w.u64(value.admission_refusals)?;
            w.u64(value.replays)?;
            w.u64(value.conflicts)?;
            w.u64(value.commits)?;
            w.u64(value.commit_failures)?;
            w.u64(value.commit_time_nanos)?;
            usage(w, value.confirmed_usage)?;
            w.u64(value.last_checkpoint_bytes)?;
            w.byte(u8::from(value.fenced))?;
            w.byte(u8::from(value.saturated))
        }
    }
}
fn read_outcome(r: &mut Reader<'_>) -> Result<OutcomeV1, CodecErrorV1> {
    Ok(match r.byte()? {
        0 => OutcomeV1::Append(read_append(r)?),
        1 => OutcomeV1::Read(ReadBatchV1 {
            events: read_events(r)?,
            retained_from: EventSequenceV1(r.u64()?),
            published_edge: r.option(|r| r.u64().map(EventSequenceV1))?,
            gap: r.option(read_gap)?,
        }),
        2 => OutcomeV1::Acknowledged(read_cursor(r)?),
        3 => OutcomeV1::Cursor(r.option(read_cursor)?),
        4 => OutcomeV1::Compacted(read_compaction(r)?),
        5 => OutcomeV1::Status(StatusV1 {
            storage_revision: r.option(Reader::u64)?,
            fenced: boolean(r.byte()?)?,
        }),
        6 => OutcomeV1::Usage(read_usage(r)?),
        7 => OutcomeV1::StreamObservation(r.option(read_stream_observation)?),
        8 => OutcomeV1::Signals(OperationalSignalsV1 {
            offers: read_u64_array(r)?,
            residence_nanos: read_u64_array(r)?,
            domain_refusals: r.u64()?,
            admission_refusals: r.u64()?,
            replays: r.u64()?,
            conflicts: r.u64()?,
            commits: r.u64()?,
            commit_failures: r.u64()?,
            commit_time_nanos: r.u64()?,
            confirmed_usage: read_usage(r)?,
            last_checkpoint_bytes: r.u64()?,
            fenced: boolean(r.byte()?)?,
            saturated: boolean(r.byte()?)?,
        }),
        _ => return Err(CodecErrorV1::Unsupported),
    })
}

fn usage(w: &mut Writer, value: UsageV1) -> Result<(), CodecErrorV1> {
    w.u64(value.streams)?;
    w.u64(value.events)?;
    w.u64(value.idempotency_records)?;
    w.u64(value.compacted_records)?;
    w.u64(value.cursors)?;
    w.byte(u8::from(value.saturated))
}
fn read_usage(r: &mut Reader<'_>) -> Result<UsageV1, CodecErrorV1> {
    Ok(UsageV1 {
        streams: r.u64()?,
        events: r.u64()?,
        idempotency_records: r.u64()?,
        compacted_records: r.u64()?,
        cursors: r.u64()?,
        saturated: boolean(r.byte()?)?,
    })
}
fn stream_observation(w: &mut Writer, value: StreamObservationV1) -> Result<(), CodecErrorV1> {
    w.u64(value.retained_from.0)?;
    w.option(value.published_edge, |w, value| w.u64(value.0))?;
    w.u64(value.retained_events)?;
    w.u64(value.consumer_count)?;
    w.u64(value.stalled_consumers)?;
    w.u64(value.max_consumer_backlog)
}
fn read_stream_observation(r: &mut Reader<'_>) -> Result<StreamObservationV1, CodecErrorV1> {
    Ok(StreamObservationV1 {
        retained_from: EventSequenceV1(r.u64()?),
        published_edge: r.option(|r| r.u64().map(EventSequenceV1))?,
        retained_events: r.u64()?,
        consumer_count: r.u64()?,
        stalled_consumers: r.u64()?,
        max_consumer_backlog: r.u64()?,
    })
}
fn read_u64_array(r: &mut Reader<'_>) -> Result<[u64; 5], CodecErrorV1> {
    Ok([r.u64()?, r.u64()?, r.u64()?, r.u64()?, r.u64()?])
}

fn append(w: &mut Writer, value: AppendOutcomeV1) -> Result<(), CodecErrorV1> {
    match value {
        AppendOutcomeV1::Appended(value) => {
            w.byte(0)?;
            w.u64(value.0)
        }
        AppendOutcomeV1::Replayed(value) => {
            w.byte(1)?;
            w.u64(value.0)
        }
        AppendOutcomeV1::ReplayHistoryLost(value) => {
            w.byte(2)?;
            w.u64(value.0)
        }
        AppendOutcomeV1::Conflict => w.byte(3),
        AppendOutcomeV1::Indeterminate => w.byte(4),
    }
}
fn read_append(r: &mut Reader<'_>) -> Result<AppendOutcomeV1, CodecErrorV1> {
    Ok(match r.byte()? {
        0 => AppendOutcomeV1::Appended(EventSequenceV1(r.u64()?)),
        1 => AppendOutcomeV1::Replayed(EventSequenceV1(r.u64()?)),
        2 => AppendOutcomeV1::ReplayHistoryLost(EventSequenceV1(r.u64()?)),
        3 => AppendOutcomeV1::Conflict,
        4 => AppendOutcomeV1::Indeterminate,
        _ => return Err(CodecErrorV1::Unsupported),
    })
}
fn read_events(r: &mut Reader<'_>) -> Result<Vec<StoredEventV1>, CodecErrorV1> {
    let count = usize::try_from(r.u32()?).map_err(|_| CodecErrorV1::Limit)?;
    if count > r.remaining() {
        return Err(CodecErrorV1::Limit);
    }
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| CodecErrorV1::Capacity)?;
    for _ in 0..count {
        values.push(StoredEventV1 {
            sequence: EventSequenceV1(r.u64()?),
            envelope: read_envelope(r)?,
        });
    }
    Ok(values)
}
fn gap(w: &mut Writer, value: ReadGapV1) -> Result<(), CodecErrorV1> {
    match value {
        ReadGapV1::BeforeRetention {
            requested,
            retained_from,
        } => {
            w.byte(0)?;
            w.u64(requested.0)?;
            w.u64(retained_from.0)
        }
        ReadGapV1::AheadOfPublication {
            requested,
            next_sequence,
        } => {
            w.byte(1)?;
            w.u64(requested.0)?;
            w.u64(next_sequence.0)
        }
    }
}
fn read_gap(r: &mut Reader<'_>) -> Result<ReadGapV1, CodecErrorV1> {
    Ok(match r.byte()? {
        0 => ReadGapV1::BeforeRetention {
            requested: EventSequenceV1(r.u64()?),
            retained_from: EventSequenceV1(r.u64()?),
        },
        1 => ReadGapV1::AheadOfPublication {
            requested: EventSequenceV1(r.u64()?),
            next_sequence: EventSequenceV1(r.u64()?),
        },
        _ => return Err(CodecErrorV1::Unsupported),
    })
}
fn cursor(w: &mut Writer, value: ConsumerCursorV1) -> Result<(), CodecErrorV1> {
    w.raw(&value.consumer.0)?;
    w.raw(&value.stream.0)?;
    w.u64(value.next_sequence.0)
}
fn read_cursor(r: &mut Reader<'_>) -> Result<ConsumerCursorV1, CodecErrorV1> {
    Ok(ConsumerCursorV1 {
        consumer: ConsumerIdV1(r.raw()?),
        stream: StreamIdV1(r.raw()?),
        next_sequence: EventSequenceV1(r.u64()?),
    })
}
fn compaction(w: &mut Writer, value: CompactionOutcomeV1) -> Result<(), CodecErrorV1> {
    match value {
        CompactionOutcomeV1::Compacted {
            removed,
            retained_from,
        } => {
            w.byte(0)?;
            w.u64(removed)?;
            w.u64(retained_from.0)
        }
        CompactionOutcomeV1::Identity { retained_from } => {
            w.byte(1)?;
            w.u64(retained_from.0)
        }
    }
}
fn read_compaction(r: &mut Reader<'_>) -> Result<CompactionOutcomeV1, CodecErrorV1> {
    Ok(match r.byte()? {
        0 => CompactionOutcomeV1::Compacted {
            removed: r.u64()?,
            retained_from: EventSequenceV1(r.u64()?),
        },
        1 => CompactionOutcomeV1::Identity {
            retained_from: EventSequenceV1(r.u64()?),
        },
        _ => return Err(CodecErrorV1::Unsupported),
    })
}

fn refusal(w: &mut Writer, value: RefusalV1) -> Result<(), CodecErrorV1> {
    match value {
        RefusalV1::UnknownSession => w.byte(0),
        RefusalV1::NotAuthorized => w.byte(1),
        RefusalV1::InvalidReadLimit => w.byte(2),
        RefusalV1::PayloadLimit => w.byte(3),
        RefusalV1::StreamLimit => w.byte(4),
        RefusalV1::StreamEventLimit => w.byte(5),
        RefusalV1::IdempotencyLimit => w.byte(6),
        RefusalV1::SequenceExhausted => w.byte(7),
        RefusalV1::PredecessorMismatch { expected, actual } => {
            w.byte(8)?;
            w.option(expected, |w, value| w.u64(value.0))?;
            w.option(actual, |w, value| w.u64(value.0))
        }
        RefusalV1::UnknownStream => w.byte(9),
        RefusalV1::ConsumerLimit => w.byte(10),
        RefusalV1::CursorBeyondPublished => w.byte(11),
        RefusalV1::CursorRegression => w.byte(12),
        RefusalV1::CursorBehindRetention { retained_from } => {
            w.byte(13)?;
            w.u64(retained_from.0)
        }
        RefusalV1::CompactionBeyondPublished => w.byte(14),
        RefusalV1::CompactionBlocked => w.byte(15),
    }
}
fn read_refusal(r: &mut Reader<'_>) -> Result<RefusalV1, CodecErrorV1> {
    Ok(match r.byte()? {
        0 => RefusalV1::UnknownSession,
        1 => RefusalV1::NotAuthorized,
        2 => RefusalV1::InvalidReadLimit,
        3 => RefusalV1::PayloadLimit,
        4 => RefusalV1::StreamLimit,
        5 => RefusalV1::StreamEventLimit,
        6 => RefusalV1::IdempotencyLimit,
        7 => RefusalV1::SequenceExhausted,
        8 => RefusalV1::PredecessorMismatch {
            expected: r.option(|r| r.u64().map(EventSequenceV1))?,
            actual: r.option(|r| r.u64().map(EventSequenceV1))?,
        },
        9 => RefusalV1::UnknownStream,
        10 => RefusalV1::ConsumerLimit,
        11 => RefusalV1::CursorBeyondPublished,
        12 => RefusalV1::CursorRegression,
        13 => RefusalV1::CursorBehindRetention {
            retained_from: EventSequenceV1(r.u64()?),
        },
        14 => RefusalV1::CompactionBeyondPublished,
        15 => RefusalV1::CompactionBlocked,
        _ => return Err(CodecErrorV1::Unsupported),
    })
}

fn write_len(w: &mut Writer, value: usize) -> Result<(), CodecErrorV1> {
    w.u32(u32::try_from(value).map_err(|_| CodecErrorV1::Limit)?)
}
fn boolean(value: u8) -> Result<bool, CodecErrorV1> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(CodecErrorV1::Malformed),
    }
}
