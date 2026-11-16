use hermes_protocol::*;
use hermes_wire::{
    CodecErrorV1, LimitsV1, ReadyV1, decode_ready, decode_reply, decode_request, encode_ready,
    encode_reply, encode_request,
};

fn envelope(payload: Vec<u8>) -> EventEnvelopeV1 {
    EventEnvelopeV1 {
        producer: ProducerIdV1([1; 16]),
        schema: SchemaIdV1([2; 16]),
        idempotency_key: IdempotencyKeyV1([3; 16]),
        correlation: CorrelationIdV1([4; 16]),
        causation: Some(CausationIdV1([5; 16])),
        payload,
    }
}

fn round_request(value: CommandV1) {
    let bytes = encode_request(&value, LimitsV1::default()).unwrap();
    assert_eq!(decode_request(&bytes, LimitsV1::default()), Ok(value));
    for end in 0..bytes.len() {
        assert!(decode_request(&bytes[..end], LimitsV1::default()).is_err());
    }
    let mut trailing = bytes;
    trailing.push(0);
    assert_eq!(
        decode_request(&trailing, LimitsV1::default()),
        Err(CodecErrorV1::Malformed)
    );
}
fn round_reply(value: ReplyV1) {
    let bytes = encode_reply(&value, LimitsV1::default()).unwrap();
    assert_eq!(decode_reply(&bytes, LimitsV1::default()), Ok(value));
}

#[test]
fn every_request_roundtrips_with_payload_redaction_and_bounds() {
    let stream = StreamIdV1([6; 16]);
    let consumer = ConsumerIdV1([7; 16]);
    for command in [
        CommandV1::Append {
            stream,
            expected_predecessor: Some(EventSequenceV1(3)),
            envelope: envelope(vec![8, 9]),
        },
        CommandV1::Read {
            stream,
            start: EventSequenceV1(2),
            limit: 4,
        },
        CommandV1::Acknowledge {
            consumer,
            stream,
            through: EventSequenceV1(3),
        },
        CommandV1::Cursor { consumer, stream },
        CommandV1::Compact {
            stream,
            through: EventSequenceV1(1),
        },
        CommandV1::Status,
        CommandV1::Usage,
        CommandV1::StreamObservation { stream },
        CommandV1::Signals,
    ] {
        round_request(command);
    }
    assert!(!format!("{:?}", envelope(vec![111, 112, 101, 110])).contains("111"));
    assert_eq!(
        encode_request(&CommandV1::Status, LimitsV1 { max_frame_bytes: 8 }),
        Err(CodecErrorV1::Limit)
    );
}

#[test]
fn every_outcome_and_refusal_roundtrips() {
    let stream = StreamIdV1([6; 16]);
    let consumer = ConsumerIdV1([7; 16]);
    for value in [
        AppendOutcomeV1::Appended(EventSequenceV1(0)),
        AppendOutcomeV1::Replayed(EventSequenceV1(0)),
        AppendOutcomeV1::ReplayHistoryLost(EventSequenceV1(0)),
        AppendOutcomeV1::Conflict,
        AppendOutcomeV1::Indeterminate,
    ] {
        round_reply(ReplyV1::Accepted(OutcomeV1::Append(value)));
    }
    for outcome in [
        OutcomeV1::Read(ReadBatchV1 {
            events: vec![StoredEventV1 {
                sequence: EventSequenceV1(1),
                envelope: envelope(vec![8]),
            }],
            retained_from: EventSequenceV1(1),
            published_edge: Some(EventSequenceV1(1)),
            gap: Some(ReadGapV1::BeforeRetention {
                requested: EventSequenceV1(0),
                retained_from: EventSequenceV1(1),
            }),
        }),
        OutcomeV1::Read(ReadBatchV1 {
            events: Vec::new(),
            retained_from: EventSequenceV1(0),
            published_edge: Some(EventSequenceV1(1)),
            gap: Some(ReadGapV1::AheadOfPublication {
                requested: EventSequenceV1(3),
                next_sequence: EventSequenceV1(2),
            }),
        }),
        OutcomeV1::Acknowledged(ConsumerCursorV1 {
            consumer,
            stream,
            next_sequence: EventSequenceV1(2),
        }),
        OutcomeV1::Cursor(None),
        OutcomeV1::Compacted(CompactionOutcomeV1::Compacted {
            removed: 2,
            retained_from: EventSequenceV1(2),
        }),
        OutcomeV1::Compacted(CompactionOutcomeV1::Identity {
            retained_from: EventSequenceV1(2),
        }),
        OutcomeV1::Status(StatusV1 {
            storage_revision: Some(4),
            fenced: false,
        }),
        OutcomeV1::Usage(UsageV1 {
            streams: 1,
            events: 2,
            idempotency_records: 3,
            compacted_records: 1,
            cursors: 1,
            saturated: true,
        }),
        OutcomeV1::StreamObservation(Some(StreamObservationV1 {
            retained_from: EventSequenceV1(1),
            published_edge: Some(EventSequenceV1(4)),
            retained_events: 4,
            consumer_count: 2,
            stalled_consumers: 1,
            max_consumer_backlog: 3,
        })),
        OutcomeV1::StreamObservation(None),
        OutcomeV1::Signals(OperationalSignalsV1 {
            offers: [1, 2, 3, 4, 5],
            residence_nanos: [6, 7, 8, 9, 10],
            domain_refusals: 11,
            admission_refusals: 12,
            replays: 13,
            conflicts: 14,
            commits: 15,
            commit_failures: 16,
            commit_time_nanos: 17,
            confirmed_usage: UsageV1 {
                streams: 18,
                events: 19,
                idempotency_records: 20,
                compacted_records: 21,
                cursors: 22,
                saturated: false,
            },
            last_checkpoint_bytes: 23,
            fenced: false,
            saturated: true,
        }),
    ] {
        round_reply(ReplyV1::Accepted(outcome));
    }
    for refusal in [
        RefusalV1::UnknownSession,
        RefusalV1::NotAuthorized,
        RefusalV1::InvalidReadLimit,
        RefusalV1::PayloadLimit,
        RefusalV1::StreamLimit,
        RefusalV1::StreamEventLimit,
        RefusalV1::IdempotencyLimit,
        RefusalV1::SequenceExhausted,
        RefusalV1::PredecessorMismatch {
            expected: Some(EventSequenceV1(1)),
            actual: None,
        },
        RefusalV1::UnknownStream,
        RefusalV1::ConsumerLimit,
        RefusalV1::CursorBeyondPublished,
        RefusalV1::CursorRegression,
        RefusalV1::CursorBehindRetention {
            retained_from: EventSequenceV1(2),
        },
        RefusalV1::CompactionBeyondPublished,
        RefusalV1::CompactionBlocked,
    ] {
        round_reply(ReplyV1::Refused(refusal));
    }
}

#[test]
fn attacker_counts_and_payload_lengths_refuse_before_allocation() {
    let reply = ReplyV1::Accepted(OutcomeV1::Read(ReadBatchV1 {
        events: Vec::new(),
        retained_from: EventSequenceV1(0),
        published_edge: None,
        gap: None,
    }));
    let mut bytes = encode_reply(&reply, LimitsV1::default()).unwrap();
    // magic + kind + accepted + read outcome.
    bytes[11..15].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(
        decode_reply(&bytes, LimitsV1::default()),
        Err(CodecErrorV1::Limit)
    );

    let command = CommandV1::Append {
        stream: StreamIdV1([6; 16]),
        expected_predecessor: None,
        envelope: envelope(Vec::new()),
    };
    let mut bytes = encode_request(&command, LimitsV1::default()).unwrap();
    let length_offset = bytes.len() - 4;
    bytes[length_offset..].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(
        decode_request(&bytes, LimitsV1::default()),
        Err(CodecErrorV1::Limit)
    );
}

#[test]
fn readiness_is_closed_and_bounded() {
    let value = ReadyV1 {
        protocol_version: 1,
        max_frame_bytes: 4096,
        max_requests: 8,
    };
    let bytes = encode_ready(value, LimitsV1::default()).unwrap();
    assert_eq!(decode_ready(&bytes, LimitsV1::default()), Ok(value));
}
