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

fn assert_mutation_canonical<T: std::fmt::Debug>(
    canonical: &[u8],
    decode: impl Fn(&[u8]) -> Result<T, CodecErrorV1>,
    encode: impl Fn(&T) -> Result<Vec<u8>, CodecErrorV1>,
) {
    let mut rejected = 0_usize;
    let mut checked = 0_usize;
    let mut check = |mutated: Vec<u8>| {
        checked += 1;
        match decode(&mutated) {
            Ok(value) => assert_eq!(encode(&value).unwrap(), mutated),
            Err(_) => rejected += 1,
        }
    };

    for index in 0..canonical.len() {
        for bit in 0..8 {
            let mut mutated = canonical.to_vec();
            mutated[index] ^= 1 << bit;
            check(mutated);
        }
        let mut deleted = canonical.to_vec();
        deleted.remove(index);
        check(deleted);
    }
    for index in 0..=canonical.len() {
        let mut inserted = canonical.to_vec();
        inserted.insert(index, 0xff);
        check(inserted);
    }
    assert!(checked > 0);
    assert!(
        rejected > 0,
        "mutation corpus unexpectedly accepted everything"
    );
}

#[test]
fn every_request_shape_has_canonical_single_byte_and_bit_mutation_behavior() {
    let stream = StreamIdV1([6; 16]);
    let consumer = ConsumerIdV1([7; 16]);
    let commands = [
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
    ];
    for command in commands {
        let bytes = encode_request(&command, LimitsV1::default()).unwrap();
        assert_mutation_canonical(
            &bytes,
            |value| decode_request(value, LimitsV1::default()),
            |value| encode_request(value, LimitsV1::default()),
        );
    }
}

fn reply_samples() -> Vec<ReplyV1> {
    let stream = StreamIdV1([6; 16]);
    let consumer = ConsumerIdV1([7; 16]);
    let mut replies = [
        AppendOutcomeV1::Appended(EventSequenceV1(0)),
        AppendOutcomeV1::Replayed(EventSequenceV1(0)),
        AppendOutcomeV1::ReplayHistoryLost(EventSequenceV1(0)),
        AppendOutcomeV1::Conflict,
        AppendOutcomeV1::Indeterminate,
    ]
    .into_iter()
    .map(|value| ReplyV1::Accepted(OutcomeV1::Append(value)))
    .collect::<Vec<_>>();
    replies.extend([
        ReplyV1::Accepted(OutcomeV1::Read(ReadBatchV1 {
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
        })),
        ReplyV1::Accepted(OutcomeV1::Read(ReadBatchV1 {
            events: Vec::new(),
            retained_from: EventSequenceV1(0),
            published_edge: Some(EventSequenceV1(1)),
            gap: Some(ReadGapV1::AheadOfPublication {
                requested: EventSequenceV1(3),
                next_sequence: EventSequenceV1(2),
            }),
        })),
        ReplyV1::Accepted(OutcomeV1::Acknowledged(ConsumerCursorV1 {
            consumer,
            stream,
            next_sequence: EventSequenceV1(2),
        })),
        ReplyV1::Accepted(OutcomeV1::Cursor(Some(ConsumerCursorV1 {
            consumer,
            stream,
            next_sequence: EventSequenceV1(2),
        }))),
        ReplyV1::Accepted(OutcomeV1::Cursor(None)),
        ReplyV1::Accepted(OutcomeV1::Compacted(CompactionOutcomeV1::Compacted {
            removed: 2,
            retained_from: EventSequenceV1(2),
        })),
        ReplyV1::Accepted(OutcomeV1::Compacted(CompactionOutcomeV1::Identity {
            retained_from: EventSequenceV1(2),
        })),
        ReplyV1::Accepted(OutcomeV1::Status(StatusV1 {
            storage_revision: Some(4),
            fenced: false,
        })),
        ReplyV1::Accepted(OutcomeV1::Usage(UsageV1 {
            streams: 1,
            events: 2,
            idempotency_records: 3,
            compacted_records: 1,
            cursors: 1,
            saturated: true,
        })),
        ReplyV1::Accepted(OutcomeV1::StreamObservation(Some(StreamObservationV1 {
            retained_from: EventSequenceV1(1),
            published_edge: Some(EventSequenceV1(4)),
            retained_events: 4,
            consumer_count: 2,
            stalled_consumers: 1,
            max_consumer_backlog: 3,
        }))),
        ReplyV1::Accepted(OutcomeV1::StreamObservation(None)),
        ReplyV1::Accepted(OutcomeV1::Signals(OperationalSignalsV1 {
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
        })),
    ]);
    replies.extend(
        [
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
        ]
        .into_iter()
        .map(ReplyV1::Refused),
    );
    replies
}

#[test]
fn every_reply_shape_has_canonical_single_byte_and_bit_mutation_behavior() {
    for reply in reply_samples() {
        let bytes = encode_reply(&reply, LimitsV1::default()).unwrap();
        assert_mutation_canonical(
            &bytes,
            |value| decode_reply(value, LimitsV1::default()),
            |value| encode_reply(value, LimitsV1::default()),
        );
    }
}

#[test]
fn readiness_has_canonical_single_byte_and_bit_mutation_behavior() {
    let ready = ReadyV1 {
        protocol_version: 1,
        max_frame_bytes: 4096,
        max_requests: 8,
    };
    let bytes = encode_ready(ready, LimitsV1::default()).unwrap();
    assert_mutation_canonical(
        &bytes,
        |value| decode_ready(value, LimitsV1::default()),
        |value| encode_ready(*value, LimitsV1::default()),
    );
}
