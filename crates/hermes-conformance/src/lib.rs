//! Reusable public-contract qualification for Hermes inbox and outbox adapters.
//!
//! This crate is test infrastructure, not a runtime adapter. Implementations
//! supply an authenticated public command path plus explicit test controls for
//! restart and discarded replies. The suites never choose retry, retention,
//! authorization, storage, or transport policy for a consumer.
#![forbid(unsafe_code)]

use hermes_protocol::{
    AppendOutcomeV1, CausationIdV1, CommandV1, CompactionOutcomeV1, ConsumerCursorV1, ConsumerIdV1,
    CorrelationIdV1, EventEnvelopeV1, EventSequenceV1, IdempotencyKeyV1, OutcomeV1, ProducerIdV1,
    ReadGapV1, RefusalV1, ReplyV1, SchemaIdV1, StreamIdV1,
};
use std::fmt::Debug;

/// Test control over one authenticated Hermes public command path.
///
/// `dispatch_without_reply` must dispatch exactly once and discard any reply;
/// it models an outcome-unknown delivery failure and must not replay internally.
/// `restart` must reopen the same durable ledger and establish a fresh session.
pub trait ConformanceHarnessV1 {
    type Error: Debug;

    fn execute(&mut self, command: CommandV1) -> Result<ReplyV1, Self::Error>;
    fn dispatch_without_reply(&mut self, command: CommandV1) -> Result<(), Self::Error>;
    fn restart(&mut self) -> Result<(), Self::Error>;
}

fn envelope(key: u8, payload: u8) -> EventEnvelopeV1 {
    EventEnvelopeV1 {
        producer: ProducerIdV1([0x21; 16]),
        schema: SchemaIdV1([0x22; 16]),
        idempotency_key: IdempotencyKeyV1([key; 16]),
        correlation: CorrelationIdV1([0x23; 16]),
        causation: Some(CausationIdV1([0x24; 16])),
        payload: vec![payload],
    }
}

fn append(stream: StreamIdV1, predecessor: Option<u64>, key: u8, payload: u8) -> CommandV1 {
    CommandV1::Append {
        stream,
        expected_predecessor: predecessor.map(EventSequenceV1),
        envelope: envelope(key, payload),
    }
}

fn accepted_append(reply: ReplyV1) -> AppendOutcomeV1 {
    match reply {
        ReplyV1::Accepted(OutcomeV1::Append(outcome)) => outcome,
        other => panic!("expected append outcome, got {other:?}"),
    }
}

/// Lost-reply, exact replay, changed-input conflict, restart and compacted-key
/// history-loss contract for an outbox producer.
pub fn qualify_outbox_v1<H: ConformanceHarnessV1>(harness: &mut H) {
    let stream = StreamIdV1([0x31; 16]);
    let first = append(stream, None, 0x41, 0x51);
    harness.dispatch_without_reply(first.clone()).unwrap();
    harness.restart().unwrap();
    assert_eq!(
        accepted_append(harness.execute(first.clone()).unwrap()),
        AppendOutcomeV1::Replayed(EventSequenceV1(0))
    );
    assert_eq!(
        accepted_append(harness.execute(append(stream, None, 0x41, 0x52)).unwrap()),
        AppendOutcomeV1::Conflict
    );
    assert_eq!(
        accepted_append(
            harness
                .execute(append(stream, Some(0), 0x42, 0x53))
                .unwrap()
        ),
        AppendOutcomeV1::Appended(EventSequenceV1(1))
    );
    assert!(matches!(
        harness
            .execute(CommandV1::Compact {
                stream,
                through: EventSequenceV1(0),
            })
            .unwrap(),
        ReplyV1::Accepted(OutcomeV1::Compacted(CompactionOutcomeV1::Compacted {
            removed: 1,
            retained_from: EventSequenceV1(1),
        }))
    ));
    harness.restart().unwrap();
    assert_eq!(
        accepted_append(harness.execute(first).unwrap()),
        AppendOutcomeV1::ReplayHistoryLost(EventSequenceV1(0))
    );
}

/// Independent cursors, acknowledgement-gated compaction, truthful retained
/// gaps and restart contract for inbox consumers.
pub fn qualify_inbox_v1<H: ConformanceHarnessV1>(harness: &mut H) {
    let stream = StreamIdV1([0x61; 16]);
    for ordinal in 0..3_u64 {
        assert_eq!(
            accepted_append(
                harness
                    .execute(append(
                        stream,
                        ordinal.checked_sub(1),
                        0x70 + ordinal as u8,
                        0x80 + ordinal as u8,
                    ))
                    .unwrap()
            ),
            AppendOutcomeV1::Appended(EventSequenceV1(ordinal))
        );
    }
    let slow = ConsumerIdV1([0x91; 16]);
    let fast = ConsumerIdV1([0x92; 16]);
    for (consumer, through, next) in [(slow, 0, 1), (fast, 1, 2)] {
        assert_eq!(
            harness
                .execute(CommandV1::Acknowledge {
                    consumer,
                    stream,
                    through: EventSequenceV1(through),
                })
                .unwrap(),
            ReplyV1::Accepted(OutcomeV1::Acknowledged(ConsumerCursorV1 {
                consumer,
                stream,
                next_sequence: EventSequenceV1(next),
            }))
        );
    }
    assert_eq!(
        harness
            .execute(CommandV1::Compact {
                stream,
                through: EventSequenceV1(1),
            })
            .unwrap(),
        ReplyV1::Refused(RefusalV1::CompactionBlocked)
    );
    assert!(matches!(
        harness
            .execute(CommandV1::Compact {
                stream,
                through: EventSequenceV1(0),
            })
            .unwrap(),
        ReplyV1::Accepted(OutcomeV1::Compacted(CompactionOutcomeV1::Compacted {
            removed: 1,
            retained_from: EventSequenceV1(1),
        }))
    ));
    harness.restart().unwrap();
    assert!(matches!(
        harness
            .execute(CommandV1::Read {
                stream,
                start: EventSequenceV1(0),
                limit: 8,
            })
            .unwrap(),
        ReplyV1::Accepted(OutcomeV1::Read(batch))
            if batch.retained_from == EventSequenceV1(1)
                && batch.published_edge == Some(EventSequenceV1(2))
                && matches!(batch.gap, Some(ReadGapV1::BeforeRetention {
                    requested: EventSequenceV1(0),
                    retained_from: EventSequenceV1(1),
                }))
                && batch.events.iter().map(|event| event.sequence).collect::<Vec<_>>()
                    == vec![EventSequenceV1(1), EventSequenceV1(2)]
    ));
    for (consumer, next) in [(slow, 1), (fast, 2)] {
        assert_eq!(
            harness
                .execute(CommandV1::Cursor { consumer, stream })
                .unwrap(),
            ReplyV1::Accepted(OutcomeV1::Cursor(Some(ConsumerCursorV1 {
                consumer,
                stream,
                next_sequence: EventSequenceV1(next),
            })))
        );
    }
    assert_eq!(
        harness
            .execute(CommandV1::Acknowledge {
                consumer: slow,
                stream,
                through: EventSequenceV1(0),
            })
            .unwrap(),
        ReplyV1::Accepted(OutcomeV1::Acknowledged(ConsumerCursorV1 {
            consumer: slow,
            stream,
            next_sequence: EventSequenceV1(1),
        }))
    );
}
