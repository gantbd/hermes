use hermes_ledger::{
    AppendOutcomeV1, CausationId, CompactionOutcomeV1, ConsumerId, CorrelationId, Engine,
    EventEnvelopeV1, EventSequence, IdempotencyKey, Limits, ProducerId, ReadGapV1, RefusalV1,
    SchemaId, StreamId, persistence::CheckpointBudgetV1,
};

fn event(key: u8, payload: u8) -> EventEnvelopeV1 {
    EventEnvelopeV1::new(
        ProducerId::new([1; 16]),
        SchemaId::new([2; 16]),
        IdempotencyKey::new([key; 16]),
        CorrelationId::new([3; 16]),
        Some(CausationId::new([4; 16])),
        vec![payload],
    )
}

#[test]
fn compaction_exposes_truthful_gaps_and_never_reuses_a_producer_key() {
    let stream = StreamId::new([5; 16]);
    let mut engine = Engine::try_new(Limits::default()).unwrap();
    engine.append(stream, None, event(6, 10)).unwrap();
    engine
        .append(stream, Some(EventSequence(0)), event(7, 11))
        .unwrap();

    assert_eq!(
        engine.compact_through(stream, EventSequence(0)).unwrap(),
        CompactionOutcomeV1::Compacted {
            removed: 1,
            retained_from: EventSequence(1),
        }
    );
    let behind = engine.read(stream, EventSequence(0), 10).unwrap();
    assert_eq!(
        behind.gap,
        Some(ReadGapV1::BeforeRetention {
            requested: EventSequence(0),
            retained_from: EventSequence(1),
        })
    );
    assert_eq!(behind.events[0].sequence, EventSequence(1));
    assert_eq!(behind.published_edge, Some(EventSequence(1)));

    let end = engine.read(stream, EventSequence(2), 10).unwrap();
    assert!(end.events.is_empty());
    assert_eq!(end.gap, None);
    let ahead = engine.read(stream, EventSequence(3), 10).unwrap();
    assert_eq!(
        ahead.gap,
        Some(ReadGapV1::AheadOfPublication {
            requested: EventSequence(3),
            next_sequence: EventSequence(2),
        })
    );
    assert_eq!(
        engine.acknowledge(ConsumerId::new([8; 16]), stream, EventSequence(0)),
        Err(RefusalV1::CursorBehindRetention {
            retained_from: EventSequence(1),
        })
    );

    assert_eq!(
        engine.append(stream, Some(EventSequence(1)), event(6, 99)),
        Ok(AppendOutcomeV1::ReplayHistoryLost(EventSequence(0)))
    );
    assert_eq!(
        engine.append(stream, Some(EventSequence(1)), event(8, 12)),
        Ok(AppendOutcomeV1::Appended(EventSequence(2)))
    );
}

#[test]
fn cursors_block_compaction_and_retention_survives_checkpoint_recovery() {
    let stream = StreamId::new([5; 16]);
    let consumer = ConsumerId::new([9; 16]);
    let mut engine = Engine::try_new(Limits::default()).unwrap();
    engine.append(stream, None, event(6, 10)).unwrap();
    engine
        .append(stream, Some(EventSequence(0)), event(7, 11))
        .unwrap();
    engine
        .acknowledge(consumer, stream, EventSequence(0))
        .unwrap();
    assert_eq!(
        engine.compact_through(stream, EventSequence(1)),
        Err(RefusalV1::CompactionBlocked)
    );
    engine
        .acknowledge(consumer, stream, EventSequence(1))
        .unwrap();
    engine.compact_through(stream, EventSequence(1)).unwrap();

    let bytes = engine.checkpoint(CheckpointBudgetV1::default()).unwrap();
    let mut recovered =
        Engine::recover_checkpoint(&bytes, Limits::default(), CheckpointBudgetV1::default())
            .unwrap();
    let batch = recovered.read(stream, EventSequence(0), 1).unwrap();
    assert_eq!(batch.retained_from, EventSequence(2));
    assert!(batch.events.is_empty());
    assert!(matches!(batch.gap, Some(ReadGapV1::BeforeRetention { .. })));
    assert_eq!(
        recovered.append(stream, Some(EventSequence(1)), event(6, 10)),
        Ok(AppendOutcomeV1::ReplayHistoryLost(EventSequence(0)))
    );
}
