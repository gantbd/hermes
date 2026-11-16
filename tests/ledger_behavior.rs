use hermes_ledger::{
    AppendOutcomeV1, CausationId, ConsumerId, CorrelationId, Engine, EventEnvelopeV1,
    EventSequence, IdempotencyKey, Limits, ProducerId, RefusalV1, SchemaId, StreamId,
};

fn envelope(key: u8, payload: &[u8]) -> EventEnvelopeV1 {
    EventEnvelopeV1::new(
        ProducerId::new([1; 16]),
        SchemaId::new([2; 16]),
        IdempotencyKey::new([key; 16]),
        CorrelationId::new([3; 16]),
        Some(CausationId::new([4; 16])),
        payload.to_vec(),
    )
}

#[test]
fn lost_append_reply_replays_original_sequence_before_predecessor_check() {
    let mut engine = Engine::try_new(Limits::default()).unwrap();
    let stream = StreamId::new([5; 16]);
    let event = envelope(6, b"event");

    assert_eq!(
        engine.append(stream, None, event.clone()).unwrap(),
        AppendOutcomeV1::Appended(EventSequence(0))
    );
    assert_eq!(
        engine.append(stream, None, event).unwrap(),
        AppendOutcomeV1::Replayed(EventSequence(0))
    );
    assert_eq!(
        engine
            .read(stream, EventSequence(0), 10)
            .unwrap()
            .events
            .len(),
        1
    );
}

#[test]
fn changed_content_under_a_producer_key_conflicts_without_append() {
    let mut engine = Engine::try_new(Limits::default()).unwrap();
    let stream = StreamId::new([5; 16]);
    engine.append(stream, None, envelope(6, b"first")).unwrap();

    assert_eq!(
        engine.append(stream, Some(EventSequence(0)), envelope(6, b"changed")),
        Ok(AppendOutcomeV1::Conflict)
    );
    assert_eq!(
        engine
            .read(stream, EventSequence(0), 10)
            .unwrap()
            .events
            .len(),
        1
    );
}

#[test]
fn predecessor_and_consumer_progress_are_explicit_and_monotone() {
    let mut engine = Engine::try_new(Limits::default()).unwrap();
    let stream = StreamId::new([5; 16]);
    engine.append(stream, None, envelope(6, b"zero")).unwrap();
    assert_eq!(
        engine.append(stream, None, envelope(7, b"one")),
        Err(RefusalV1::PredecessorMismatch {
            expected: None,
            actual: Some(EventSequence(0)),
        })
    );
    engine
        .append(stream, Some(EventSequence(0)), envelope(7, b"one"))
        .unwrap();

    let consumer = ConsumerId::new([8; 16]);
    let cursor = engine
        .acknowledge(consumer, stream, EventSequence(1))
        .unwrap();
    assert_eq!(cursor.next_sequence, EventSequence(2));
    assert_eq!(
        engine.acknowledge(consumer, stream, EventSequence(0)),
        Err(RefusalV1::CursorRegression)
    );
    assert_eq!(
        engine.acknowledge(consumer, stream, EventSequence(2)),
        Err(RefusalV1::CursorBeyondPublished)
    );
}

#[test]
fn bounds_refuse_before_publication_and_debug_redacts_payload() {
    let mut engine = Engine::try_new(Limits {
        max_payload_bytes: 4,
        ..Limits::default()
    })
    .unwrap();
    let stream = StreamId::new([5; 16]);
    let event = envelope(6, b"secret");
    assert!(!format!("{event:?}").contains("secret"));
    assert_eq!(
        engine.append(stream, None, event),
        Err(RefusalV1::PayloadLimit)
    );
    assert_eq!(
        engine.read(stream, EventSequence(0), 1),
        Err(RefusalV1::UnknownStream)
    );
}

#[test]
fn stream_observation_reports_edges_and_aggregate_backlog_without_identities() {
    let mut engine = Engine::try_new(Limits::default()).unwrap();
    let stream = StreamId::new([5; 16]);
    engine.append(stream, None, envelope(6, b"zero")).unwrap();
    engine
        .append(stream, Some(EventSequence(0)), envelope(7, b"one"))
        .unwrap();
    engine
        .append(stream, Some(EventSequence(1)), envelope(8, b"two"))
        .unwrap();
    engine
        .acknowledge(ConsumerId::new([9; 16]), stream, EventSequence(0))
        .unwrap();
    engine
        .acknowledge(ConsumerId::new([10; 16]), stream, EventSequence(2))
        .unwrap();

    let observation = engine.stream_observation(stream).unwrap();
    assert_eq!(observation.retained_from, EventSequence(0));
    assert_eq!(observation.published_edge, Some(EventSequence(2)));
    assert_eq!(observation.retained_events, 3);
    assert_eq!(observation.consumer_count, 2);
    assert_eq!(observation.stalled_consumers, 1);
    assert_eq!(observation.max_consumer_backlog, 2);
    assert_eq!(engine.stream_observation(StreamId::new([11; 16])), None);
}
