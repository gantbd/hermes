use hermes_ledger::{
    AppendOutcomeV1, ConsumerId, CorrelationId, Engine, EventEnvelopeV1, EventSequence,
    IdempotencyKey, Limits, ProducerId, SchemaId, StreamId,
    persistence::{CheckpointBudgetV1, CheckpointErrorV1},
};

fn envelope(key: u8) -> EventEnvelopeV1 {
    EventEnvelopeV1::new(
        ProducerId::new([1; 16]),
        SchemaId::new([2; 16]),
        IdempotencyKey::new([key; 16]),
        CorrelationId::new([3; 16]),
        None,
        b"private-payload".to_vec(),
    )
}

fn decode_fixture(source: &str) -> Vec<u8> {
    let source = source.trim();
    assert_eq!(source.len() % 2, 0);
    source
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).unwrap();
            u8::from_str_radix(text, 16).unwrap()
        })
        .collect()
}

#[test]
fn canonical_v1_fixture_upgrades_to_canonical_v2() {
    let engine = Engine::try_new(Limits::default()).unwrap();
    let fixture = decode_fixture(include_str!("fixtures/checkpoint-v1-empty.hex"));
    let v2_fixture = decode_fixture(include_str!("fixtures/checkpoint-v2-empty.hex"));
    let recovered =
        Engine::recover_checkpoint(&fixture, Limits::default(), CheckpointBudgetV1::default())
            .unwrap();
    let upgraded = recovered.checkpoint(CheckpointBudgetV1::default()).unwrap();
    assert_eq!(upgraded, v2_fixture);
    assert_eq!(
        Engine::recover_checkpoint(&upgraded, Limits::default(), CheckpointBudgetV1::default())
            .unwrap()
            .checkpoint(CheckpointBudgetV1::default())
            .unwrap(),
        upgraded
    );
    assert_eq!(
        engine.checkpoint(CheckpointBudgetV1::default()).unwrap(),
        upgraded
    );
}

#[test]
fn restart_preserves_original_replay_and_independent_consumer_progress() {
    let stream = StreamId::new([5; 16]);
    let first = ConsumerId::new([7; 16]);
    let second = ConsumerId::new([8; 16]);
    let mut engine = Engine::try_new(Limits::default()).unwrap();
    engine.append(stream, None, envelope(6)).unwrap();
    engine
        .append(stream, Some(EventSequence(0)), envelope(9))
        .unwrap();
    engine.acknowledge(first, stream, EventSequence(1)).unwrap();
    engine
        .acknowledge(second, stream, EventSequence(0))
        .unwrap();
    let bytes = engine.checkpoint(CheckpointBudgetV1::default()).unwrap();
    assert_eq!(
        bytes,
        decode_fixture(include_str!("fixtures/checkpoint-v2-live.hex"))
    );
    let mut recovered =
        Engine::recover_checkpoint(&bytes, Limits::default(), CheckpointBudgetV1::default())
            .unwrap();
    assert_eq!(
        recovered.append(stream, None, envelope(6)),
        Ok(AppendOutcomeV1::Replayed(EventSequence(0)))
    );
    assert_eq!(
        recovered
            .consumer_cursor(first, stream)
            .unwrap()
            .next_sequence,
        EventSequence(2)
    );
    assert_eq!(
        recovered
            .consumer_cursor(second, stream)
            .unwrap()
            .next_sequence,
        EventSequence(1)
    );
    assert_eq!(
        recovered
            .read(stream, EventSequence(0), 2)
            .unwrap()
            .events
            .len(),
        2
    );
    assert_eq!(
        recovered.checkpoint(CheckpointBudgetV1::default()).unwrap(),
        bytes
    );
}

#[test]
fn checkpoint_order_is_independent_of_cross_stream_and_cursor_insertion_order() {
    fn snapshot(reverse: bool) -> Vec<u8> {
        let mut engine = Engine::try_new(Limits::default()).unwrap();
        let streams = if reverse { [9, 5] } else { [5, 9] };
        for stream in streams {
            engine
                .append(StreamId::new([stream; 16]), None, envelope(stream))
                .unwrap();
        }
        for consumer in streams {
            engine
                .acknowledge(
                    ConsumerId::new([consumer; 16]),
                    StreamId::new([5; 16]),
                    EventSequence(0),
                )
                .unwrap();
        }
        engine.checkpoint(CheckpointBudgetV1::default()).unwrap()
    }
    assert_eq!(snapshot(false), snapshot(true));
}

#[test]
fn malformed_versions_truncation_trailing_data_and_budget_excess_refuse() {
    let engine = Engine::try_new(Limits::default()).unwrap();
    let bytes = engine.checkpoint(CheckpointBudgetV1::default()).unwrap();
    for end in 0..bytes.len() {
        assert!(
            Engine::recover_checkpoint(
                &bytes[..end],
                Limits::default(),
                CheckpointBudgetV1::default()
            )
            .is_err()
        );
    }
    let mut changed = bytes.clone();
    changed[12] = 3;
    assert!(matches!(
        Engine::recover_checkpoint(&changed, Limits::default(), CheckpointBudgetV1::default()),
        Err(CheckpointErrorV1::UnsupportedVersion)
    ));
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(
        Engine::recover_checkpoint(&trailing, Limits::default(), CheckpointBudgetV1::default())
            .is_err()
    );
    for budget in [
        CheckpointBudgetV1 {
            max_bytes: bytes.len() - 1,
            ..CheckpointBudgetV1::default()
        },
        CheckpointBudgetV1 {
            max_fields: 1,
            ..CheckpointBudgetV1::default()
        },
    ] {
        assert_eq!(engine.checkpoint(budget), Err(CheckpointErrorV1::Limit));
        assert!(matches!(
            Engine::recover_checkpoint(&bytes, Limits::default(), budget),
            Err(CheckpointErrorV1::Limit)
        ));
    }
}

#[test]
fn recovery_does_not_accept_forged_sequence_or_relax_host_limits() {
    let mut engine = Engine::try_new(Limits::default()).unwrap();
    engine
        .append(StreamId::new([5; 16]), None, envelope(6))
        .unwrap();
    let bytes = engine.checkpoint(CheckpointBudgetV1::default()).unwrap();
    let mut forged = bytes.clone();
    forged[65..73].copy_from_slice(&1_u64.to_be_bytes());
    assert!(matches!(
        Engine::recover_checkpoint(&forged, Limits::default(), CheckpointBudgetV1::default()),
        Err(CheckpointErrorV1::InvalidState)
    ));
    assert!(matches!(
        Engine::recover_checkpoint(
            &bytes,
            Limits {
                max_payload_bytes: 4,
                ..Limits::default()
            },
            CheckpointBudgetV1::default()
        ),
        Err(CheckpointErrorV1::Limit)
    ));
    let mut unknown_option = bytes.clone();
    unknown_option[153] = 2;
    assert!(
        Engine::recover_checkpoint(
            &unknown_option,
            Limits::default(),
            CheckpointBudgetV1::default()
        )
        .is_err()
    );
}

#[test]
fn binary_payload_and_optional_causation_survive_every_complete_round_trip() {
    let stream = StreamId::new([5; 16]);
    let event = EventEnvelopeV1::new(
        ProducerId::new([1; 16]),
        SchemaId::new([2; 16]),
        IdempotencyKey::new([6; 16]),
        CorrelationId::new([3; 16]),
        Some(hermes_ledger::CausationId::new([4; 16])),
        vec![0, 255, 128, 0],
    );
    let mut engine = Engine::try_new(Limits::default()).unwrap();
    engine.append(stream, None, event.clone()).unwrap();
    let bytes = engine.checkpoint(CheckpointBudgetV1::default()).unwrap();
    for end in 0..bytes.len() {
        assert!(
            Engine::recover_checkpoint(
                &bytes[..end],
                Limits::default(),
                CheckpointBudgetV1::default()
            )
            .is_err()
        );
    }
    let recovered =
        Engine::recover_checkpoint(&bytes, Limits::default(), CheckpointBudgetV1::default())
            .unwrap();
    assert_eq!(
        recovered.read(stream, EventSequence(0), 1).unwrap().events[0].envelope,
        event
    );
    assert_eq!(
        recovered.checkpoint(CheckpointBudgetV1::default()).unwrap(),
        bytes
    );
}

#[test]
fn duplicate_streams_producer_keys_and_impossible_cursors_are_rejected() {
    let mut engine = Engine::try_new(Limits::default()).unwrap();
    engine
        .append(StreamId::new([5; 16]), None, envelope(6))
        .unwrap();
    engine
        .append(StreamId::new([9; 16]), None, envelope(9))
        .unwrap();
    engine
        .acknowledge(
            ConsumerId::new([7; 16]),
            StreamId::new([5; 16]),
            EventSequence(0),
        )
        .unwrap();
    let bytes = engine.checkpoint(CheckpointBudgetV1::default()).unwrap();
    let mut marker = vec![0, 0, 0, 16];
    marker.extend_from_slice(&[9; 16]);
    let offsets: Vec<_> = bytes
        .windows(marker.len())
        .enumerate()
        .filter_map(|(index, value)| (value == marker).then_some(index))
        .collect();
    assert_eq!(offsets.len(), 2);
    for (offset, replacement) in [(offsets[0], 5), (offsets[1], 6)] {
        let mut forged = bytes.clone();
        forged[offset + 4..offset + 20].fill(replacement);
        assert!(matches!(
            Engine::recover_checkpoint(&forged, Limits::default(), CheckpointBudgetV1::default()),
            Err(CheckpointErrorV1::InvalidState)
        ));
    }
    let mut zero_cursor = bytes.clone();
    let length = zero_cursor.len();
    zero_cursor[length - 8..].fill(0);
    assert!(matches!(
        Engine::recover_checkpoint(
            &zero_cursor,
            Limits::default(),
            CheckpointBudgetV1::default()
        ),
        Err(CheckpointErrorV1::InvalidState)
    ));
}
