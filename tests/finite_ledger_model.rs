//! Independent linear reference model, exhaustively comparing 10^5 small programs.
//! This checks the declared finite domain, not arbitrary histories or storage.
use hermes_ledger::{
    AppendOutcomeV1, CompactionOutcomeV1, ConsumerId, CorrelationId, Engine, EventEnvelopeV1,
    EventSequence, IdempotencyKey, Limits, ProducerId, ReadGapV1, RefusalV1, SchemaId, StreamId,
    persistence::CheckpointBudgetV1,
};

#[derive(Debug, Eq, PartialEq)]
enum Observation {
    Append(AppendOutcomeV1),
    Cursor(u64),
    Read(
        usize,
        Option<EventSequence>,
        EventSequence,
        Option<ReadGapV1>,
    ),
    Compact(CompactionOutcomeV1),
}

#[derive(Default)]
struct Reference {
    // Chronological linear history; no engine maps or replay index are reused.
    events: Vec<(usize, u8, u8)>,
    cursors: [Option<u64>; 2],
    retained_from: [u64; 2],
}

fn append_input(action: u32) -> (usize, u8, u8, Option<EventSequence>) {
    match action {
        0 => (0, 0, 0, None),
        1 => (0, 1, 1, None),
        2 => (0, 1, 1, Some(EventSequence(0))),
        3 => (1, 0, 0, None),
        4 => (0, 0, 1, Some(EventSequence(0))),
        _ => unreachable!("closed test command family"),
    }
}

impl Reference {
    fn count(&self, stream: usize) -> usize {
        self.events.iter().filter(|event| event.0 == stream).count()
    }
    fn edge(&self, stream: usize) -> Option<EventSequence> {
        self.count(stream)
            .checked_sub(1)
            .map(|edge| EventSequence(edge as u64))
    }
    fn apply(&mut self, action: u32) -> Result<Observation, RefusalV1> {
        match action {
            0..=4 => {
                let (stream, key, payload, expected) = append_input(action);
                if let Some(index) = self.events.iter().position(|event| event.1 == key) {
                    let existing = self.events[index];
                    let ordinal = self.events[..index]
                        .iter()
                        .filter(|event| event.0 == existing.0)
                        .count();
                    return Ok(Observation::Append(
                        if (ordinal as u64) < self.retained_from[existing.0] {
                            AppendOutcomeV1::ReplayHistoryLost(EventSequence(ordinal as u64))
                        } else if existing == (stream, key, payload) {
                            AppendOutcomeV1::Replayed(EventSequence(ordinal as u64))
                        } else {
                            AppendOutcomeV1::Conflict
                        },
                    ));
                }
                let actual = self.edge(stream);
                if expected != actual {
                    return Err(RefusalV1::PredecessorMismatch { expected, actual });
                }
                if self.count(stream) == 2 {
                    return Err(RefusalV1::StreamEventLimit);
                }
                if self.events.len() == 2 {
                    return Err(RefusalV1::IdempotencyLimit);
                }
                let sequence = self.count(stream) as u64;
                self.events.push((stream, key, payload));
                Ok(Observation::Append(AppendOutcomeV1::Appended(
                    EventSequence(sequence),
                )))
            }
            5..=6 => {
                if self.count(0) == 0 {
                    return Err(RefusalV1::UnknownStream);
                }
                let consumer = (action - 5) as usize;
                if self.cursors[consumer].is_none() && self.retained_from[0] > 0 {
                    return Err(RefusalV1::CursorBehindRetention {
                        retained_from: EventSequence(self.retained_from[0]),
                    });
                }
                self.cursors[consumer] = Some(1);
                Ok(Observation::Cursor(1))
            }
            7 => {
                if self.count(0) == 0 {
                    return Err(RefusalV1::UnknownStream);
                }
                let retained = self.retained_from[0];
                Ok(Observation::Read(
                    usize::from(self.count(0) as u64 > retained),
                    self.edge(0),
                    EventSequence(retained),
                    (retained > 0).then_some(ReadGapV1::BeforeRetention {
                        requested: EventSequence(0),
                        retained_from: EventSequence(retained),
                    }),
                ))
            }
            8 => {
                if self.count(0) == 0 {
                    return Err(RefusalV1::UnknownStream);
                }
                if self.cursors.iter().flatten().any(|next| *next < 1) {
                    return Err(RefusalV1::CompactionBlocked);
                }
                if self.retained_from[0] >= 1 {
                    return Ok(Observation::Compact(CompactionOutcomeV1::Identity {
                        retained_from: EventSequence(self.retained_from[0]),
                    }));
                }
                self.retained_from[0] = 1;
                Ok(Observation::Compact(CompactionOutcomeV1::Compacted {
                    removed: 1,
                    retained_from: EventSequence(1),
                }))
            }
            9 => {
                if self.count(0) == 0 {
                    return Err(RefusalV1::UnknownStream);
                }
                Ok(Observation::Read(
                    0,
                    self.edge(0),
                    EventSequence(self.retained_from[0]),
                    Some(ReadGapV1::AheadOfPublication {
                        requested: EventSequence(3),
                        next_sequence: EventSequence(self.count(0) as u64),
                    }),
                ))
            }
            _ => unreachable!("closed test command family"),
        }
    }
}

fn apply(engine: &mut Engine, action: u32) -> Result<Observation, RefusalV1> {
    match action {
        0..=4 => {
            let (stream, key, payload, predecessor) = append_input(action);
            engine
                .append(
                    StreamId::new([stream as u8; 16]),
                    predecessor,
                    EventEnvelopeV1::new(
                        ProducerId::new([1; 16]),
                        SchemaId::new([2; 16]),
                        IdempotencyKey::new([key; 16]),
                        CorrelationId::new([3; 16]),
                        None,
                        vec![payload],
                    ),
                )
                .map(Observation::Append)
        }
        5..=6 => engine
            .acknowledge(
                ConsumerId::new([(action - 5) as u8; 16]),
                StreamId::new([0; 16]),
                EventSequence(0),
            )
            .map(|cursor| Observation::Cursor(cursor.next_sequence.0)),
        7 => engine
            .read(StreamId::new([0; 16]), EventSequence(0), 1)
            .map(|batch| {
                Observation::Read(
                    batch.events.len(),
                    batch.published_edge,
                    batch.retained_from,
                    batch.gap,
                )
            }),
        8 => engine
            .compact_through(StreamId::new([0; 16]), EventSequence(0))
            .map(Observation::Compact),
        9 => engine
            .read(StreamId::new([0; 16]), EventSequence(3), 1)
            .map(|batch| {
                Observation::Read(
                    batch.events.len(),
                    batch.published_edge,
                    batch.retained_from,
                    batch.gap,
                )
            }),
        _ => unreachable!("closed test command family"),
    }
}

#[test]
fn all_five_step_programs_preserve_reference_results_and_restart_correspondence() {
    let limits = Limits {
        max_streams: 2,
        max_events_per_stream: 2,
        max_idempotency_records: 2,
        max_consumers: 2,
        max_payload_bytes: 8,
        max_read_batch: 1,
    };
    for program in 0_u32..10_u32.pow(5) {
        let mut engine = Engine::try_new(limits).unwrap();
        let mut reference = Reference::default();
        let mut word = program;
        for step in 0..5 {
            let action = word % 10;
            word /= 10;
            let before = engine.checkpoint(CheckpointBudgetV1::default()).unwrap();
            let expected = reference.apply(action);
            let actual = apply(&mut engine, action);
            assert_eq!(actual, expected, "program {program}, step {step}");
            let after = engine.checkpoint(CheckpointBudgetV1::default()).unwrap();
            if matches!(
                actual,
                Err(_)
                    | Ok(Observation::Append(
                        AppendOutcomeV1::Conflict | AppendOutcomeV1::Replayed(_)
                    ))
                    | Ok(Observation::Read(..))
                    | Ok(Observation::Compact(CompactionOutcomeV1::Identity { .. }))
            ) {
                assert_eq!(before, after);
            }
            engine =
                Engine::recover_checkpoint(&after, limits, CheckpointBudgetV1::default()).unwrap();
            assert_eq!(
                engine.checkpoint(CheckpointBudgetV1::default()).unwrap(),
                after
            );
            for consumer in 0..2 {
                assert_eq!(
                    engine
                        .consumer_cursor(
                            ConsumerId::new([consumer as u8; 16]),
                            StreamId::new([0; 16])
                        )
                        .map(|cursor| cursor.next_sequence.0),
                    reference.cursors[consumer]
                );
            }
            for stream in 0..2 {
                match engine.read(StreamId::new([stream as u8; 16]), EventSequence(0), 1) {
                    Ok(batch) => {
                        assert_eq!(batch.published_edge, reference.edge(stream));
                        assert_eq!(
                            batch.retained_from,
                            EventSequence(reference.retained_from[stream])
                        );
                        let expected = reference
                            .events
                            .iter()
                            .filter(|event| event.0 == stream)
                            .nth(reference.retained_from[stream] as usize);
                        assert_eq!(
                            batch
                                .events
                                .first()
                                .map(|event| event.envelope.payload()[0]),
                            expected.map(|event| event.2)
                        );
                    }
                    Err(error) => {
                        assert_eq!(error, RefusalV1::UnknownStream);
                        assert_eq!(reference.count(stream), 0);
                    }
                }
            }
        }
    }
}
