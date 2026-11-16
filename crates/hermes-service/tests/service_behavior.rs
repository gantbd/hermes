use hermes_host::DurableLedger;
use hermes_ledger::{Limits, persistence::CheckpointBudgetV1};
use hermes_protocol::{
    AppendOutcomeV1, CausationIdV1, CommandV1, ConsumerIdV1, CorrelationIdV1, EventEnvelopeV1,
    EventSequenceV1, IdempotencyKeyV1, OutcomeV1, ProducerIdV1, ReadGapV1, RefusalV1, ReplyV1,
    SchemaIdV1, StreamIdV1,
};
use hermes_service::{
    AuthenticatedSessionV1, AuthorizationActionV1, AuthorizationResourceV1, Authorizer, Service,
    ServiceLimitsV1,
};
use keystone_storage::{
    AcknowledgementStrength, Checkpoint, CheckpointStore, CommitOutcome, CommitReceipt, Revision,
    StoreRefusal,
};
use std::{
    convert::Infallible,
    time::{Duration, Instant},
};

#[derive(Default)]
struct Store(Option<Checkpoint>);
impl CheckpointStore for Store {
    type Error = Infallible;
    fn load(&self, _: &str) -> Result<Option<Checkpoint>, Self::Error> {
        Ok(self.0.clone())
    }
    fn compare_exchange(
        &mut self,
        _: &str,
        expected: Option<Revision>,
        bytes: &[u8],
    ) -> Result<CommitOutcome, Self::Error> {
        let actual = self.0.as_ref().map(|value| value.revision);
        if actual != expected {
            return Ok(CommitOutcome::Refused(StoreRefusal::RevisionConflict {
                expected,
                actual,
            }));
        }
        let revision = Revision(actual.map_or(0, |value| value.0 + 1));
        self.0 = Some(Checkpoint {
            revision,
            bytes: bytes.to_vec(),
            strength: AcknowledgementStrength::Durable,
        });
        Ok(CommitOutcome::Committed(CommitReceipt {
            revision,
            mechanism: "hermes-service-test",
            strength: AcknowledgementStrength::Durable,
        }))
    }
}

#[derive(Default)]
struct Policy(Vec<([u8; 16], AuthorizationActionV1, AuthorizationResourceV1)>);
impl Authorizer for Policy {
    type Error = Infallible;
    fn authorize(
        &mut self,
        subject: [u8; 16],
        action: AuthorizationActionV1,
        resource: AuthorizationResourceV1,
    ) -> Result<bool, Self::Error> {
        self.0.push((subject, action, resource));
        Ok(subject == [1; 16])
    }
}

fn envelope(key: u8, payload: u8) -> EventEnvelopeV1 {
    EventEnvelopeV1 {
        producer: ProducerIdV1([2; 16]),
        schema: SchemaIdV1([3; 16]),
        idempotency_key: IdempotencyKeyV1([key; 16]),
        correlation: CorrelationIdV1([4; 16]),
        causation: Some(CausationIdV1([5; 16])),
        payload: vec![payload],
    }
}

#[test]
fn authorization_precedes_durable_work_and_gap_semantics_survive_reopen() {
    let limits = Limits {
        max_read_batch: 4,
        ..Limits::default()
    };
    let budget = CheckpointBudgetV1::default();
    let ledger = DurableLedger::create_new(Store::default(), limits, budget).unwrap();
    let mut service = Service::new(
        ledger,
        Policy::default(),
        ServiceLimitsV1 {
            max_sessions: 2,
            max_read_batch: 4,
        },
    )
    .unwrap();
    let allowed = service
        .open_session(AuthenticatedSessionV1 { subject: [1; 16] })
        .unwrap();
    let denied = service
        .open_session(AuthenticatedSessionV1 { subject: [9; 16] })
        .unwrap();
    let stream = StreamIdV1([6; 16]);
    let append0 = CommandV1::Append {
        stream,
        expected_predecessor: None,
        envelope: envelope(7, 10),
    };
    assert_eq!(
        service.execute(denied, append0.clone()).unwrap(),
        ReplyV1::Refused(RefusalV1::NotAuthorized)
    );
    assert_eq!(
        service.execute(allowed, CommandV1::Usage).unwrap(),
        ReplyV1::Accepted(OutcomeV1::Usage(hermes_protocol::UsageV1 {
            streams: 0,
            events: 0,
            idempotency_records: 0,
            compacted_records: 0,
            cursors: 0,
            saturated: false,
        }))
    );
    assert_eq!(
        service.execute(allowed, append0.clone()).unwrap(),
        ReplyV1::Accepted(OutcomeV1::Append(AppendOutcomeV1::Appended(
            EventSequenceV1(0)
        )))
    );
    assert_eq!(
        service.execute(allowed, append0).unwrap(),
        ReplyV1::Accepted(OutcomeV1::Append(AppendOutcomeV1::Replayed(
            EventSequenceV1(0)
        )))
    );
    service
        .execute(
            allowed,
            CommandV1::Append {
                stream,
                expected_predecessor: Some(EventSequenceV1(0)),
                envelope: envelope(8, 11),
            },
        )
        .unwrap();
    assert!(matches!(
        service
            .execute(
                allowed,
                CommandV1::Compact {
                    stream,
                    through: EventSequenceV1(0),
                },
            )
            .unwrap(),
        ReplyV1::Accepted(OutcomeV1::Compacted(_))
    ));
    let (ledger, policy) = service.into_parts();
    assert!(policy.0.iter().any(|(_, action, resource)| {
        *action == AuthorizationActionV1::Append
            && *resource == AuthorizationResourceV1::Stream(stream)
    }));
    let store = ledger.into_store();

    let ledger = DurableLedger::open(store, limits, budget).unwrap();
    let mut reopened = Service::new(ledger, Policy::default(), ServiceLimitsV1::default()).unwrap();
    assert_eq!(
        reopened.execute(allowed, CommandV1::Status).unwrap(),
        ReplyV1::Refused(RefusalV1::UnknownSession)
    );
    let session = reopened
        .open_session(AuthenticatedSessionV1 { subject: [1; 16] })
        .unwrap();
    assert!(matches!(
        reopened
            .execute(
                session,
                CommandV1::Read {
                    stream,
                    start: EventSequenceV1(0),
                    limit: 4,
                },
            )
            .unwrap(),
        ReplyV1::Accepted(OutcomeV1::Read(batch))
            if batch.retained_from == EventSequenceV1(1)
                && matches!(batch.gap, Some(ReadGapV1::BeforeRetention {
                    requested: EventSequenceV1(0),
                    retained_from: EventSequenceV1(1),
                }))
                && batch.events.len() == 1
                && batch.events[0].envelope.payload == vec![11]
    ));
    assert_eq!(
        reopened
            .execute(
                session,
                CommandV1::Append {
                    stream,
                    expected_predecessor: Some(EventSequenceV1(1)),
                    envelope: envelope(7, 99),
                },
            )
            .unwrap(),
        ReplyV1::Accepted(OutcomeV1::Append(AppendOutcomeV1::ReplayHistoryLost(
            EventSequenceV1(0)
        )))
    );
    assert_eq!(
        reopened
            .execute(
                session,
                CommandV1::Acknowledge {
                    consumer: ConsumerIdV1([10; 16]),
                    stream,
                    through: EventSequenceV1(0),
                },
            )
            .unwrap(),
        ReplyV1::Refused(RefusalV1::CursorBehindRetention {
            retained_from: EventSequenceV1(1),
        })
    );
    assert!(matches!(
        reopened
            .execute(session, CommandV1::StreamObservation { stream })
            .unwrap(),
        ReplyV1::Accepted(OutcomeV1::StreamObservation(Some(value)))
            if value.retained_from == EventSequenceV1(1)
                && value.published_edge == Some(EventSequenceV1(1))
                && value.retained_events == 1
                && value.consumer_count == 0
                && value.stalled_consumers == 0
                && value.max_consumer_backlog == 0
    ));
    assert!(matches!(
        reopened.execute(session, CommandV1::Signals).unwrap(),
        ReplyV1::Accepted(OutcomeV1::Signals(value))
            if value.confirmed_usage.streams == 1
                && value.confirmed_usage.events == 1
                && value.last_checkpoint_bytes > 0
                && !value.fenced
    ));
    let (_, policy) = reopened.into_parts();
    assert!(policy.0.iter().any(|(_, action, resource)| {
        *action == AuthorizationActionV1::Observe
            && *resource == AuthorizationResourceV1::Stream(stream)
    }));
    assert!(policy.0.iter().any(|(_, action, resource)| {
        *action == AuthorizationActionV1::Observe && *resource == AuthorizationResourceV1::Service
    }));
}

#[test]
#[ignore = "opt-in deterministic append/replay/compaction/reopen service soak"]
fn five_thousand_service_lifecycles_preserve_replay_gaps_and_reopen() {
    for cycle in 0_u64..5_000 {
        let limits = Limits {
            max_read_batch: 4,
            ..Limits::default()
        };
        let budget = CheckpointBudgetV1::default();
        let ledger = DurableLedger::create_new(Store::default(), limits, budget).unwrap();
        let mut service = Service::new(
            ledger,
            Policy::default(),
            ServiceLimitsV1 {
                max_sessions: 1,
                max_read_batch: 4,
            },
        )
        .unwrap();
        let session = service
            .open_session(AuthenticatedSessionV1 { subject: [1; 16] })
            .unwrap();
        let stream = StreamIdV1([6; 16]);
        let first = CommandV1::Append {
            stream,
            expected_predecessor: None,
            envelope: envelope(7, 10),
        };
        assert!(matches!(
            service.execute(session, first.clone()).unwrap(),
            ReplyV1::Accepted(OutcomeV1::Append(AppendOutcomeV1::Appended(
                EventSequenceV1(0)
            )))
        ));
        assert!(matches!(
            service.execute(session, first).unwrap(),
            ReplyV1::Accepted(OutcomeV1::Append(AppendOutcomeV1::Replayed(
                EventSequenceV1(0)
            )))
        ));
        service
            .execute(
                session,
                CommandV1::Append {
                    stream,
                    expected_predecessor: Some(EventSequenceV1(0)),
                    envelope: envelope(8, 11),
                },
            )
            .unwrap();
        service
            .execute(
                session,
                CommandV1::Compact {
                    stream,
                    through: EventSequenceV1(0),
                },
            )
            .unwrap();
        let (ledger, _) = service.into_parts();
        let ledger = DurableLedger::open(ledger.into_store(), limits, budget).unwrap();
        let mut reopened =
            Service::new(ledger, Policy::default(), ServiceLimitsV1::default()).unwrap();
        let session = reopened
            .open_session(AuthenticatedSessionV1 { subject: [1; 16] })
            .unwrap();
        assert!(
            matches!(
                reopened
                    .execute(
                        session,
                        CommandV1::Read {
                            stream,
                            start: EventSequenceV1(0),
                            limit: 4,
                        },
                    )
                    .unwrap(),
                ReplyV1::Accepted(OutcomeV1::Read(batch))
                    if batch.events.len() == 1
                        && matches!(batch.gap, Some(ReadGapV1::BeforeRetention { .. }))
            ),
            "cycle {cycle}"
        );
    }
}

fn percentile(samples: &mut [Duration], percent: usize) -> Duration {
    assert!(!samples.is_empty());
    assert!((1..=100).contains(&percent));
    samples.sort_unstable();
    samples[(samples.len() * percent).div_ceil(100) - 1]
}

fn report(operation: &str, samples: &mut [Duration]) {
    let p50 = percentile(samples, 50);
    let p95 = percentile(samples, 95);
    let p99 = percentile(samples, 99);
    println!(
        "operation={operation} count={} p50_ns={} p95_ns={} p99_ns={} max_ns={}",
        samples.len(),
        p50.as_nanos(),
        p95.as_nanos(),
        p99.as_nanos(),
        samples.last().unwrap().as_nanos()
    );
    assert!(
        p95 <= Duration::from_millis(10),
        "{operation}: provisional p95 engineering budget exceeded"
    );
    assert!(
        p99 <= Duration::from_millis(25),
        "{operation}: provisional p99 engineering budget exceeded"
    );
}

#[test]
#[ignore = "explicit optimized reference-host performance acceptance gate"]
fn local_candidate_performance_gate() {
    if cfg!(debug_assertions) {
        panic!("qualification requires --release");
    }
    const SAMPLES: usize = 64;
    println!(
        "profile=receipt_gated_in_memory samples={SAMPLES} os={} arch={} optimized=true p95_budget_ms=10 p99_budget_ms=25",
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    let mut appends = Vec::with_capacity(SAMPLES);
    let mut reads = Vec::with_capacity(SAMPLES);
    let mut compactions = Vec::with_capacity(SAMPLES);
    let mut recoveries = Vec::with_capacity(SAMPLES);

    for _ in 0..SAMPLES {
        let limits = Limits {
            max_read_batch: 4,
            ..Limits::default()
        };
        let budget = CheckpointBudgetV1::default();
        let ledger = DurableLedger::create_new(Store::default(), limits, budget).unwrap();
        let mut service = Service::new(
            ledger,
            Policy::default(),
            ServiceLimitsV1 {
                max_sessions: 1,
                max_read_batch: 4,
            },
        )
        .unwrap();
        let session = service
            .open_session(AuthenticatedSessionV1 { subject: [1; 16] })
            .unwrap();
        let stream = StreamIdV1([6; 16]);

        let started = Instant::now();
        assert!(matches!(
            service
                .execute(
                    session,
                    CommandV1::Append {
                        stream,
                        expected_predecessor: None,
                        envelope: envelope(7, 10),
                    },
                )
                .unwrap(),
            ReplyV1::Accepted(OutcomeV1::Append(AppendOutcomeV1::Appended(
                EventSequenceV1(0)
            )))
        ));
        appends.push(started.elapsed());

        let started = Instant::now();
        assert!(matches!(
            service
                .execute(
                    session,
                    CommandV1::Read {
                        stream,
                        start: EventSequenceV1(0),
                        limit: 4,
                    },
                )
                .unwrap(),
            ReplyV1::Accepted(OutcomeV1::Read(batch)) if batch.events.len() == 1
        ));
        reads.push(started.elapsed());

        let started = Instant::now();
        assert!(matches!(
            service
                .execute(
                    session,
                    CommandV1::Compact {
                        stream,
                        through: EventSequenceV1(0),
                    },
                )
                .unwrap(),
            ReplyV1::Accepted(OutcomeV1::Compacted(_))
        ));
        compactions.push(started.elapsed());

        let (ledger, _) = service.into_parts();
        let started = Instant::now();
        let reopened = DurableLedger::open(ledger.into_store(), limits, budget).unwrap();
        recoveries.push(started.elapsed());
        assert_eq!(reopened.signals().usage.streams, 1);
    }

    report("append", &mut appends);
    report("read", &mut reads);
    report("compact", &mut compactions);
    report("recover", &mut recoveries);
}
