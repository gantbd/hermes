use aegis_authority::{EngineId, Limits as AegisLimits, PrincipalId, SessionId};
use aegis_client::{AuthorityClient, ServicePort};
use aegis_host::{DurableAuthority, HostError};
use aegis_protocol::{
    ActionV1, CommandV1 as AegisCommand, EngineNamespaceV1, OutcomeV1 as AegisOutcome,
    ReplyV1 as AegisReply, ScopeV1, SessionHandleV1,
};
use aegis_service::{
    AuthenticatedSessionV1 as AegisSession, Service as AegisService,
    ServiceLimitsV1 as AegisServiceLimits,
};
use hermes_aegis::{AegisAuthorizer, AuthorizationTime, SERVICE_RESOURCE_V1};
use hermes_host::DurableLedger;
use hermes_ledger::{Limits, persistence::CheckpointBudgetV1};
use hermes_protocol::{
    CommandV1, CorrelationIdV1, EventEnvelopeV1, IdempotencyKeyV1, OutcomeV1, ProducerIdV1,
    RefusalV1, ReplyV1, SchemaIdV1, StreamIdV1,
};
use hermes_service::{AuthenticatedSessionV1, Service, ServiceLimitsV1};
use keystone_storage::{
    AcknowledgementStrength, Checkpoint, CheckpointStore, CommitOutcome, CommitReceipt, Revision,
    StoreRefusal,
};
use std::convert::Infallible;

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
            mechanism: "hermes-aegis-test",
            strength: AcknowledgementStrength::Durable,
        }))
    }
}

struct AegisPort(AegisService<Store>);
impl ServicePort for AegisPort {
    type Error = HostError<Infallible>;
    fn execute(
        &mut self,
        session: SessionHandleV1,
        command: AegisCommand,
    ) -> Result<AegisReply, Self::Error> {
        self.0.execute(session, command)
    }
}

struct Time(u64);
impl AuthorizationTime for Time {
    fn now(&mut self) -> u64 {
        self.0
    }
}

fn event() -> EventEnvelopeV1 {
    EventEnvelopeV1 {
        producer: ProducerIdV1([7; 16]),
        schema: SchemaIdV1([8; 16]),
        idempotency_key: IdempotencyKeyV1([9; 16]),
        correlation: CorrelationIdV1([10; 16]),
        causation: None,
        payload: vec![11],
    }
}

#[test]
fn public_aegis_client_authorizes_exact_stream_action_and_denies_other_actions() {
    let authority_id = EngineId::new([1; 16]);
    let authority =
        DurableAuthority::create_new(Store::default(), authority_id, AegisLimits::default())
            .unwrap();
    let mut aegis =
        AegisService::new(authority, authority_id, AegisServiceLimits::default()).unwrap();
    let subject = [2; 16];
    let aegis_session = aegis
        .open_session(AegisSession {
            principal: PrincipalId::new(subject),
            session: SessionId::new([3; 16]),
            administrator: true,
        })
        .unwrap();
    let stream = StreamIdV1([4; 16]);
    let AegisReply::Accepted(AegisOutcome::GrantIssued(grant)) = aegis
        .execute(
            aegis_session,
            AegisCommand::Issue {
                expected_epoch: 0,
                now: 1,
                expires_at: 10,
                scopes: vec![ScopeV1 {
                    engine: EngineNamespaceV1::Hermes,
                    resource: stream.0,
                    actions: 1 << ActionV1::Submit as u8,
                }],
            },
        )
        .unwrap()
    else {
        panic!("submit grant expected")
    };
    let authorizer = AegisAuthorizer::new(
        AuthorityClient::new(aegis_session, AegisPort(aegis)),
        grant.handle,
        subject,
        Time(2),
    );
    let ledger = DurableLedger::create_new(
        Store::default(),
        Limits::default(),
        CheckpointBudgetV1::default(),
    )
    .unwrap();
    let mut hermes = Service::new(ledger, authorizer, ServiceLimitsV1::default()).unwrap();
    let session = hermes
        .open_session(AuthenticatedSessionV1 { subject })
        .unwrap();
    assert!(matches!(
        hermes
            .execute(
                session,
                CommandV1::Append {
                    stream,
                    expected_predecessor: None,
                    envelope: event(),
                },
            )
            .unwrap(),
        ReplyV1::Accepted(OutcomeV1::Append(_))
    ));
    assert_eq!(
        hermes
            .execute(
                session,
                CommandV1::Read {
                    stream,
                    start: hermes_protocol::EventSequenceV1(0),
                    limit: 1,
                },
            )
            .unwrap(),
        ReplyV1::Refused(RefusalV1::NotAuthorized)
    );
    assert_eq!(
        hermes
            .execute(
                session,
                CommandV1::Append {
                    stream: StreamIdV1(SERVICE_RESOURCE_V1),
                    expected_predecessor: None,
                    envelope: event(),
                },
            )
            .unwrap(),
        ReplyV1::Refused(RefusalV1::NotAuthorized)
    );
    let wrong_subject = hermes
        .open_session(AuthenticatedSessionV1 { subject: [6; 16] })
        .unwrap();
    assert_eq!(
        hermes
            .execute(
                wrong_subject,
                CommandV1::Append {
                    stream,
                    expected_predecessor: Some(hermes_protocol::EventSequenceV1(0)),
                    envelope: EventEnvelopeV1 {
                        idempotency_key: IdempotencyKeyV1([12; 16]),
                        ..event()
                    },
                },
            )
            .unwrap(),
        ReplyV1::Refused(RefusalV1::NotAuthorized)
    );
}
