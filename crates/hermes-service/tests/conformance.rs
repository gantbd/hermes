use hermes_conformance::{ConformanceHarnessV1, qualify_inbox_v1, qualify_outbox_v1};
use hermes_host::DurableLedger;
use hermes_ledger::{Limits, persistence::CheckpointBudgetV1};
use hermes_protocol::{CommandV1, ReplyV1, SessionHandleV1};
use hermes_service::{
    AuthenticatedSessionV1, AuthorizationActionV1, AuthorizationResourceV1, Authorizer, Service,
    ServiceErrorV1, ServiceLimitsV1,
};
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
        let actual = self.0.as_ref().map(|checkpoint| checkpoint.revision);
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
            mechanism: "hermes-conformance",
            strength: AcknowledgementStrength::Durable,
        }))
    }
}

struct Allow;
impl Authorizer for Allow {
    type Error = Infallible;

    fn authorize(
        &mut self,
        _: [u8; 16],
        _: AuthorizationActionV1,
        _: AuthorizationResourceV1,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

type PublicService = Service<Store, Allow>;

struct Harness {
    service: Option<PublicService>,
    session: SessionHandleV1,
    limits: Limits,
    budget: CheckpointBudgetV1,
}

impl Harness {
    fn new() -> Self {
        let limits = Limits {
            max_read_batch: 8,
            ..Limits::default()
        };
        let budget = CheckpointBudgetV1::default();
        let ledger = DurableLedger::create_new(Store::default(), limits, budget).unwrap();
        let mut service = Service::new(
            ledger,
            Allow,
            ServiceLimitsV1 {
                max_sessions: 1,
                max_read_batch: 8,
            },
        )
        .unwrap();
        let session = service
            .open_session(AuthenticatedSessionV1 {
                subject: [0x11; 16],
            })
            .unwrap();
        Self {
            service: Some(service),
            session,
            limits,
            budget,
        }
    }
}

impl ConformanceHarnessV1 for Harness {
    type Error = ServiceErrorV1<Infallible, Infallible>;

    fn execute(&mut self, command: CommandV1) -> Result<ReplyV1, Self::Error> {
        self.service
            .as_mut()
            .expect("service is present")
            .execute(self.session, command)
    }

    fn dispatch_without_reply(&mut self, command: CommandV1) -> Result<(), Self::Error> {
        let _reply = self.execute(command)?;
        Ok(())
    }

    fn restart(&mut self) -> Result<(), Self::Error> {
        let service = self.service.take().expect("service is present");
        let (ledger, _) = service.into_parts();
        let ledger = DurableLedger::open(ledger.into_store(), self.limits, self.budget).unwrap();
        let mut service = Service::new(
            ledger,
            Allow,
            ServiceLimitsV1 {
                max_sessions: 1,
                max_read_batch: 8,
            },
        )
        .unwrap();
        self.session = service
            .open_session(AuthenticatedSessionV1 {
                subject: [0x11; 16],
            })
            .unwrap();
        self.service = Some(service);
        Ok(())
    }
}

#[test]
fn public_service_passes_reusable_inbox_and_outbox_contracts() {
    let mut harness = Harness::new();
    qualify_outbox_v1(&mut harness);
    qualify_inbox_v1(&mut harness);
}
