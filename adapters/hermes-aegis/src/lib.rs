//! Stream authorization through Aegis' public session-bound client contract.
//!
//! Grant handles and time-source mechanisms remain private to this adapter.

#![forbid(unsafe_code)]

use aegis_client::{AuthorityClient, ServicePort};
use aegis_protocol::{ActionV1, EngineNamespaceV1, GrantHandleV1, OutcomeV1, ReplyV1};
use hermes_service::{AuthorizationActionV1, AuthorizationResourceV1, Authorizer};

pub const SERVICE_RESOURCE_V1: [u8; 16] = *b"HERMESSERVICEV1!";

pub trait AuthorizationTime {
    fn now(&mut self) -> u64;
}

#[derive(Debug, Eq, PartialEq)]
pub enum AdapterErrorV1<E> {
    Transport(E),
    UnexpectedReply,
}

/// One authenticated subject, one Aegis session-owned grant and one explicit
/// authorization-time mechanism. Construction itself grants no authority.
pub struct AegisAuthorizer<P, T> {
    authority: AuthorityClient<P>,
    grant: GrantHandleV1,
    subject: [u8; 16],
    time: T,
}

impl<P: ServicePort, T: AuthorizationTime> AegisAuthorizer<P, T> {
    pub const fn new(
        authority: AuthorityClient<P>,
        grant: GrantHandleV1,
        subject: [u8; 16],
        time: T,
    ) -> Self {
        Self {
            authority,
            grant,
            subject,
            time,
        }
    }

    pub fn into_parts(self) -> (P, T) {
        (self.authority.into_port(), self.time)
    }
}

impl<P: ServicePort, T: AuthorizationTime> Authorizer for AegisAuthorizer<P, T> {
    type Error = AdapterErrorV1<P::Error>;

    fn authorize(
        &mut self,
        subject: [u8; 16],
        action: AuthorizationActionV1,
        resource: AuthorizationResourceV1,
    ) -> Result<bool, Self::Error> {
        if subject != self.subject {
            return Ok(false);
        }
        let (resource, aegis_action) = match resource {
            AuthorizationResourceV1::Service => (SERVICE_RESOURCE_V1, ActionV1::Observe),
            AuthorizationResourceV1::Stream(stream) => {
                if stream.0 == SERVICE_RESOURCE_V1 {
                    return Ok(false);
                }
                (stream.0, action_v1(action))
            }
        };
        let now = self.time.now();
        let reply = self
            .authority
            .authorize(
                self.grant,
                now,
                EngineNamespaceV1::Hermes,
                resource,
                aegis_action,
            )
            .map_err(AdapterErrorV1::Transport)?;
        match reply {
            ReplyV1::Accepted(OutcomeV1::Authorized(decision))
                if decision.engine == EngineNamespaceV1::Hermes
                    && decision.action == aegis_action
                    && decision.expires_at >= now =>
            {
                Ok(true)
            }
            ReplyV1::Refused(_) => Ok(false),
            ReplyV1::Accepted(_) => Err(AdapterErrorV1::UnexpectedReply),
        }
    }
}

const fn action_v1(value: AuthorizationActionV1) -> ActionV1 {
    match value {
        AuthorizationActionV1::Append => ActionV1::Submit,
        AuthorizationActionV1::Read => ActionV1::Observe,
        AuthorizationActionV1::Acknowledge => ActionV1::Modify,
        AuthorizationActionV1::Compact => ActionV1::Administer,
        AuthorizationActionV1::Observe => ActionV1::Observe,
    }
}
