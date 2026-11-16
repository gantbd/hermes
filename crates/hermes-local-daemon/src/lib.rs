//! Bounded length-prefixed connection boundary for Hermes.

use hermes_protocol::{CommandV1, ReplyV1, SessionHandleV1};
use hermes_service::AuthenticatedSessionV1;
use hermes_wire::{CodecErrorV1, LimitsV1, decode_request, encode_reply};
use keystone_framing::FrameError;
use std::io::{Read, Write};

#[cfg(unix)]
pub mod unix;

pub trait ServicePort {
    type Error;
    fn open_session(&mut self, facts: AuthenticatedSessionV1) -> Option<SessionHandleV1>;
    fn close_session(&mut self, session: SessionHandleV1) -> bool;
    fn execute(
        &mut self,
        session: SessionHandleV1,
        command: CommandV1,
    ) -> Result<ReplyV1, Self::Error>;
}

impl<S, A> ServicePort for hermes_service::Service<S, A>
where
    S: keystone_storage::CheckpointStore,
    A: hermes_service::Authorizer,
{
    type Error = hermes_service::ServiceErrorV1<S::Error, A::Error>;
    fn open_session(&mut self, facts: AuthenticatedSessionV1) -> Option<SessionHandleV1> {
        hermes_service::Service::open_session(self, facts)
    }
    fn close_session(&mut self, session: SessionHandleV1) -> bool {
        hermes_service::Service::close_session(self, session)
    }
    fn execute(
        &mut self,
        session: SessionHandleV1,
        command: CommandV1,
    ) -> Result<ReplyV1, Self::Error> {
        hermes_service::Service::execute(self, session, command)
    }
}

pub type ConnectionErrorV1<E> = keystone_framing::ConnectionError<CodecErrorV1, E>;

pub fn serve_up_to<IO: Read + Write, P: ServicePort>(
    io: &mut IO,
    session: SessionHandleV1,
    service: &mut P,
    limits: LimitsV1,
    max_requests: usize,
) -> Result<usize, ConnectionErrorV1<P::Error>> {
    keystone_framing::serve_up_to(
        io,
        limits.max_frame_bytes,
        max_requests,
        |bytes| decode_request(bytes, limits),
        |command| service.execute(session, command),
        |reply| encode_reply(reply, limits),
        map_frame_error,
    )
}

fn map_frame_error(error: FrameError) -> CodecErrorV1 {
    match error {
        FrameError::Capacity => CodecErrorV1::Capacity,
        FrameError::RequestLimit | FrameError::FrameLimit => CodecErrorV1::Limit,
    }
}
