//! Private Unix listener with explicit authentication and owned-path cleanup.

use crate::{ConnectionErrorV1, ServicePort, serve_up_to};
use hermes_service::AuthenticatedSessionV1;
use hermes_wire::{LimitsV1, ReadyV1, encode_ready};
use keystone_unix_listener::OwnedUnixListener;
use std::{
    io::{self, Write},
    os::unix::net::UnixStream,
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

pub trait Authenticator {
    type Error;
    fn authenticate(&mut self, stream: &UnixStream) -> Result<AuthenticatedSessionV1, Self::Error>;
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HealthV1 {
    pub ready: bool,
    pub active_connections: u64,
    pub accepted_connections: u64,
    pub refused_connections: u64,
    pub outcome_unknown: u64,
    pub saturated: bool,
}

#[derive(Debug)]
pub enum DaemonErrorV1<A, S> {
    Io(io::Error),
    Authentication(A),
    SessionCapacity,
    Connection(ConnectionErrorV1<S>),
}

pub struct Daemon<P: ServicePort, A: Authenticator> {
    listener: OwnedUnixListener,
    service: P,
    authenticator: A,
    wire_limits: LimitsV1,
    io_timeout: Duration,
    max_requests: usize,
    health: HealthV1,
}

impl<P: ServicePort, A: Authenticator> Daemon<P, A> {
    pub fn bind_new(
        socket_path: impl AsRef<Path>,
        service: P,
        authenticator: A,
        wire_limits: LimitsV1,
        io_timeout: Duration,
        max_requests: usize,
    ) -> io::Result<Self> {
        if io_timeout.is_zero() || max_requests == 0 || wire_limits.max_frame_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "disabled limit",
            ));
        }
        let listener = OwnedUnixListener::bind_new(socket_path)?;
        Ok(Self {
            listener,
            service,
            authenticator,
            wire_limits,
            io_timeout,
            max_requests,
            health: HealthV1 {
                ready: true,
                ..HealthV1::default()
            },
        })
    }

    pub fn health(&self) -> HealthV1 {
        self.health
    }

    pub fn serve_next(&mut self) -> Result<(), DaemonErrorV1<A::Error, P::Error>> {
        let (mut stream, _) = self.listener.accept().map_err(DaemonErrorV1::Io)?;
        self.serve_accepted(&mut stream)
    }

    /// Serves complete authenticated connections until a caller-owned stop
    /// flag is set. A shutdown requester wakes the blocking accept by making a
    /// local connection after setting the flag; that wake connection is never
    /// authenticated or dispatched.
    pub fn serve_until(
        &mut self,
        stop: &AtomicBool,
    ) -> Result<(), DaemonErrorV1<A::Error, P::Error>> {
        loop {
            if stop.load(Ordering::Acquire) {
                return Ok(());
            }
            let (mut stream, _) = self.listener.accept().map_err(DaemonErrorV1::Io)?;
            if stop.load(Ordering::Acquire) {
                return Ok(());
            }
            self.serve_accepted(&mut stream)?;
        }
    }

    fn serve_accepted(
        &mut self,
        stream: &mut UnixStream,
    ) -> Result<(), DaemonErrorV1<A::Error, P::Error>> {
        increment(
            &mut self.health.active_connections,
            &mut self.health.saturated,
        );
        let result = self.serve_stream(stream);
        self.health.active_connections = self.health.active_connections.saturating_sub(1);
        if result.is_err() {
            increment(
                &mut self.health.refused_connections,
                &mut self.health.saturated,
            );
        }
        if matches!(
            &result,
            Err(DaemonErrorV1::Connection(
                ConnectionErrorV1::OutcomeUnknown(_)
            ))
        ) {
            increment(&mut self.health.outcome_unknown, &mut self.health.saturated);
        }
        result
    }

    fn serve_stream(
        &mut self,
        stream: &mut UnixStream,
    ) -> Result<(), DaemonErrorV1<A::Error, P::Error>> {
        stream
            .set_read_timeout(Some(self.io_timeout))
            .and_then(|()| stream.set_write_timeout(Some(self.io_timeout)))
            .map_err(DaemonErrorV1::Io)?;
        let facts = self
            .authenticator
            .authenticate(stream)
            .map_err(DaemonErrorV1::Authentication)?;
        let session = self
            .service
            .open_session(facts)
            .ok_or(DaemonErrorV1::SessionCapacity)?;
        let result = (|| {
            write_ready(stream, self.wire_limits, self.max_requests).map_err(DaemonErrorV1::Io)?;
            serve_up_to(
                stream,
                session,
                &mut self.service,
                self.wire_limits,
                self.max_requests,
            )
            .map(|_| ())
            .map_err(DaemonErrorV1::Connection)
        })();
        self.service.close_session(session);
        if result.is_ok() {
            increment(
                &mut self.health.accepted_connections,
                &mut self.health.saturated,
            );
        }
        result
    }
}

fn write_ready(stream: &mut UnixStream, limits: LimitsV1, max_requests: usize) -> io::Result<()> {
    let value = ReadyV1 {
        protocol_version: 1,
        max_frame_bytes: u32::try_from(limits.max_frame_bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame limit"))?,
        max_requests: u32::try_from(max_requests)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "request limit"))?,
    };
    let bytes = encode_ready(value, limits)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "ready frame"))?;
    let length = u32::try_from(bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "ready length"))?;
    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(&bytes)?;
    stream.flush()
}

fn increment(value: &mut u64, saturated: &mut bool) {
    if let Some(next) = value.checked_add(1) {
        *value = next;
    } else {
        *saturated = true;
    }
}
