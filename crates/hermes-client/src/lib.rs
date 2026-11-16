//! Capability-shaped clients bound to one authenticated Hermes session.

use hermes_protocol::{
    CommandV1, ConsumerIdV1, EventEnvelopeV1, EventSequenceV1, ReplyV1, SessionHandleV1, StreamIdV1,
};

pub trait ServicePort {
    type Error;
    fn execute(
        &mut self,
        session: SessionHandleV1,
        command: CommandV1,
    ) -> Result<ReplyV1, Self::Error>;
}

struct Client<P> {
    session: SessionHandleV1,
    port: P,
}

impl<P: ServicePort> Client<P> {
    const fn new(session: SessionHandleV1, port: P) -> Self {
        Self { session, port }
    }
    fn execute(&mut self, command: CommandV1) -> Result<ReplyV1, P::Error> {
        self.port.execute(self.session, command)
    }
    fn into_port(self) -> P {
        self.port
    }
}

pub struct AppendClient<P>(Client<P>);
impl<P: ServicePort> AppendClient<P> {
    pub const fn new(session: SessionHandleV1, port: P) -> Self {
        Self(Client::new(session, port))
    }
    pub fn append(
        &mut self,
        stream: StreamIdV1,
        expected_predecessor: Option<EventSequenceV1>,
        envelope: EventEnvelopeV1,
    ) -> Result<ReplyV1, P::Error> {
        self.0.execute(CommandV1::Append {
            stream,
            expected_predecessor,
            envelope,
        })
    }
    pub fn into_port(self) -> P {
        self.0.into_port()
    }
}

pub struct ConsumerClient<P> {
    client: Client<P>,
    max_read_batch: u64,
}

/// Bounded event disclosure without acknowledgement authority.
pub struct DisclosureClient<P> {
    client: Client<P>,
    max_read_batch: u64,
}
impl<P: ServicePort> DisclosureClient<P> {
    pub fn new(session: SessionHandleV1, port: P, max_read_batch: u64) -> Option<Self> {
        (max_read_batch > 0).then(|| Self {
            client: Client::new(session, port),
            max_read_batch,
        })
    }
    pub fn read(
        &mut self,
        stream: StreamIdV1,
        start: EventSequenceV1,
        limit: u64,
    ) -> Result<ReplyV1, ClientError<P::Error>> {
        if limit == 0 || limit > self.max_read_batch {
            return Err(ClientError::Limit);
        }
        self.client
            .execute(CommandV1::Read {
                stream,
                start,
                limit,
            })
            .map_err(ClientError::Transport)
    }
    pub fn into_port(self) -> P {
        self.client.into_port()
    }
}
impl<P: ServicePort> ConsumerClient<P> {
    pub fn new(session: SessionHandleV1, port: P, max_read_batch: u64) -> Option<Self> {
        (max_read_batch > 0).then(|| Self {
            client: Client::new(session, port),
            max_read_batch,
        })
    }
    pub fn read(
        &mut self,
        stream: StreamIdV1,
        start: EventSequenceV1,
        limit: u64,
    ) -> Result<ReplyV1, ClientError<P::Error>> {
        if limit == 0 || limit > self.max_read_batch {
            return Err(ClientError::Limit);
        }
        self.client
            .execute(CommandV1::Read {
                stream,
                start,
                limit,
            })
            .map_err(ClientError::Transport)
    }
    pub fn acknowledge(
        &mut self,
        consumer: ConsumerIdV1,
        stream: StreamIdV1,
        through: EventSequenceV1,
    ) -> Result<ReplyV1, ClientError<P::Error>> {
        self.client
            .execute(CommandV1::Acknowledge {
                consumer,
                stream,
                through,
            })
            .map_err(ClientError::Transport)
    }
    pub fn cursor(
        &mut self,
        consumer: ConsumerIdV1,
        stream: StreamIdV1,
    ) -> Result<ReplyV1, ClientError<P::Error>> {
        self.client
            .execute(CommandV1::Cursor { consumer, stream })
            .map_err(ClientError::Transport)
    }
    pub fn into_port(self) -> P {
        self.client.into_port()
    }
}

pub struct OperatorClient<P>(Client<P>);
impl<P: ServicePort> OperatorClient<P> {
    pub const fn new(session: SessionHandleV1, port: P) -> Self {
        Self(Client::new(session, port))
    }
    pub fn compact(
        &mut self,
        stream: StreamIdV1,
        through: EventSequenceV1,
    ) -> Result<ReplyV1, P::Error> {
        self.0.execute(CommandV1::Compact { stream, through })
    }
    pub fn into_port(self) -> P {
        self.0.into_port()
    }
}

pub struct ReadOnlyClient<P>(Client<P>);
impl<P: ServicePort> ReadOnlyClient<P> {
    pub const fn new(session: SessionHandleV1, port: P) -> Self {
        Self(Client::new(session, port))
    }
    pub fn status(&mut self) -> Result<ReplyV1, P::Error> {
        self.0.execute(CommandV1::Status)
    }
    pub fn usage(&mut self) -> Result<ReplyV1, P::Error> {
        self.0.execute(CommandV1::Usage)
    }
    pub fn stream_observation(&mut self, stream: StreamIdV1) -> Result<ReplyV1, P::Error> {
        self.0.execute(CommandV1::StreamObservation { stream })
    }
    pub fn cursor(
        &mut self,
        consumer: ConsumerIdV1,
        stream: StreamIdV1,
    ) -> Result<ReplyV1, P::Error> {
        self.0.execute(CommandV1::Cursor { consumer, stream })
    }
    pub fn signals(&mut self) -> Result<ReplyV1, P::Error> {
        self.0.execute(CommandV1::Signals)
    }
    pub fn into_port(self) -> P {
        self.0.into_port()
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum ClientError<E> {
    Limit,
    Transport(E),
}
