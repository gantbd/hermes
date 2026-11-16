use hermes_client::{
    AppendClient, ConsumerClient, DisclosureClient, OperatorClient, ReadOnlyClient, ServicePort,
};
use hermes_protocol::{
    CausationIdV1, CommandV1, ConsumerIdV1, CorrelationIdV1, EventEnvelopeV1, EventSequenceV1,
    IdempotencyKeyV1, ProducerIdV1, ReplyV1, SchemaIdV1, SessionHandleV1, StreamIdV1,
};

#[derive(Default)]
struct Port(Vec<(SessionHandleV1, CommandV1)>);
impl ServicePort for Port {
    type Error = ();
    fn execute(
        &mut self,
        session: SessionHandleV1,
        command: CommandV1,
    ) -> Result<ReplyV1, Self::Error> {
        self.0.push((session, command));
        Ok(ReplyV1::Refused(hermes_protocol::RefusalV1::NotAuthorized))
    }
}

fn event() -> EventEnvelopeV1 {
    EventEnvelopeV1 {
        producer: ProducerIdV1([1; 16]),
        schema: SchemaIdV1([2; 16]),
        idempotency_key: IdempotencyKeyV1([3; 16]),
        correlation: CorrelationIdV1([4; 16]),
        causation: Some(CausationIdV1([5; 16])),
        payload: vec![6],
    }
}

#[test]
fn capability_facades_emit_only_their_command_families() {
    let session = SessionHandleV1(7);
    let stream = StreamIdV1([8; 16]);
    let consumer = ConsumerIdV1([9; 16]);
    let mut append = AppendClient::new(session, Port::default());
    append.append(stream, None, event()).unwrap();
    assert!(matches!(
        append.into_port().0[0].1,
        CommandV1::Append { .. }
    ));

    let mut reader = ConsumerClient::new(session, Port::default(), 4).unwrap();
    assert!(reader.read(stream, EventSequenceV1(0), 5).is_err());
    reader.read(stream, EventSequenceV1(0), 4).unwrap();
    reader
        .acknowledge(consumer, stream, EventSequenceV1(0))
        .unwrap();
    reader.cursor(consumer, stream).unwrap();
    assert_eq!(reader.into_port().0.len(), 3);

    let mut disclosure = DisclosureClient::new(session, Port::default(), 2).unwrap();
    assert!(disclosure.read(stream, EventSequenceV1(0), 3).is_err());
    disclosure.read(stream, EventSequenceV1(0), 2).unwrap();
    let commands = disclosure.into_port().0;
    assert_eq!(commands.len(), 1);
    assert!(matches!(commands[0].1, CommandV1::Read { .. }));

    let mut operator = OperatorClient::new(session, Port::default());
    operator.compact(stream, EventSequenceV1(0)).unwrap();
    assert!(matches!(
        operator.into_port().0[0].1,
        CommandV1::Compact { .. }
    ));

    let mut observer = ReadOnlyClient::new(session, Port::default());
    observer.status().unwrap();
    observer.usage().unwrap();
    observer.stream_observation(stream).unwrap();
    observer.cursor(consumer, stream).unwrap();
    observer.signals().unwrap();
    let commands = observer.into_port().0;
    assert_eq!(commands.len(), 5);
    assert!(matches!(commands[3].1, CommandV1::Cursor { .. }));
}
