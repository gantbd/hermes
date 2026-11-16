use hermes_local_daemon::{ConnectionErrorV1, ServicePort, serve_up_to};
use hermes_protocol::{CommandV1, OutcomeV1, ReplyV1, SessionHandleV1, StatusV1};
use hermes_service::AuthenticatedSessionV1;
use hermes_wire::{LimitsV1, decode_reply, encode_request};
use std::io::{Cursor, Read, Result as IoResult, Write};

#[derive(Default)]
struct Port {
    calls: usize,
}
impl ServicePort for Port {
    type Error = ();
    fn open_session(&mut self, _: AuthenticatedSessionV1) -> Option<SessionHandleV1> {
        Some(SessionHandleV1(1))
    }
    fn close_session(&mut self, _: SessionHandleV1) -> bool {
        true
    }
    fn execute(&mut self, _: SessionHandleV1, _: CommandV1) -> Result<ReplyV1, Self::Error> {
        self.calls += 1;
        Ok(ReplyV1::Accepted(OutcomeV1::Status(StatusV1 {
            storage_revision: Some(2),
            fenced: false,
        })))
    }
}

struct Duplex {
    input: Cursor<Vec<u8>>,
    output: Vec<u8>,
    fail_write: bool,
}
impl Read for Duplex {
    fn read(&mut self, bytes: &mut [u8]) -> IoResult<usize> {
        self.input.read(bytes)
    }
}
impl Write for Duplex {
    fn write(&mut self, bytes: &[u8]) -> IoResult<usize> {
        if self.fail_write {
            return Err(std::io::ErrorKind::BrokenPipe.into());
        }
        self.output.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}
fn framed(command: CommandV1) -> Vec<u8> {
    let bytes = encode_request(&command, LimitsV1::default()).unwrap();
    [(bytes.len() as u32).to_be_bytes().as_slice(), &bytes].concat()
}

#[test]
fn bounded_sequence_accepts_clean_eof_and_writes_replies() {
    let input = [framed(CommandV1::Status), framed(CommandV1::Usage)].concat();
    let mut io = Duplex {
        input: Cursor::new(input),
        output: Vec::new(),
        fail_write: false,
    };
    let mut port = Port::default();
    assert_eq!(
        serve_up_to(
            &mut io,
            SessionHandleV1(1),
            &mut port,
            LimitsV1::default(),
            4,
        )
        .unwrap(),
        2
    );
    assert_eq!(port.calls, 2);
    let first = u32::from_be_bytes(io.output[..4].try_into().unwrap()) as usize;
    assert!(matches!(
        decode_reply(&io.output[4..4 + first], LimitsV1::default()),
        Ok(ReplyV1::Accepted(OutcomeV1::Status(_)))
    ));
}

#[test]
fn reply_loss_is_outcome_unknown_after_exactly_one_dispatch() {
    let mut io = Duplex {
        input: Cursor::new(framed(CommandV1::Status)),
        output: Vec::new(),
        fail_write: true,
    };
    let mut port = Port::default();
    assert!(matches!(
        serve_up_to(
            &mut io,
            SessionHandleV1(1),
            &mut port,
            LimitsV1::default(),
            1,
        ),
        Err(ConnectionErrorV1::OutcomeUnknown(_))
    ));
    assert_eq!(port.calls, 1);
}
