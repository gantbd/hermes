#![cfg(unix)]

use hermes_ledger::{
    CorrelationId, EventEnvelopeV1, IdempotencyKey, Limits, ProducerId, SchemaId, StreamId,
    persistence::CheckpointBudgetV1,
};
use hermes_protocol::{
    CommandV1, ConsumerIdV1, CorrelationIdV1, EventEnvelopeV1 as ProtocolEnvelopeV1,
    EventSequenceV1, IdempotencyKeyV1, OutcomeV1, ProducerIdV1, RefusalV1, ReplyV1, SchemaIdV1,
    StreamIdV1,
};
use hermes_wire::{LimitsV1, decode_ready, decode_reply, encode_request};
use nix::{
    sys::signal::{Signal, kill},
    unistd::{Pid, getegid, geteuid},
};
use std::{
    fs,
    io::{Read, Write},
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::Duration,
};

fn unique(kind: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "hermes-application-{kind}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

fn spawn(mode: &str, directory: &Path, socket: &Path, access: &[&str]) -> Child {
    let mut command = Command::new(env!("CARGO_BIN_EXE_hermes-daemon"));
    command
        .args([
            mode,
            directory.to_str().unwrap(),
            socket.to_str().unwrap(),
            &geteuid().as_raw().to_string(),
            &getegid().as_raw().to_string(),
            "07070707070707070707070707070707",
        ])
        .args(access)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    command.spawn().unwrap()
}

fn connect_when_ready(child: &mut Child, socket: &Path) -> UnixStream {
    for _ in 0..400 {
        if let Ok(stream) = UnixStream::connect(socket) {
            return stream;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("Hermes daemon stopped before readiness: {status}");
        }
        thread::sleep(Duration::from_millis(5));
    }
    panic!("Hermes daemon did not publish {}", socket.display());
}

fn frame(stream: &mut UnixStream) -> Vec<u8> {
    let mut header = [0_u8; 4];
    stream.read_exact(&mut header).unwrap();
    let mut bytes = vec![0_u8; u32::from_be_bytes(header) as usize];
    stream.read_exact(&mut bytes).unwrap();
    bytes
}

fn send(stream: &mut UnixStream, command: &CommandV1) -> ReplyV1 {
    let request = encode_request(command, LimitsV1::default()).unwrap();
    stream
        .write_all(&u32::try_from(request.len()).unwrap().to_be_bytes())
        .unwrap();
    stream.write_all(&request).unwrap();
    decode_reply(&frame(stream), LimitsV1::default()).unwrap()
}

fn stop(child: Child) {
    kill(
        Pid::from_raw(i32::try_from(child.id()).unwrap()),
        Signal::SIGTERM,
    )
    .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "Hermes daemon shutdown failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn standalone_create_and_reopen_are_read_only_and_cleanly_owned() {
    let directory = unique("ledger");
    let socket = unique("service.sock");
    let mut created = spawn("create", &directory, &socket, &["observer"]);
    let mut stream = connect_when_ready(&mut created, &socket);
    let ready = decode_ready(&frame(&mut stream), LimitsV1::default()).unwrap();
    assert_eq!(ready.protocol_version, 1);
    assert!(matches!(
        send(&mut stream, &CommandV1::Status),
        ReplyV1::Accepted(OutcomeV1::Status(_))
    ));
    assert_eq!(
        send(
            &mut stream,
            &CommandV1::Acknowledge {
                consumer: ConsumerIdV1([1; 16]),
                stream: StreamIdV1([2; 16]),
                through: EventSequenceV1(0),
            },
        ),
        ReplyV1::Refused(RefusalV1::NotAuthorized)
    );
    drop(stream);
    stop(created);
    assert!(!socket.exists());

    let mut reopened = spawn("open", &directory, &socket, &["observer"]);
    let mut stream = connect_when_ready(&mut reopened, &socket);
    decode_ready(&frame(&mut stream), LimitsV1::default()).unwrap();
    assert!(matches!(
        send(&mut stream, &CommandV1::Signals),
        ReplyV1::Accepted(OutcomeV1::Signals(_))
    ));
    drop(stream);
    stop(reopened);
    assert!(!socket.exists());
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn selected_operator_profile_acknowledges_and_compacts_only_its_exact_stream() {
    let directory = unique("operator-ledger");
    let socket = unique("operator.sock");
    let selected = [5; 16];
    let mut ledger =
        hermes_local::create_new(&directory, Limits::default(), CheckpointBudgetV1::default())
            .unwrap();
    ledger
        .append(
            StreamId::new(selected),
            None,
            EventEnvelopeV1::new(
                ProducerId::new([1; 16]),
                SchemaId::new([2; 16]),
                IdempotencyKey::new([3; 16]),
                CorrelationId::new([4; 16]),
                None,
                b"private-event".to_vec(),
            ),
        )
        .unwrap();
    drop(ledger);

    let mut operator = spawn(
        "open",
        &directory,
        &socket,
        &["operator", "05050505050505050505050505050505"],
    );
    let mut connection = connect_when_ready(&mut operator, &socket);
    decode_ready(&frame(&mut connection), LimitsV1::default()).unwrap();
    assert!(matches!(
        send(
            &mut connection,
            &CommandV1::Read {
                stream: StreamIdV1(selected),
                start: EventSequenceV1(0),
                limit: 1,
            },
        ),
        ReplyV1::Accepted(OutcomeV1::Read(_))
    ));
    assert_eq!(
        send(
            &mut connection,
            &CommandV1::Append {
                stream: StreamIdV1(selected),
                expected_predecessor: Some(EventSequenceV1(0)),
                envelope: ProtocolEnvelopeV1 {
                    producer: ProducerIdV1([1; 16]),
                    schema: SchemaIdV1([2; 16]),
                    idempotency_key: IdempotencyKeyV1([8; 16]),
                    correlation: CorrelationIdV1([4; 16]),
                    causation: None,
                    payload: b"denied-event".to_vec(),
                },
            },
        ),
        ReplyV1::Refused(RefusalV1::NotAuthorized)
    );
    assert!(matches!(
        send(
            &mut connection,
            &CommandV1::Acknowledge {
                consumer: ConsumerIdV1([9; 16]),
                stream: StreamIdV1(selected),
                through: EventSequenceV1(0),
            },
        ),
        ReplyV1::Accepted(OutcomeV1::Acknowledged(_))
    ));
    assert!(matches!(
        send(
            &mut connection,
            &CommandV1::Compact {
                stream: StreamIdV1(selected),
                through: EventSequenceV1(0),
            },
        ),
        ReplyV1::Accepted(OutcomeV1::Compacted(_))
    ));
    assert_eq!(
        send(
            &mut connection,
            &CommandV1::Read {
                stream: StreamIdV1([6; 16]),
                start: EventSequenceV1(0),
                limit: 1,
            },
        ),
        ReplyV1::Refused(RefusalV1::NotAuthorized)
    );
    drop(connection);
    stop(operator);
    assert!(!socket.exists());
    fs::remove_dir_all(directory).unwrap();
}
