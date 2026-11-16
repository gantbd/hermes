#![cfg(unix)]

use hermes_ledger::{Limits, persistence::CheckpointBudgetV1};
use hermes_local_daemon::unix::{Authenticator, Daemon};
use hermes_protocol::{
    AppendOutcomeV1, CommandV1, ConsumerIdV1, CorrelationIdV1, EventEnvelopeV1, EventSequenceV1,
    IdempotencyKeyV1, OutcomeV1, ProducerIdV1, ReadGapV1, ReplyV1, SchemaIdV1, StreamIdV1,
};
use hermes_service::{
    AuthenticatedSessionV1, AuthorizationActionV1, AuthorizationResourceV1, Authorizer, Service,
    ServiceLimitsV1,
};
use hermes_wire::{LimitsV1, decode_ready, decode_reply, encode_request};
use std::{
    convert::Infallible,
    fs,
    io::{Read, Write},
    net::Shutdown,
    os::unix::{fs::PermissionsExt, net::UnixStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::Duration,
};

struct Authentication;
impl Authenticator for Authentication {
    type Error = Infallible;
    fn authenticate(&mut self, _: &UnixStream) -> Result<AuthenticatedSessionV1, Self::Error> {
        Ok(AuthenticatedSessionV1 { subject: [1; 16] })
    }
}
struct Policy;
impl Authorizer for Policy {
    type Error = Infallible;
    fn authorize(
        &mut self,
        subject: [u8; 16],
        _: AuthorizationActionV1,
        _: AuthorizationResourceV1,
    ) -> Result<bool, Self::Error> {
        Ok(subject == [1; 16])
    }
}

fn path(kind: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "hermes-daemon-{kind}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
fn frame(stream: &mut UnixStream) -> Vec<u8> {
    let mut header = [0; 4];
    stream.read_exact(&mut header).unwrap();
    let mut bytes = vec![0; u32::from_be_bytes(header) as usize];
    stream.read_exact(&mut bytes).unwrap();
    bytes
}
fn send(stream: &mut UnixStream, command: CommandV1) -> ReplyV1 {
    let bytes = encode_request(&command, LimitsV1::default()).unwrap();
    stream
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .unwrap();
    stream.write_all(&bytes).unwrap();
    decode_reply(&frame(stream), LimitsV1::default()).unwrap()
}
fn event(key: u8, payload: u8) -> EventEnvelopeV1 {
    EventEnvelopeV1 {
        producer: ProducerIdV1([2; 16]),
        schema: SchemaIdV1([3; 16]),
        idempotency_key: IdempotencyKeyV1([key; 16]),
        correlation: CorrelationIdV1([5; 16]),
        causation: None,
        payload: vec![payload],
    }
}

fn wait_for_socket(socket: &Path) {
    for _ in 0..400 {
        if socket.exists() {
            return;
        }
        thread::sleep(Duration::from_millis(5));
    }
    panic!(
        "Hermes process socket was not published: {}",
        socket.display()
    );
}

fn spawn_process_daemon(mode: &str, directory: &Path, socket: &Path) -> Child {
    Command::new(std::env::current_exe().unwrap())
        .arg("process_daemon_helper")
        .arg("--exact")
        .arg("--nocapture")
        .env("HERMES_DAEMON_HELPER", "1")
        .env("HERMES_DAEMON_MODE", mode)
        .env("HERMES_DAEMON_DIRECTORY", directory)
        .env("HERMES_DAEMON_SOCKET", socket)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

fn assert_child(child: Child, label: &str) {
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{label} process failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn process_daemon_helper() {
    if std::env::var_os("HERMES_DAEMON_HELPER").is_none() {
        return;
    }
    let directory = PathBuf::from(std::env::var_os("HERMES_DAEMON_DIRECTORY").unwrap());
    let socket = PathBuf::from(std::env::var_os("HERMES_DAEMON_SOCKET").unwrap());
    let limits = Limits::default();
    let budget = CheckpointBudgetV1::default();
    let ledger = match std::env::var("HERMES_DAEMON_MODE").unwrap().as_str() {
        "create" => hermes_local::create_new(&directory, limits, budget).unwrap(),
        "open" => hermes_local::open_existing(&directory, limits, budget).unwrap(),
        mode => panic!("unknown Hermes daemon mode {mode}"),
    };
    let service = Service::new(ledger, Policy, ServiceLimitsV1::default()).unwrap();
    let mut daemon = Daemon::bind_new(
        &socket,
        service,
        Authentication,
        LimitsV1::default(),
        Duration::from_secs(2),
        4,
    )
    .unwrap();
    // A deliberately discarded reply may make reply delivery fail after the
    // append commits. The parent reconciles through canonical reopen/replay.
    let _ = daemon.serve_next();
}

fn run_process_recovery_cycle() {
    let directory = path("process-ledger");
    let socket = path("process.sock");
    let stream_id = StreamIdV1([8; 16]);

    let first = spawn_process_daemon("create", &directory, &socket);
    wait_for_socket(&socket);
    let mut stream = UnixStream::connect(&socket).unwrap();
    decode_ready(&frame(&mut stream), LimitsV1::default()).unwrap();
    let bytes = encode_request(
        &CommandV1::Append {
            stream: stream_id,
            expected_predecessor: None,
            envelope: event(4, 6),
        },
        LimitsV1::default(),
    )
    .unwrap();
    stream
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .unwrap();
    stream.write_all(&bytes).unwrap();
    stream.flush().unwrap();
    stream.shutdown(Shutdown::Both).unwrap();
    drop(stream);
    assert_child(first, "first Hermes daemon");
    assert!(!socket.exists());

    let recovered = spawn_process_daemon("open", &directory, &socket);
    wait_for_socket(&socket);
    let mut stream = UnixStream::connect(&socket).unwrap();
    decode_ready(&frame(&mut stream), LimitsV1::default()).unwrap();
    assert_eq!(
        send(
            &mut stream,
            CommandV1::Append {
                stream: stream_id,
                expected_predecessor: None,
                envelope: event(4, 6),
            },
        ),
        ReplyV1::Accepted(OutcomeV1::Append(AppendOutcomeV1::Replayed(
            EventSequenceV1(0)
        )))
    );
    assert!(matches!(
        send(
            &mut stream,
            CommandV1::Read {
                stream: stream_id,
                start: EventSequenceV1(0),
                limit: 2,
            },
        ),
        ReplyV1::Accepted(OutcomeV1::Read(batch))
            if batch.events.len() == 1 && batch.events[0].envelope.payload == vec![6]
    ));
    drop(stream);
    assert_child(recovered, "recovered Hermes daemon");
    assert!(!socket.exists());
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn subprocess_lost_append_reply_replays_after_durable_reopen() {
    run_process_recovery_cycle();
}

#[test]
#[ignore = "opt-in durable 32-cycle two-process ledger soak"]
fn thirty_two_process_recovery_cycles_preserve_exact_append_replay() {
    for cycle in 0..32 {
        run_process_recovery_cycle();
        eprintln!("completed_cycle={cycle}");
    }
}

#[test]
fn private_socket_preserves_append_read_ack_compaction_and_gaps_across_reopen() {
    let directory = path("ledger");
    let socket = path("service.sock");
    let limits = Limits::default();
    let budget = CheckpointBudgetV1::default();
    let ledger = hermes_local::create_new(&directory, limits, budget).unwrap();
    let service = Service::new(ledger, Policy, ServiceLimitsV1::default()).unwrap();
    let mut daemon = Daemon::bind_new(
        &socket,
        service,
        Authentication,
        LimitsV1::default(),
        Duration::from_secs(2),
        4,
    )
    .unwrap();
    assert_eq!(
        fs::metadata(&socket).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let stream_id = StreamIdV1([8; 16]);
    let client_socket = socket.clone();
    let first = thread::spawn(move || {
        let mut stream = UnixStream::connect(client_socket).unwrap();
        let ready = decode_ready(&frame(&mut stream), LimitsV1::default()).unwrap();
        assert_eq!(ready.protocol_version, 1);
        let append = send(
            &mut stream,
            CommandV1::Append {
                stream: stream_id,
                expected_predecessor: None,
                envelope: event(4, 6),
            },
        );
        let status = send(&mut stream, CommandV1::Status);
        drop(stream);
        (append, status)
    });
    daemon.serve_next().unwrap();
    let (append, status) = first.join().unwrap();
    assert_eq!(
        append,
        ReplyV1::Accepted(OutcomeV1::Append(AppendOutcomeV1::Appended(
            hermes_protocol::EventSequenceV1(0)
        )))
    );
    assert!(matches!(status, ReplyV1::Accepted(OutcomeV1::Status(_))));
    assert_eq!(daemon.health().accepted_connections, 1);
    drop(daemon);
    assert!(!socket.exists());

    let ledger = hermes_local::open_existing(&directory, limits, budget).unwrap();
    let service = Service::new(ledger, Policy, ServiceLimitsV1::default()).unwrap();
    let mut daemon = Daemon::bind_new(
        &socket,
        service,
        Authentication,
        LimitsV1::default(),
        Duration::from_secs(2),
        5,
    )
    .unwrap();
    let replay_socket = socket.clone();
    let replay = thread::spawn(move || {
        let mut stream = UnixStream::connect(replay_socket).unwrap();
        decode_ready(&frame(&mut stream), LimitsV1::default()).unwrap();
        let replay = send(
            &mut stream,
            CommandV1::Append {
                stream: stream_id,
                expected_predecessor: None,
                envelope: event(4, 6),
            },
        );
        let append = send(
            &mut stream,
            CommandV1::Append {
                stream: stream_id,
                expected_predecessor: Some(EventSequenceV1(0)),
                envelope: event(9, 10),
            },
        );
        let consumer = ConsumerIdV1([11; 16]);
        let acknowledged = send(
            &mut stream,
            CommandV1::Acknowledge {
                consumer,
                stream: stream_id,
                through: EventSequenceV1(0),
            },
        );
        let compacted = send(
            &mut stream,
            CommandV1::Compact {
                stream: stream_id,
                through: EventSequenceV1(0),
            },
        );
        let read = send(
            &mut stream,
            CommandV1::Read {
                stream: stream_id,
                start: EventSequenceV1(0),
                limit: 4,
            },
        );
        drop(stream);
        (replay, append, acknowledged, compacted, read)
    });
    daemon.serve_next().unwrap();
    let (replay, second, acknowledged, compacted, read) = replay.join().unwrap();
    assert_eq!(
        replay,
        ReplyV1::Accepted(OutcomeV1::Append(AppendOutcomeV1::Replayed(
            EventSequenceV1(0)
        )))
    );
    assert_eq!(
        second,
        ReplyV1::Accepted(OutcomeV1::Append(AppendOutcomeV1::Appended(
            EventSequenceV1(1)
        )))
    );
    assert!(matches!(
        acknowledged,
        ReplyV1::Accepted(OutcomeV1::Acknowledged(_))
    ));
    assert!(matches!(
        compacted,
        ReplyV1::Accepted(OutcomeV1::Compacted(_))
    ));
    assert!(matches!(
        read,
        ReplyV1::Accepted(OutcomeV1::Read(batch))
            if batch.events.len() == 1
                && batch.events[0].sequence == EventSequenceV1(1)
                && batch.events[0].envelope.payload == vec![10]
                && matches!(batch.gap, Some(ReadGapV1::BeforeRetention {
                    requested: EventSequenceV1(0),
                    retained_from: EventSequenceV1(1),
                }))
    ));
    drop(daemon);
    assert!(!socket.exists());

    let ledger = hermes_local::open_existing(&directory, limits, budget).unwrap();
    let service = Service::new(ledger, Policy, ServiceLimitsV1::default()).unwrap();
    let mut daemon = Daemon::bind_new(
        &socket,
        service,
        Authentication,
        LimitsV1::default(),
        Duration::from_secs(2),
        5,
    )
    .unwrap();
    let recovered_socket = socket.clone();
    let recovered = thread::spawn(move || {
        let mut stream = UnixStream::connect(recovered_socket).unwrap();
        decode_ready(&frame(&mut stream), LimitsV1::default()).unwrap();
        let cursor = send(
            &mut stream,
            CommandV1::Cursor {
                consumer: ConsumerIdV1([11; 16]),
                stream: stream_id,
            },
        );
        let read = send(
            &mut stream,
            CommandV1::Read {
                stream: stream_id,
                start: EventSequenceV1(0),
                limit: 4,
            },
        );
        let old_key = send(
            &mut stream,
            CommandV1::Append {
                stream: stream_id,
                expected_predecessor: Some(EventSequenceV1(1)),
                envelope: event(4, 99),
            },
        );
        let observation = send(
            &mut stream,
            CommandV1::StreamObservation { stream: stream_id },
        );
        let signals = send(&mut stream, CommandV1::Signals);
        drop(stream);
        (cursor, read, old_key, observation, signals)
    });
    daemon.serve_next().unwrap();
    let (cursor, read, old_key, observation, signals) = recovered.join().unwrap();
    assert!(matches!(
        cursor,
        ReplyV1::Accepted(OutcomeV1::Cursor(Some(value)))
            if value.next_sequence == EventSequenceV1(1)
    ));
    assert!(matches!(
        read,
        ReplyV1::Accepted(OutcomeV1::Read(batch))
            if matches!(batch.gap, Some(ReadGapV1::BeforeRetention { .. }))
    ));
    assert_eq!(
        old_key,
        ReplyV1::Accepted(OutcomeV1::Append(AppendOutcomeV1::ReplayHistoryLost(
            EventSequenceV1(0)
        )))
    );
    assert!(matches!(
        observation,
        ReplyV1::Accepted(OutcomeV1::StreamObservation(Some(value)))
            if value.retained_from == EventSequenceV1(1)
                && value.published_edge == Some(EventSequenceV1(1))
                && value.retained_events == 1
                && value.consumer_count == 1
                && value.stalled_consumers == 1
                && value.max_consumer_backlog == 1
    ));
    assert!(matches!(
        signals,
        ReplyV1::Accepted(OutcomeV1::Signals(value))
            if value.offers == [1, 0, 1, 1, 0]
                && value.replays == 1
                && value.confirmed_usage.streams == 1
                && value.confirmed_usage.events == 1
                && value.confirmed_usage.cursors == 1
                && value.last_checkpoint_bytes > 0
                && !value.fenced
    ));
    drop(daemon);
    assert!(!socket.exists());
    fs::remove_dir_all(&directory).unwrap();
}
