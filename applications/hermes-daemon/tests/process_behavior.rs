#![cfg(unix)]

use hermes_protocol::{
    CommandV1, ConsumerIdV1, EventSequenceV1, OutcomeV1, RefusalV1, ReplyV1, StreamIdV1,
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

fn spawn(mode: &str, directory: &Path, socket: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_hermes-daemon"))
        .args([
            mode,
            directory.to_str().unwrap(),
            socket.to_str().unwrap(),
            &geteuid().as_raw().to_string(),
            &getegid().as_raw().to_string(),
            "07070707070707070707070707070707",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

fn wait_for_socket(child: &mut Child, socket: &Path) {
    for _ in 0..400 {
        if socket.exists() {
            return;
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
    let mut created = spawn("create", &directory, &socket);
    wait_for_socket(&mut created, &socket);
    let mut stream = UnixStream::connect(&socket).unwrap();
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

    let mut reopened = spawn("open", &directory, &socket);
    wait_for_socket(&mut reopened, &socket);
    let mut stream = UnixStream::connect(&socket).unwrap();
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
