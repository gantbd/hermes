#![cfg(unix)]

use hermes_local_daemon::{
    ServicePort,
    unix::{Daemon, DaemonErrorV1},
};
use hermes_protocol::{CommandV1, ReplyV1, SessionHandleV1};
use hermes_service::AuthenticatedSessionV1;
use hermes_unix_peer::{AuthenticationErrorV1, MappingV1, UnixPeerAuthenticatorV1};
use hermes_wire::{LimitsV1, decode_ready};
use std::{
    convert::Infallible,
    io::Read,
    os::unix::net::UnixStream,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::Duration,
};

struct Port(Arc<Mutex<Option<[u8; 16]>>>);

impl ServicePort for Port {
    type Error = Infallible;

    fn open_session(&mut self, facts: AuthenticatedSessionV1) -> Option<SessionHandleV1> {
        *self.0.lock().unwrap() = Some(facts.subject);
        Some(SessionHandleV1(0))
    }

    fn close_session(&mut self, _: SessionHandleV1) -> bool {
        true
    }

    fn execute(&mut self, _: SessionHandleV1, _: CommandV1) -> Result<ReplyV1, Self::Error> {
        unreachable!("credential qualification sends no domain command")
    }
}

fn socket_path(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "hermes-peer-{label}-{}-{}.sock",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

fn mapping(uid_offset: u32) -> MappingV1 {
    MappingV1 {
        effective_uid: nix::unistd::geteuid().as_raw().wrapping_add(uid_offset),
        effective_gid: nix::unistd::getegid().as_raw(),
        subject: [7; 16],
    }
}

fn read_frame(stream: &mut UnixStream) -> Vec<u8> {
    let mut header = [0; 4];
    stream.read_exact(&mut header).unwrap();
    let mut bytes = vec![0; u32::from_be_bytes(header) as usize];
    stream.read_exact(&mut bytes).unwrap();
    bytes
}

#[test]
fn daemon_accepts_the_mapped_kernel_peer_and_rejects_an_unknown_peer() {
    let path = socket_path("accepted");
    let observed = Arc::new(Mutex::new(None));
    let authenticator = UnixPeerAuthenticatorV1::new(1, vec![mapping(0)]).unwrap();
    let mut daemon = Daemon::bind_new(
        &path,
        Port(Arc::clone(&observed)),
        authenticator,
        LimitsV1::default(),
        Duration::from_secs(2),
        1,
    )
    .unwrap();
    let client_path = path.clone();
    let client = thread::spawn(move || {
        let mut stream = UnixStream::connect(client_path).unwrap();
        decode_ready(&read_frame(&mut stream), LimitsV1::default()).unwrap();
    });
    daemon.serve_next().unwrap();
    client.join().unwrap();
    assert_eq!(*observed.lock().unwrap(), Some([7; 16]));
    drop(daemon);
    assert!(!path.exists());

    let path = socket_path("unknown");
    let observed = Arc::new(Mutex::new(None));
    let authenticator = UnixPeerAuthenticatorV1::new(1, vec![mapping(1)]).unwrap();
    let mut daemon = Daemon::bind_new(
        &path,
        Port(Arc::clone(&observed)),
        authenticator,
        LimitsV1::default(),
        Duration::from_secs(2),
        1,
    )
    .unwrap();
    let client_path = path.clone();
    let client = thread::spawn(move || {
        let mut stream = UnixStream::connect(client_path).unwrap();
        let mut byte = [0];
        stream.read(&mut byte).unwrap_or(0) == 0
    });
    assert!(matches!(
        daemon.serve_next(),
        Err(DaemonErrorV1::Authentication(
            AuthenticationErrorV1::UnknownPeer
        ))
    ));
    assert!(client.join().unwrap());
    assert_eq!(*observed.lock().unwrap(), None);
    drop(daemon);
    assert!(!path.exists());
}

#[test]
fn owner_loop_drains_after_an_identity_free_wake_connection() {
    let path = socket_path("drain");
    let observed = Arc::new(Mutex::new(None));
    let authenticator = UnixPeerAuthenticatorV1::new(1, vec![mapping(0)]).unwrap();
    let daemon = Daemon::bind_new(
        &path,
        Port(Arc::clone(&observed)),
        authenticator,
        LimitsV1::default(),
        Duration::from_secs(2),
        1,
    )
    .unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let owner_stop = Arc::clone(&stop);
    let owner = thread::spawn(move || {
        let mut daemon = daemon;
        daemon.serve_until(&owner_stop)
    });

    let mut client = UnixStream::connect(&path).unwrap();
    decode_ready(&read_frame(&mut client), LimitsV1::default()).unwrap();
    drop(client);
    stop.store(true, Ordering::Release);
    let wake = UnixStream::connect(&path).unwrap();
    drop(wake);

    assert!(owner.join().unwrap().is_ok());
    assert_eq!(*observed.lock().unwrap(), Some([7; 16]));
    assert!(!path.exists());
}
