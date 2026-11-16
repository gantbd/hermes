#![cfg(unix)]

use aegis_authority::{EngineId, Limits as AegisLimits, PrincipalId, SessionId};
use aegis_client::{AuthorityClient, ServicePort as AegisServicePort};
use aegis_local_daemon::unix::{Authenticator as AegisAuthenticator, Daemon as AegisDaemon};
use aegis_protocol::{
    ActionV1, CommandV1 as AegisCommandV1, EngineNamespaceV1, OutcomeV1 as AegisOutcomeV1,
    ReplyV1 as AegisReplyV1, ScopeV1, SessionHandleV1,
};
use aegis_service::{
    AuthenticatedSessionV1 as AegisSession, Service as AegisService,
    ServiceLimitsV1 as AegisServiceLimits,
};
use aegis_wire::{
    LimitsV1 as AegisWireLimitsV1, RequestFrameV1, decode_reply as decode_aegis_reply,
    decode_session_opened, encode_request as encode_aegis_request,
};
use hermes_aegis::{AegisAuthorizer, AuthorizationTime};
use hermes_ledger::{Limits as HermesLimits, persistence::CheckpointBudgetV1};
use hermes_local_daemon::unix::{Authenticator as HermesAuthenticator, Daemon as HermesDaemon};
use hermes_protocol::{
    CommandV1, CorrelationIdV1, EventEnvelopeV1, IdempotencyKeyV1, OutcomeV1, ProducerIdV1,
    RefusalV1, ReplyV1, SchemaIdV1, StreamIdV1,
};
use hermes_service::{
    AuthenticatedSessionV1 as HermesSession, Service as HermesService,
    ServiceLimitsV1 as HermesServiceLimits,
};
use hermes_wire::{
    LimitsV1 as HermesWireLimitsV1, decode_ready, decode_reply as decode_hermes_reply,
    encode_request as encode_hermes_request,
};
use std::{
    convert::Infallible,
    fs,
    io::{Read, Write},
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::Duration,
};

const SUBJECT: [u8; 16] = [0x21; 16];
const STREAM: StreamIdV1 = StreamIdV1([0x31; 16]);

fn read_frame(stream: &mut UnixStream) -> std::io::Result<Vec<u8>> {
    let mut header = [0; 4];
    stream.read_exact(&mut header)?;
    let mut bytes = vec![0; u32::from_be_bytes(header) as usize];
    stream.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn write_frame(stream: &mut UnixStream, bytes: &[u8]) -> std::io::Result<()> {
    stream.write_all(&(bytes.len() as u32).to_be_bytes())?;
    stream.write_all(bytes)?;
    stream.flush()
}

struct AegisSocketPort(UnixStream);

impl AegisServicePort for AegisSocketPort {
    type Error = std::io::Error;

    fn execute(
        &mut self,
        session: SessionHandleV1,
        command: AegisCommandV1,
    ) -> Result<AegisReplyV1, Self::Error> {
        let bytes = encode_aegis_request(
            &RequestFrameV1 { session, command },
            AegisWireLimitsV1::default(),
        )
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "Aegis request"))?;
        write_frame(&mut self.0, &bytes)?;
        decode_aegis_reply(&read_frame(&mut self.0)?, AegisWireLimitsV1::default())
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "Aegis reply"))
    }
}

struct AegisAuthentication;

impl AegisAuthenticator for AegisAuthentication {
    type Error = Infallible;

    fn authenticate(&mut self, _: &UnixStream) -> Result<AegisSession, Self::Error> {
        Ok(AegisSession {
            principal: PrincipalId::new(SUBJECT),
            session: SessionId::new([0x22; 16]),
            administrator: true,
        })
    }
}

struct HermesAuthentication;

impl HermesAuthenticator for HermesAuthentication {
    type Error = Infallible;

    fn authenticate(&mut self, _: &UnixStream) -> Result<HermesSession, Self::Error> {
        Ok(HermesSession { subject: SUBJECT })
    }
}

struct Time;

impl AuthorizationTime for Time {
    fn now(&mut self) -> u64 {
        10
    }
}

fn event() -> EventEnvelopeV1 {
    EventEnvelopeV1 {
        producer: ProducerIdV1([0x41; 16]),
        schema: SchemaIdV1([0x42; 16]),
        idempotency_key: IdempotencyKeyV1([0x43; 16]),
        correlation: CorrelationIdV1([0x44; 16]),
        causation: None,
        payload: vec![0x45],
    }
}

fn wait_for_path(path: &Path, child: &mut Child) {
    for _ in 0..400 {
        if path.exists() {
            return;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!(
                "helper exited before publishing {}: {status}",
                path.display()
            );
        }
        thread::sleep(Duration::from_millis(5));
    }
    panic!("helper did not publish {}", path.display());
}

fn spawn_helper(mode: &str, root: &Path) -> Child {
    Command::new(std::env::current_exe().unwrap())
        .arg("aegis_process_helper")
        .arg("--exact")
        .arg("--nocapture")
        .env("HERMES_AEGIS_HELPER", "1")
        .env("HERMES_AEGIS_MODE", mode)
        .env("HERMES_AEGIS_ROOT", root)
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
fn aegis_process_helper() {
    if std::env::var_os("HERMES_AEGIS_HELPER").is_none() {
        return;
    }
    let root = PathBuf::from(std::env::var_os("HERMES_AEGIS_ROOT").unwrap());
    match std::env::var("HERMES_AEGIS_MODE").unwrap().as_str() {
        "aegis" => {
            let authority_id = EngineId::new([0x11; 16]);
            let authority = aegis_local::create_new(
                root.join("aegis-state"),
                authority_id,
                AegisLimits::default(),
            )
            .unwrap();
            let service =
                AegisService::new(authority, authority_id, AegisServiceLimits::default()).unwrap();
            let mut daemon = AegisDaemon::bind_new(
                root.join("aegis.sock"),
                service,
                AegisAuthentication,
                AegisWireLimitsV1::default(),
                Duration::from_secs(3),
                16,
            )
            .unwrap();
            daemon.serve_next().unwrap();
        }
        "hermes" => {
            let mut aegis_stream = UnixStream::connect(root.join("aegis.sock")).unwrap();
            let session = decode_session_opened(
                &read_frame(&mut aegis_stream).unwrap(),
                AegisWireLimitsV1::default(),
            )
            .unwrap();
            let mut authority = AuthorityClient::new(session, AegisSocketPort(aegis_stream));
            let AegisReplyV1::Accepted(AegisOutcomeV1::GrantIssued(grant)) = authority
                .issue(
                    0,
                    1,
                    100,
                    vec![ScopeV1 {
                        engine: EngineNamespaceV1::Hermes,
                        resource: STREAM.0,
                        actions: 1 << ActionV1::Submit as u8,
                    }],
                )
                .unwrap()
            else {
                panic!("Aegis grant issuance failed")
            };
            let authorizer = AegisAuthorizer::new(authority, grant.handle, SUBJECT, Time);
            let ledger = hermes_local::create_new(
                root.join("hermes-state"),
                HermesLimits::default(),
                CheckpointBudgetV1::default(),
            )
            .unwrap();
            let service =
                HermesService::new(ledger, authorizer, HermesServiceLimits::default()).unwrap();
            let mut daemon = HermesDaemon::bind_new(
                root.join("hermes.sock"),
                service,
                HermesAuthentication,
                HermesWireLimitsV1::default(),
                Duration::from_secs(3),
                4,
            )
            .unwrap();
            daemon.serve_next().unwrap();
        }
        mode => panic!("unknown helper mode {mode}"),
    }
}

#[test]
fn live_aegis_process_authorizes_exact_hermes_action_and_resource() {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "hermes-aegis-process-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&root).unwrap();

    let mut aegis = spawn_helper("aegis", &root);
    wait_for_path(&root.join("aegis.sock"), &mut aegis);
    let mut hermes = spawn_helper("hermes", &root);
    wait_for_path(&root.join("hermes.sock"), &mut hermes);

    let mut stream = UnixStream::connect(root.join("hermes.sock")).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .and_then(|()| stream.set_write_timeout(Some(Duration::from_secs(3))))
        .unwrap();
    let ready = decode_ready(
        &read_frame(&mut stream).unwrap(),
        HermesWireLimitsV1::default(),
    )
    .unwrap();
    assert_eq!(ready.protocol_version, 1);

    let append = encode_hermes_request(
        &CommandV1::Append {
            stream: STREAM,
            expected_predecessor: None,
            envelope: event(),
        },
        HermesWireLimitsV1::default(),
    )
    .unwrap();
    write_frame(&mut stream, &append).unwrap();
    assert!(matches!(
        decode_hermes_reply(
            &read_frame(&mut stream).unwrap(),
            HermesWireLimitsV1::default()
        )
        .unwrap(),
        ReplyV1::Accepted(OutcomeV1::Append(_))
    ));

    let read = encode_hermes_request(
        &CommandV1::Read {
            stream: STREAM,
            start: hermes_protocol::EventSequenceV1(0),
            limit: 1,
        },
        HermesWireLimitsV1::default(),
    )
    .unwrap();
    write_frame(&mut stream, &read).unwrap();
    assert_eq!(
        decode_hermes_reply(
            &read_frame(&mut stream).unwrap(),
            HermesWireLimitsV1::default()
        )
        .unwrap(),
        ReplyV1::Refused(RefusalV1::NotAuthorized)
    );
    drop(stream);

    assert_child(hermes, "Hermes");
    assert_child(aegis, "Aegis");
    assert!(!root.join("hermes.sock").exists());
    assert!(!root.join("aegis.sock").exists());
    fs::remove_dir_all(root).unwrap();
}
