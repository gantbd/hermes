#![forbid(unsafe_code)]

#[cfg(unix)]
mod unix {
    use hermes_ledger::{Limits, persistence::CheckpointBudgetV1};
    use hermes_local_daemon::{ConnectionErrorV1, unix::DaemonErrorV1};
    use hermes_protocol::StreamIdV1;
    use hermes_service::{
        AuthorizationActionV1, AuthorizationResourceV1, Authorizer, Service, ServiceLimitsV1,
    };
    use hermes_unix_peer::{MappingV1, UnixPeerAuthenticatorV1};
    use signal_hook::{
        consts::{SIGINT, SIGTERM},
        iterator::Signals,
    };
    use std::{
        convert::Infallible,
        error::Error,
        ffi::OsString,
        os::unix::net::UnixStream,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread,
        time::Duration,
    };

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum OpenMode {
        Create,
        Open,
    }

    #[derive(Debug, Eq, PartialEq)]
    struct Config {
        mode: OpenMode,
        directory: PathBuf,
        socket: PathBuf,
        effective_uid: u32,
        effective_gid: u32,
        subject: [u8; 16],
        access: AccessProfile,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum AccessProfile {
        Observer,
        Operator(StreamIdV1),
    }

    impl Config {
        fn parse(values: impl IntoIterator<Item = OsString>) -> Result<Self, &'static str> {
            let mut values = values.into_iter();
            let mode = match values.next().and_then(|value| value.into_string().ok()) {
                Some(value) if value == "create" => OpenMode::Create,
                Some(value) if value == "open" => OpenMode::Open,
                _ => return Err("MODE must be create or open"),
            };
            let directory = values
                .next()
                .map(PathBuf::from)
                .ok_or("DIRECTORY required")?;
            let socket = values.next().map(PathBuf::from).ok_or("SOCKET required")?;
            let effective_uid = parse_u32(values.next(), "EFFECTIVE_UID required")?;
            let effective_gid = parse_u32(values.next(), "EFFECTIVE_GID required")?;
            let subject = parse_subject(values.next().ok_or("SUBJECT_HEX required")?)?;
            let access = match values
                .next()
                .and_then(|value| value.into_string().ok())
                .as_deref()
            {
                Some("observer") => AccessProfile::Observer,
                Some("operator") => AccessProfile::Operator(StreamIdV1(parse_identifier(
                    values.next().ok_or("OPERATOR_STREAM_HEX required")?,
                )?)),
                _ => return Err("ACCESS must be observer or operator"),
            };
            if values.next().is_some() {
                return Err("unexpected argument");
            }
            Ok(Self {
                mode,
                directory,
                socket,
                effective_uid,
                effective_gid,
                subject,
                access,
            })
        }
    }

    fn parse_u32(value: Option<OsString>, missing: &'static str) -> Result<u32, &'static str> {
        let value = value
            .ok_or(missing)?
            .into_string()
            .map_err(|_| "identity must be UTF-8 decimal")?;
        if value.is_empty()
            || !value.bytes().all(|byte| byte.is_ascii_digit())
            || (value.len() > 1 && value.starts_with('0'))
        {
            return Err("identity must be canonical decimal u32");
        }
        value
            .parse()
            .map_err(|_| "identity must be canonical decimal u32")
    }

    fn parse_subject(value: OsString) -> Result<[u8; 16], &'static str> {
        parse_identifier(value)
    }

    fn parse_identifier(value: OsString) -> Result<[u8; 16], &'static str> {
        let value = value
            .into_string()
            .map_err(|_| "identifier must be UTF-8 lowercase hexadecimal")?;
        if value.len() != 32
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err("identifier must be exactly 32 lowercase hexadecimal digits");
        }
        let mut subject = [0_u8; 16];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            subject[index] = (hex(pair[0])? << 4) | hex(pair[1])?;
        }
        Ok(subject)
    }

    fn hex(value: u8) -> Result<u8, &'static str> {
        match value {
            b'0'..=b'9' => Ok(value - b'0'),
            b'a'..=b'f' => Ok(value - b'a' + 10),
            _ => Err("identifier must be lowercase hexadecimal"),
        }
    }

    struct SelectedPolicy {
        subject: [u8; 16],
        access: AccessProfile,
    }

    impl Authorizer for SelectedPolicy {
        type Error = Infallible;

        fn authorize(
            &mut self,
            subject: [u8; 16],
            action: AuthorizationActionV1,
            resource: AuthorizationResourceV1,
        ) -> Result<bool, Self::Error> {
            if subject != self.subject {
                return Ok(false);
            }
            Ok(match self.access {
                AccessProfile::Observer => matches!(
                    action,
                    AuthorizationActionV1::Read | AuthorizationActionV1::Observe
                ),
                AccessProfile::Operator(selected) => match resource {
                    AuthorizationResourceV1::Service => action == AuthorizationActionV1::Observe,
                    AuthorizationResourceV1::Stream(stream) if stream == selected => matches!(
                        action,
                        AuthorizationActionV1::Read
                            | AuthorizationActionV1::Acknowledge
                            | AuthorizationActionV1::Compact
                            | AuthorizationActionV1::Observe
                    ),
                    AuthorizationResourceV1::Stream(_) => false,
                },
            })
        }
    }

    pub fn run() -> Result<(), Box<dyn Error>> {
        let config = Config::parse(std::env::args_os().skip(1)).map_err(usage)?;
        let ledger = match config.mode {
            OpenMode::Create => hermes_local::create_new(
                &config.directory,
                Limits::default(),
                CheckpointBudgetV1::default(),
            ),
            OpenMode::Open => hermes_local::open_existing(
                &config.directory,
                Limits::default(),
                CheckpointBudgetV1::default(),
            ),
        }
        .map_err(|error| format!("ledger startup refused: {error:?}"))?;
        let service = Service::new(
            ledger,
            SelectedPolicy {
                subject: config.subject,
                access: config.access,
            },
            ServiceLimitsV1::default(),
        )
        .ok_or("service limits refused")?;
        let authentication = UnixPeerAuthenticatorV1::new(
            1,
            vec![MappingV1 {
                effective_uid: config.effective_uid,
                effective_gid: config.effective_gid,
                subject: config.subject,
            }],
        )
        .map_err(|error| format!("peer mapping refused: {error:?}"))?;
        let mut daemon = hermes_local_daemon::unix::Daemon::bind_new(
            &config.socket,
            service,
            authentication,
            hermes_wire::LimitsV1::default(),
            Duration::from_secs(5),
            8,
        )?;

        let stop = Arc::new(AtomicBool::new(false));
        let mut signals = Signals::new([SIGINT, SIGTERM])?;
        let signal_handle = signals.handle();
        let signal_stop = Arc::clone(&stop);
        let wake_path = config.socket.clone();
        let signal_thread = thread::Builder::new()
            .name("hermes-signal-owner".into())
            .spawn(move || {
                if signals.forever().next().is_some() {
                    signal_stop.store(true, Ordering::Release);
                    let _ = UnixStream::connect(wake_path);
                }
            })?;

        let result = serve(&mut daemon, &stop);
        signal_handle.close();
        let _ = signal_thread.join();
        result
    }

    fn serve<P, A>(
        daemon: &mut hermes_local_daemon::unix::Daemon<P, A>,
        stop: &AtomicBool,
    ) -> Result<(), Box<dyn Error>>
    where
        P: hermes_local_daemon::ServicePort,
        A: hermes_local_daemon::unix::Authenticator,
        P::Error: std::fmt::Debug,
        A::Error: std::fmt::Debug,
    {
        while !stop.load(Ordering::Acquire) {
            match daemon.serve_until(stop) {
                Ok(()) => return Ok(()),
                Err(
                    DaemonErrorV1::Authentication(_)
                    | DaemonErrorV1::SessionCapacity
                    | DaemonErrorV1::Connection(
                        ConnectionErrorV1::BeforeDispatch(_)
                        | ConnectionErrorV1::Wire(_)
                        | ConnectionErrorV1::OutcomeUnknown(_),
                    ),
                ) => {}
                Err(error) => return Err(format!("daemon stopped: {error:?}").into()),
            }
        }
        Ok(())
    }

    fn usage(message: &'static str) -> String {
        format!(
            "{message}; usage: hermes-daemon create|open DIRECTORY SOCKET EFFECTIVE_UID EFFECTIVE_GID SUBJECT_HEX observer|operator [OPERATOR_STREAM_HEX]"
        )
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn configuration_is_closed_and_subject_is_canonical() {
            let config = Config::parse(
                [
                    "create",
                    "ledger",
                    "hermes.sock",
                    "501",
                    "20",
                    "01010101010101010101010101010101",
                    "observer",
                ]
                .into_iter()
                .map(OsString::from),
            )
            .unwrap();
            assert_eq!(config.mode, OpenMode::Create);
            assert_eq!(config.subject, [1; 16]);
            assert_eq!(config.access, AccessProfile::Observer);
            let operator = Config::parse(
                [
                    "open",
                    "ledger",
                    "hermes.sock",
                    "501",
                    "20",
                    "01010101010101010101010101010101",
                    "operator",
                    "02020202020202020202020202020202",
                ]
                .into_iter()
                .map(OsString::from),
            )
            .unwrap();
            assert_eq!(
                operator.access,
                AccessProfile::Operator(StreamIdV1([2; 16]))
            );
            for invalid in [
                "0101",
                "0101010101010101010101010101010A",
                "0101010101010101010101010101010g",
            ] {
                assert!(parse_subject(OsString::from(invalid)).is_err());
            }
        }

        #[test]
        fn observer_policy_is_exact_subject_read_and_observe_only() {
            let mut policy = SelectedPolicy {
                subject: [7; 16],
                access: AccessProfile::Observer,
            };
            let resource = AuthorizationResourceV1::Service;
            assert!(
                policy
                    .authorize([7; 16], AuthorizationActionV1::Read, resource)
                    .unwrap()
            );
            assert!(
                policy
                    .authorize([7; 16], AuthorizationActionV1::Observe, resource)
                    .unwrap()
            );
            assert!(
                !policy
                    .authorize([7; 16], AuthorizationActionV1::Append, resource)
                    .unwrap()
            );
            assert!(
                !policy
                    .authorize([8; 16], AuthorizationActionV1::Read, resource)
                    .unwrap()
            );
        }

        #[test]
        fn operator_policy_is_exact_subject_stream_and_closed_action_set() {
            let selected = StreamIdV1([5; 16]);
            let mut policy = SelectedPolicy {
                subject: [7; 16],
                access: AccessProfile::Operator(selected),
            };
            for action in [
                AuthorizationActionV1::Read,
                AuthorizationActionV1::Acknowledge,
                AuthorizationActionV1::Compact,
                AuthorizationActionV1::Observe,
            ] {
                assert!(
                    policy
                        .authorize([7; 16], action, AuthorizationResourceV1::Stream(selected),)
                        .unwrap()
                );
            }
            assert!(
                !policy
                    .authorize(
                        [7; 16],
                        AuthorizationActionV1::Append,
                        AuthorizationResourceV1::Stream(selected),
                    )
                    .unwrap()
            );
            assert!(
                !policy
                    .authorize(
                        [7; 16],
                        AuthorizationActionV1::Compact,
                        AuthorizationResourceV1::Stream(StreamIdV1([6; 16])),
                    )
                    .unwrap()
            );
            assert!(
                policy
                    .authorize(
                        [7; 16],
                        AuthorizationActionV1::Observe,
                        AuthorizationResourceV1::Service,
                    )
                    .unwrap()
            );
        }
    }
}

#[cfg(unix)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    unix::run()
}

#[cfg(not(unix))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    Err("hermes-daemon requires a Unix peer-credential platform".into())
}
