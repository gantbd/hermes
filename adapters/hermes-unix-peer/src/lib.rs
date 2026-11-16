//! OS-backed peer authentication for Hermes's selected local Unix profile.
//!
//! This adapter maps a kernel-established effective UID/GID pair to a
//! Hermes-owned subject. It does not interpret account names, stream IDs,
//! producer IDs, consumer IDs or authorization policy.

#![forbid(unsafe_code)]

use hermes_local_daemon::unix::Authenticator;
use hermes_service::AuthenticatedSessionV1;
use std::{collections::BTreeMap, os::unix::net::UnixStream};

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct MappingV1 {
    pub effective_uid: u32,
    pub effective_gid: u32,
    pub subject: [u8; 16],
}

impl std::fmt::Debug for MappingV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MappingV1")
            .field("effective_identity", &"<redacted>")
            .field("subject", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigurationErrorV1 {
    InvalidLimit,
    Empty,
    Capacity,
    DuplicatePeer,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthenticationErrorV1 {
    CredentialUnavailable,
    UnknownPeer,
}

/// Bounded exact mapping from kernel peer identities to Hermes subjects.
pub struct UnixPeerAuthenticatorV1 {
    mappings: BTreeMap<(u32, u32), [u8; 16]>,
}

impl UnixPeerAuthenticatorV1 {
    pub fn new(
        max_mappings: usize,
        mappings: Vec<MappingV1>,
    ) -> Result<Self, ConfigurationErrorV1> {
        if max_mappings == 0 {
            return Err(ConfigurationErrorV1::InvalidLimit);
        }
        if mappings.is_empty() {
            return Err(ConfigurationErrorV1::Empty);
        }
        if mappings.len() > max_mappings {
            return Err(ConfigurationErrorV1::Capacity);
        }
        let mut retained = BTreeMap::new();
        for mapping in mappings {
            if retained
                .insert(
                    (mapping.effective_uid, mapping.effective_gid),
                    mapping.subject,
                )
                .is_some()
            {
                return Err(ConfigurationErrorV1::DuplicatePeer);
            }
        }
        Ok(Self { mappings: retained })
    }

    pub fn mapping_count(&self) -> usize {
        self.mappings.len()
    }
}

impl Authenticator for UnixPeerAuthenticatorV1 {
    type Error = AuthenticationErrorV1;

    fn authenticate(&mut self, stream: &UnixStream) -> Result<AuthenticatedSessionV1, Self::Error> {
        let peer = peer_effective_ids(stream)
            .map_err(|()| AuthenticationErrorV1::CredentialUnavailable)?;
        let subject = self
            .mappings
            .get(&peer)
            .copied()
            .ok_or(AuthenticationErrorV1::UnknownPeer)?;
        Ok(AuthenticatedSessionV1 { subject })
    }
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "netbsd",
    target_os = "openbsd"
))]
fn peer_effective_ids(stream: &UnixStream) -> Result<(u32, u32), ()> {
    nix::unistd::getpeereid(stream)
        .map(|(uid, gid)| (uid.as_raw(), gid.as_raw()))
        .map_err(|_| ())
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn peer_effective_ids(stream: &UnixStream) -> Result<(u32, u32), ()> {
    nix::sys::socket::getsockopt(stream, nix::sys::socket::sockopt::PeerCredentials)
        .map(|credentials| (credentials.uid(), credentials.gid()))
        .map_err(|_| ())
}

#[cfg(not(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "linux",
    target_os = "android"
)))]
fn peer_effective_ids(_: &UnixStream) -> Result<(u32, u32), ()> {
    Err(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn actual_mapping() -> MappingV1 {
        MappingV1 {
            effective_uid: nix::unistd::geteuid().as_raw(),
            effective_gid: nix::unistd::getegid().as_raw(),
            subject: [7; 16],
        }
    }

    #[test]
    fn kernel_peer_identity_maps_to_a_redacted_hermes_subject() {
        let (server, _client) = UnixStream::pair().unwrap();
        let mapping = actual_mapping();
        let mut authenticator = UnixPeerAuthenticatorV1::new(1, vec![mapping]).unwrap();
        assert_eq!(
            authenticator.authenticate(&server).unwrap().subject,
            [7; 16]
        );
        let debug = format!("{mapping:?}");
        assert!(!debug.contains(&nix::unistd::geteuid().to_string()));
        assert!(!debug.contains("[7, 7"));
    }

    #[test]
    fn bounds_duplicates_and_unknown_peers_fail_closed() {
        let mapping = actual_mapping();
        assert!(matches!(
            UnixPeerAuthenticatorV1::new(0, vec![mapping]),
            Err(ConfigurationErrorV1::InvalidLimit)
        ));
        assert!(matches!(
            UnixPeerAuthenticatorV1::new(1, vec![]),
            Err(ConfigurationErrorV1::Empty)
        ));
        assert!(matches!(
            UnixPeerAuthenticatorV1::new(1, vec![mapping, mapping]),
            Err(ConfigurationErrorV1::Capacity)
        ));
        assert!(matches!(
            UnixPeerAuthenticatorV1::new(2, vec![mapping, mapping]),
            Err(ConfigurationErrorV1::DuplicatePeer)
        ));
        let (server, _client) = UnixStream::pair().unwrap();
        let mut unknown = UnixPeerAuthenticatorV1::new(
            1,
            vec![MappingV1 {
                effective_uid: mapping.effective_uid.wrapping_add(1),
                ..mapping
            }],
        )
        .unwrap();
        assert_eq!(
            unknown.authenticate(&server),
            Err(AuthenticationErrorV1::UnknownPeer)
        );
    }
}
