//! Local Unix delivery wiring, not ledger semantics or credential authentication.
//!
//! ```no_run
//! fn main() -> Result<(), hermes_local::LocalError> {
//!     use hermes_ledger::{Limits, persistence::CheckpointBudgetV1};
//!     let path = std::path::Path::new("/trusted-parent/new-ledger");
//!     let ledger = hermes_local::create_new(path, Limits::default(), CheckpointBudgetV1::default())?;
//!     drop(ledger); // Releases the OS writer lock, not an extra commit.
//!     let _recovered = hermes_local::open_existing(path, Limits::default(), CheckpointBudgetV1::default())?;
//!     Ok(())
//! }
//! ```
#![forbid(unsafe_code)]

#[cfg(unix)]
mod local {
    use hermes_host::{DurableLedger, HostError, storage_value_limit};
    use hermes_ledger::{
        Engine, Limits,
        persistence::{CheckpointBudgetV1, CheckpointErrorV1},
    };
    use keystone_storage_directory::{DirectoryCheckpointStore, DirectoryErrorV1, LimitsV1};
    use std::path::Path;

    pub type LocalLedger = DurableLedger<DirectoryCheckpointStore>;
    pub type LocalError = HostError<DirectoryErrorV1>;

    fn storage_limits(budget: CheckpointBudgetV1) -> Result<LimitsV1, LocalError> {
        Ok(LimitsV1 {
            max_key_bytes: 64,
            max_value_bytes: storage_value_limit(budget).ok_or(HostError::InvalidRecovery)?,
        })
    }

    /// Privileged explicit initialization in a new leaf under a trusted parent.
    pub fn create_new(
        path: impl AsRef<Path>,
        limits: Limits,
        budget: CheckpointBudgetV1,
    ) -> Result<LocalLedger, LocalError> {
        // Refuse known bad semantic/codec configuration before creating resources.
        Engine::try_new(limits)
            .map_err(|_| HostError::Checkpoint(CheckpointErrorV1::Limit))?
            .checkpoint(budget)
            .map_err(HostError::Checkpoint)?;
        let store = DirectoryCheckpointStore::create_new(path, storage_limits(budget)?)
            .map_err(HostError::Store)?;
        DurableLedger::create_new(store, limits, budget)
    }

    /// Exclusive recovery. Does not reset missing, corrupt or uncertain state.
    pub fn open_existing(
        path: impl AsRef<Path>,
        limits: Limits,
        budget: CheckpointBudgetV1,
    ) -> Result<LocalLedger, LocalError> {
        let store = DirectoryCheckpointStore::open_existing(path, storage_limits(budget)?)
            .map_err(HostError::Store)?;
        DurableLedger::open(store, limits, budget)
    }
}

#[cfg(unix)]
pub use local::*;
