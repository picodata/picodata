use crate::instance::InstanceName;
use crate::resharding_loop::ReshardingStatus;
#[allow(unused_imports)]
use crate::simulation::cluster::PretendCluster;
use crate::simulation::fiber::FiberState;
use crate::simulation::fiber::PretendFiberId;
#[allow(unused_imports)]
use crate::simulation::instance::PretendInstance;
use crate::simulation::raft::PretendCasOutcome;
use crate::simulation::sharded_wal::WalOutcome;
use crate::traft::RaftIndex;

/// An enumeration of actions which the simulation engine can perform.
///
/// Actions are performed in [`do_action`].
///
/// Each performed action is recorded into [`PretendCluster::trace`] which is
/// used for debugging.
///
/// [`do_action`]: crate::simulation::engine::do_action
#[derive(Clone, PartialEq, Eq)]
pub enum PretendAction {
    /// Wakeup the resharding_loop on the given `instance` and set it's
    /// requested `status`.
    RequestReshardingStatus {
        instance: InstanceName,
        status: ReshardingStatus,
    },

    /// `instance` applies a raft entry at index `applied` + 1 to it's local state.
    /// Applies global DML and advances the instance's [`PretendInstance::applied_index`].
    AdvanceGlobal {
        instance: InstanceName,
        applied: RaftIndex,
    },

    /// `instance` applies the sharded WAL entry at given `lsn` to it's local
    /// state. Applies the corresponding WAL entry payload and advances
    /// instance's [`PretendInstance::sharded_lsn`].
    AdvanceSharded { instance: InstanceName, lsn: usize },

    /// Resolves the first CAS request in [`PretendInstance::cas_outbox`] of
    /// given `instance`.
    ///
    /// Request resolution is stored to `outcome` after the action is performed
    /// and saved to [`PretendCluster::trace`].
    ResolveCasRequest {
        instance: InstanceName,
        outcome: Option<PretendCasOutcome>,
    },

    /// Resolves the first enqueued WAL write in
    /// [`PretendInstance::unstable_wal`] of given `instance`.
    ///
    /// The `outcome` is stored when the action is performed and saved to the
    /// trace.
    CommitWalWrite {
        instance: InstanceName,
        outcome: Option<WalOutcome>,
    },

    /// Wakes the given fiber. It's wait must be satisfiable.
    SatisfyWait {
        fiber: PretendFiberId,
        state: FiberState,
    },

    /// Same as `RequestReshardingAction` but more specific.
    // TODO: remove in favour of `RequestReshardingStatus`.
    GovernorPoke { instance: InstanceName },

    /// Pauses the sharded replication stream of given instance for testing purposes.
    SetShardedReplicationPaused {
        instance: InstanceName,
        paused: bool,
    },

    /// Wakes the given fiber without satisfying it's wait. For example if the
    /// fiber is parked on `wait_index`, that operation will return a timeout
    /// error.
    TimeoutWait {
        fiber: PretendFiberId,
        state: FiberState,
    },

    /// Simulates a crash of a given `instance`.
    CrashInstance { instance: InstanceName },

    /// Simulates a restart of a previously crashed `instance`.
    RestartInstance { instance: InstanceName },
}

impl std::fmt::Debug for PretendAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RequestReshardingStatus { instance, status } => {
                write!(f, "RequestReshardingStatus({instance}, {status:?})")
            }
            Self::AdvanceGlobal { instance, applied } => {
                write!(f, "AdvanceGlobal({instance}, {applied})")
            }
            Self::AdvanceSharded { instance, lsn } => {
                write!(f, "AdvanceSharded({instance}, {lsn})")
            }
            Self::ResolveCasRequest { instance, outcome } => {
                write!(f, "ResolveCasRequest({instance}, {outcome:?})")
            }
            Self::CommitWalWrite { instance, outcome } => {
                write!(f, "CommitWalWrite({instance}, {outcome:?})")
            }
            Self::SatisfyWait { fiber, state } => {
                write!(f, "SatisfyWait({fiber}, {state:?})")
            }
            Self::GovernorPoke { instance } => {
                write!(f, "GovernorPoke({instance})")
            }
            Self::SetShardedReplicationPaused { instance, paused } => {
                write!(f, "SetShardedReplicationPaused({instance}, {paused})")
            }
            Self::TimeoutWait { fiber, state } => {
                write!(f, "TimeoutWait({fiber}, {state:?})")
            }
            Self::CrashInstance { instance } => {
                write!(f, "CrashInstance({instance})")
            }
            Self::RestartInstance { instance } => {
                write!(f, "RestartInstance({instance})")
            }
        }
    }
}
