use crate::cas::Range;
use crate::error_code::ErrorCode;
use crate::info::InstanceInfo;
use crate::instance::{InstanceName, StateVariant};
use crate::plugin::{reenterable_plugin_cas_request, PluginOp, PreconditionCheckResult};
use crate::storage::{self, PropertyName, SystemTable};
use crate::traft::node;
use crate::traft::op::{Dml, Op};
use crate::util::effective_user_id;
use crate::{tlog, traft};
use serde::{Deserialize, Serialize};
use tarantool::error::BoxError;
use tarantool::time::Instant;

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct PicoPropertyLock {
    /// The lock is considered to be acquired by this instance.
    instance_name: InstanceName,
    /// State incarnation of the instance at the moment it acquired the lock.
    ///
    /// This is used to determine if the instance has terminated while holding the lock.
    incarnation: u64,
}

impl PicoPropertyLock {
    pub fn new(instance_name: InstanceName, incarnation: u64) -> Self {
        Self {
            instance_name,
            incarnation,
        }
    }
}

impl From<InstanceInfo> for PicoPropertyLock {
    fn from(instance_info: InstanceInfo) -> Self {
        PicoPropertyLock::new(instance_info.name, instance_info.current_state.incarnation)
    }
}

impl std::fmt::Display for PicoPropertyLock {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}:{}", self.instance_name, self.incarnation)
    }
}

/// Try to acquire a lock.
///
/// # Errors
/// Return an error if any other instance already acquires the lock.
/// Return error if underlying CAS fails.
pub fn try_acquire(deadline: Instant) -> traft::Result<()> {
    let node = node::global().expect("node must be already initialized");
    let precondition = || {
        let my_instance_info = InstanceInfo::try_get(node, None).unwrap();

        if let Some(op) = node.storage.properties.pending_plugin_op()? {
            let existed_lock = match op {
                PluginOp::MigrationLock(v) => v,
                op => {
                    return Err(BoxError::new(
                        ErrorCode::PluginError,
                        format!("Another plugin operation is in progress: {op}"),
                    )
                    .into());
                }
            };

            let owner_name = existed_lock.instance_name;
            let info = InstanceInfo::try_get(node, Some(&owner_name)).ok();
            match info {
                None => {
                    tlog!(
                        Warning,
                        "Lock by non-existent instance found, acquire new lock"
                    );
                }
                Some(info) if info.current_state.variant != StateVariant::Online => {
                    tlog!(
                        Warning,
                        "Lock by offline or expelled instance found, acquire new lock"
                    );
                }
                Some(info) if info.current_state.incarnation != existed_lock.incarnation => {
                    tlog!(
                        Warning,
                        "Lock by instance with changed incarnation, acquire new lock"
                    );
                }
                _ => {
                    return Err(
                        BoxError::new(
                            ErrorCode::PluginError,
                            format!("Another plugin migration is in progress, initiated by instance {owner_name}"),
                        ).into()
                    );
                }
            }
        }

        let new_lock = PluginOp::MigrationLock(PicoPropertyLock::from(my_instance_info));
        let dml = Dml::replace(
            storage::Properties::TABLE_ID,
            &(&PropertyName::PendingPluginOperation, new_lock),
            effective_user_id(),
        )?;
        let ranges = vec![
            //  if someone acquires the lock
            Range::new(storage::Properties::TABLE_ID).eq([PropertyName::PendingPluginOperation]),
        ];

        Ok(PreconditionCheckResult::DoOp((Op::Dml(dml), ranges)))
    };

    reenterable_plugin_cas_request(node, precondition, deadline)?;

    Ok(())
}

/// Return `Ok` if lock already acquired by current instance, and it's actual
/// (this means that the instance incarnation has not changed since the lock was taken).
///
/// # Errors
/// Return an error if lock should be already acquired by current instance,
/// but it's not. Return errors if storage fails.
#[track_caller]
pub fn lock_is_acquired_by_us() -> traft::Result<()> {
    let node = node::global()?;
    let op = node.storage.properties.pending_plugin_op()?;

    let lock = match op {
        Some(PluginOp::MigrationLock(v)) => v,
        other => {
            tlog!(Error, "migration lock disappeared, found {other:?} instead");
            return Err(error_migration_lock_lost().into());
        }
    };

    let instance_info = InstanceInfo::try_get(node, None)?;

    let expected_lock = PicoPropertyLock::from(instance_info);
    if lock != expected_lock {
        tlog!(
            Error,
            "migration lock owner changed, expected {expected_lock}, got {lock}"
        );
        return Err(error_migration_lock_lost().into());
    }

    Ok(())
}

/// Release a lock.
///
/// # Errors
///
/// Return error if lock already released or underlying CAS fails.
pub fn release(deadline: Instant) -> traft::Result<()> {
    let node = node::global().expect("node must be already initialized");
    let precondition = || {
        lock_is_acquired_by_us()?;

        let dml = Dml::delete(
            storage::Properties::TABLE_ID,
            &[PropertyName::PendingPluginOperation],
            effective_user_id(),
        )?;

        let ranges =
            vec![Range::new(storage::Properties::TABLE_ID)
                .eq([PropertyName::PendingPluginOperation])];

        Ok(PreconditionCheckResult::DoOp((Op::Dml(dml), ranges)))
    };

    reenterable_plugin_cas_request(node, precondition, deadline)?;

    Ok(())
}

#[track_caller]
fn error_migration_lock_lost() -> BoxError {
    BoxError::new(
        ErrorCode::PluginError,
        "Migration got interrupted likely because connection to cluster was lost",
    )
}
