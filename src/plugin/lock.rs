use crate::cas::Range;
use crate::info::InstanceInfo;
use crate::instance::{InstanceName, StateVariant};
use crate::plugin::migration::Error;
use crate::plugin::{
    reenterable_plugin_cas_request, PluginError, PluginOp, PreconditionCheckResult,
};
use crate::storage::{ClusterwideTable, PropertyName};
use crate::traft::node;
use crate::traft::op::{Dml, Op};
use crate::util::effective_user_id;
use crate::{tlog, traft};
use serde::{Deserialize, Serialize};
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
        PicoPropertyLock::new(
            instance_info.instance_name,
            instance_info.current_state.incarnation,
        )
    }
}

/// Try to acquire a lock.
///
/// # Errors
/// Return [`PluginError::LockAlreadyAcquired`] if any other instance already acquires the lock.
/// Return error if underlying CAS fails.
pub fn try_acquire(deadline: Instant) -> crate::plugin::Result<()> {
    let node = node::global().expect("node must be already initialized");
    let precondition = || {
        let my_instance_info = InstanceInfo::try_get(node, None).unwrap();

        if let Some(op) = node.storage.properties.pending_plugin_op()? {
            let PluginOp::MigrationLock(existed_lock) = op else {
                return Err(PluginError::LockAlreadyAcquired.into());
            };

            let info = InstanceInfo::try_get(node, Some(&existed_lock.instance_name)).ok();
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
                    return Err(PluginError::LockAlreadyAcquired.into());
                }
            }
        }

        let new_lock = PluginOp::MigrationLock(PicoPropertyLock::from(my_instance_info));
        let dml = Dml::replace(
            ClusterwideTable::Property,
            &(&PropertyName::PendingPluginOperation, new_lock),
            effective_user_id(),
        )?;
        let ranges = vec![
            //  if someone acquires the lock
            Range::new(ClusterwideTable::Property).eq([PropertyName::PendingPluginOperation]),
        ];

        Ok(PreconditionCheckResult::DoOp((Op::Dml(dml), ranges)))
    };

    if let Err(e) = reenterable_plugin_cas_request(node, precondition, deadline) {
        if matches!(
            e,
            traft::error::Error::Plugin(PluginError::LockAlreadyAcquired)
        ) {
            return Err(PluginError::LockAlreadyAcquired);
        }

        return Err(Error::AcquireLock(e.to_string()).into());
    }

    Ok(())
}

/// Return `Ok` if lock already acquired by current instance, and it's actual
/// (this means that the instance incarnation has not changed since the lock was taken).
///
/// # Errors
/// Return [`PluginError::LockGoneUnexpected`] if lock should be already acquired by current instance,
/// but it's not. Return errors if storage fails.
pub fn lock_is_acquired_by_us() -> traft::Result<()> {
    let node = node::global()?;
    let instance_info = InstanceInfo::try_get(node, None)?;
    let Some(PluginOp::MigrationLock(lock)) = node.storage.properties.pending_plugin_op()? else {
        return Err(PluginError::LockGoneUnexpected.into());
    };
    if lock != PicoPropertyLock::from(instance_info) {
        return Err(PluginError::LockGoneUnexpected.into());
    }
    Ok(())
}

/// Release a lock.
///
/// # Errors
///
/// Return error if lock already released or underlying CAS fails.
pub fn release(deadline: Instant) -> crate::plugin::Result<()> {
    let node = node::global().expect("node must be already initialized");
    let precondition = || {
        let existed_lock = node.storage.properties.pending_plugin_op()?;

        match existed_lock {
            None => return Err(PluginError::LockAlreadyReleased.into()),
            Some(PluginOp::MigrationLock(_)) => {}
            _ => return Err(PluginError::LockGoneUnexpected.into()),
        };

        let dml = Dml::delete(
            ClusterwideTable::Property,
            &[PropertyName::PendingPluginOperation],
            effective_user_id(),
        )?;

        let ranges =
            vec![Range::new(ClusterwideTable::Property).eq([PropertyName::PendingPluginOperation])];

        Ok(PreconditionCheckResult::DoOp((Op::Dml(dml), ranges)))
    };

    if let Err(e) = reenterable_plugin_cas_request(node, precondition, deadline) {
        if matches!(
            e,
            traft::error::Error::Plugin(PluginError::LockAlreadyReleased)
        ) {
            return Err(PluginError::LockAlreadyReleased);
        }

        return Err(Error::ReleaseLock(e.to_string()).into());
    }

    Ok(())
}
