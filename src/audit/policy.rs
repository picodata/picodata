use crate::access_control;
use crate::schema;
use crate::traft;
use crate::util;
use smol_str::ToSmolStr;
use sql::errors::SbroadError;
use sql::ir;
use sql::ir::value::{DisplayValues, Value};
use tarantool::session::with_su;

pub type AuditPolicyId = u32;

/// Retrieves the audit policy ID associated with the given policy name.
///
/// # Returns
/// * `Some(AuditPolicyId)` if a policy with the specified name exists
/// * `None` if no matching policy is found
pub fn get_audit_policy_id_by_name(policy_name: &str) -> Option<AuditPolicyId> {
    if policy_name == DmlDefaultPolicy::NAME {
        Some(DmlDefaultPolicy::ID)
    } else {
        None
    }
}

/// Retrieves the audit policy name associated with the given policy ID.
///
/// # Returns
/// * `Some(&'static str)` if a policy with the specified ID exists
/// * `None` if no matching policy is found
pub fn get_audit_policy_name_by_id(policy_id: AuditPolicyId) -> Option<&'static str> {
    if policy_id == DmlDefaultPolicy::ID {
        Some(DmlDefaultPolicy::NAME)
    } else {
        None
    }
}

/// Determines whether DML operations performed by a user require audit logging.
pub fn is_dml_audit_enabled_for_user(plan: &ir::Plan) -> Result<bool, SbroadError> {
    if crate::audit::root().is_none() {
        return Ok(false);
    }
    if !plan.is_dml()? {
        return Ok(false);
    }

    let current_user = util::effective_user_id();
    with_su(schema::ADMIN_ID, || {
        let node = traft::node::global()?;
        let Ok(space) = node.storage.users_audit_policies.get_space() else {
            // During the upgrade process, the table might temporarily not exist.
            // Do not log in this case.
            return Ok(false);
        };
        if space.get(&(current_user, DmlDefaultPolicy::ID))?.is_none() {
            return Ok(false);
        }
        Ok::<bool, traft::error::Error>(true)
    })?
    .map_err(|e| SbroadError::Other(e.to_smolstr()))
}

/// Performs the DML operation audit logging.
pub fn log_dml_for_user(query: &str, params: Option<&[Value]>) {
    let current_user = util::effective_user_id();
    let initiator = with_su(schema::ADMIN_ID, || {
        access_control::user_by_id(current_user)
            .expect("user must exist")
            .name
    })
    .expect("must be able to su into admin");

    let message = match params {
        Some(params) if !params.is_empty() => {
            // TODO: use `format_args!` when update MSRV
            // (https://github.com/rust-lang/rust/issues/92698)
            format!("apply `{}` with params {}", query, DisplayValues(params))
        }
        _ => format!("apply `{}`", query),
    };
    crate::audit!(
        message: "{message}",
        title: "dml",
        severity: Medium,
        initiator: initiator,
    );
}

/// Default audit policy that enables audit logging for all DML operations.
struct DmlDefaultPolicy;

impl DmlDefaultPolicy {
    const ID: AuditPolicyId = 0;
    const NAME: &'static str = "dml_default";
}
