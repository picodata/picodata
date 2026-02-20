use crate::config::{DEFAULT_SQL_RUNTIME_CONCURRENCY_MAX, DYNAMIC_CONFIG};
use crate::limiter::{AcquireResult, ReleaseResult, TicketPool};
use smol_str::format_smolstr;
use sql::errors::{Entity, SbroadError};
use std::cell::OnceCell;
use tarantool::fiber;
use uuid::Uuid;

struct SqlRuntimeLease<'a> {
    pool: &'a TicketPool<Uuid>,
    request_id: Uuid,
}

impl Drop for SqlRuntimeLease<'_> {
    fn drop(&mut self) {
        match self.pool.release(&self.request_id) {
            ReleaseResult::Released | ReleaseResult::StillAcquired => {}
            ReleaseResult::Unknown => {
                crate::tlog!(
                    Error,
                    "sql runtime limiter release returned Unknown";
                    "request_id" => %self.request_id,
                    "fiber_id" => fiber::id()
                );
                debug_assert!(
                    false,
                    "sql runtime slot released by unknown request_id: {}",
                    self.request_id
                );
            }
        }
    }
}

thread_local!(static SQL_RUNTIME_LIMITER: OnceCell<TicketPool<Uuid>> = const { OnceCell::new() });

fn sql_runtime_concurrency_max() -> u64 {
    DYNAMIC_CONFIG
        .sql_runtime_concurrency_max
        .try_current_value()
        .unwrap_or(DEFAULT_SQL_RUNTIME_CONCURRENCY_MAX)
}

pub(crate) fn with_sql_runtime_limit<T>(
    request_id: Uuid,
    f: impl FnOnce() -> T,
) -> Result<T, SbroadError> {
    SQL_RUNTIME_LIMITER.with(|cell| {
        let limiter = cell.get_or_init(TicketPool::default);
        let acquire = limiter.acquire_blocking(request_id, sql_runtime_concurrency_max());
        match acquire {
            AcquireResult::Acquired | AcquireResult::Reentrant => {}
            AcquireResult::Cancelled => {
                return Err(SbroadError::Other(format_smolstr!(
                    "sql runtime concurrency wait cancelled"
                )));
            }
            AcquireResult::Saturated => {
                return Err(SbroadError::Other(format_smolstr!(
                    "sql runtime concurrency acquire stuck in saturated state"
                )));
            }
        }

        let _lease = SqlRuntimeLease {
            pool: limiter,
            request_id,
        };
        Ok(f())
    })
}

#[inline(always)]
pub(crate) fn runtime_owner_key(request_id: &str) -> Result<Uuid, SbroadError> {
    Uuid::parse_str(request_id).map_err(|e| {
        SbroadError::Invalid(
            Entity::Value,
            Some(format_smolstr!(
                "invalid SQL request_id '{request_id}': {e}"
            )),
        )
    })
}
