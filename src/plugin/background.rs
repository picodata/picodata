use crate::plugin::ServiceState;
use crate::tlog;
use crate::traft::node;
use picodata_plugin::background::FfiBackgroundJobCancellationToken;
use picodata_plugin::background::JobCancellationResult;
use picodata_plugin::error_code::ErrorCode;
use picodata_plugin::plugin::interface::ServiceId;
use smol_str::SmolStr;
use std::time::Duration;
use tarantool::error::BoxError;
use tarantool::fiber;
use tarantool::time::Instant;

////////////////////////////////////////////////////////////////////////////////
// register_background_job_cancellation_token
////////////////////////////////////////////////////////////////////////////////

pub fn register_background_job_cancellation_token(
    service: ServiceId,
    job_tag: SmolStr,
    token: FfiBackgroundJobCancellationToken,
) -> Result<(), BoxError> {
    let node = node::global().expect("initialized before plugins");
    let manager = &node.plugin_manager;

    if manager.get_service_state(&service).is_none() {
        crate::warn_or_panic!("plugin callback called for non-existent service {service}");
        #[rustfmt::skip]
        return Err(BoxError::new(ErrorCode::NoSuchService, format!("service `{service}` not found")));
    }

    let mut guard = manager.background_job_cancellation_tokens.lock();
    let all_jobs = guard.entry(service).or_default();
    all_jobs.push((job_tag, token));

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
// cancel_background_jobs_by_tag
////////////////////////////////////////////////////////////////////////////////

pub fn cancel_background_jobs_by_tag(
    service_id: ServiceId,
    job_tag: SmolStr,
    timeout: Duration,
) -> Result<JobCancellationResult, BoxError> {
    let node = node::global().expect("initialized before plugins");
    let manager = &node.plugin_manager;

    let Some(service) = manager.get_service_state(&service_id) else {
        crate::warn_or_panic!("plugin callback called for non-existent service {service_id}");
        #[rustfmt::skip]
        return Err(BoxError::new(ErrorCode::NoSuchService, format!("service `{service_id}` not found")));
    };

    let mut guard = manager.background_job_cancellation_tokens.lock();
    let Some(all_jobs) = guard.get_mut(&service.id) else {
        return Ok(JobCancellationResult::new(0, 0));
    };

    let target_tag = job_tag;
    let mut jobs_to_cancel = vec![];
    let mut cursor = 0;
    while cursor < all_jobs.len() {
        let (job_tag, _) = &all_jobs[cursor];
        if &target_tag == job_tag {
            let token = all_jobs.swap_remove(cursor);
            jobs_to_cancel.push(token);
            continue;
        }

        cursor += 1;
    }

    // Release the lock.
    drop(guard);

    cancel_jobs(&service, &jobs_to_cancel);

    let deadline = fiber::clock().saturating_add(timeout);
    let n_timeouts = wait_jobs_finished(&service, &jobs_to_cancel, deadline);

    let n_total = jobs_to_cancel.len();
    Ok(JobCancellationResult::new(n_total as _, n_timeouts))
}

pub fn cancel_jobs(service: &ServiceState, jobs: &[(SmolStr, FfiBackgroundJobCancellationToken)]) {
    let service_id = &service.id;
    for (job_tag, token) in jobs {
        #[rustfmt::skip]
        tlog!(Debug, "cancelling service {service_id} background job `{job_tag}`");
        token.cancel_job();
    }
}

/// Returns the number of jobs which is didn't finish in time.
pub fn wait_jobs_finished(
    service: &ServiceState,
    jobs: &[(SmolStr, FfiBackgroundJobCancellationToken)],
    deadline: Instant,
) -> u32 {
    let service_id = &service.id;

    let mut n_timeouts = 0;
    for (job_tag, token) in jobs {
        let timeout = deadline.duration_since(fiber::clock());
        let res = token.wait_job_finished(timeout);
        if res.is_err() {
            let e = BoxError::last();
            #[rustfmt::skip]
            tlog!(Warning, "service {service_id} job `{job_tag}` didn't finish in time: {e}");

            n_timeouts += 1;
        }
    }

    n_timeouts
}

////////////////////////////////////////////////////////////////////////////////
// set_shutdown_jobs_shutdown_timeout
////////////////////////////////////////////////////////////////////////////////

pub fn set_jobs_shutdown_timeout(service: ServiceId, timeout: Duration) {
    let node = node::global().expect("initialized before plugins");
    let manager = &node.plugin_manager;

    let Some(service) = manager.get_service_state(&service) else {
        crate::warn_or_panic!("plugin callback called for non-existent service {service}");
        return;
    };

    service.background_job_shutdown_timeout.set(Some(timeout));
}
