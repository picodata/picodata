use crate::plugin::ServiceState;
use crate::tlog;
use crate::traft::node;
use picodata_plugin::background::FfiBackgroundJobCancellationToken;
use picodata_plugin::background::JobCancellationResult;
use picodata_plugin::error_code::ErrorCode;
use picodata_plugin::plugin::interface::ServiceId;
use smol_str::SmolStr;
use std::rc::Rc;
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
    all_jobs.push((job_tag, Rc::new(token)));

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

    let target_tag = job_tag;
    let mut jobs_to_cancel = vec![];

    {
        let guard = manager.background_job_cancellation_tokens.lock();
        let Some(all_jobs) = guard.get(&service.id) else {
            return Ok(JobCancellationResult::new(0, 0));
        };

        // Clone Rcs of matching tokens. They stay in the map so that
        // stop_background_jobs can see them if called concurrently.
        for (job_tag, token) in all_jobs {
            if &target_tag == job_tag {
                jobs_to_cancel.push((job_tag.clone(), Rc::clone(token)));
            }
        }
    }

    if jobs_to_cancel.is_empty() {
        return Ok(JobCancellationResult::new(0, 0));
    }

    cancel_jobs(&service, &jobs_to_cancel);

    let deadline = fiber::clock().saturating_add(timeout);
    let (finished_jobs, timed_out_jobs) =
        wait_jobs_finished_deadline(&service, &jobs_to_cancel, Some(deadline));

    let n_timeouts = timed_out_jobs.len();
    let n_total = finished_jobs.len() + n_timeouts;

    // Remove finished jobs from the map.
    if !finished_jobs.is_empty() {
        let mut guard = manager.background_job_cancellation_tokens.lock();
        if let Some(all_jobs) = guard.get_mut(&service.id) {
            all_jobs.retain(|(_, token)| {
                !finished_jobs
                    .iter()
                    .any(|finished| Rc::ptr_eq(token, finished))
            });
        }
    }

    Ok(JobCancellationResult::new(n_total as _, n_timeouts as _))
}

pub fn cancel_jobs(
    service: &ServiceState,
    jobs: &[(SmolStr, Rc<FfiBackgroundJobCancellationToken>)],
) {
    let service_id = &service.id;
    for (job_tag, token) in jobs {
        #[rustfmt::skip]
        tlog!(Debug, "cancelling service {service_id} background job `{job_tag}`");
        token.cancel_job();
    }
}

/// Waits for all jobs to finish without any timeout.
/// Used during plugin shutdown to ensure all fibers terminate before unloading.
pub fn wait_jobs_finished(
    service: &ServiceState,
    jobs: Vec<(SmolStr, Rc<FfiBackgroundJobCancellationToken>)>,
) {
    let _ = wait_jobs_finished_deadline(service, &jobs, None);
}

/// Waits for jobs to finish and partitions them into finished and timed-out.
/// Returns (finished jobs, timed-out jobs) so caller can identify which jobs completed.
///
/// If `deadline` is `None`, waits indefinitely.
fn wait_jobs_finished_deadline(
    service: &ServiceState,
    jobs: &[(SmolStr, Rc<FfiBackgroundJobCancellationToken>)],
    deadline: Option<Instant>,
) -> (
    Vec<Rc<FfiBackgroundJobCancellationToken>>,
    Vec<Rc<FfiBackgroundJobCancellationToken>>,
) {
    let service_id = &service.id;

    let mut finished = vec![];
    let mut timed_out = vec![];

    for (job_tag, token) in jobs {
        let timeout = match deadline {
            Some(dl) => dl.duration_since(fiber::clock()),
            None => tarantool::clock::INFINITY,
        };
        let res = token.wait_job_finished(timeout);
        if res.is_ok() {
            finished.push(Rc::clone(token));
        } else {
            let e = BoxError::last();
            #[rustfmt::skip]
            tlog!(Warning, "service {service_id} job `{job_tag}` didn't finish in time: {e}");
            timed_out.push(Rc::clone(token));
            if deadline.is_none() {
                unreachable!("infinite timeout expired");
            }
        }
    }

    (finished, timed_out)
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
