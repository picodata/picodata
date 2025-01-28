//! Background container for long live jobs.
//! Background container guarantees job liveness across plugin life cycle.

use crate::internal::ffi;
#[allow(unused_imports)]
use crate::plugin::interface::PicoContext;
use crate::plugin::interface::ServiceId;
use crate::util::tarantool_error_to_box_error;
use crate::util::DisplayErrorLocation;
use std::cell::Cell;
use std::time::Duration;
use tarantool::error::BoxError;
use tarantool::error::TarantoolErrorCode;
use tarantool::fiber;
use tarantool::fiber::FiberId;
use tarantool::fiber::{Channel, RecvError};
use tarantool::util::IntoClones;

/// Same as [`PicoContext::register_job`].
#[track_caller]
pub fn register_job<F>(service_id: &ServiceId, job: F) -> Result<(), BoxError>
where
    F: FnOnce(CancellationToken) + 'static,
{
    let loc = std::panic::Location::caller();
    let tag = format!("{}:{}", loc.file(), loc.line());

    register_tagged_job(service_id, job, &tag)
}

/// Same as [`PicoContext::register_tagged_job`].
#[allow(deprecated)]
pub fn register_tagged_job<F>(service_id: &ServiceId, job: F, tag: &str) -> Result<(), BoxError>
where
    F: FnOnce(CancellationToken) + 'static,
{
    let (token, handle) = CancellationToken::new();
    let finish_channel = handle.finish_channel.clone();

    let fiber_id = fiber::Builder::new()
        .name(tag)
        .func(move || {
            job(token);
            // send shutdown signal to the waiter side
            _ = finish_channel.send(());
        })
        .start_non_joinable()
        .map_err(tarantool_error_to_box_error)?;

    let plugin = &service_id.plugin;
    let service = &service_id.service;
    let version = &service_id.version;

    let token = FfiBackgroundJobCancellationToken::new(
        fiber_id,
        handle.cancel_channel,
        handle.finish_channel,
    );
    register_background_job_cancellation_token(plugin, service, version, tag, token)?;

    Ok(())
}

fn register_background_job_cancellation_token(
    plugin: &str,
    service: &str,
    version: &str,
    job_tag: &str,
    token: FfiBackgroundJobCancellationToken,
) -> Result<(), BoxError> {
    // SAFETY: safe as long as picodata version is compatible
    let rc = unsafe {
        ffi::pico_ffi_background_register_job_cancellation_token(
            plugin.into(),
            service.into(),
            version.into(),
            job_tag.into(),
            token,
        )
    };

    if rc != 0 {
        return Err(BoxError::last());
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
// cancel_jobs_by_tag
////////////////////////////////////////////////////////////////////////////////

/// Outcome of the request to cancel a set of background jobs.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct JobCancellationResult {
    /// Attempted to cancel this many background jobs.
    pub n_total: u32,
    /// This many jobs didn't finish in time.
    pub n_timeouts: u32,
}

impl JobCancellationResult {
    #[inline(always)]
    pub fn new(n_total: u32, n_timeouts: u32) -> Self {
        Self {
            n_total,
            n_timeouts,
        }
    }
}

/// Same as [`PicoContext::cancel_tagged_jobs`].
#[inline(always)]
pub fn cancel_jobs_by_tag(
    service_id: &ServiceId,
    job_tag: &str,
    timeout: Duration,
) -> Result<JobCancellationResult, BoxError> {
    cancel_background_jobs_by_tag_inner(
        &service_id.plugin,
        &service_id.service,
        &service_id.version,
        job_tag,
        timeout,
    )
}

/// Same as [`PicoContext::cancel_tagged_jobs`].
pub fn cancel_background_jobs_by_tag_inner(
    plugin: &str,
    service: &str,
    version: &str,
    job_tag: &str,
    timeout: Duration,
) -> Result<JobCancellationResult, BoxError> {
    let mut result = JobCancellationResult::default();
    // SAFETY: safe as long as picodata version is compatible
    let rc = unsafe {
        ffi::pico_ffi_background_cancel_jobs_by_tag(
            plugin.into(),
            service.into(),
            version.into(),
            job_tag.into(),
            timeout.as_secs_f64(),
            &mut result,
        )
    };

    if rc != 0 {
        return Err(BoxError::last());
    }

    Ok(result)
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Some of jobs are not fully completed, expected: {0}, completed: {1}")]
    PartialCompleted(usize, usize),
    #[error("timeout")]
    CancellationTimeout,
}

////////////////////////////////////////////////////////////////////////////////
// CancellationToken
////////////////////////////////////////////////////////////////////////////////

/// A token which can be used to signal a cancellation request to the job.
#[non_exhaustive]
#[derive(Debug)]
pub struct CancellationToken {
    cancel_channel: Channel<()>,
}

impl CancellationToken {
    /// Create a cancellation token and cancellation token handle pair.
    /// User should use cancellation token for graceful shutdown their job.
    /// Cancellation token handle used by `picodata` for sending cancel signal to a user job.
    #[allow(deprecated)]
    pub fn new() -> (CancellationToken, CancellationTokenHandle) {
        let (cancel_tx, cancel_rx) = Channel::new(1).into_clones();
        (
            CancellationToken {
                cancel_channel: cancel_rx,
            },
            CancellationTokenHandle {
                cancel_channel: cancel_tx,
                finish_channel: Channel::new(1),
            },
        )
    }

    /// Wait until token are canceled.
    ///
    /// # Arguments
    ///
    /// * `timeout`: if cancellation does not occur within the specified timeout - return error
    ///
    /// # Errors
    ///
    /// Return `Ok` if token is canceled or [`Error::CancellationTimeout`] if timeout is reached.
    pub fn wait_timeout(&self, timeout: Duration) -> Result<(), Error> {
        match self.cancel_channel.recv_timeout(timeout) {
            Err(RecvError::Timeout) => Err(Error::CancellationTimeout),
            _ => Ok(()),
        }
    }
}

#[derive(Debug)]
#[deprecated = "don't use this"]
pub struct CancellationTokenHandle {
    cancel_channel: Channel<()>,
    finish_channel: Channel<()>,
}

#[allow(deprecated)]
impl CancellationTokenHandle {
    /// Cancel related job and return a backpressure channel.
    /// Caller should wait a message in the backpressure channel
    /// to make sure the job is completed successfully (graceful shutdown occurred).
    pub fn cancel(self) -> Channel<()> {
        let Self {
            cancel_channel,
            finish_channel,
        } = self;
        _ = cancel_channel.send(());
        finish_channel
    }
}

////////////////////////////////////////////////////////////////////////////////
// Java-style API
////////////////////////////////////////////////////////////////////////////////

/// [`ServiceWorkerManager`] allows plugin services
/// to create long-live jobs and manage their life cycle.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ServiceWorkerManager {
    service_id: ServiceId,
}

impl ServiceWorkerManager {
    #[inline(always)]
    pub(crate) fn new(service_id: ServiceId) -> Self {
        Self { service_id }
    }

    /// Add a new job to the execution.
    /// Job work life cycle will be tied to the service life cycle;
    /// this means that job will be canceled just before service is stopped.
    ///
    /// # Arguments
    ///
    /// * `job`: callback that will be executed in separated fiber.
    /// Note that it is your responsibility to organize job graceful shutdown, see a
    /// [`CancellationToken`] for details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::time::Duration;
    /// use picodata_plugin::background::CancellationToken;
    ///
    /// # use picodata_plugin::background::ServiceWorkerManager;
    /// # fn test(worker_manager: ServiceWorkerManager) {
    ///
    /// // this job will print "hello" every second,
    /// // and print "bye" after being canceled
    /// fn hello_printer(cancel: CancellationToken) {
    ///     while cancel.wait_timeout(Duration::from_secs(1)).is_err() {
    ///         println!("hello!");
    ///     }
    ///     println!("job cancelled, bye!")
    /// }
    /// worker_manager.register_job(hello_printer).unwrap();
    ///
    /// # }
    /// ```
    #[track_caller]
    #[inline(always)]
    pub fn register_job<F>(&self, job: F) -> tarantool::Result<()>
    where
        F: FnOnce(CancellationToken) + 'static,
    {
        register_job(&self.service_id, job)?;

        Ok(())
    }

    /// Same as [`ServiceWorkerManager::register_job`] but caller may provide a special tag.
    /// This tag may be used for manual job cancellation using [`ServiceWorkerManager::cancel_tagged`].
    ///
    /// # Arguments
    ///
    /// * `job`: callback that will be executed in separated fiber
    /// * `tag`: tag, that will be related to a job, single tag may be related to the multiple jobs
    #[inline(always)]
    pub fn register_tagged_job<F>(&self, job: F, tag: &str) -> tarantool::Result<()>
    where
        F: FnOnce(CancellationToken) + 'static,
    {
        register_tagged_job(&self.service_id, job, tag)?;

        Ok(())
    }

    /// Cancel all jobs related to the given tag.
    /// This function return after all related jobs will be gracefully shutdown or
    /// after `timeout` duration.
    /// May return [`Error::PartialCompleted`] if timeout is reached.
    ///
    /// # Arguments
    ///
    /// * `tag`: determine what jobs should be cancelled
    /// * `timeout`: shutdown timeout
    pub fn cancel_tagged(&self, tag: &str, timeout: Duration) -> Result<(), Error> {
        let res = cancel_jobs_by_tag(&self.service_id, tag, timeout);
        let res = match res {
            Ok(res) => res,
            Err(e) => {
                let loc = DisplayErrorLocation(&e);
                tarantool::say_error!("unexpected error: {loc}{e}");
                return Ok(());
            }
        };

        if res.n_timeouts != 0 {
            let n_completed = res.n_total - res.n_timeouts;
            return Err(Error::PartialCompleted(res.n_total as _, n_completed as _));
        }

        Ok(())
    }

    /// In case when jobs were canceled by `picodata` use this function for determine
    /// a shutdown timeout - time duration that `picodata` uses to ensure that all
    /// jobs gracefully end.
    ///
    /// By default, 5-second timeout are used.
    #[deprecated = "use `PicoContext::set_jobs_shutdown_timeout` instead"]
    pub fn set_shutdown_timeout(&self, timeout: Duration) {
        let plugin = &self.service_id.plugin;
        let service = &self.service_id.service;
        let version = &self.service_id.version;
        set_jobs_shutdown_timeout(plugin, service, version, timeout)
    }
}

////////////////////////////////////////////////////////////////////////////////
// set_background_jobs_shutdown_timeout
////////////////////////////////////////////////////////////////////////////////

/// In case when jobs were canceled by `picodata` use this function to determine
/// a shutdown timeout - time duration that `picodata` uses to ensure that all
/// jobs gracefully end.
///
/// By default, 5-second timeout are used.
///
/// Consider using [`PicoContext::set_jobs_shutdown_timeout`] instead
pub fn set_jobs_shutdown_timeout(plugin: &str, service: &str, version: &str, timeout: Duration) {
    // SAFETY: safe as long as picodata version is compatible
    let rc = unsafe {
        ffi::pico_ffi_background_set_jobs_shutdown_timeout(
            plugin.into(),
            service.into(),
            version.into(),
            timeout.as_secs_f64(),
        )
    };
    debug_assert!(
        rc == 0,
        "return code is only for future compatibility at the moment"
    );
}

////////////////////////////////////////////////////////////////////////////////
// CancellationCallbackState
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct CancellationCallbackState {
    cancel_channel: Channel<()>,
    finish_channel: Channel<()>,
    status: Cell<CancellationCallbackStatus>,
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum CancellationCallbackStatus {
    #[default]
    Initial = 0,
    JobCancelled = 1,
    JobFinished = 2,
}

impl CancellationCallbackState {
    fn new(cancel_channel: Channel<()>, finish_channel: Channel<()>) -> Self {
        Self {
            cancel_channel,
            finish_channel,
            status: Cell::new(CancellationCallbackStatus::Initial),
        }
    }

    fn cancellation_callback(&self, action: u64, timeout: Duration) -> Result<(), BoxError> {
        use CancellationCallbackStatus::*;
        let next_status = action;

        if next_status == JobCancelled as u64 {
            debug_assert_eq!(self.status.get(), Initial);
            _ = self.cancel_channel.send(());

            self.status.set(JobCancelled);
        } else if next_status == JobFinished as u64 {
            debug_assert_eq!(self.status.get(), JobCancelled);
            self.finish_channel.recv_timeout(timeout).map_err(|e| {
                BoxError::new(TarantoolErrorCode::Timeout, stupid_recv_error_to_string(e))
            })?;

            self.status.set(JobFinished);
        } else {
            return Err(BoxError::new(
                TarantoolErrorCode::IllegalParams,
                format!("unexpected action: {action}"),
            ));
        }

        Ok(())
    }
}

#[inline]
fn stupid_recv_error_to_string(e: fiber::channel::RecvError) -> &'static str {
    match e {
        fiber::channel::RecvError::Timeout => "timeout",
        fiber::channel::RecvError::Disconnected => "disconnected",
    }
}

////////////////////////////////////////////////////////////////////////////////
// ffi wrappers
////////////////////////////////////////////////////////////////////////////////

/// **For internal use**.
#[repr(C)]
pub struct FfiBackgroundJobCancellationToken {
    /// This is just a way to do arbitrarily complex things across ABI boundary
    /// without introducing a large amount of FFI-safe wrappers. This will
    /// always be [`CancellationCallbackState::cancellation_callback`].
    callback: extern "C-unwind" fn(data: *const Self, action: u64, timeout: f64) -> i32,
    drop: extern "C-unwind" fn(*mut Self),

    /// The pointer to the closure object.
    closure_pointer: *mut (),

    /// This is the background job fiber which will be cancelled by this token.
    pub fiber_id: FiberId,
}

impl Drop for FfiBackgroundJobCancellationToken {
    #[inline(always)]
    fn drop(&mut self) {
        (self.drop)(self)
    }
}

impl FfiBackgroundJobCancellationToken {
    fn new(fiber_id: FiberId, cancel_channel: Channel<()>, finish_channel: Channel<()>) -> Self {
        let callback_state = CancellationCallbackState::new(cancel_channel, finish_channel);
        let callback = move |action, timeout| {
            let res = callback_state.cancellation_callback(action, timeout);
            if let Err(e) = res {
                e.set_last();
                return -1;
            }

            0
        };

        Self::new_inner(fiber_id, callback)
    }

    /// This function is needed, because we need this `F` type parameter so that
    /// we can specialize the `callback` and `drop` with it inside this function.
    /// If rust supported something like `type F = type_of(callback);` we
    /// wouldn't need this additional function and would just write this code in
    /// the [`Self::new`] above.
    // FIXME: just define an explicit extern "C" fn for cancellation_callback?
    fn new_inner<F>(fiber_id: FiberId, f: F) -> Self
    where
        F: FnMut(u64, Duration) -> i32,
    {
        let closure = Box::new(f);
        let closure_pointer: *mut F = Box::into_raw(closure);

        Self {
            callback: Self::trampoline::<F>,
            drop: Self::drop_handler::<F>,
            closure_pointer: closure_pointer.cast(),

            fiber_id,
        }
    }

    /// An ABI-safe wrapper which calls the rust closure stored in `handler`.
    ///
    /// The result of the closure is copied onto the fiber's region allocation
    /// and the pointer to that allocation is written into `output`.
    extern "C-unwind" fn trampoline<F>(data: *const Self, action: u64, timeout: f64) -> i32
    where
        F: FnMut(u64, Duration) -> i32,
    {
        // This is safe. To verify see `register_rpc_handler` above.
        let closure_pointer: *mut F = unsafe { (*data).closure_pointer.cast::<F>() };
        let closure = unsafe { &mut *closure_pointer };

        closure(action, Duration::from_secs_f64(timeout))
    }

    extern "C-unwind" fn drop_handler<F>(handler: *mut Self) {
        unsafe {
            let closure_pointer: *mut F = (*handler).closure_pointer.cast::<F>();
            let closure = Box::from_raw(closure_pointer);
            drop(closure);

            if cfg!(debug_assertions) {
                // Overwrite the pointer with garbage so that we fail loudly is case of a bug
                (*handler).closure_pointer = 0xcccccccccccccccc_u64 as _;
            }
        }
    }

    /// The error is returned via [`BoxError::set_last`].
    #[inline(always)]
    pub fn cancel_job(&self) {
        let rc = (self.callback)(self, CancellationCallbackStatus::JobCancelled as _, 0.0);
        debug_assert!(rc == 0);
    }

    /// The error is returned via [`BoxError::set_last`].
    #[inline(always)]
    pub fn wait_job_finished(&self, timeout: Duration) -> Result<(), ()> {
        let rc = (self.callback)(
            self,
            CancellationCallbackStatus::JobFinished as _,
            timeout.as_secs_f64(),
        );
        if rc == -1 {
            // Actual error is passed through tarantool. Can't return BoxError
            // here, because tarantool-module version may be different in picodata.
            return Err(());
        }

        Ok(())
    }
}
