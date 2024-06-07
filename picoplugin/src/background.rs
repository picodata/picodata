//! Background container for long live jobs.
//! Background container guarantees job liveness across plugin life cycle.

use crate::system::tarantool::fiber;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::mem;
use std::rc::Rc;
use std::string::ToString;
use std::sync::OnceLock;
use std::time::Duration;
use tarantool::fiber::{Channel, RecvError};
use tarantool::time::Instant;
use tarantool::util::IntoClones;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Some of jobs are not fully completed, expected: {0}, completed: {1}")]
    PartialCompleted(usize, usize),
    #[error("timeout")]
    CancellationTimeout,
}

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
    fn new() -> (CancellationToken, CancellationTokenHandle) {
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
struct CancellationTokenHandle {
    cancel_channel: Channel<()>,
    finish_channel: Channel<()>,
}

impl CancellationTokenHandle {
    /// Cancel related job and return a backpressure channel.
    /// Caller should wait a message in the backpressure channel
    /// to make sure the job is completed successfully (graceful shutdown occurred).
    fn cancel(self) -> Channel<()> {
        let Self {
            cancel_channel,
            finish_channel,
        } = self;
        _ = cancel_channel.send(());
        finish_channel
    }
}

type Tag = String;

#[derive(PartialEq, Eq, Hash, Clone)]
pub struct ServiceId {
    plugin_name: String,
    service_name: String,
    version: String,
}

impl Display for ServiceId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "{}.{}:{}",
            self.plugin_name, self.service_name, self.version
        ))
    }
}

impl ServiceId {
    pub fn new<S: Into<String>>(plugin_name: S, service_name: S, version: S) -> Self {
        Self {
            plugin_name: plugin_name.into(),
            service_name: service_name.into(),
            version: version.into(),
        }
    }
}

type TaggedJobs = HashMap<Tag, Vec<CancellationTokenHandle>>;
type UnTaggedJobs = Vec<CancellationTokenHandle>;

fn cancel_handles(handles: Vec<CancellationTokenHandle>, deadline: Instant) -> usize {
    let mut shutdown_channels = Vec::with_capacity(handles.len());

    for handle in handles {
        let shutdown_channel = handle.cancel();
        shutdown_channels.push(shutdown_channel);
    }

    let mut completed_counter = 0;
    for sd_chan in shutdown_channels {
        // recalculate timeout at every iteration
        let timeout = deadline.duration_since(Instant::now_fiber());

        if sd_chan.recv_timeout(timeout).is_ok() {
            completed_counter += 1;
        }
    }

    completed_counter
}

/// [`ServiceWorkerManager`] allows plugin services
/// to create long-live jobs and manage their life cycle.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ServiceWorkerManager {
    jobs: Rc<fiber::Mutex<(TaggedJobs, UnTaggedJobs)>>,
    shutdown_timeout: Rc<fiber::Mutex<Duration>>,
}

impl ServiceWorkerManager {
    fn register_job_inner<F>(&self, job: F, maybe_tag: Option<&str>) -> tarantool::Result<()>
    where
        F: Fn(CancellationToken) + 'static,
    {
        let (token, handle) = CancellationToken::new();
        let finish_chan = handle.finish_channel.clone();
        let mut jobs = self.jobs.lock();
        if let Some(tag) = maybe_tag {
            jobs.0.entry(tag.to_string()).or_default().push(handle);
        } else {
            jobs.1.push(handle);
        }

        fiber::Builder::new()
            .func(move || {
                job(token);
                // send shutdown signal to the waiter side
                _ = finish_chan.send(());
            })
            .start_non_joinable()?;
        Ok(())
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
    /// use picoplugin::background::{CancellationToken, InternalGlobalWorkerManager, ServiceId, ServiceWorkerManager};
    ///
    /// # let worker_manager = InternalGlobalWorkerManager::instance().get_or_init_manager(ServiceId::new("any_plugin", "any_service", "0.1.0"));
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
    /// ```
    pub fn register_job<F>(&self, job: F) -> tarantool::Result<()>
    where
        F: Fn(CancellationToken) + 'static,
    {
        self.register_job_inner(job, None)
    }

    /// Same as [`ServiceWorkerManager::register_job`] but caller may provide a special tag.
    /// This tag may be used for manual job cancellation using [`ServiceWorkerManager::cancel_tagged`].
    ///
    /// # Arguments
    ///
    /// * `job`: callback that will be executed in separated fiber
    /// * `tag`: tag, that will be related to a job, single tag may be related to the multiple jobs
    pub fn register_tagged_job<F>(&self, job: F, tag: &str) -> tarantool::Result<()>
    where
        F: Fn(CancellationToken) + 'static,
    {
        self.register_job_inner(job, Some(tag))
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
        let deadline = fiber::clock().saturating_add(timeout);

        let mut jobs = self.jobs.lock();
        let handles = jobs.0.remove(tag).unwrap_or_default();

        let job_count = handles.len();

        let completed_counter = cancel_handles(handles, deadline);
        if job_count != completed_counter {
            return Err(Error::PartialCompleted(job_count, completed_counter));
        }

        Ok(())
    }

    /// In case when jobs were canceled by `picodata` use this function for determine
    /// a shutdown timeout - time duration that `picodata` uses to ensure that all
    /// jobs gracefully end.
    ///
    /// By default, 5-second timeout are used.
    pub fn set_shutdown_timeout(&self, timeout: Duration) {
        *self.shutdown_timeout.lock() = timeout;
    }
}

/// This component is using by `picodata` for manage all worker managers.
///
/// *For internal usage, don't use it in your code*.
#[derive(Default)]
pub struct InternalGlobalWorkerManager {
    managers: fiber::Mutex<HashMap<ServiceId, ServiceWorkerManager>>,
}

// SAFETY: `GlobalWorkerManager` must be used only in the tx thread
unsafe impl Send for InternalGlobalWorkerManager {}
unsafe impl Sync for InternalGlobalWorkerManager {}

static IGWM: OnceLock<InternalGlobalWorkerManager> = OnceLock::new();

impl InternalGlobalWorkerManager {
    fn get_or_insert_worker_manager(&self, service_id: ServiceId) -> ServiceWorkerManager {
        let mut managers = self.managers.lock();
        match managers.get(&service_id) {
            None => {
                let mgr = ServiceWorkerManager {
                    jobs: Rc::new(Default::default()),
                    shutdown_timeout: Rc::new(fiber::Mutex::new(Duration::from_secs(5))),
                };
                managers.insert(service_id, mgr.clone());
                mgr
            }
            Some(mgr) => mgr.clone(),
        }
    }

    fn remove_plugin_worker_manager(
        &self,
        service_id: &ServiceId,
        timeout: Duration,
    ) -> Result<(), Error> {
        let deadline = fiber::clock().saturating_add(timeout);

        let wm = self.managers.lock().remove(service_id);

        // drain all jobs manually cause user may have shared references to worker manager
        if let Some(wm) = wm {
            let mut jobs = wm.jobs.lock();
            let (tagged_jobs, untagged_jobs) = mem::take(&mut *jobs);

            let mut job_counter = 0;
            let mut completed_job_counter = 0;

            for (_, handles) in tagged_jobs {
                job_counter += handles.len();
                completed_job_counter += cancel_handles(handles, deadline);
            }

            job_counter += untagged_jobs.len();
            completed_job_counter += cancel_handles(untagged_jobs, deadline);
            if job_counter != completed_job_counter {
                return Err(Error::PartialCompleted(job_counter, completed_job_counter));
            }
        }
        Ok(())
    }

    /// Create a new worker manager for given `id` or return existed.
    pub fn get_or_init_manager(&self, id: ServiceId) -> ServiceWorkerManager {
        self.get_or_insert_worker_manager(id)
    }

    /// Return preferred shutdown timeout for worker manager jobs.
    pub fn get_shutdown_timeout(&self, service_id: &ServiceId) -> Option<Duration> {
        self.managers
            .lock()
            .get(service_id)
            .map(|mgr| *mgr.shutdown_timeout.lock())
    }

    /// Remove worker manager by `id` with all jobs.
    /// This function return after all related jobs will be gracefully shutdown or
    /// after `timeout` duration (with [`Error::PartialCompleted`] error.
    pub fn unregister_service(&self, id: &ServiceId, timeout: Duration) -> Result<(), Error> {
        self.remove_plugin_worker_manager(id, timeout)
    }

    /// Return reference to global internal worker manager.
    pub fn instance() -> &'static Self {
        let igwm_ref = IGWM.get_or_init(InternalGlobalWorkerManager::default);
        igwm_ref
    }
}

#[cfg(feature = "internal_test")]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use tarantool::fiber;

    static _1_MS: Duration = Duration::from_millis(1);
    static _10_MS: Duration = Duration::from_millis(10);
    static _100_MS: Duration = Duration::from_millis(100);
    static _200_MS: Duration = Duration::from_millis(200);

    fn make_job(
        counter: &'static AtomicU64,
        iteration_duration: Duration,
    ) -> impl Fn(CancellationToken) {
        move |cancel_token: CancellationToken| {
            while cancel_token.wait_timeout(_1_MS).is_err() {
                counter.fetch_add(1, Ordering::SeqCst);
                fiber::sleep(iteration_duration);
            }
            counter.store(0, Ordering::SeqCst);
        }
    }

    #[::tarantool::test]
    fn test_work_manager_works() {
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let gwm = InternalGlobalWorkerManager::instance();
        let svc_id = ServiceId::new("plugin_x", "svc_x", "0.1.0");
        let wm = gwm.get_or_init_manager(svc_id.clone());

        wm.register_job(make_job(&COUNTER, _1_MS)).unwrap();
        wm.register_job(make_job(&COUNTER, _1_MS)).unwrap();

        fiber::sleep(_10_MS);
        assert!(COUNTER.load(Ordering::SeqCst) > 0);

        gwm.unregister_service(&svc_id, _100_MS).unwrap();
        assert_eq!(COUNTER.load(Ordering::SeqCst), 0);
    }

    #[::tarantool::test]
    fn test_work_manager_tagged_jobs_works() {
        static COUNTER_1: AtomicU64 = AtomicU64::new(0);
        static COUNTER_2: AtomicU64 = AtomicU64::new(0);

        let gwm = InternalGlobalWorkerManager::instance();
        let svc_id = ServiceId::new("plugin_x", "svc_x", "0.1.0");
        let wm = gwm.get_or_init_manager(svc_id.clone());

        wm.register_tagged_job(make_job(&COUNTER_1, _1_MS), "j1")
            .unwrap();
        wm.register_tagged_job(make_job(&COUNTER_1, _1_MS), "j1")
            .unwrap();
        wm.register_tagged_job(make_job(&COUNTER_2, _1_MS), "j2")
            .unwrap();

        fiber::sleep(_10_MS);
        assert!(COUNTER_1.load(Ordering::SeqCst) > 0);
        assert!(COUNTER_2.load(Ordering::SeqCst) > 0);

        wm.cancel_tagged("j1", _10_MS).unwrap();

        assert_eq!(COUNTER_1.load(Ordering::SeqCst), 0);
        assert_ne!(COUNTER_2.load(Ordering::SeqCst), 0);

        gwm.unregister_service(&svc_id, _10_MS).unwrap();

        assert_eq!(COUNTER_1.load(Ordering::SeqCst), 0);
        assert_eq!(COUNTER_2.load(Ordering::SeqCst), 0);
    }

    #[::tarantool::test]
    fn test_work_manager_graceful_shutdown() {
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let gwm = InternalGlobalWorkerManager::instance();
        let svc_id = ServiceId::new("plugin_x", "svc_x", "0.1.0");
        let wm = gwm.get_or_init_manager(svc_id.clone());
        // this job cannot stop at 10ms interval
        wm.register_job(make_job(&COUNTER, _100_MS)).unwrap();
        // but this job can
        wm.register_job(make_job(&COUNTER, _1_MS)).unwrap();
        fiber::sleep(_10_MS);
        let result = gwm.unregister_service(&svc_id, _10_MS);
        assert!(
            matches!(result, Err(Error::PartialCompleted(all, completed)) if all == 2 && completed == 1)
        );
    }
}
