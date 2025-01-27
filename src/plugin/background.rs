use crate::plugin::ServiceId;
use crate::traft::node;
use std::time::Duration;

////////////////////////////////////////////////////////////////////////////////
// set_shutdown_jobs_shutdown_timeout
////////////////////////////////////////////////////////////////////////////////

pub fn set_jobs_shutdown_timeout(service: ServiceId, timeout: Duration) {
    let node = node::global().expect("initialized before plugins");
    let manager = &node.plugin_manager;

    let Some(service) = manager.get_service_state(&service) else {
        crate::warn_or_panic!("plugin callback called for non-existent service");
        return;
    };

    service.background_job_shutdown_timeout.set(Some(timeout));
}
