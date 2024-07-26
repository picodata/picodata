use crate::background::ServiceId;
use abi_stable::std_types::RString;
use std::collections::HashMap;
use std::sync::OnceLock;
use tarantool::fiber;

pub type MetricsCallback = fn() -> String;

// FIXME: this trampoline is not totally stable, cause Fn() layout may be changed across rust versions.
// We persist this closure at plugin side and read at Picodata side, so we need a stable closure for this,
// but there is not safe closures in `stable_abi` crate.
type StableMetricsCallback = dyn Fn() -> RString + 'static;

/// This component is using by `picodata` for manage all user defined metrics.
///
/// *For internal usage, don't use it in your code*.
#[derive(Default)]
pub struct InternalGlobalMetricsCollection {
    metrics: fiber::Mutex<HashMap<ServiceId, Box<StableMetricsCallback>>>,
}

// SAFETY: `InternalGlobalMetricsCollection` must be used only in the tx thread
unsafe impl Send for InternalGlobalMetricsCollection {}
unsafe impl Sync for InternalGlobalMetricsCollection {}

static MGM: OnceLock<InternalGlobalMetricsCollection> = OnceLock::new();

impl InternalGlobalMetricsCollection {
    /// Return reference to a global metrics collection.
    pub fn instance() -> &'static Self {
        let mgm_ref = MGM.get_or_init(InternalGlobalMetricsCollection::default);
        mgm_ref
    }

    /// Remove metrics by service identifier.
    pub fn remove(&self, service_id: &ServiceId) {
        self.metrics.lock().remove(service_id);
    }

    /// Return joined metrics from all registered callbacks.
    pub fn all_metrics(&self) -> String {
        let mut result = String::new();
        for cb in self.metrics.lock().values() {
            let metrics = cb();
            result += "\n";
            result += metrics.as_str();
        }
        result
    }
}

pub struct MetricsCollection<'a> {
    plugin_name: &'a str,
    plugin_version: &'a str,
    service_name: &'a str,
    global_collection: &'static InternalGlobalMetricsCollection,
}

impl<'a> MetricsCollection<'a> {
    pub(crate) fn new(
        plugin_name: &'a str,
        plugin_version: &'a str,
        service_name: &'a str,
        global_collection: &'static InternalGlobalMetricsCollection,
    ) -> Self {
        Self {
            plugin_name,
            plugin_version,
            service_name,
            global_collection,
        }
    }

    /// Append callback with stringified metrics representation to a global metrics collection.
    /// This callback will be called at every metrics poll request (by request to a "/metrics" http endpoint).
    pub fn append(&self, callback: MetricsCallback) {
        let mut lock = self.global_collection.metrics.lock();
        lock.insert(
            ServiceId::new(self.plugin_name, self.service_name, self.plugin_version),
            Box::new(move || callback().into()),
        );
    }
}
