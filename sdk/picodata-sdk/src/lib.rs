type NameCb = fn() -> &'static str;
type StratCb = fn();
type StopCb = fn();

#[derive(Debug)]
pub struct PluginContext {
    /// need some field to describe plugin priority for cluster life
    pub shutdown_on_error: bool,
    pub is_master: bool,
}

impl PluginContext {
    // what else may be here?
    pub fn empty() -> Self {
        Self {
            shutdown_on_error: false,
            is_master: false,
        }
    }
}

#[derive(Debug)]
pub struct Plugin {
    pub context: PluginContext,

    lib: Option<libloading::Library>,

    name: NameCb,
    start: StratCb,
    stop: StopCb,
}

impl Plugin {
    pub fn new() -> Self {
        Self {
            context: PluginContext::empty(),

            lib: None,
            // all default callbacks must be defined here
            name: || "not implemented",
            start: || {},
            stop: || {},
        }
    }

    pub fn load(mut self, plugin_path: &str) -> Option<Plugin> {
        if let Ok(plugin_library) = unsafe { libloading::Library::new(plugin_path) } {
            if let Ok(symbol) = unsafe { plugin_library.get("name".as_bytes()) } {
                self.name = *symbol;
            }

            if let Ok(symbol) = unsafe { plugin_library.get("start".as_bytes()) } {
                self.start = *symbol;
            }

            if let Ok(symbol) = unsafe { plugin_library.get("stop".as_bytes()) } {
                self.stop = *symbol;
            }

            self.lib = Some(plugin_library);

            return Some(self);
        }

        None
    }

    pub fn name(&self) -> &'static str {
        (self.name)()
    }

    pub fn start(&self) {
        (self.start)()
    }

    pub fn stop(&self) {
        (self.stop)()
    }
}

impl Default for Plugin {
    fn default() -> Self {
        Self::new()
    }
}

pub trait PluginVtable {
    /***************OnAny***************/
    /// Return unique plugin name
    fn name() -> &'static str;
    fn start() {}
    fn stop() {}
}

#[cfg(test)]
mod tests {
    use super::*;

    const PLUGIN_1_PATH: &str = "../../target/debug/deps/libplugin_example.so";
    const PLUGIN_2_PATH: &str = "../../target/debug/deps/libplugin_example2.so";

    #[test]
    fn plugin_loading() {
        let plugin = Plugin::new().load(PLUGIN_1_PATH);
        assert!(plugin.is_some());

        let plugin = plugin.unwrap();

        assert!(std::matches!(plugin.name(), "plugin 1"));
    }

    #[test]
    fn no_conflicts_between_two_plugins() {
        let plugin_1 = Plugin::new().load(PLUGIN_1_PATH);
        assert!(plugin_1.is_some());
        let plugin_1 = plugin_1.unwrap();

        let plugin_2 = Plugin::new().load(PLUGIN_2_PATH);
        assert!(plugin_2.is_some());
        let plugin_2 = plugin_2.unwrap();

        assert!(std::matches!(plugin_1.name(), "plugin 1"));

        assert!(std::matches!(plugin_2.name(), "plugin 2"));
    }
}
