use picodata_sdk::*;

use crate::tlog;

static mut PLUGIN_LIST: Vec<Plugin> = Vec::new();

pub struct PluginList {}

impl PluginList {
    pub fn global_init(plugins: &[String]) {
        let mut plugin_list: Vec<Plugin> = Vec::new();
        plugins.iter().for_each(|path| {
            if let Some(plugin) = Plugin::new().load(path) {
                plugin_list.push(plugin);
            }
        });

        unsafe { PLUGIN_LIST = plugin_list };

        unsafe { &PLUGIN_LIST }.iter().for_each(|plugin| {
            tlog!(Info, "Plugin: '{}' loaded", plugin.name());
        });
    }

    pub fn get() -> &'static Vec<Plugin> {
        unsafe { &PLUGIN_LIST }
    }
}
