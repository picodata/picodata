use picodata_sdk::PluginVtable;

struct Plugin1 {}

impl PluginVtable for Plugin1 {
    #[no_mangle]
    fn name() -> &'static str {
        "plugin 1"
    }

    #[no_mangle]
    fn start() {
        println!("PLUGIN 1 START CALLBACK");
    }

    #[no_mangle]
    fn stop() {
        println!("PLUGIN 1 STOP CALLBACK");
    }
}
