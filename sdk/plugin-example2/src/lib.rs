use picodata_sdk::PluginVtable;

struct Plugin2 {}

impl PluginVtable for Plugin2 {
    #[no_mangle]
    fn name() -> &'static str {
        "plugin 2"
    }

    #[no_mangle]
    fn start() {
        println!("PLUGIN 2 START CALLBACK");
    }

    #[no_mangle]
    fn stop() {
        println!("PLUGIN 2 STOP CALLBACK");
    }
}
