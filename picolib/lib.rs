use std::os::raw::c_int;
mod tarantool;

#[no_mangle]
pub extern "C" fn luaopen_picolib(_l: std::ffi::c_void) -> c_int {
    for (key, value) in std::env::vars() {
        if key.starts_with("PICODATA_") {
            println!("{}: {}", key, value);
        }
    }

    println!();
    println!("Hello from Rust {}", std::module_path!());
    println!(
        "Running on {} {}",
        tarantool::package(),
        tarantool::version()
    );

    0
}
