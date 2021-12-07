use std::os::raw::c_int;
mod tarantool;

#[no_mangle]
pub extern "C" fn luaopen_picolib(_l: std::ffi::c_void) -> c_int {
    for (key, value) in std::env::vars() {
        if key.starts_with("PICODATA_") {
            println!("{}: {:?}", key, value);
        }
    }

    let command = std::env::var("PICODATA_COMMAND");
    match command.as_deref() {
        Ok("run") => {
            main_run();
        }
        Ok(_) => {}
        Err(_) => {}
    };

    0
}

fn main_run() {
    println!(
        "Picodata running on {} {}",
        tarantool::package(),
        tarantool::version()
    );

    println!();
    println!("Hello from Rust {}", std::module_path!());
    println!(
        "Running on {} {}",
        tarantool::package(),
        tarantool::version()
    );

    let mut cfg = tarantool::Cfg::default();

    std::env::var("PICODATA_LISTEN").ok().and_then(|v| {
        cfg.listen = v.clone();
        Some(v)
    });

    std::env::var("PICODATA_DATA_DIR").ok().and_then(|v| {
        std::fs::create_dir_all(&v).unwrap();
        cfg.wal_dir = v.clone();
        cfg.memtx_dir = v.clone();
        Some(v)
    });

    println!("{:?}", cfg);
    tarantool::set_cfg(cfg);
}
