use slog::{debug, info, o};
use std::os::raw::c_int;
use ::tarantool::hlua;
mod tarantool;

pub struct InnerTest {
    pub name: &'static str,
    pub body: fn(),
}
inventory::collect!(InnerTest);

#[no_mangle]
pub extern "C" fn luaopen_picolib(l: *mut std::ffi::c_void) -> c_int {
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

    unsafe {
        let l = hlua::Lua::from_existing_state(l, false);

        let mut test = Vec::new();
        for t in inventory::iter::<InnerTest> {
            test.push((t.name, hlua::function0(t.body)));
        }

        let luamod: hlua::LuaTable<_> = (&l).push(vec![()]).read().unwrap();
        luamod.set("VERSION", env!("CARGO_PKG_VERSION"));
        luamod.set("test", test);

        use hlua::AsLua;
        (&l).push(&luamod).forget();
        1
    }
}

fn main_run() {
    let mut cfg = tarantool::Cfg {
        listen: None,
        ..Default::default()
    };

    std::env::var("PICODATA_DATA_DIR").ok().and_then(|v| {
        std::fs::create_dir_all(&v).unwrap();
        cfg.wal_dir = v.clone();
        cfg.memtx_dir = v.clone();
        Some(v)
    });

    tarantool::set_cfg(&cfg);
    tarantool::eval(
        r#"
        box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})
        box.cfg({log_level = 6})
    "#,
    );

    std::env::var("PICODATA_LISTEN").ok().and_then(|v| {
        cfg.listen = Some(v.clone());
        Some(v)
    });

    tarantool::set_cfg(&cfg);

    let logger = slog::Logger::root(tarantool::SlogDrain, o!());

    info!(logger, "Hello, Rust!"; "module" => std::module_path!());
    debug!(
        logger,
        "Picodata running on {} {}",
        tarantool::package(),
        tarantool::version()
    );
}
