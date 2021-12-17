use std::ffi::CStr;
use tarantool::hlua::{self, Lua, LuaFunction};

mod ffi {
    use libc::c_char;

    extern "C" {
        pub fn tarantool_version() -> *const c_char;
        pub fn tarantool_package() -> *const c_char;
    }
}

pub fn version() -> &'static str {
    let c_ptr = unsafe { ffi::tarantool_version() };
    let c_str = unsafe { CStr::from_ptr(c_ptr) };
    return c_str.to_str().unwrap();
}

pub fn package() -> &'static str {
    let c_ptr = unsafe { ffi::tarantool_package() };
    let c_str = unsafe { CStr::from_ptr(c_ptr) };
    return c_str.to_str().unwrap();
}

inventory::submit!(crate::InnerTest {
    name: "test_version",
    body: || {
        let l = tarantool_L();
        let t: hlua::LuaTable<_> = l.eval("return require('tarantool')").unwrap();
        assert_eq!(version(), t.get::<String, _>("version").unwrap());
        assert_eq!(package(), t.get::<String, _>("package").unwrap());
    }
});

#[allow(non_snake_case)]
fn tarantool_L() -> Lua {
    unsafe {
        use tarantool::ffi::tarantool::luaT_state;
        Lua::from_existing_state(luaT_state(), false)
    }
}

#[derive(Clone, Debug, hlua::Push, hlua::LuaRead, PartialEq)]
pub struct Cfg {
    pub listen: Option<String>,
    pub wal_dir: String,
    pub memtx_dir: String,
}

impl Default for Cfg {
    fn default() -> Self {
        Self {
            listen: Some("3301".to_owned()),
            wal_dir: ".".to_owned(),
            memtx_dir: ".".to_owned(),
        }
    }
}

#[allow(dead_code)]
pub fn cfg() -> Option<Cfg> {
    let l = tarantool_L();
    let cfg: Result<Cfg, _> = l.eval("return box.cfg");
    match cfg {
        Ok(v) => Some(v),
        Err(_) => None,
    }
}

pub fn set_cfg(cfg: &Cfg) {
    let l = tarantool_L();
    let box_cfg = LuaFunction::load(l, "return box.cfg(...)").unwrap();
    box_cfg.call_with_args(cfg).unwrap()
}

pub fn eval(code: &str) {
    let l = tarantool_L();
    let f = LuaFunction::load(l, code).unwrap();
    f.call().unwrap()
}

pub use self::slog::Drain as SlogDrain;

mod slog {
    pub struct Drain;

    impl slog::Drain for Drain {
        type Ok = ();
        type Err = slog::Never;
        fn log(
            &self,
            record: &slog::Record,
            values: &slog::OwnedKVList,
        ) -> Result<Self::Ok, Self::Err> {
            use ::tarantool::log::say;
            use ::tarantool::log::SayLevel;

            // Max level is constant = trace
            // It's hardcoded in Cargo.toml dependency features
            // In runtime it's managed by tarantool box.cfg.log_level
            let lvl = match record.level() {
                slog::Level::Critical => SayLevel::Crit,
                slog::Level::Error => SayLevel::Error,
                slog::Level::Warning => SayLevel::Warn,
                slog::Level::Info => SayLevel::Info,
                slog::Level::Debug => SayLevel::Verbose,
                slog::Level::Trace => SayLevel::Debug,
            };

            let mut s = StrSerializer {
                str: format!("{}", record.msg()),
            };

            use slog::KV;
            // It's safe to use .unwrap() here since
            // StrSerializer doesn't return anything but Ok()
            record.kv().serialize(record, &mut s).unwrap();
            values.serialize(record, &mut s).unwrap();

            say(lvl, record.file(), record.line() as i32, None, &s.str);
            Ok(())
        }
    }

    struct StrSerializer {
        pub str: String,
    }

    impl slog::Serializer for StrSerializer {
        fn emit_arguments(&mut self, key: slog::Key, val: &std::fmt::Arguments) -> slog::Result {
            self.str.push_str(&format!(", {}: {}", key, val));
            Ok(())
        }
    }
}

pub use self::raft::RaftNode;
pub use self::raft::NodeStorage;

mod raft {
    use slog::{debug, o};
    use raft::prelude::*;
    use raft::Error as RaftError;
    use raft::storage::MemStorage;
    use raft::eraftpb::ConfState;

    pub struct NodeStorage(pub MemStorage);
    pub type RaftNode = RawNode<NodeStorage>;

    impl NodeStorage {
        pub fn new() -> Self {
            let stor = MemStorage::new_with_conf_state(ConfState::from((vec![1], vec![])));
            Self(stor)
        }
    }

    impl Storage for NodeStorage {
        fn initial_state(&self) -> Result<RaftState, RaftError> {
            let ret = self.0.initial_state();
            let logger = slog::Logger::root(crate::tarantool::SlogDrain, o!());
            debug!(logger, "+++ initial_state() -> {:?}", ret);
            ret
        }

        fn entries(
            &self,
            low: u64,
            high: u64,
            max_size: impl Into<Option<u64>>,
        ) -> Result<Vec<Entry>, RaftError> {
            let max_size: Option<u64> = max_size.into();
            let ret = self.0.entries(low, high, max_size);
            let logger = slog::Logger::root(crate::tarantool::SlogDrain, o!());
            debug!(logger, "+++ entries(low={}, high={}, max_size={:?}) -> {:?}",
                low, high, max_size, ret
            );
            ret
        }

        fn term(&self, idx: u64) -> Result<u64, RaftError> {
            let ret = self.0.term(idx);
            let logger = slog::Logger::root(crate::tarantool::SlogDrain, o!());
            debug!(logger, "+++ term(idx={}) -> {:?}", idx, ret);
            ret
        }

        fn first_index(&self) -> Result<u64, RaftError> {
            let ret = self.0.first_index();
            let logger = slog::Logger::root(crate::tarantool::SlogDrain, o!());
            debug!(logger, "+++ first_index() -> {:?}", ret);
            ret
        }

        fn last_index(&self) -> Result<u64, RaftError> {
            let ret = self.0.last_index();
            let logger = slog::Logger::root(crate::tarantool::SlogDrain, o!());
            debug!(logger, "+++ last_index() -> {:?}", ret);
            ret
        }

        fn snapshot(&self, request_index: u64) -> Result<Snapshot, RaftError> {
            let ret = self.0.snapshot(request_index);
            let logger = slog::Logger::root(crate::tarantool::SlogDrain, o!());
            debug!(logger, "+++ snapshot(idx={}) -> {:?}", request_index, ret);
            ret
        }
    }
}
