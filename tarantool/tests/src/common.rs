use serde::{Deserialize, Serialize};

use tarantool::{
    tlua::{self, AsLua, LuaState},
    tuple::Encode,
};

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct S1Record {
    pub id: u32,
    pub text: String,
}

impl Encode for S1Record {}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct S2Record {
    pub id: u32,
    pub key: String,
    pub value: String,
    pub a: i32,
    pub b: i32,
}

impl Encode for S2Record {}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct S2Key {
    pub id: u32,
    pub a: i32,
    pub b: i32,
}

impl Encode for S2Key {}

#[derive(Serialize)]
pub struct QueryOperation {
    pub op: String,
    pub field_id: u32,
    pub value: serde_json::Value,
}

impl Encode for QueryOperation {}

#[derive(Clone, Debug)]
pub(crate) struct DropCounter(pub(crate) std::rc::Rc<std::cell::Cell<usize>>);

impl Drop for DropCounter {
    fn drop(&mut self) {
        let old_count = self.0.get();
        self.0.set(old_count + 1);
    }
}

pub(crate) fn capture_value<T>(_: &T) {}

pub(crate) struct LuaStackIntegrityGuard {
    name: &'static str,
    lua: LuaState,
}

impl LuaStackIntegrityGuard {
    pub fn global(name: &'static str) -> Self {
        Self::new(name, global_lua())
    }

    pub fn new(name: &'static str, lua: impl AsLua) -> Self {
        let lua = lua.as_lua();
        unsafe { lua.push_one(name).forget() };
        Self { name, lua }
    }
}

impl Drop for LuaStackIntegrityGuard {
    fn drop(&mut self) {
        let single_value = unsafe { tlua::PushGuard::new(self.lua, 1) };
        let msg: tlua::StringInLua<_> = single_value
            .read()
            .map_err(|l| {
                panic!(
                    "Lua stack integrity violation
    {:?}
    Expected string: \"{}\"",
                    l, self.name
                )
            })
            .unwrap();
        assert_eq!(msg, self.name);
    }
}

pub(crate) struct LuaContextSpoiler {
    fix: Option<Box<dyn FnOnce()>>,
}

impl LuaContextSpoiler {
    pub fn new(spoil: &str, fix: &'static str) -> Self {
        tarantool::lua_state().exec(spoil).unwrap();
        Self {
            fix: Some(Box::new(move || global_lua().exec(fix).unwrap())),
        }
    }
}

impl Drop for LuaContextSpoiler {
    fn drop(&mut self) {
        (self.fix.take().unwrap())()
    }
}

fn global_lua() -> tlua::StaticLua {
    unsafe { tlua::Lua::from_static(tarantool::ffi::tarantool::luaT_state()) }
}

pub trait BoolExt {
    fn so(&self) -> bool;

    #[inline(always)]
    fn as_some<T>(&self, v: T) -> Option<T> {
        if self.so() {
            Some(v)
        } else {
            None
        }
    }

    #[inline(always)]
    fn as_some_from<T>(&self, f: impl FnOnce() -> T) -> Option<T> {
        if self.so() {
            Some(f())
        } else {
            None
        }
    }
}

impl BoolExt for bool {
    #[inline(always)]
    fn so(&self) -> bool {
        *self
    }
}

use once_cell::unsync::OnceCell;

pub fn lib_name() -> String {
    thread_local! {
        static LIB_NAME: OnceCell<String> = OnceCell::new();
    }
    LIB_NAME.with(|lib_name| {
        lib_name
            .get_or_init(|| format!("lib{}", env!("CARGO_PKG_NAME").replace('-', "_")))
            .clone()
    })
}
