use tarantool::tlua::{
    self,
    AsLua,
    Lua,
    LuaFunction,
    LuaTable,
    PushGuard,
    TuplePushError::{First, Other},
};
use crate::common::LuaStackIntegrityGuard;

pub fn print() {
    let lua = tarantool::lua_state();
    let print: LuaFunction<_> = lua.get("print").unwrap();
    let () = print.call_with_args("hello").unwrap();
}

pub fn json() {
    let lua = tarantool::lua_state();
    let require: LuaFunction<_> = lua.get("require").unwrap();
    let json: LuaTable<_> = require.call_with_args("json").unwrap();
    let encode: LuaFunction<_> = json.get("encode").unwrap();
    let mut table = std::collections::HashMap::new();
    let res: String = encode.call_with_args(vec![1, 2, 3]).unwrap();
    assert_eq!(res, "[1,2,3]");
    table.insert("a", "b");
    let res: String = encode.call_with_args(table).unwrap();
    assert_eq!(res, r#"{"a":"b"}"#);
}

#[rustfmt::skip]
pub fn dump_stack() {
    eprintln!();
    let lua = Lua::new();
    lua.openlibs();
    let mut buf = std::io::Cursor::new(Vec::with_capacity(0x1000));
    let lua = lua
        .push("hello")
        .push(3.14)
        .push(false)
        .push(420);
    unsafe {
        tarantool::tlua::debug::dump_stack_raw_to(lua.as_lua(), &mut buf).unwrap()
    }
    assert_eq!(
        String::from_utf8_lossy(buf.into_inner().as_slice()),
        r#"1: string(hello)
2: number(3.14)
3: boolean(false)
4: number(420)
"#
    )
}

#[rustfmt::skip]
pub fn dump_stack_raw() {
    eprintln!();
    let lua = Lua::new();
    lua.openlibs();
    let mut buf = std::io::Cursor::new(Vec::with_capacity(0x1000));
    let lua = lua
        .push("hello")
        .push(3.14)
        .push(false)
        .push(420);
    unsafe {
        tarantool::tlua::debug::dump_stack_raw_to(lua.as_lua(), &mut buf).unwrap();
    }
    assert_eq!(
        String::from_utf8_lossy(buf.into_inner().as_slice()),
        r#"1: string(hello)
2: number(3.14)
3: boolean(false)
4: number(420)
"#
    )
}

pub fn error_during_push_tuple() {
    #[derive(Debug, PartialEq, Eq)]
    struct CustomError;
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct S;
    impl<L: AsLua> tlua::Push<L> for S {
        type Err = CustomError;
        fn push_to_lua(&self, lua: L) -> Result<PushGuard<L>, (CustomError, L)> {
            Err((CustomError, lua))
        }
    }
    impl<L: AsLua> tlua::PushOne<L> for S {}
    impl<L: AsLua> tlua::PushInto<L> for S {
        type Err = CustomError;
        fn push_into_lua(self, lua: L) -> Result<PushGuard<L>, (CustomError, L)> {
            Err((CustomError, lua))
        }
    }
    impl<L: AsLua> tlua::PushOneInto<L> for S {}

    let lua = Lua::new();

    let lua = {
        let _guard = LuaStackIntegrityGuard::new("push_tuple_by_val_error", &lua);
        let (e, lua) = lua.try_push((1, 2, 3, S)).unwrap_err();
        assert_eq!(e, Other(Other(Other(First(CustomError)))));
        lua
    };

    let lua = {
        let _guard = LuaStackIntegrityGuard::new("push_tuple_by_ref_error", &lua);
        let (e, lua) = lua.try_push(&(1, 2, 3, S)).unwrap_err();
        assert_eq!(e, Other(Other(Other(First(CustomError)))));
        lua
    };

    drop(lua);
}

pub fn hash() {
    assert_eq!(tlua::util::hash(""), 0);
    assert_eq!(tlua::util::hash("a"), 0x20e3223e);
    assert_eq!(tlua::util::hash("ab"), 0x6c811ed5);
    assert_eq!(tlua::util::hash("abc"), 0x6c811ed5);
    assert_eq!(tlua::util::hash("abd"), 0x86500903);
    assert_eq!(tlua::util::hash("foobar"), 0x9e91cce9);
    let s = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Laoreet suspendisse interdum consectetur libero id faucibus nisl tincidunt. Mattis ullamcorper velit sed ullamcorper morbi tincidunt ornare massa eget. Suspendisse ultrices gravida dictum fusce ut placerat orci nulla pellentesque. Iaculis at erat pellentesque adipiscing commodo elit. Pellentesque id nibh tortor id aliquet lectus proin. Velit laoreet id donec ultrices tincidunt arcu non sodales. Sollicitudin nibh sit amet commodo nulla facilisi nullam. Donec ac odio tempor orci dapibus ultrices in iaculis nunc. Lectus nulla at volutpat diam ut venenatis tellus. Nascetur ridiculus mus mauris vitae ultricies. Elit scelerisque mauris pellentesque pulvinar pellentesque. Mauris cursus mattis molestie a iaculis at erat. Vitae turpis massa sed elementum tempus egestas sed sed risus. Arcu cursus vitae congue mauris rhoncus aenean.

Facilisi cras fermentum odio eu feugiat. Id cursus metus aliquam eleifend mi in. Mauris sit amet massa vitae tortor condimentum lacinia quis vel. Gravida in fermentum et sollicitudin ac orci phasellus. Mattis pellentesque id nibh tortor id aliquet lectus. Integer malesuada nunc vel risus. Semper risus in hendrerit gravida rutrum quisque. Et netus et malesuada fames ac. Ultrices eros in cursus turpis. Feugiat nisl pretium fusce id velit ut tortor. Dictum at tempor commodo ullamcorper. Accumsan lacus vel facilisis volutpat est velit egestas dui. Eget nunc scelerisque viverra mauris in aliquam sem. Massa placerat duis ultricies lacus sed turpis tincidunt.

";
    assert_eq!(tlua::util::hash(s), 0x9949b070);

    // fn lj_hash(s: &str) -> u32 {
    //     extern "C" {
    //         fn lua_hash(s: *const std::os::raw::c_char, len: u32) -> u32;
    //     }
    //     unsafe {
    //         lua_hash(s.as_ptr() as _, s.len() as _)
    //     }
    // }

    // let s = "";
    // assert_eq!(tlua::util::hash(s), lj_hash(s));
    // let s = "a";
    // assert_eq!(tlua::util::hash(s), lj_hash(s));
    // let s = "ab";
    // assert_eq!(tlua::util::hash(s), lj_hash(s));
    // let s = "abc";
    // assert_eq!(tlua::util::hash(s), lj_hash(s));
    // let s = "abd";
    // assert_eq!(tlua::util::hash(s), lj_hash(s));
    // let s = "foobar";
    // assert_eq!(tlua::util::hash(s), lj_hash(s));
    // let s = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Laoreet suspendisse interdum consectetur libero id faucibus nisl tincidunt. Mattis ullamcorper velit sed ullamcorper morbi tincidunt ornare massa eget. Suspendisse ultrices gravida dictum fusce ut placerat orci nulla pellentesque. Iaculis at erat pellentesque adipiscing commodo elit. Pellentesque id nibh tortor id aliquet lectus proin. Velit laoreet id donec ultrices tincidunt arcu non sodales. Sollicitudin nibh sit amet commodo nulla facilisi nullam. Donec ac odio tempor orci dapibus ultrices in iaculis nunc. Lectus nulla at volutpat diam ut venenatis tellus. Nascetur ridiculus mus mauris vitae ultricies. Elit scelerisque mauris pellentesque pulvinar pellentesque. Mauris cursus mattis molestie a iaculis at erat. Vitae turpis massa sed elementum tempus egestas sed sed risus. Arcu cursus vitae congue mauris rhoncus aenean.

// Facilisi cras fermentum odio eu feugiat. Id cursus metus aliquam eleifend mi in. Mauris sit amet massa vitae tortor condimentum lacinia quis vel. Gravida in fermentum et sollicitudin ac orci phasellus. Mattis pellentesque id nibh tortor id aliquet lectus. Integer malesuada nunc vel risus. Semper risus in hendrerit gravida rutrum quisque. Et netus et malesuada fames ac. Ultrices eros in cursus turpis. Feugiat nisl pretium fusce id velit ut tortor. Dictum at tempor commodo ullamcorper. Accumsan lacus vel facilisis volutpat est velit egestas dui. Eget nunc scelerisque viverra mauris in aliquam sem. Massa placerat duis ultricies lacus sed turpis tincidunt.

// ";
    // assert_eq!(tlua::util::hash(s), lj_hash(s));
}

