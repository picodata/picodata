use tarantool::{
    proc::ReturnMsgpack,
    tlua::{
        self,
        Call, PushGuard, LuaState, PushInto, LuaFunction, LuaThread,
        CallError, LuaRead, AsTable,
    },
};
use crate::common::lib_name;
use rmpv::Value;

fn call_proc<A, R>(name: &str, args: A) -> Result<R, CallError<A::Err>>
where
    A: PushInto<LuaState>,
    R: for<'a> LuaRead<PushGuard<LuaFunction<PushGuard<LuaThread>>>>,
{
    let lua = tarantool::lua_state();
    let create = LuaFunction::load(lua, "
        return (
            function(f, ...)
                if box.func[f] == nil then
                    box.schema.func.create(f, { language = 'C' })
                end
                return box.func[f]:call{...}
            end
        )(...)
    ").unwrap();
    create.into_call_with((format!("{}.{}", lib_name(), name), args))
        .map_err(|e| e.map(|e| e.other().first()))
}

pub fn simple() {
    #[tarantool::proc]
    fn proc_simple(x: i32) -> i32 {
        x + 1
    }

    assert_eq!(call_proc("proc_simple", 1).ok(), Some(2));
    assert_eq!(call_proc("proc_simple", 2).ok(), Some(3));
}

pub fn with_error() {
    #[tarantool::proc]
    fn proc_with_error(x: i32, y: String) -> Result<(i32, i32), String> {
        if x == 3 {
            Ok((1, 2))
        } else {
            Err(y)
        }
    }

    assert_eq!(call_proc("proc_with_error", (3, "good")).ok(), Some([1, 2]));
    assert_eq!(
        call_proc("proc_with_error", (0, "FAIL")).map_err(|e| e.to_string()),
        Err::<(), _>("Lua error: Execution error: FAIL".into()),
    );
}

pub fn packed() {
    #[derive(serde::Deserialize)]
    struct MyStruct {
        x: usize,
        y: String,
    }

    #[tarantool::proc(packed_args)]
    fn proc_packed(MyStruct { x, y }: MyStruct) -> String {
        y.repeat(x)
    }

    assert_eq!(call_proc("proc_packed", (3, "X")).ok(), Some("XXX".to_string()));
}

pub fn debug() {
    #[tarantool::proc(debug, packed_args)]
    fn proc_debug(v: Value) -> String {
        format!("{:?}", v)
    }

    assert_eq!(
        call_proc("proc_debug",
            (3.14, [1, 2, 3], AsTable((("nice", 69), ("foo", "bar"))))
        ).ok(),
        Some(r#"Array([F64(3.14), Array([Integer(PosInt(1)), Integer(PosInt(2)), Integer(PosInt(3))]), Map([(String(Utf8String { s: Ok("nice") }), Integer(PosInt(69))), (String(Utf8String { s: Ok("foo") }), String(Utf8String { s: Ok("bar") }))])])"#.to_string())
    );
}

pub fn tarantool_reimport() {
    use ::tarantool as blabla; // comment out to see the difference

    #[tarantool::proc(tarantool = "blabla")]
    unsafe fn proc_tarantool_reimport() -> usize {
        42
    }

    assert_eq!(call_proc::<(), i32>("proc_tarantool_reimport", ()).unwrap(), 42);
}

pub fn custom_ret() {
    #[derive(serde::Serialize, tlua::LuaRead, PartialEq, Eq, Debug)]
    struct MyStruct {
        x: i32,
        y: String,
    }

    #[tarantool::proc]
    fn proc_custom_ret(x: i32) -> ReturnMsgpack<MyStruct> {
        ReturnMsgpack(MyStruct { x, y: format!("{:x}", x) })
    }

    #[tarantool::proc(custom_ret)]
    fn proc_custom_ret_attr(x: i32) -> MyStruct {
        MyStruct { x, y: format!("{:x}", x) }
    }

    assert_eq!(
        call_proc::<_, MyStruct>("proc_custom_ret", 69).unwrap(),
        MyStruct { x: 69, y: "45".into() }
    );

    assert_eq!(
        call_proc::<_, MyStruct>("proc_custom_ret_attr", 69).unwrap(),
        MyStruct { x: 69, y: "45".into() }
    );
}
