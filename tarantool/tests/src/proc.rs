use crate::common::lib_name;
use rmpv::Value;
use std::ffi::OsStr;
use tarantool::{
    proc::ReturnMsgpack,
    tlua::{
        self, AsTable, Call, CallError, LuaFunction, LuaRead, LuaState, LuaThread, PushGuard,
        PushInto,
    },
    tuple::{RawByteBuf, RawBytes, Tuple, TupleBuffer},
};

fn call_proc<A, R>(name: &str, args: A) -> Result<R, CallError<A::Err>>
where
    A: PushInto<LuaState>,
    R: for<'a> LuaRead<PushGuard<LuaFunction<PushGuard<LuaThread>>>>,
{
    let lua = tarantool::lua_state();
    let create = LuaFunction::load(
        lua,
        "
        return (
            function(f, ...)
                if box.func[f] == nil then
                    box.schema.func.create(f, { language = 'C' })
                end
                return box.func[f]:call{...}
            end
        )(...)
    ",
    )
    .unwrap();
    create
        .into_call_with((format!("{}.{}", lib_name(), name), args))
        .map_err(|e| e.map(|e| e.other().first()))
}

pub fn simple() {
    #[tarantool::proc]
    fn proc_simple(x: i32) -> i32 {
        x + 1
    }

    assert_eq!(call_proc("proc_simple", 1).ok(), Some(2));
    assert_eq!(call_proc("proc_simple", 2).ok(), Some(3));

    #[tarantool::proc]
    fn proc_simple_str(s: &str) -> String {
        format!("{s} pong")
    }

    assert_eq!(
        call_proc("proc_simple_str", "ping").ok(),
        Some("ping pong".to_string())
    );
}

pub fn return_tuple() {
    #[tarantool::proc]
    fn proc_return_tuple(x: i32, y: String) -> tarantool::Result<Tuple> {
        Tuple::new(&(x, y))
    }

    #[tarantool::proc]
    fn proc_return_tuple_buf() -> tarantool::Result<TupleBuffer> {
        TupleBuffer::try_from_vec((&b"\x92\xa5hello\xa6sailor"[..]).into())
    }

    let tuple: Tuple = call_proc("proc_return_tuple", (1998, "March")).unwrap();
    let data: (u32, String) = tuple.decode().unwrap();
    assert_eq!(data, (1998, "March".to_string()));

    let data: [String; 2] = call_proc("proc_return_tuple_buf", ()).unwrap();
    assert_eq!(data, ["hello", "sailor"]);
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
        Err::<(), _>("Lua error: FAIL".into()),
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

    assert_eq!(
        call_proc("proc_packed", (3, "X")).ok(),
        Some("XXX".to_string())
    );
}

pub fn return_raw_bytes() {
    #[tarantool::proc(packed_args)]
    fn proc_returns_raw_bytes(x: &RawBytes) -> &RawBytes {
        x
    }

    assert_eq!(call_proc("proc_returns_raw_bytes", 1).ok(), Some([1]));
    assert_eq!(
        call_proc("proc_returns_raw_bytes", "hi").ok(),
        Some(["hi".to_string()])
    );
    assert_eq!(
        call_proc("proc_returns_raw_bytes", ("hello!", [1, 2, 3])).ok(),
        Some(AsTable(("hello!".to_string(), [1, 2, 3])))
    );

    #[tarantool::proc(packed_args)]
    fn proc_returns_raw_byte_buf(x: &RawBytes) -> RawByteBuf {
        let mut res = vec![0x92];
        res.extend(&x.0);
        res.extend([0xa4, 0x70, 0x6f, 0x6e, 0x67]);
        RawByteBuf(res)
    }

    assert_eq!(
        call_proc("proc_returns_raw_byte_buf", [1, 2, 3]).ok(),
        Some(AsTable(([[1, 2, 3]], "pong".to_string())))
    );
    assert_eq!(
        call_proc("proc_returns_raw_byte_buf", "ping").ok(),
        Some(AsTable((["ping".to_string()], "pong".to_string())))
    );
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

    assert_eq!(
        call_proc::<(), i32>("proc_tarantool_reimport", ()).unwrap(),
        42
    );
}

pub fn custom_ret() {
    #[derive(serde::Serialize, tlua::LuaRead, PartialEq, Eq, Debug)]
    struct MyStruct {
        x: i32,
        y: String,
    }

    #[tarantool::proc]
    fn proc_custom_ret(x: i32) -> ReturnMsgpack<MyStruct> {
        ReturnMsgpack(MyStruct {
            x,
            y: format!("{:x}", x),
        })
    }

    #[tarantool::proc(custom_ret)]
    fn proc_custom_ret_attr(x: i32) -> MyStruct {
        MyStruct {
            x,
            y: format!("{:x}", x),
        }
    }

    assert_eq!(
        call_proc::<_, MyStruct>("proc_custom_ret", 69).unwrap(),
        MyStruct {
            x: 69,
            y: "45".into()
        }
    );

    assert_eq!(
        call_proc::<_, MyStruct>("proc_custom_ret_attr", 69).unwrap(),
        MyStruct {
            x: 69,
            y: "45".into()
        }
    );
}

pub fn inject() {
    #[tarantool::proc]
    fn proc_inject<'a>(
        #[inject(&vec!["hello", "how", "are", "you"])] injected: &'a [&'static str],
        start: usize,
        end: usize,
    ) -> &'a [&'static str] {
        &injected[start..end]
    }

    assert_eq!(
        call_proc::<_, Vec<String>>("proc_inject", (1, 3)).unwrap(),
        vec!["how".to_string(), "are".to_string()]
    );

    #[tarantool::proc]
    fn proc_inject_2<'a>(
        #[inject("left")] injected_1: &'a str,
        #[inject("right")] injected_2: &'a str,
        second: bool,
    ) -> &'a str {
        if second {
            injected_2
        } else {
            injected_1
        }
    }

    assert_eq!(
        call_proc::<_, String>("proc_inject_2", false).unwrap(),
        "left".to_string(),
    );

    assert_eq!(
        call_proc::<_, String>("proc_inject_2", true).unwrap(),
        "right".to_string(),
    );

    struct GlobalData {
        data: Vec<String>,
    }

    fn global() -> &'static GlobalData {
        static mut GLOBAL: Option<GlobalData> = None;
        unsafe {
            GLOBAL.get_or_insert_with(|| GlobalData {
                data: vec!["some".into(), "global".into(), "data".into()],
            })
        }
    }

    #[tarantool::proc]
    fn proc_inject_global<'a>(#[inject(&global())] data: &'a GlobalData, i: usize) -> &'a str {
        &data.data[i]
    }

    assert_eq!(
        call_proc::<_, String>("proc_inject_global", 0).unwrap(),
        "some".to_string(),
    );

    assert_eq!(
        call_proc::<_, String>("proc_inject_global", 1).unwrap(),
        "global".to_string(),
    );

    assert_eq!(
        call_proc::<_, String>("proc_inject_global", 2).unwrap(),
        "data".to_string(),
    );
}

pub fn inject_with_packed() {
    #[tarantool::proc(packed_args)]
    fn proc_inject_with_packed<'a>(
        #[inject(&[0, 1, 2, 3, 4, 5])] data: &'a [i32],
        args: Vec<usize>,
    ) -> &'a [i32] {
        match *args.as_slice() {
            [start, end, ..] => &data[start..end],
            [start] => &data[start..],
            [] => data,
        }
    }

    assert_eq!(
        call_proc("proc_inject_with_packed", ()).ok(),
        Some(vec![0, 1, 2, 3, 4, 5]),
    );

    assert_eq!(
        call_proc("proc_inject_with_packed", 3).ok(),
        Some(vec![3, 4, 5]),
    );

    assert_eq!(
        call_proc("proc_inject_with_packed", (2, 5)).ok(),
        Some(vec![2, 3, 4]),
    );
}

#[::tarantool::test]
#[cfg(target_os = "linux")]
fn module_path() {
    let path = ::tarantool::proc::module_path(module_path as _).unwrap();
    assert_eq!(
        path.file_stem(),
        Some(OsStr::new("libtarantool_module_test_runner"))
    );
    assert_eq!(
        ::tarantool::proc::module_path(::tarantool::ffi::tarantool::box_txn as _),
        Some(std::path::Path::new(&std::env::args().next().unwrap())),
    );
}

#[::tarantool::test]
#[cfg(target_os = "macos")]
fn module_path() {
    let path = ::tarantool::proc::module_path(module_path as _).unwrap();
    assert_eq!(
        path.file_stem(),
        Some(OsStr::new("libtarantool_module_test_runner"))
    );
    assert_eq!(
        ::tarantool::proc::module_path(::tarantool::ffi::tarantool::box_txn as _)
            .unwrap()
            .file_stem(),
        Some(OsStr::new("tarantool"))
    );
}
