use std::io;
use std::os::raw::{c_int, c_schar};

use luajit::ffi::{lua_pushinteger, lua_State, luaL_error};
use tester::{
    ColorConfig, Options, OutputFormat, run_tests_console, RunIgnored, ShouldPanic, TestDesc, TestDescAndFn,
    TestFn, TestName, TestOpts, TestType
};

mod test_fiber;
mod test_space;

fn add_test_default(name: &'static str, f: fn()) -> TestDescAndFn {
    TestDescAndFn{
        desc: TestDesc{
            name: TestName::StaticTestName(name),
            ignore: false,
            should_panic: ShouldPanic::No,
            allow_fail: false,
            test_type: TestType::UnitTest
        },
        testfn: TestFn::StaticTestFn(f)
    }
}

fn run() -> Result<bool, io::Error>{
    let opts = TestOpts{
        list: false,
        filter: None,
        filter_exact: false,
        force_run_in_process: false,
        exclude_should_panic: false,
        run_ignored: RunIgnored::No,
        run_tests: true,
        bench_benchmarks: false,
        logfile: None,
        nocapture: false,
        color: ColorConfig::AutoColor,
        format: OutputFormat::Pretty,
        test_threads: Some(1),
        skip: vec![],
        time_options: None,
        options: Options::new()
    };

    let tests = vec![
        add_test_default("fiber", test_fiber::test_fiber),
        add_test_default("fiber_arg", test_fiber::test_fiber_arg),
        add_test_default("fiber_cancel", test_fiber::test_fiber_cancel),
        add_test_default("fiber_wake", test_fiber::test_fiber_wake),
    ];

    run_tests_console(&opts, tests)
}

#[no_mangle]
pub extern "C" fn luaopen_libtarantool_module_test_runner(l: *mut lua_State) -> c_int {
    match run() {
        Ok(is_success) => {
            unsafe { lua_pushinteger(l, (!is_success) as isize) };
            1
        }
        Err(e) => {
            unsafe { luaL_error(l, e.to_string().as_ptr() as *const c_schar) };
            0
        }
    }
}
