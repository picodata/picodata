use std::io;
use std::os::raw::{c_int, c_schar};

use luajit::ffi::{lua_pushinteger, lua_State, luaL_error};
use tester::{
    ColorConfig, Options, OutputFormat, run_tests_console, RunIgnored, ShouldPanic, TestDesc, TestDescAndFn,
    TestFn, TestName, TestOpts, TestType
};

mod common;
mod test_box;
mod test_coio;
mod test_error;
mod test_fiber;
mod test_transaction;
mod test_tuple;

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
        add_test_default("fiber_cond_signal", test_fiber::test_fiber_cond_signal),
        add_test_default("fiber_cond_broadcast", test_fiber::test_fiber_cond_broadcast),
        add_test_default("fiber_cond_timeout", test_fiber::test_fiber_cond_timeout),

        add_test_default("box_space_get_by_name", test_box::test_space_get_by_name),
        add_test_default("box_index_get_by_name", test_box::test_index_get_by_name),
        add_test_default("box_insert", test_box::test_box_insert),
        add_test_default("box_replace", test_box::test_box_replace),
        add_test_default("box_delete", test_box::test_box_delete),
        add_test_default("box_update", test_box::test_box_update),
        add_test_default("box_upsert", test_box::test_box_upsert),
        add_test_default("box_truncate", test_box::test_box_truncate),
        add_test_default("box_get", test_box::test_box_get),
        add_test_default("box_select", test_box::test_box_select),
        add_test_default("box_select_composite_key", test_box::test_box_select_composite_key),
        add_test_default("box_len", test_box::test_box_len),
        add_test_default("box_random", test_box::test_box_random),
        add_test_default("box_min_max", test_box::test_box_min_max),
        add_test_default("box_count", test_box::test_box_count),
        add_test_default("box_extract_key", test_box::test_box_extract_key),

        add_test_default("tuple_new_from_struct", test_tuple::test_tuple_new_from_struct),
        add_test_default("tuple_field_count", test_tuple::test_tuple_field_count),
        add_test_default("tuple_size", test_tuple::test_tuple_size),
        add_test_default("tuple_into_struct", test_tuple::test_tuple_into_struct),
        add_test_default("tuple_clone", test_tuple::test_tuple_clone),

        add_test_default("transaction_commit", test_transaction::test_transaction_commit),
        add_test_default("transaction_rollback", test_transaction::test_transaction_rollback),

        add_test_default("error_last", test_error::test_error_last),

        add_test_default("coio_accept", test_coio::test_coio_accept),
        add_test_default("coio_read_write", test_coio::test_coio_read_write),
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
