#![allow(clippy::approx_constant)]
#![allow(clippy::blacklisted_name)]
#![allow(clippy::bool_assert_comparison)]
#![allow(clippy::upper_case_acronyms)]
use std::ffi::CStr;
use std::io;
use std::os::raw::{c_int, c_char};

use serde::Deserialize;
use tester::{
    run_tests_console, ColorConfig, Options, OutputFormat, RunIgnored, ShouldPanic, TestDesc,
    TestDescAndFn, TestFn, TestName, TestOpts, TestType,
};

use tarantool::error::Error;
use tarantool::ffi::lua as ffi_lua;
use tarantool::index::IndexType;
use tarantool::space::{Space, Field};

mod bench_bulk_insert;
mod common;
mod decimal;
mod proc;
mod r#box;
mod coio;
mod error;
mod fiber;
mod latch;
mod log;
mod net_box;
mod raft;
mod session;
mod transaction;
mod tuple;
mod tlua;
mod uuid;
mod enums;
mod sql;

macro_rules! tests {
    (@should_panic should_panic) => { Some(ShouldPanic::Yes) };
    (@should_panic $( $_:tt )?)  => { None };
    ($([should_panic_if: $should_panic_if:expr])? $( $( #[ $attr:tt ] )? $func_name:path,)*) => {{
        #[allow(unused_mut, unused_variables)]
        let mut should_panic = ShouldPanic::No;
        $(
            if $should_panic_if {
                should_panic = ShouldPanic::Yes
            }
        )?
        vec![
            $(TestDescAndFn{
                desc: TestDesc{
                    name: TestName::StaticTestName(stringify!($func_name)),
                    ignore: false,
                    should_panic: tests!(@should_panic $($attr)?).unwrap_or(should_panic),
                    allow_fail: false,
                    test_type: TestType::IntegrationTest
                },
                testfn: TestFn::StaticTestFn($func_name)
            },)*
        ]
    }}
}

#[derive(Clone, Deserialize)]
struct TestConfig {
    #[serde(default)]
    bench: bool,

    filter: Option<String>,

    #[serde(default = "default_listen")]
    listen: u16,
}

const fn default_listen() -> u16 {
    3301
}

fn create_test_spaces() -> Result<(), Error> {
    // space.test_s1
    let test_s1 = Space::builder("test_s1")
        .field(Field::unsigned("id"))
        .field(Field::string("text"))
        .create()?;

    // space.test_s1.index.primary
    test_s1.index_builder("primary")
        .index_type(IndexType::Tree)
        .part(1)
        .create()?;

    // space.test_s2
    let mut test_s2 = Space::builder("test_s2")
        .field(Field::unsigned("id"))
        .field(Field::string("key"))
        .field(Field::string("value"))
        .field(Field::integer("a"))
        .field(Field::integer("b"))
        .create()?;

    // space.test_s2.index.primary
    test_s2.index_builder("primary")
        .index_type(IndexType::Tree)
        .part(1)
        .create()?;

    // space.test_s2.index.idx_1
    test_s2.index_builder("idx_1")
        .index_type(IndexType::Hash)
        .part(2)
        .create()?;

    // space.test_s2.index.idx_2
    test_s2.index_builder("idx_2")
        .index_type(IndexType::Tree)
        .part("id")
        .part("a")
        .part("b")
        .create()?;

    // space.test_s2.index.idx_3
    test_s2.index_builder("idx_3")
        .index_type(IndexType::Tree)
        .unique(false)
        .part("a")
        .create()?;

    // Insert test data into space.test_s2
    for i in 1..21 {
        let rec = common::S2Record {
            id: i,
            key: format!("key_{}", i),
            value: format!("value_{}", i),
            a: (i as i32) % 5,
            b: (i as f32 / 5.0).floor() as i32,
        };
        test_s2.insert(&rec)?;
    }

    // space.with_array
    let mut with_array = Space::builder("with_array")
        .field(Field::unsigned("id"))
        .field(Field::array("array"))
        .create()?;

    // space.with_array.index.pk
    with_array.index_builder("pk")
        .part("id")
        .create()?;

    with_array.insert(&(1, vec![1, 2, 3]))?;
    with_array.insert(&(2, ("foo", ("bar", [69, 420]), 3.14)))?;

    Ok(())
}

fn drop_test_spaces() -> Result<(), Error> {
    let space_names = vec!["test_s1".to_string(), "test_s2".to_string()];

    for s in space_names.iter() {
        if let Some(space) = Space::find(s) {
            space.drop()?;
        }
    }

    Ok(())
}

static mut LISTEN: u16 = default_listen();

fn run_tests(cfg: TestConfig) -> Result<bool, io::Error> {
    unsafe { LISTEN = cfg.listen };
    run_tests_console(
        &TestOpts {
            list: false,
            filter: cfg.filter,
            filter_exact: false,
            force_run_in_process: false,
            exclude_should_panic: false,
            run_ignored: RunIgnored::No,
            run_tests: true,
            bench_benchmarks: cfg.bench,
            logfile: None,
            nocapture: false,
            color: ColorConfig::AutoColor,
            format: OutputFormat::Pretty,
            test_threads: Some(1),
            skip: vec![],
            time_options: None,
            options: Options::new(),
        },
        if cfg.bench {
            vec![TestDescAndFn {
                desc: TestDesc {
                    name: TestName::StaticTestName("bench_bulk_insert"),
                    ignore: false,
                    should_panic: ShouldPanic::No,
                    allow_fail: false,
                    test_type: TestType::Unknown,
                },
                testfn: TestFn::DynBenchFn(Box::new(bench_bulk_insert::BulkInsertBenchmark {
                    test_size: 64,
                    num_fibers: 256,
                    num_rows: 1000,
                })),
            }]
        } else {
            #[allow(unused_mut)]
            let mut tests = tests![];

            tests.append(&mut tests![
                [should_panic_if: !tarantool::ffi::has_decimal()]
                decimal::from_lua,
                decimal::from_string,
                decimal::from_tuple,
                decimal::to_tuple,
                decimal::from_num,
                decimal::to_num,
                decimal::cmp,
                decimal::hash,
                decimal::ops,
            ]);

            tests.append(&mut tests![
                decimal::to_lua,

                tlua::lua_functions::basic,
                tlua::lua_functions::two_functions_at_the_same_time,
                tlua::lua_functions::args,
                tlua::lua_functions::args_in_order,
                tlua::lua_functions::syntax_error,
                tlua::lua_functions::execution_error,
                tlua::lua_functions::check_types,
                tlua::lua_functions::call_and_read_table,
                tlua::lua_functions::table_as_args,
                tlua::lua_functions::table_method_call,
                tlua::lua_functions::lua_function_returns_function,
                tlua::lua_functions::error,
                tlua::lua_functions::either_or,
                tlua::lua_functions::multiple_return_values,
                tlua::lua_functions::multiple_return_values_fail,
                tlua::lua_functions::execute_from_reader_errors_if_cant_read,
                tlua::lua_functions::from_function_call_error,
                tlua::lua_functions::non_string_error,
                tlua::lua_functions::push_function,
                tlua::lua_functions::push_iter_no_err,
                tlua::lua_functions::eval_with,

                tlua::lua_tables::iterable,
                tlua::lua_tables::iterable_multipletimes,
                tlua::lua_tables::get_set,
                tlua::lua_tables::get_nil,
                tlua::lua_tables::table_over_table,
                tlua::lua_tables::metatable,
                tlua::lua_tables::empty_array,
                tlua::lua_tables::by_value,
                tlua::lua_tables::registry,
                tlua::lua_tables::registry_metatable,
                #[should_panic] tlua::lua_tables::table_iter_stack_invariance,
                tlua::lua_tables::iter_table_of_tables,

                tlua::functions_write::simple_function,
                tlua::functions_write::one_argument,
                tlua::functions_write::two_arguments,
                tlua::functions_write::wrong_arguments_types,
                tlua::functions_write::return_result,
                tlua::functions_write::closures,
                tlua::functions_write::closures_lifetime,
                tlua::functions_write::closures_extern_access,
                tlua::functions_write::closures_drop_env,
                tlua::functions_write::global_data,
                tlua::functions_write::push_callback_by_ref,
                tlua::functions_write::closures_must_be_static,
                tlua::functions_write::pcall,
                tlua::functions_write::optional_params,

                tlua::any::read_numbers,
                tlua::any::read_hashable_numbers,
                tlua::any::read_strings,
                tlua::any::read_hashable_strings,
                tlua::any::read_booleans,
                tlua::any::read_hashable_booleans,
                tlua::any::read_tables,
                tlua::any::read_hashable_tables,
                tlua::any::push_numbers,
                tlua::any::push_hashable_numbers,
                tlua::any::push_strings,
                tlua::any::push_hashable_strings,
                tlua::any::push_booleans,
                tlua::any::push_hashable_booleans,
                tlua::any::push_nil,
                tlua::any::push_hashable_nil,
                tlua::any::non_utf_8_string,

                tlua::misc::print,
                tlua::misc::json,
                tlua::misc::dump_stack,
                tlua::misc::dump_stack_raw,
                tlua::misc::error_during_push_tuple,
                tlua::misc::hash,

                tlua::object::callable_builtin,
                tlua::object::callable_ffi,
                tlua::object::callable_meta,
                tlua::object::indexable_builtin,
                tlua::object::indexable_ffi,
                tlua::object::indexable_meta,
                tlua::object::cannot_get_mutltiple_values,
                tlua::object::indexable_rw_builtin,
                tlua::object::indexable_rw_meta,

                tlua::userdata::readwrite,
                tlua::userdata::destructor_called,
                tlua::userdata::type_check,
                tlua::userdata::metatables,
                tlua::userdata::multiple_userdata,

                tlua::rust_tables::push_array,
                tlua::rust_tables::push_vec,
                tlua::rust_tables::push_hashmap,
                tlua::rust_tables::push_hashset,
                tlua::rust_tables::globals_table,
                tlua::rust_tables::read_array,
                tlua::rust_tables::read_array_partial,
                tlua::rust_tables::reading_vec_works,
                tlua::rust_tables::reading_vec_from_sparse_table_doesnt_work,
                tlua::rust_tables::reading_vec_with_empty_table_works,
                tlua::rust_tables::reading_vec_with_complex_indexes_doesnt_work,
                tlua::rust_tables::reading_heterogenous_vec_works,
                tlua::rust_tables::reading_vec_set_from_lua_works,
                tlua::rust_tables::reading_hashmap_works,
                tlua::rust_tables::reading_hashmap_from_sparse_table_works,
                tlua::rust_tables::reading_hashmap_with_empty_table_works,
                tlua::rust_tables::reading_hashmap_with_complex_indexes_works,
                tlua::rust_tables::reading_hashmap_with_floating_indexes_works,
                tlua::rust_tables::reading_heterogenous_hashmap_works,
                tlua::rust_tables::reading_hashmap_set_from_lua_works,
                tlua::rust_tables::read_wrong_type_fail,
                tlua::rust_tables::derive_struct_push,
                tlua::rust_tables::derive_struct_lua_read,
                tlua::rust_tables::derive_enum_push,
                tlua::rust_tables::derive_push_into,
                tlua::rust_tables::derive_enum_lua_read,
                tlua::rust_tables::derive_generic_struct_push,
                tlua::rust_tables::derive_generic_struct_lua_read,
                tlua::rust_tables::derive_generic_enum_push,
                tlua::rust_tables::derive_generic_enum_lua_read,
                tlua::rust_tables::derive_generic_push_into,
                tlua::rust_tables::enum_variants_order_matters,
                tlua::rust_tables::struct_of_enums_vs_enum_of_structs,
                tlua::rust_tables::derive_unit_structs_lua_read,
                tlua::rust_tables::derive_unit_structs_push,
                tlua::rust_tables::push_custom_iter,
                tlua::rust_tables::error_during_push_iter,
                tlua::rust_tables::push_custom_collection,
                tlua::rust_tables::table_from_iter,
                tlua::rust_tables::push_struct_of_nones,

                tlua::values::read_i32s,
                tlua::values::write_i32s,
                tlua::values::int64,
                tlua::values::cdata_numbers,
                tlua::values::readwrite_floats,
                tlua::values::readwrite_bools,
                tlua::values::readwrite_strings,
                tlua::values::i32_to_string,
                tlua::values::string_to_i32,
                tlua::values::string_on_lua,
                tlua::values::push_opt,
                tlua::values::read_nil,
                tlua::values::typename,
                tlua::values::tuple_as_table,

                fiber::old::fiber_new,
                fiber::old::fiber_new_with_attr,
                fiber::old::fiber_arg,
                fiber::old::fiber_cancel,
                fiber::old::fiber_wake,
                fiber::old::fiber_wake_multiple,
                fiber::old::fiber_cond_signal,
                fiber::old::fiber_cond_broadcast,
                fiber::old::fiber_cond_timeout,

                fiber::immediate,
                fiber::immediate_with_attrs,
                fiber::multiple_immediate,
                fiber::unit_immediate,
                fiber::unit_immediate_with_attrs,
                fiber::multiple_unit_immediate,
                fiber::deferred,
                fiber::deferred_with_attrs,
                fiber::multiple_deferred,
                fiber::unit_deferred,
                fiber::unit_deferred_with_attrs,
                fiber::multiple_unit_deferred,
                fiber::deferred_doesnt_yield,
                fiber::immediate_yields,
                fiber::start_error,
                fiber::require_error,
                #[should_panic] fiber::start_dont_join,
                #[should_panic] fiber::start_proc_dont_join,
                #[should_panic] fiber::defer_dont_join,
                #[should_panic] fiber::defer_proc_dont_join,
                fiber::immediate_with_cond,
                fiber::deferred_with_cond,
                fiber::lifetime,
            ]);

            tests.append(&mut tests![
                [should_panic_if: !tarantool::ffi::has_fiber_channel()]
                fiber::lua_thread,

                fiber::channel::send_self,
                fiber::channel::send_full,
                fiber::channel::recv_empty,
                fiber::channel::drop_sender,
                fiber::channel::dont_drop_msg,
                fiber::channel::unbuffered,
                fiber::channel::one_v_two,
                fiber::channel::two_v_one,
                fiber::channel::drop_msgs,
                fiber::channel::circle,
                fiber::channel::as_mutex,
                fiber::channel::iter,
                fiber::channel::into_iter,
                fiber::channel::try_iter,
                fiber::channel::demo,
                fiber::channel::drop_rx,
                fiber::channel::into_clones,
                fiber::channel::cannot_send_ref,

                fiber::mutex::advanced,
            ]);

            tests.append(&mut tests![
                fiber::mutex::simple,
                fiber::mutex::try_lock,
                fiber::mutex::debug,

                r#box::space_get_by_name,
                r#box::space_get_by_name_cached,
                r#box::space_get_system,
                r#box::index_get_by_name,
                r#box::index_get_by_name_cached,
                r#box::insert,
                r#box::replace,
                r#box::delete,
                r#box::update,
                r#box::update_macro,
                r#box::update_index_macro,
                r#box::upsert,
                r#box::upsert_macro,
                r#box::truncate,
                r#box::get,
                r#box::select,
                r#box::select_composite_key,
                r#box::len,
                r#box::random,
                r#box::min_max,
                r#box::count,
                r#box::extract_key,
                r#box::sequence_get_by_name,
                r#box::sequence_iterate,
                r#box::sequence_set,
                r#box::space_create_opt_default,
                r#box::space_create_opt_if_not_exists,
                r#box::space_create_id_increment,
                r#box::space_create_opt_user,
                r#box::space_create_opt_id,
                r#box::space_create_is_sync,
                r#box::space_meta,
                r#box::space_drop,
                r#box::index_create_drop,
                r#box::index_parts,

                tuple::tuple_new_from_struct,
                tuple::new_tuple_from_flutten_struct,
                tuple::tuple_field_count,
                tuple::tuple_size,
                tuple::tuple_decode,
                tuple::tuple_clone,
                tuple::tuple_iterator,
                tuple::tuple_iterator_seek_rewind,
                tuple::tuple_get_format,
                tuple::tuple_get_field,
                tuple::raw_bytes,
            ]);
            tests.append(&mut tests![
                [should_panic_if: !tarantool::ffi::has_tuple_field_by_path()]
                tuple::tuple_get_field_path,
            ]);
            tests.append(&mut tests![
                tuple::tuple_compare,
                tuple::tuple_compare_with_key,
                tuple::to_and_from_lua,
                tuple::tuple_debug_fmt,
                tuple::tuple_buffer_from_vec_fail,

                error::error_last,
                error::set_error,
                coio::coio_accept,
                coio::coio_read_write,
                coio::coio_call,
                coio::coio_channel,
                coio::channel_rx_closed,
                coio::channel_tx_closed,
                transaction::transaction_commit,
                transaction::transaction_rollback,
                log::log_with_user_defined_mapping,
                #[should_panic] log::zlog,
                latch::latch_lock,
                latch::latch_try_lock,
                net_box::immediate_close,
                net_box::ping,
                net_box::ping_timeout,
                net_box::ping_concurrent,
                net_box::call,
                net_box::call_async,
                net_box::call_async_error,
                net_box::call_async_disconnected,
                net_box::call_timeout,
                net_box::call_async_timeout,
                net_box::call_async_wait_disconnected,
                net_box::eval,
                net_box::eval_async,
                net_box::async_common_cond,
                net_box::connection_error,
                net_box::is_connected,
                net_box::schema_sync,
                net_box::select,
                net_box::get,
                net_box::insert,
                net_box::replace,
                net_box::update,
                net_box::upsert,
                net_box::delete,
                net_box::cancel_recv,
                net_box::triggers_connect,
                net_box::triggers_reject,
                net_box::triggers_schema_sync,
                net_box::execute,
                session::uid,
                session::euid,
                raft::bootstrap_solo,
                raft::bootstrap_2n,

                proc::simple,
                proc::return_tuple,
                proc::with_error,
                proc::packed,
                proc::debug,
                proc::tarantool_reimport,
                proc::custom_ret,
                proc::inject,
                proc::inject_with_packed,

                uuid::to_tuple,
                uuid::from_tuple,
                uuid::to_lua,
                uuid::from_lua,

                enums::space_engine_type,
                enums::space_field_type,
                enums::index_type,
                enums::index_field_type,
                enums::rtree_index_distance_type,
            ]);

            #[cfg(feature = "picodata")] {
                tests.append(&mut tests![
                    sql::prepared_source_query,
                    sql::prepared_invalid_query,
                    sql::prepared_no_params,
                    sql::prepared_large_query,
                    sql::prepared_with_unnamed_params,
                    sql::prepared_with_named_params,
                    sql::prepared_invalid_params,
                ])
            }

            tests
        },
    )
}

#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn start(l: *mut ffi_lua::lua_State) -> c_int {
    let cfg_src = ffi_lua::lua_tostring(l, 1);
    let cfg_src = if !cfg_src.is_null() {
        CStr::from_ptr(cfg_src).to_str().unwrap()
    } else {
        "{}"
    };
    let cfg: TestConfig = serde_json::from_str(cfg_src).unwrap();

    if let Err(e) = create_test_spaces() {
        ffi_lua::luaL_error(l, e.to_string().as_ptr() as *const c_char);
        return 0;
    }

    let is_success = match run_tests(cfg) {
        Ok(success) => success,
        Err(e) => {
            // Clenaup without handling error to avoid code mess.
            let _ = drop_test_spaces();
            ffi_lua::luaL_error(l, e.to_string().as_ptr() as *const c_char);
            return 0;
        }
    };

    if let Err(e) = drop_test_spaces() {
        ffi_lua::luaL_error(l, e.to_string().as_ptr() as *const c_char);
        return 0;
    }

    ffi_lua::lua_pushinteger(l, (!is_success) as isize);
    1
}

#[allow(clippy::missing_safety_doc)]
#[no_mangle]
pub unsafe extern "C" fn luaopen_libtarantool_module_test_runner(l: *mut ffi_lua::lua_State) -> c_int {
    ffi_lua::lua_pushcfunction(l, start);
    1
}
