#![allow(clippy::approx_constant)]
#![allow(clippy::blacklisted_name)]
#![allow(clippy::bool_assert_comparison)]
#![allow(clippy::upper_case_acronyms)]
use std::ffi::CStr;
use std::io;
use std::os::raw::{c_int, c_schar};

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
mod test_box;
mod test_coio;
mod test_error;
mod fiber;
mod test_latch;
mod test_log;
mod test_net_box;
mod test_raft;
mod test_session;
mod test_transaction;
mod test_tuple;
mod tlua;
mod uuid;

macro_rules! tests {
    (@should_panic should_panic) => { ShouldPanic::Yes };
    (@should_panic $( $_:tt )?)  => { ShouldPanic::No };
    ($( $( #[ $attr:tt ] )? $func_name:path,)*) => {
        vec![
            $(TestDescAndFn{
                desc: TestDesc{
                    name: TestName::StaticTestName(stringify!($func_name)),
                    ignore: false,
                    should_panic: tests!(@should_panic $($attr)?),
                    allow_fail: false,
                    test_type: TestType::IntegrationTest
                },
                testfn: TestFn::StaticTestFn($func_name)
            },)*
        ]
    }
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
            tests![
                decimal::from_lua,
                decimal::to_lua,
                decimal::from_string,
                decimal::from_tuple,
                decimal::to_tuple,
                decimal::from_num,
                decimal::to_num,
                decimal::cmp,
                decimal::ops,

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

                fiber::old::test_fiber_new,
                fiber::old::test_fiber_new_with_attr,
                fiber::old::test_fiber_arg,
                fiber::old::test_fiber_cancel,
                fiber::old::test_fiber_wake,
                fiber::old::test_fiber_wake_multiple,
                fiber::old::test_fiber_cond_signal,
                fiber::old::test_fiber_cond_broadcast,
                fiber::old::test_fiber_cond_timeout,

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
                fiber::lua_thread,
                fiber::lifetime,

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
                fiber::channel::cannot_send_ref,

                fiber::mutex::simple,
                fiber::mutex::try_lock,
                fiber::mutex::debug,
                fiber::mutex::advanced,

                test_box::test_space_get_by_name,
                test_box::test_space_get_system,
                test_box::test_index_get_by_name,
                test_box::test_box_insert,
                test_box::test_box_replace,
                test_box::test_box_delete,
                test_box::test_box_update,
                test_box::test_box_update_macro,
                test_box::test_box_update_index_macro,
                test_box::test_box_upsert,
                test_box::test_box_truncate,
                test_box::test_box_get,
                test_box::test_box_select,
                test_box::test_box_select_composite_key,
                test_box::test_box_len,
                test_box::test_box_random,
                test_box::test_box_min_max,
                test_box::test_box_count,
                test_box::test_box_extract_key,
                test_box::test_box_sequence_get_by_name,
                test_box::test_box_sequence_iterate,
                test_box::test_box_sequence_set,
                test_box::test_space_create_opt_default,
                test_box::test_space_create_opt_if_not_exists,
                test_box::test_space_create_id_increment,
                test_box::test_space_create_opt_user,
                test_box::test_space_create_opt_id,
                test_box::test_space_drop,
                test_box::test_index_create_drop,
                test_box::index_parts,

                test_tuple::test_tuple_new_from_struct,
                test_tuple::new_tuple_from_flutten_struct,
                test_tuple::test_tuple_field_count,
                test_tuple::test_tuple_size,
                test_tuple::test_tuple_into_struct,
                test_tuple::test_tuple_clone,
                test_tuple::test_tuple_iterator,
                test_tuple::test_tuple_iterator_seek_rewind,
                test_tuple::test_tuple_get_format,
                test_tuple::test_tuple_get_field,
                test_tuple::tuple_get_field_path,
                test_tuple::test_tuple_compare,
                test_tuple::test_tuple_compare_with_key,
                test_tuple::to_and_from_lua,
                test_tuple::tuple_debug_fmt,
                test_tuple::tuple_buffer_from_vec_fail,

                test_error::test_error_last,
                test_coio::test_coio_accept,
                test_coio::test_coio_read_write,
                test_coio::test_coio_call,
                test_coio::test_channel,
                test_coio::test_channel_rx_closed,
                test_coio::test_channel_tx_closed,
                test_transaction::test_transaction_commit,
                test_transaction::test_transaction_rollback,
                test_log::log_with_user_defined_mapping,
                #[should_panic] test_log::test_log,
                test_latch::test_latch_lock,
                test_latch::test_latch_try_lock,
                test_net_box::test_immediate_close,
                test_net_box::test_ping,
                test_net_box::test_ping_timeout,
                test_net_box::test_ping_concurrent,
                test_net_box::test_call,
                test_net_box::test_call_timeout,
                test_net_box::test_eval,
                test_net_box::test_connection_error,
                test_net_box::test_is_connected,
                test_net_box::test_schema_sync,
                test_net_box::test_select,
                test_net_box::test_get,
                test_net_box::test_insert,
                test_net_box::test_replace,
                test_net_box::test_update,
                test_net_box::test_upsert,
                test_net_box::test_delete,
                test_net_box::test_cancel_recv,
                test_net_box::test_triggers_connect,
                test_net_box::test_triggers_reject,
                test_net_box::test_triggers_schema_sync,
                test_session::test_uid,
                test_session::test_euid,
                test_raft::test_bootstrap_solo,
                test_raft::test_bootstrap_2n,

                uuid::to_tuple,
                uuid::from_tuple,
                uuid::to_lua,
                uuid::from_lua,
            ]
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
        ffi_lua::luaL_error(l, e.to_string().as_ptr() as *const c_schar);
        return 0;
    }

    let is_success = match run_tests(cfg) {
        Ok(success) => success,
        Err(e) => {
            // Clenaup without handling error to avoid code mess.
            let _ = drop_test_spaces();
            ffi_lua::luaL_error(l, e.to_string().as_ptr() as *const c_schar);
            return 0;
        }
    };

    if let Err(e) = drop_test_spaces() {
        ffi_lua::luaL_error(l, e.to_string().as_ptr() as *const c_schar);
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
