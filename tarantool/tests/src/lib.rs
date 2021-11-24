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
use tarantool::index::{IndexFieldType, IndexOptions, IndexPart, IndexType};
use tarantool::space::{Space, SpaceCreateOptions, SpaceFieldFormat, SpaceFieldType};

mod bench_bulk_insert;
mod common;
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
mod hlua;

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

#[derive(Clone, Default, Deserialize)]
struct TestConfig {
    #[serde(default)]
    bench: bool,

    filter: Option<String>,
}

fn create_test_spaces() -> Result<(), Error> {
    // space.test_s1
    let mut test_s1_opts = SpaceCreateOptions::default();
    test_s1_opts.format = Some(vec![
        SpaceFieldFormat::new("id", SpaceFieldType::Unsigned),
        SpaceFieldFormat::new("text", SpaceFieldType::String),
    ]);
    let test_s1 = match Space::create("test_s1", &test_s1_opts) {
        Ok(s) => s,
        Err(e) => return Err(e),
    };

    // space.test_s1.index.primary
    let mut test_s1_idx_primary = IndexOptions::default();
    test_s1_idx_primary.index_type = Some(IndexType::Tree);
    test_s1_idx_primary.parts = Some(vec![IndexPart::new(1, IndexFieldType::Unsigned)]);
    test_s1.create_index("primary", &test_s1_idx_primary)?;

    // space.test_s2
    let mut test_s2_opts = SpaceCreateOptions::default();
    test_s2_opts.format = Some(vec![
        SpaceFieldFormat::new("id", SpaceFieldType::Unsigned),
        SpaceFieldFormat::new("key", SpaceFieldType::String),
        SpaceFieldFormat::new("value", SpaceFieldType::String),
        SpaceFieldFormat::new("a", SpaceFieldType::Integer),
        SpaceFieldFormat::new("b", SpaceFieldType::Integer),
    ]);
    let mut test_s2 = match Space::create("test_s2", &test_s1_opts) {
        Ok(s) => s,
        Err(e) => return Err(e),
    };

    // space.test_s2.index.primary
    let mut test_s2_idx_primary = IndexOptions::default();
    test_s2_idx_primary.index_type = Some(IndexType::Tree);
    test_s2_idx_primary.parts = Some(vec![IndexPart::new(1, IndexFieldType::Unsigned)]);
    test_s2.create_index("primary", &test_s2_idx_primary)?;

    // space.test_s2.index.idx_1
    let mut test_s2_idx_sec_1 = IndexOptions::default();
    test_s2_idx_sec_1.index_type = Some(IndexType::Hash);
    test_s2_idx_sec_1.parts = Some(vec![IndexPart::new(2, IndexFieldType::String)]);
    test_s2.create_index("idx_1", &test_s2_idx_sec_1)?;

    // space.test_s2.index.idx_2
    let mut test_s2_idx_sec_2 = IndexOptions::default();
    test_s2_idx_sec_2.index_type = Some(IndexType::Tree);
    test_s2_idx_sec_2.parts = Some(vec![
        IndexPart::new(1, IndexFieldType::Unsigned),
        IndexPart::new(4, IndexFieldType::Integer),
        IndexPart::new(5, IndexFieldType::Integer),
    ]);
    test_s2.create_index("idx_2", &test_s2_idx_sec_2)?;

    // space.test_s2.index.idx_3
    let mut test_s2_idx_sec_3 = IndexOptions::default();
    test_s2_idx_sec_3.index_type = Some(IndexType::Tree);
    test_s2_idx_sec_3.unique = Some(false);
    test_s2_idx_sec_3.parts = Some(vec![IndexPart::new(4, IndexFieldType::Integer)]);
    test_s2.create_index("idx_3", &test_s2_idx_sec_3)?;

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

fn run_tests(cfg: TestConfig) -> Result<bool, io::Error> {
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
                hlua::lua_functions::basic,
                hlua::lua_functions::two_functions_at_the_same_time,
                hlua::lua_functions::args,
                hlua::lua_functions::args_in_order,
                hlua::lua_functions::syntax_error,
                hlua::lua_functions::execution_error,
                hlua::lua_functions::check_types,
                hlua::lua_functions::call_and_read_table,
                hlua::lua_functions::table_as_args,
                hlua::lua_functions::table_method_call,
                hlua::lua_functions::lua_function_returns_function,
                hlua::lua_functions::error,
                hlua::lua_functions::either_or,
                hlua::lua_functions::multiple_return_values,
                hlua::lua_functions::multiple_return_values_fail,
                hlua::lua_functions::execute_from_reader_errors_if_cant_read,

                hlua::lua_tables::iterable,
                hlua::lua_tables::iterable_multipletimes,
                hlua::lua_tables::get_set,
                hlua::lua_tables::get_nil,
                hlua::lua_tables::table_over_table,
                hlua::lua_tables::metatable,
                hlua::lua_tables::empty_array,
                hlua::lua_tables::by_value,
                hlua::lua_tables::registry,
                hlua::lua_tables::registry_metatable,

                hlua::functions_write::simple_function,
                hlua::functions_write::one_argument,
                hlua::functions_write::two_arguments,
                hlua::functions_write::wrong_arguments_types,
                hlua::functions_write::return_result,
                hlua::functions_write::closures,
                hlua::functions_write::closures_lifetime,
                hlua::functions_write::closures_extern_access,
                hlua::functions_write::closures_drop_env,

                hlua::any::read_numbers,
                hlua::any::read_hashable_numbers,
                hlua::any::read_strings,
                hlua::any::read_hashable_strings,
                hlua::any::read_booleans,
                hlua::any::read_hashable_booleans,
                hlua::any::read_tables,
                hlua::any::read_hashable_tables,
                hlua::any::push_numbers,
                hlua::any::push_hashable_numbers,
                hlua::any::push_strings,
                hlua::any::push_hashable_strings,
                hlua::any::push_booleans,
                hlua::any::push_hashable_booleans,
                hlua::any::push_nil,
                hlua::any::push_hashable_nil,
                hlua::any::non_utf_8_string,

                hlua::misc::print,
                hlua::misc::json,
                hlua::misc::dump_stack,
                hlua::misc::dump_stack_raw,

                hlua::userdata::readwrite,
                hlua::userdata::destructor_called,
                hlua::userdata::type_check,
                hlua::userdata::metatables,
                hlua::userdata::multiple_userdata,

                hlua::rust_tables::write,
                hlua::rust_tables::write_map,
                hlua::rust_tables::write_set,
                hlua::rust_tables::globals_table,
                hlua::rust_tables::reading_vec_works,
                hlua::rust_tables::reading_vec_from_sparse_table_doesnt_work,
                hlua::rust_tables::reading_vec_with_empty_table_works,
                hlua::rust_tables::reading_vec_with_complex_indexes_doesnt_work,
                hlua::rust_tables::reading_heterogenous_vec_works,
                hlua::rust_tables::reading_vec_set_from_lua_works,
                hlua::rust_tables::reading_hashmap_works,
                hlua::rust_tables::reading_hashmap_from_sparse_table_works,
                hlua::rust_tables::reading_hashmap_with_empty_table_works,
                hlua::rust_tables::reading_hashmap_with_complex_indexes_works,
                hlua::rust_tables::reading_hashmap_with_floating_indexes_works,
                hlua::rust_tables::reading_heterogenous_hashmap_works,
                hlua::rust_tables::reading_hashmap_set_from_lua_works,
                hlua::rust_tables::derive_struct_push,
                hlua::rust_tables::derive_tuple_struct_push,
                hlua::rust_tables::derive_struct_lua_read,
                hlua::rust_tables::derive_tuple_struct_lua_read,
                hlua::rust_tables::derive_enum_push,
                hlua::rust_tables::derive_enum_lua_read,
                hlua::rust_tables::enum_variants_order_matters,
                hlua::rust_tables::struct_of_enums_vs_enum_of_structs,

                hlua::values::read_i32s,
                hlua::values::write_i32s,
                hlua::values::readwrite_floats,
                hlua::values::readwrite_bools,
                hlua::values::readwrite_strings,
                hlua::values::i32_to_string,
                hlua::values::string_to_i32,
                hlua::values::string_on_lua,
                hlua::values::push_opt,
                hlua::values::read_nil,

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

                test_box::test_space_get_by_name,
                test_box::test_space_get_system,
                test_box::test_index_get_by_name,
                test_box::test_box_insert,
                test_box::test_box_replace,
                test_box::test_box_delete,
                test_box::test_box_update,
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
                test_tuple::test_tuple_new_from_struct,
                test_tuple::test_tuple_field_count,
                test_tuple::test_tuple_size,
                test_tuple::test_tuple_into_struct,
                test_tuple::test_tuple_clone,
                test_tuple::test_tuple_iterator,
                test_tuple::test_tuple_iterator_seek_rewind,
                test_tuple::test_tuple_get_format,
                test_tuple::test_tuple_get_field,
                test_tuple::test_tuple_compare,
                test_tuple::test_tuple_compare_with_key,
                test_error::test_error_last,
                test_coio::test_coio_accept,
                test_coio::test_coio_read_write,
                test_coio::test_coio_call,
                test_coio::test_channel,
                test_coio::test_channel_rx_closed,
                test_coio::test_channel_tx_closed,
                test_transaction::test_transaction_commit,
                test_transaction::test_transaction_rollback,
                test_log::test_log,
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
            ]
        },
    )
}

pub extern "C" fn start(l: *mut ffi_lua::lua_State) -> c_int {
    let cfg_src = unsafe { ffi_lua::lua_tostring(l, 1) };
    let cfg = if !cfg_src.is_null() {
        let cfg_src = unsafe { CStr::from_ptr(cfg_src) }.to_str().unwrap();
        serde_json::from_str::<TestConfig>(cfg_src).unwrap()
    } else {
        TestConfig::default()
    };

    if let Err(e) = create_test_spaces() {
        unsafe { ffi_lua::luaL_error(l, e.to_string().as_ptr() as *const c_schar) };
        return 0;
    }

    let is_success = match run_tests(cfg) {
        Ok(success) => success,
        Err(e) => {
            // Clenaup without handling error to avoid code mess.
            let _ = drop_test_spaces();
            unsafe { ffi_lua::luaL_error(l, e.to_string().as_ptr() as *const c_schar) };
            return 0;
        }
    };

    if let Err(e) = drop_test_spaces() {
        unsafe { ffi_lua::luaL_error(l, e.to_string().as_ptr() as *const c_schar) };
        return 0;
    }

    unsafe { ffi_lua::lua_pushinteger(l, (!is_success) as isize) };
    1
}

#[no_mangle]
pub extern "C" fn luaopen_libtarantool_module_test_runner(l: *mut ffi_lua::lua_State) -> c_int {
    unsafe { ffi_lua::lua_pushcfunction(l, start) };
    1
}
