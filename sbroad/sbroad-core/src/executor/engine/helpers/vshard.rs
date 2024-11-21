use core::fmt;
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    rc::Rc,
    time::Duration,
};

use tarantool::{
    ffi::sql::{ibuf_reinit, Ibuf},
    fiber,
    tlua::StringInLua,
};

use crate::ir::{
    helpers::RepeatableState,
    transformation::redistribution::{MotionOpcode, MotionPolicy},
    tree::{
        relation::RelationalIterator,
        traversal::{LevelNode, PostOrderWithFilter, REL_CAPACITY},
    },
    Plan,
};
use crate::{
    executor::{
        engine::{helpers::build_optional_binary, ConvertToDispatchResult, DispatchReturnFormat},
        protocol::{FullMessage, RequiredMessage},
        result::ProducerResult,
    },
    ir::node::{
        relational::{MutRelational, Relational},
        Motion, Node, NodeId,
    },
};
use ahash::AHashMap;
use rand::{thread_rng, Rng};
use smol_str::{format_smolstr, SmolStr};
use tarantool::tlua::{CDataOnStack, LuaState, LuaTable, PushGuard, PushInto};
use tarantool::{tlua::LuaFunction, tuple::Tuple};

use crate::{
    error,
    errors::{Entity, SbroadError},
    executor::{
        bucket::Buckets,
        engine::{helpers::empty_query_result, Vshard},
        ir::{ExecutionPlan, QueryType},
    },
};

use super::{build_required_binary, filter_vtable};

/// Map between replicaset uuid and plan subtree (with additional info), sending to it.
/// (see `Message`documentation for more info).
type DMLReplicasetMessage = HashMap<String, FullMessage>;

tarantool::define_str_enum! {
    #[derive(Default)]
    pub enum CacheInfo {
        #[default]
        CacheableFirstRequest = "cacheable-1",
        CacheableSecondRequest = "cacheable-2",
    }
}

/// Function to execute DML (INSERT) query only on some replicasets (which uuid's are
/// present in the `rs_ir` argument) using vshard router choosed by `tier` argument.
///
/// Rust binding to Lua `dml_on_some` function.
fn lua_dispatch_dml(
    waiting_timeout: u64,
    rs_ir: DMLReplicasetMessage,
    tier_name: Option<&SmolStr>,
) -> Result<Box<dyn Any>, SbroadError> {
    let lua = tarantool::lua_state();
    let tier_name = tier_name.map(SmolStr::to_string);

    let Some(exec_sql): Option<LuaFunction<_>> = lua.get("dispatch_dml") else {
        return Err(SbroadError::Other("no function 'dispatch_dml'".into()));
    };

    let call_res = exec_sql.call_with_args::<Tuple, _>((rs_ir, waiting_timeout, tier_name));
    match call_res {
        Ok(v) => Ok(Box::new(v)),
        Err(e) => {
            error!(Option::from("dml_on_some"), &format!("{e:?}"));
            Err(SbroadError::LuaError(format_smolstr!(
                "Lua error (IR dispatch): {e:?}"
            )))
        }
    }
}

/// Function to execute DQL (SELECT) query on given replicasets
/// using the same plan for all replicasets.
///
/// # Panics
/// - Failed to find lua function `dql_with_single_plan`
fn lua_dql_single_plan<'lua, T>(
    lua: &'lua tarantool::tlua::LuaThread,
    args: T,
    replicasets: &[RSName],
    row_count: u64,
    vtable_max_rows: u64,
    waiting_timeout: u64,
    tier_name: Option<&SmolStr>,
) -> Result<Rc<LuaTable<LuaStackGuard<'lua>>>, SbroadError>
where
    T: PushInto<LuaState>,
    T::Err: fmt::Debug,
{
    let Some(exec_sql): Option<LuaFunction<_>> = lua.get("dispatch_dql_single_plan") else {
        return Err(SbroadError::Other(
            "no function 'dispatch_dql_single_plan'".into(),
        ));
    };

    let tier_name = tier_name.map(SmolStr::to_string);

    let call_res = exec_sql.into_call_with_args::<LuaTable<_>, _>((
        args,
        replicasets,
        waiting_timeout,
        row_count,
        vtable_max_rows,
        tier_name,
    ));
    match call_res {
        Ok(v) => Ok(Rc::new(v)),
        Err(e) => {
            error!(Option::from("dql_with_single_plan"), &format!("{e:?}"));
            Err(SbroadError::LuaError(format_smolstr!(
                "Lua error (dispatch IR): {e:?}"
            )))
        }
    }
}

/// Function to execute DML query on replicasets
/// using the same plan for each replicaset.
///
/// # Panics
/// - failed to get expected lua function
fn lua_dml_single_plan(
    waiting_timeout: u64,
    message: FullMessage,
    replicasets: &[RSName],
    tier_name: Option<&SmolStr>,
) -> Result<Box<dyn Any>, SbroadError> {
    let lua = tarantool::lua_state();

    let Some(exec_sql): Option<LuaFunction<_>> = lua.get("dispatch_dml_single_plan") else {
        return Err(SbroadError::Other(
            "no function 'dispatch_dml_single_plan'".into(),
        ));
    };

    let tier_name = tier_name.map(SmolStr::to_string);

    let call_res =
        exec_sql.call_with_args::<Tuple, _>((message, replicasets, waiting_timeout, tier_name));
    match call_res {
        Ok(v) => Ok(Box::new(v)),
        Err(e) => {
            error!(Option::from("dml_on_all"), &format!("{e:?}"));
            Err(SbroadError::LuaError(format_smolstr!(
                "Lua error (dispatch IR): {e:?}"
            )))
        }
    }
}

fn build_final_dql_result(
    rs_to_res: RSResultMap<'_>,
    return_format: DispatchReturnFormat,
) -> Result<Box<dyn Any>, SbroadError> {
    // HACK: if non-empty table was returned only from
    // single Replicaset, then we can return result
    // immediately without decoding and concatenation
    if rs_to_res.len() == 1 && return_format == DispatchReturnFormat::Tuple {
        // Fast path: when we executed on single replicaset
        // this optimisation is always possible
        let result = rs_to_res.into_values().next().unwrap();
        let tuple: Tuple = result.try_into()?;
        return Ok(Box::new(tuple));
    }
    if return_format == DispatchReturnFormat::Tuple {
        // Slow path: we need to iterate over results
        // to check if optimisation is possible
        let mut was_non_empty_res = false;
        let mut is_optimisation_possible = true;
        for res in rs_to_res.values() {
            if res.row_cnt > 0 && was_non_empty_res {
                is_optimisation_possible = false;
                break;
            } else if res.row_cnt > 0 {
                was_non_empty_res = true;
            }
        }
        if is_optimisation_possible && was_non_empty_res {
            // Only single replicaset returned non-empty
            // result, let's find it and return it as is
            let result = rs_to_res.into_values().find(|r| r.row_cnt > 0).unwrap();
            let tuple: Tuple = result.try_into()?;
            return Ok(Box::new(tuple));
        }
        if is_optimisation_possible && !rs_to_res.is_empty() {
            // All replicasets returned empty result, we can
            // return any result from map, let's take the first one
            let result = rs_to_res.into_values().next().unwrap();
            let tuple: Tuple = result.try_into()?;
            return Ok(Box::new(tuple));
        }
    }

    let producer_result = concat_results(rs_to_res)?;
    producer_result.convert(return_format)
}

type RSName = String;
type RSMap = HashMap<RSName, ExecutionPlan>;
/// Mapping between replicaset ID and result returned from that replicaset
type RSResultMap<'lua> = HashMap<String, OneRSResult<'lua>, RepeatableState>;
type DqlResult<'lua> = RSResultMap<'lua>;

#[allow(unused_variables)]
fn transform_lua_res<T, E: fmt::Display>(
    target: Option<&str>,
    result: Result<T, E>,
) -> Result<T, SbroadError> {
    result.map_err(|err| {
        let error = format!("lua error: {err:#}");
        error!(target, &error.to_string());
        SbroadError::DispatchError(error.into())
    })
}

macro_rules! unwrap_or_error {
    ($expr:expr) => {{
        $expr.ok_or_else(|| {
            SbroadError::Other(format_smolstr!(
                "{}:{}: unexpected replicaset id",
                file!(),
                line!()
            ))
        })?
    }};
}

/// Execute DQL with custom plan for each replicaset
///
/// # Errors
/// - Errors during query execution
pub fn exec_cacheable_dql_with_custom_plans<'a>(
    lua: &'a tarantool::tlua::LuaThread,
    mut rs_to_plan: RSMap,
    vtable_max_rows: u64,
    waiting_timeout: u64,
    tier_name: Option<&SmolStr>,
) -> Result<DqlResult<'a>, SbroadError> {
    let deadline = fiber::clock().saturating_add(Duration::from_secs(waiting_timeout));
    let rs_count = rs_to_plan.len();
    let mut cache_required_args = HashMap::with_capacity(rs_to_plan.len());
    for (rs, exec_plan) in &mut rs_to_plan {
        let required_binary = build_required_binary(exec_plan)?;
        cache_required_args.insert(rs.clone(), RequiredMessage::from(required_binary));
    }
    let mut row_count: u64 = 0;
    let first_round_lua_table = transform_lua_res(
        "cacheable with custom plans. first round".into(),
        lua_dispatch_dql(
            lua,
            &cache_required_args,
            row_count,
            vtable_max_rows,
            waiting_timeout,
            tier_name,
        ),
    )?;
    let mut rs_to_res = convert_lua_table_to_rust(&first_round_lua_table.clone(), rs_count)?;

    let missed_cache_cnt = rs_to_res.values().filter(|res| res.cache_miss).count();
    let mut missed_cache_rs_to_full_args = HashMap::with_capacity(missed_cache_cnt);
    for (rs, result) in &rs_to_res {
        row_count += result.row_cnt;

        if !result.cache_miss {
            continue;
        }
        let required_binary = unwrap_or_error!(cache_required_args.remove(rs)).required;
        let (rs_owned, exec_plan) = unwrap_or_error!(rs_to_plan.remove_entry(rs));
        let optional_binary = build_optional_binary(exec_plan)?;
        missed_cache_rs_to_full_args
            .insert(rs_owned, FullMessage::new(required_binary, optional_binary));
    }

    if missed_cache_rs_to_full_args.is_empty() {
        return Ok(rs_to_res);
    }
    let timeout = deadline.duration_since(fiber::clock()).as_secs();
    let second_round_lua_table = transform_lua_res(
        "cacheable with custom plans. second round".into(),
        lua_dispatch_dql(
            lua,
            missed_cache_rs_to_full_args,
            row_count,
            vtable_max_rows,
            timeout,
            tier_name,
        ),
    )?;
    let rs_to_missed_res = convert_lua_table_to_rust(&second_round_lua_table.clone(), rs_count)?;
    for (rs, results) in rs_to_missed_res {
        let mut_value = unwrap_or_error!(rs_to_res.get_mut(&rs));
        *mut_value = results;
    }

    Ok(rs_to_res)
}

fn concat_results(rs_to_res: RSResultMap<'_>) -> Result<ProducerResult, SbroadError> {
    let mut row_count = 0;
    for res in rs_to_res.values() {
        row_count += res.row_cnt;
    }
    let mut metadata = None;
    let mut rows = Vec::new();
    for (_, res) in rs_to_res {
        let decoded: ProducerResult = res.try_into()?;
        if metadata.is_none() {
            metadata = Some(decoded.metadata);
            rows = decoded.rows;
            rows.reserve(usize::try_from(row_count - rows.len() as u64).expect("too much rows"));
        } else {
            rows.extend(decoded.rows.into_iter());
        }
    }

    Ok(ProducerResult {
        metadata: metadata.ok_or_else(|| SbroadError::Other("empty result map".into()))?,
        rows,
    })
}

/// Result from single replicaset.
pub struct OneRSResult<'lua> {
    // Buffer containing result returned from storage
    ibuf_ptr: *mut Ibuf,
    // Reference to lua table, where ibuf lives. This
    // ensures ibuf_ptr is valid.
    _source: Rc<LuaTable<LuaStackGuard<'lua>>>,
    // Storage returns result as msgpack array of three elements:
    // [row count, cache miss flag, [producer result]]
    // This offset points to the start of producer result.
    read_offset: usize,
    // Number of rows returned
    pub row_cnt: u64,
    // True if the initial request contained
    // only cache required data and we missed
    // the cache on the storage.
    pub cache_miss: bool,
}

impl<'lua> OneRSResult<'lua> {
    fn to_start_and_size(&self) -> (*const u8, usize) {
        unsafe {
            // SAFETY: ibuf_ptr points to correctly intialized Ibuf
            // object because:
            // 1. Earlier it was initialized on lua stack by tarantool
            // 2. Lua table containing that ibuf is still alive on that
            // lua stack (we keep rc to that table).
            // 3. Rust aliasing rules are ensured: the memory being
            // referred is not mutated.
            let ibuf = self.ibuf_ptr.as_ref().unwrap();
            // SAFETY:
            // 1. Both starting and resulting pointers point
            // to same allocated object - true, pointers refer
            // to contiguos memory region allocated by tarantool
            // 2. The computed offset cannot overflow isize - true,
            // our offset is always small: it is len of row count
            // and chache miss msgpack combined.
            let start = ibuf.rpos.add(self.read_offset);

            // SAFETY:
            // 1. Both
            let offset = ibuf.wpos.offset_from(start);
            (start, usize::try_from(offset).unwrap())
        }
    }
}

impl<'lua> TryFrom<OneRSResult<'lua>> for ProducerResult {
    type Error = SbroadError;

    fn try_from(res: OneRSResult) -> Result<Self, Self::Error> {
        let (start, size) = res.to_start_and_size();
        // SAFETY:
        let mut reader = unsafe { std::slice::from_raw_parts(start, size) };
        let wrapped: Vec<ProducerResult> = rmp_serde::from_read(&mut reader)
            .map_err(|e| SbroadError::Other(format_smolstr!("failed to decode tuple: {e:?}")))?;
        let value = wrapped
            .into_iter()
            .next()
            .ok_or_else(|| SbroadError::Other("expected non-empty array".into()))?;
        Ok(value)
    }
}

impl<'lua> TryFrom<OneRSResult<'lua>> for Tuple {
    type Error = SbroadError;

    fn try_from(res: OneRSResult) -> Result<Self, Self::Error> {
        let (start, size) = res.to_start_and_size();
        // SAFETY:
        let bytes: &[u8] = unsafe { std::slice::from_raw_parts(start, size) };
        let tuple = unsafe { Tuple::from_slice(bytes) };
        Ok(tuple)
    }
}

impl<'lua> Drop for OneRSResult<'lua> {
    fn drop(&mut self) {
        // Don't wait for lua garbage collector,
        // free buffer memory as soon it is not needed.
        // Calling ibuf_reinit multiple times is safe,
        // as second and other calls do nothing.
        //
        // SAFETY: it is safe as lua table storing
        // ibuf referred by ibuf_ptr is still alive
        // as we keep a reference to it.
        unsafe {
            ibuf_reinit(self.ibuf_ptr);
        }
    }
}

fn convert_lua_table_to_rust<'lua>(
    lua_table: &Rc<LuaTable<LuaStackGuard<'lua>>>,
    rs_cnt: usize,
) -> Result<RSResultMap<'lua>, SbroadError> {
    let mut iter = lua_table.iter::<StringInLua<_>, CDataOnStack<_>>();
    let mut rs_to_res: RSResultMap<'lua> =
        HashMap::with_capacity_and_hasher(rs_cnt, RepeatableState);
    for maybe_kv in iter.by_ref() {
        let (key, value) = maybe_kv.map_err(|e| {
            SbroadError::Other(format_smolstr!("failed to convert lua table entry: {e:?}"))
        })?;
        let result = {
            let (mut s, ibuf): (&[u8], &Ibuf) = unsafe {
                // SAFETY: lua functions must return table with values of Ibuf
                // it is safe to read this, as long as LuaTable is alive.
                let ibuf = value.as_ptr().cast::<Ibuf>().as_ref().unwrap();
                // ptr.offset_from(ptr) requires two pointers to point to
                // the same allocated object.
                let offset = ibuf.wpos as isize - ibuf.rpos as isize;
                if offset <= 0 {
                    return Err(SbroadError::Other("invalid ibuf: negative size".into()));
                }
                let sz = usize::try_from(offset).unwrap();
                // SAFETY: data is valid for reads as long as ibuf on lua
                // stack is alive
                let s = std::slice::from_raw_parts(ibuf.rpos, sz);
                (s, ibuf)
            };
            let row_cnt: u64 = rmp_serde::decode::from_read(&mut s).map_err(|e| {
                SbroadError::Other(format_smolstr!("failed to read row cnt from ibuf: {e:?}"))
            })?;
            let cache_miss: bool = rmp_serde::decode::from_read(&mut s).map_err(|e| {
                SbroadError::Other(format_smolstr!(
                    "failed to read cache hit flag from ibuf: {e:?}"
                ))
            })?;

            // ptr.offset_from(ptr) requires two pointers to point to
            // the same allocated object.
            let read_offset = s.as_ptr() as isize - ibuf.rpos as isize;
            if read_offset <= 0 {
                return Err(SbroadError::Other("invalid rpos in ibuf".into()));
            }
            OneRSResult {
                ibuf_ptr: value.as_ptr().cast_mut().cast::<Ibuf>(),
                _source: lua_table.clone(),
                read_offset: usize::try_from(read_offset).unwrap(),
                row_cnt,
                cache_miss,
            }
        };
        rs_to_res.insert((*key).to_string(), result);
    }
    drop(iter);
    Ok(rs_to_res)
}

type LuaStackGuard<'lua> = PushGuard<LuaFunction<PushGuard<&'lua tarantool::tlua::LuaThread>>>;

/// # Panics
/// - Failed to find lua function `dispatch_by_count`
#[inline]
fn lua_dispatch_dql<'a, T>(
    lua: &'a tarantool::tlua::LuaThread,
    rs_to_plan: T,
    row_count: u64,
    vtable_max_rows: u64,
    waiting_timeout: u64,
    tier_name: Option<&SmolStr>,
) -> Result<Rc<LuaTable<LuaStackGuard<'a>>>, SbroadError>
where
    T: PushInto<LuaState>,
    T::Err: fmt::Debug,
{
    let Some(exec_sql): Option<LuaFunction<_>> = lua.get("dispatch_dql") else {
        return Err(SbroadError::Other("no function 'dispatch_dql'".into()));
    };

    let tier_name = tier_name.map(SmolStr::to_string);

    let check_bucket_count = false;
    let call_res = exec_sql.into_call_with_args::<LuaTable<_>, _>((
        rs_to_plan,
        waiting_timeout,
        row_count,
        vtable_max_rows,
        check_bucket_count,
        tier_name,
    ));
    match call_res {
        Ok(v) => Ok(Rc::new(v)),
        Err(e) => Err(SbroadError::LuaError(format_smolstr!(
            "Lua error (dispatch_by_count): {e:?}"
        ))),
    }
}

/// Execute cacheable DQL with the same plan
/// for all replicasets.
///
/// # Errors
/// - errors during query execution
pub fn exec_cacheable_dql_with_single_plan<'lua>(
    lua: &'lua tarantool::tlua::LuaThread,
    mut exec_plan: ExecutionPlan,
    replicasets: &[RSName],
    vtable_max_rows: u64,
    waiting_timeout: u64,
    tier_name: Option<&SmolStr>,
) -> Result<DqlResult<'lua>, SbroadError> {
    let deadline = fiber::clock().saturating_add(Duration::from_secs(waiting_timeout));
    let required_message = {
        let required_binary = build_required_binary(&mut exec_plan)?;
        RequiredMessage::from(required_binary)
    };
    let mut row_count: u64 = 0;
    let first_round_lua_table = transform_lua_res(
        "cacheable with single plan first round".into(),
        lua_dql_single_plan(
            lua,
            &required_message,
            replicasets,
            row_count,
            vtable_max_rows,
            waiting_timeout,
            tier_name,
        ),
    )?;
    let mut rs_to_res = convert_lua_table_to_rust(&first_round_lua_table, replicasets.len())?;

    let missed_cache_cnt = rs_to_res.values().filter(|res| res.cache_miss).count();
    let mut missed_cache_replicasets = Vec::with_capacity(missed_cache_cnt);
    for (rs, result) in &rs_to_res {
        row_count += result.row_cnt;
        if result.cache_miss {
            missed_cache_replicasets.push(rs.clone());
        }
    }
    if missed_cache_replicasets.is_empty() {
        return Ok(rs_to_res);
    }
    let full_message = {
        let required_binary = required_message.required;
        let optional_binary = build_optional_binary(exec_plan)?;
        FullMessage::new(required_binary, optional_binary)
    };

    let timeout = deadline.duration_since(fiber::clock()).as_secs();
    let second_round_lua_table = transform_lua_res(
        "cacheable dql with single second round".into(),
        lua_dql_single_plan(
            lua,
            full_message,
            replicasets,
            row_count,
            vtable_max_rows,
            timeout,
            tier_name,
        ),
    )?;
    let rs_to_missed_res = convert_lua_table_to_rust(&second_round_lua_table, replicasets.len())?;
    for (rs, results) in rs_to_missed_res {
        let mut_value = unwrap_or_error!(rs_to_res.get_mut(&rs));
        *mut_value = results;
    }

    Ok(rs_to_res)
}

fn exec_with_single_plan(
    mut exec_plan: ExecutionPlan,
    buckets: &Buckets,
    return_format: DispatchReturnFormat,
    waiting_timeout: u64,
    tier_name: Option<&SmolStr>,
) -> Result<Box<dyn Any>, SbroadError> {
    let query_type = exec_plan.query_type()?;
    let vtable_max_rows = exec_plan.get_vtable_max_rows();
    let replicasets = lua_get_replicasets_from_buckets(buckets, tier_name)?;
    match &query_type {
        QueryType::DQL => {
            let lua = tarantool::lua_state();
            let rs_to_res = exec_cacheable_dql_with_single_plan(
                &lua,
                exec_plan,
                &replicasets,
                vtable_max_rows,
                waiting_timeout,
                tier_name,
            )?;
            build_final_dql_result(rs_to_res, return_format)
        }
        QueryType::DML => {
            let required_binary = build_required_binary(&mut exec_plan)?;
            let optional_binary = build_optional_binary(exec_plan)?;
            let message = FullMessage::new(required_binary, optional_binary);
            lua_dml_single_plan(waiting_timeout, message, &replicasets, tier_name)
        }
    }
}

fn exec_with_custom_plan(
    runtime: &impl Vshard,
    sub_plan: ExecutionPlan,
    buckets: &Buckets,
    return_format: DispatchReturnFormat,
    waiting_timeout: u64,
    tier_name: Option<&SmolStr>,
) -> Result<Box<dyn Any>, SbroadError> {
    let all_buckets: HashSet<u64, RepeatableState>;

    let buckets_set = match buckets {
        Buckets::All => {
            all_buckets = (1..=runtime.bucket_count()).collect();
            &all_buckets
        }
        Buckets::Filtered(buckets) => buckets,
        Buckets::Any => unreachable!("buckets any case is handled earlier"),
    };

    // todo(ars): group should be a runtime function not global,
    // this way we could implement it for mock runtime for better testing
    // Vec of { replicaset_uuid, corresponding bucket ids }.
    let rs_bucket_vec: Vec<(String, Vec<u64>)> = group(buckets_set, tier_name)?.drain().collect();
    if rs_bucket_vec.is_empty() {
        return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
            "no replica sets were found for the buckets {buckets:?} to execute the query on"
        )));
    }

    let query_type = sub_plan.query_type()?;
    let vtable_max_rows = sub_plan.get_vtable_max_rows();
    let rs_ir = prepare_rs_to_ir_map(&rs_bucket_vec, sub_plan)?;
    match &query_type {
        QueryType::DQL => {
            let lua = tarantool::lua_state();
            let rs_to_res = exec_cacheable_dql_with_custom_plans(
                &lua,
                rs_ir,
                vtable_max_rows,
                waiting_timeout,
                tier_name,
            )?;
            build_final_dql_result(rs_to_res, return_format)
        }
        QueryType::DML => {
            let mut rs_message = HashMap::with_capacity(rs_ir.len());
            for (rs, mut ir) in rs_ir {
                let required_binary = build_required_binary(&mut ir)?;
                let optional_binary = build_optional_binary(ir)?;
                rs_message.insert(rs, FullMessage::new(required_binary, optional_binary));
            }
            lua_dispatch_dml(waiting_timeout, rs_message, tier_name)
        }
    }
}

/// Generic function over `dql_on_some` and `dml_on_some` functions.
/// Used to execute IR on some replicasets (that are discovered from given `buckets`) using vshard router choosed by `tier argument`.
pub fn impl_exec_ir_on_buckets(
    runtime: &impl Vshard,
    sub_plan: ExecutionPlan,
    buckets: &Buckets,
    return_format: DispatchReturnFormat,
    waiting_timeout: u64,
    tier_name: Option<&SmolStr>,
) -> Result<Box<dyn Any>, SbroadError> {
    if let Buckets::Filtered(buckets_set) = buckets {
        if buckets_set.is_empty() {
            return empty_query_result(&sub_plan, return_format.clone());
        }
    }

    if !sub_plan.has_segmented_tables() && !sub_plan.has_customization_opcodes() {
        exec_with_single_plan(sub_plan, buckets, return_format, waiting_timeout, tier_name)
    } else {
        exec_with_custom_plan(
            runtime,
            sub_plan,
            buckets,
            return_format,
            waiting_timeout,
            tier_name,
        )
    }
}

// Helper struct to hold information
// needed to apply SerializeAsEmpty opcode
// to subtree.
struct SerializeAsEmptyInfo {
    // ids of topmost motion nodes which have this opcode
    // with `true` value
    top_motion_ids: Vec<NodeId>,
    // ids of motions which have this opcode
    target_motion_ids: Vec<NodeId>,
    unused_motions: Vec<NodeId>,
    // ids of motions that are located below
    // top_motion_id, vtables corresponding
    // to those motions must be deleted from
    // replicaset message.
    motions_ref_count: AHashMap<NodeId, usize>,
}

impl Plan {
    // return true if given node is Motion containing seriliaze as empty
    // opcode. If `check_enabled` is true checks that the opcode is enabled.
    fn is_serialize_as_empty_motion(&self, node_id: NodeId, check_enabled: bool) -> bool {
        if let Ok(Node::Relational(Relational::Motion(Motion { program, .. }))) =
            self.get_node(node_id)
        {
            if let Some(op) = program
                .0
                .iter()
                .find(|op| matches!(op, MotionOpcode::SerializeAsEmptyTable(_)))
            {
                return !check_enabled || matches!(op, MotionOpcode::SerializeAsEmptyTable(true));
            };
        }
        false
    }

    fn collect_top_ids(&self) -> Result<Vec<NodeId>, SbroadError> {
        let mut stop_nodes: HashSet<NodeId> = HashSet::new();
        let iter_children = |node_id| -> RelationalIterator<'_> {
            if self.is_serialize_as_empty_motion(node_id, true) {
                stop_nodes.insert(node_id);
            }
            // do not traverse subtree with this child
            if stop_nodes.contains(&node_id) {
                return self.nodes.empty_rel_iter();
            }
            self.nodes.rel_iter(node_id)
        };
        let filter = |node_id: NodeId| -> bool { self.is_serialize_as_empty_motion(node_id, true) };
        let mut dfs = PostOrderWithFilter::with_capacity(iter_children, 4, Box::new(filter));

        Ok(dfs.iter(self.get_top()?).map(|id| id.1).collect())
    }

    fn serialize_as_empty_info(&self) -> Result<Option<SerializeAsEmptyInfo>, SbroadError> {
        let top_ids = self.collect_top_ids()?;

        let mut motions_ref_count: AHashMap<NodeId, usize> = AHashMap::new();
        let filter = |node_id: NodeId| -> bool {
            matches!(
                self.get_node(node_id),
                Ok(Node::Relational(Relational::Motion(_)))
            )
        };
        let mut dfs =
            PostOrderWithFilter::with_capacity(|x| self.nodes.rel_iter(x), 0, Box::new(filter));
        for LevelNode(_, motion_id) in dfs.iter(self.get_top()?) {
            motions_ref_count
                .entry(motion_id)
                .and_modify(|cnt| *cnt += 1)
                .or_insert(1);
        }

        if top_ids.is_empty() {
            return Ok(None);
        }

        // all motion nodes that are inside the subtrees
        // defined by `top_ids`
        let all_motion_nodes = {
            let is_motion = |node_id: NodeId| -> bool {
                matches!(
                    self.get_node(node_id),
                    Ok(Node::Relational(Relational::Motion(_)))
                )
            };
            let mut all_motions = Vec::new();
            for top_id in &top_ids {
                let mut dfs = PostOrderWithFilter::with_capacity(
                    |x| self.nodes.rel_iter(x),
                    REL_CAPACITY,
                    Box::new(is_motion),
                );
                all_motions.extend(dfs.iter(*top_id).map(|id| id.1));
            }
            all_motions
        };
        let mut target_motions = Vec::new();
        let mut unused_motions = Vec::new();
        for id in all_motion_nodes {
            if self.is_serialize_as_empty_motion(id, false) {
                target_motions.push(id);
            } else {
                unused_motions.push(id);
            }
        }

        Ok(Some(SerializeAsEmptyInfo {
            top_motion_ids: top_ids,
            target_motion_ids: target_motions,
            unused_motions,
            motions_ref_count,
        }))
    }
}

/// Prepares execution plan for each replicaset.
///
/// # Errors
/// - Failed to apply customization opcodes
/// - Failed to filter vtable
pub fn prepare_rs_to_ir_map(
    rs_bucket_vec: &[(String, Vec<u64>)],
    mut sub_plan: ExecutionPlan,
) -> Result<RSMap, SbroadError> {
    let mut rs_ir: RSMap = HashMap::new();
    rs_ir.reserve(rs_bucket_vec.len());
    if let Some((last, other)) = rs_bucket_vec.split_last() {
        let mut sae_info = sub_plan.get_ir_plan().serialize_as_empty_info()?;
        let mut other_plan = sub_plan.clone();

        if let Some(info) = sae_info.as_mut() {
            apply_serialize_as_empty_opcode(&mut other_plan, info)?;
        }
        if let Some((other_last, other_other)) = other.split_last() {
            for (rs, bucket_ids) in other_other {
                let mut rs_plan = other_plan.clone();
                filter_vtable(&mut rs_plan, bucket_ids)?;
                rs_ir.insert(rs.clone(), rs_plan);
            }
            let (rs, bucket_ids) = other_last;
            filter_vtable(&mut other_plan, bucket_ids)?;
            rs_ir.insert(rs.clone(), other_plan);
        }

        if let Some(ref info) = sae_info {
            disable_serialize_as_empty_opcode(&mut sub_plan, info)?;
        }
        let (rs, bucket_ids) = last;
        filter_vtable(&mut sub_plan, bucket_ids)?;
        rs_ir.insert(rs.clone(), sub_plan);
    }

    Ok(rs_ir)
}

fn apply_serialize_as_empty_opcode(
    sub_plan: &mut ExecutionPlan,
    info: &mut SerializeAsEmptyInfo,
) -> Result<(), SbroadError> {
    if let Some(vtables_map) = sub_plan.get_mut_vtables() {
        let unused_motions = std::mem::take(&mut info.unused_motions);
        for motion_id in &unused_motions {
            let Some(use_count) = info.motions_ref_count.get_mut(motion_id) else {
                return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                    "no ref count for motion={motion_id:?}"
                )));
            };
            if *use_count > 1 {
                *use_count -= 1;
            } else {
                vtables_map.remove(motion_id);
            }
        }
    }

    for top_id in &info.top_motion_ids {
        sub_plan.unlink_motion_subtree(*top_id)?;
    }
    Ok(())
}

fn disable_serialize_as_empty_opcode(
    sub_plan: &mut ExecutionPlan,
    info: &SerializeAsEmptyInfo,
) -> Result<(), SbroadError> {
    for motion_id in &info.target_motion_ids {
        let program = if let MutRelational::Motion(Motion {
            policy, program, ..
        }) = sub_plan
            .get_mut_ir_plan()
            .get_mut_relation_node(*motion_id)?
        {
            if !matches!(policy, MotionPolicy::Local) {
                continue;
            }
            program
        } else {
            return Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!("expected motion node on id {motion_id:?}")),
            ));
        };
        for op in &mut program.0 {
            if let MotionOpcode::SerializeAsEmptyTable(enabled) = op {
                *enabled = false;
            }
        }
    }

    Ok(())
}

/// Map between replicaset uuid and the set of buckets (their ids) which correspond to that replicaset.
/// This set is defined by vshard `router.route` function call. See `group_buckets_by_replicasets`
/// function for more details.
pub(crate) type GroupedBuckets = HashMap<String, Vec<u64>>;

/// Function that transforms `Buckets` (set of bucket_ids)
/// into vector of replicasets where those buckets reside
/// according to router cache.
///
/// Rust binding to Lua `get_replicasets_from_buckets` function.
fn lua_get_replicasets_from_buckets(
    buckets: &Buckets,
    tier_name: Option<&SmolStr>,
) -> Result<Vec<RSName>, SbroadError> {
    if let Buckets::All = buckets {
        return Ok(Vec::new());
    }
    let lua_buckets: Vec<u64> = match buckets {
        Buckets::Any => {
            return Err(SbroadError::Unsupported(
                Entity::Buckets,
                Some("grouping buckets is not supported for Buckets::Any".into()),
            ))
        }
        Buckets::All => return Ok(Vec::new()),
        Buckets::Filtered(list) => list.iter().copied().collect(),
    };

    let lua = tarantool::lua_state();

    let Some(func): Option<LuaFunction<_>> = lua.get("get_replicasets_from_buckets") else {
        return Err(SbroadError::Other(
            "no function 'get_replicasets_from_buckets'".into(),
        ));
    };

    let tier_name = tier_name.map(SmolStr::to_string);

    let res: Vec<RSName> = match func.call_with_args((lua_buckets, tier_name)) {
        Ok(v) => v,
        Err(e) => {
            error!(
                Option::from("get buckets from replicasets"),
                &format!("{e:?}")
            );
            return Err(SbroadError::LuaError(format_smolstr!("{e:?}")));
        }
    };

    Ok(res)
}

/// Function that transforms `Buckets` (set of bucket_ids)
/// into `GroupedBuckets` (map from replicaset uuid to set of bucket_ids).
///
/// Rust binding to Lua `group_buckets_by_replicasets` function.
fn group(
    buckets: &HashSet<u64, RepeatableState>,
    tier_name: Option<&SmolStr>,
) -> Result<GroupedBuckets, SbroadError> {
    let lua_buckets: Vec<u64> = buckets.iter().copied().collect();
    let tier_name = tier_name.map(SmolStr::to_string);

    let lua = tarantool::lua_state();

    let fn_group: LuaFunction<_> = lua.get("group_buckets_by_replicasets").ok_or_else(|| {
        SbroadError::LuaError("Lua function `group_buckets_by_replicasets` not found".into())
    })?;

    let res: GroupedBuckets = match fn_group.call_with_args((lua_buckets, tier_name)) {
        Ok(v) => v,
        Err(e) => {
            error!(Option::from("buckets group"), &format!("{e:?}"));
            return Err(SbroadError::LuaError(format_smolstr!("{e:?}")));
        }
    };

    Ok(res)
}

pub fn get_random_bucket(runtime: &impl Vshard) -> Buckets {
    let mut rng = thread_rng();
    let bucket_id: u64 = rng.gen_range(1..=runtime.bucket_count());
    let bucket_set: HashSet<u64, RepeatableState> = HashSet::from_iter(vec![bucket_id]);
    Buckets::Filtered(bucket_set)
}
