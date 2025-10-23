use crate::sql::lua::{
    bucket_into_rs, escape_bytes, lua_custom_plan_dispatch, lua_decode_rs_ibufs,
    lua_single_plan_dispatch, IbufTable,
};
use ::tarantool::fiber;
use ahash::{AHashMap, AHashSet};
use rmp::decode::{read_array_len, read_bool};
use rmp::encode::{write_array_len, write_uint};
use smol_str::format_smolstr;
use sql::errors::{Action, Entity, SbroadError};
use sql::executor::bucket::Buckets;
use sql::executor::engine::helpers::vshard::prepare_rs_to_ir_map;
use sql::executor::engine::helpers::{
    build_optional_binary, build_required_binary, try_get_metadata_from_plan,
};
use sql::executor::engine::Vshard;
use sql::executor::ir::{ExecutionPlan, QueryType};
use sql::executor::protocol::{Binary, RequiredMessage};
use sql::executor::Port;
use sql_protocol::decode::{execute_read_response, SqlExecute, TupleIter};
use sql_protocol::encode::write_metadata;
use std::collections::HashMap;
use std::io::{Cursor, Error as IoError, Result as IoResult};
use std::rc::Rc;
use std::time::Duration;
use tarantool::time::Instant;
use tarantool::tlua::{self, LuaThread, Push, PushInto};

pub type SqlResult<T> = Result<T, SbroadError>;

pub(crate) fn single_plan_dispatch<'p>(
    port: &mut impl Port<'p>,
    ex_plan: ExecutionPlan,
    buckets: &Buckets,
    timeout: u64,
    tier: Option<&str>,
) -> SqlResult<()> {
    let lua = tarantool::lua_state();
    let replicasets = replicasets_from_buckets(&lua, buckets, tier)?;
    let query_type = ex_plan.query_type()?;
    match &query_type {
        QueryType::DQL => {
            port_write_metadata(port, &ex_plan)?;
            let max_rows = ex_plan.get_sql_motion_row_max();
            single_plan_dispatch_dql(port, &lua, ex_plan, &replicasets, max_rows, timeout, tier)?
        }
        QueryType::DML => {
            single_plan_dispatch_dml(port, &lua, ex_plan, &replicasets, timeout, tier)?
        }
    };
    Ok(())
}

pub(crate) fn custom_plan_dispatch<'p>(
    port: &mut impl Port<'p>,
    runtime: &impl Vshard,
    ex_plan: ExecutionPlan,
    buckets: &Buckets,
    timeout: u64,
    tier: Option<&str>,
) -> SqlResult<()> {
    let lua = tarantool::lua_state();
    let rs_buckets = buckets_by_replicasets(&lua, buckets, runtime.bucket_count(), tier)?;
    if rs_buckets.is_empty() {
        return Err(SbroadError::DispatchError(
            "No replicasets found for the given buckets".into(),
        ));
    }
    let query_type = ex_plan.query_type()?;
    match &query_type {
        QueryType::DQL => {
            // All custom plans must return the same metadata,
            // so we can use the original plan to write it to the port.
            port_write_metadata(port, &ex_plan)?;
            let max_rows = ex_plan.get_sql_motion_row_max();
            custom_plan_dispatch_dql(port, &lua, ex_plan, rs_buckets, max_rows, timeout, tier)?;
        }
        QueryType::DML => {
            custom_plan_dispatch_dml(port, &lua, ex_plan, rs_buckets, timeout, tier)?;
        }
    };
    Ok(())
}

pub(crate) fn port_write_metadata<'p>(
    port: &mut impl Port<'p>,
    ex_plan: &ExecutionPlan,
) -> SqlResult<()> {
    let metadata = try_get_metadata_from_plan(ex_plan)?.ok_or_else(|| {
        SbroadError::FailedTo(
            Action::Get,
            Some(Entity::Query),
            "Failed to get metadata from execution plan".into(),
        )
    })?;
    let length = metadata.len() as u32;
    write_metadata(
        port,
        metadata
            .iter()
            .map(|c| (c.name.as_str(), c.r#type.as_str())),
        length,
    )
    .map_err(|e| SbroadError::DispatchError(format_smolstr!("{e}")))?;
    Ok(())
}

#[derive(PushInto, Push, Debug)]
struct FirstMessage {
    required: Binary,
}

impl FirstMessage {
    fn new(required: Binary) -> Self {
        Self { required }
    }
}

#[derive(PushInto, Push, Debug)]
struct SecondMessage {
    required: Binary,
    optional: Binary,
}

impl SecondMessage {
    fn new(required: Binary, optional: Binary) -> Self {
        Self { required, optional }
    }
}

fn single_plan_dispatch_dql<'lua, 'p>(
    port: &mut impl Port<'p>,
    lua: &'lua LuaThread,
    mut ex_plan: ExecutionPlan,
    replicasets: &[String],
    max_rows: u64,
    timeout: u64,
    tier: Option<&str>,
) -> SqlResult<()> {
    let row_len = row_len(&ex_plan)?;
    let required_message = {
        let required_binary = build_required_binary(&mut ex_plan)?;
        FirstMessage::new(required_binary)
    };
    let deadline = fiber::clock().saturating_add(Duration::from_secs(timeout));
    let lua_table = lua_single_plan_dispatch(lua, &required_message, replicasets, timeout, tier)
        .map_err(|e| SbroadError::DispatchError(format_smolstr!("{e}")))?;
    let timeout = match first_round_process(
        port,
        lua_table,
        replicasets.len(),
        row_len,
        deadline,
        max_rows,
    )? {
        FirstRoundResult::Completed => return Ok(()),
        FirstRoundResult::Missed(timeout) => timeout,
    };
    let full_message = {
        let required_binary = required_message.required;
        let optional_binary = build_optional_binary(ex_plan)?;
        SecondMessage::new(required_binary, optional_binary)
    };
    let lua_table = lua_single_plan_dispatch(lua, full_message, replicasets, timeout, tier)
        .map_err(|e| SbroadError::DispatchError(format_smolstr!("{e}")))?;
    second_round_process(port, lua_table, replicasets.len(), row_len, max_rows)?;
    Ok(())
}

fn custom_plan_dispatch_dql<'lua, 'p>(
    port: &mut impl Port<'p>,
    lua: &'lua LuaThread,
    ex_plan: ExecutionPlan,
    rs_buckets: Vec<(String, Vec<u64>)>,
    max_rows: u64,
    timeout: u64,
    tier: Option<&str>,
) -> SqlResult<()> {
    let row_len = row_len(&ex_plan)?;
    let mut rs_plan = prepare_rs_to_ir_map(&rs_buckets, ex_plan)?;
    let mut first_args = HashMap::with_capacity(rs_plan.len());
    for (rs, ex_plan) in &mut rs_plan {
        let required_binary = build_required_binary(ex_plan)?;
        first_args.insert(rs.clone(), RequiredMessage::from(required_binary));
    }
    let deadline = fiber::clock().saturating_add(Duration::from_secs(timeout));
    let lua_table = lua_custom_plan_dispatch(lua, &first_args, timeout, tier)
        .map_err(|e| SbroadError::DispatchError(format_smolstr!("{e}")))?;
    let timeout = match first_round_process(
        port,
        lua_table,
        first_args.len(),
        row_len,
        deadline,
        max_rows,
    )? {
        FirstRoundResult::Completed => return Ok(()),
        FirstRoundResult::Missed(timeout) => timeout,
    };
    let mut second_args = HashMap::with_capacity(rs_plan.len());
    for (rs, ex_plan) in rs_plan.into_iter() {
        let required_binary = first_args
            .remove(&rs)
            .expect("First round arguments must contain all replicasets")
            .required;
        let optional_binary = build_optional_binary(ex_plan)?;
        let full_message = SecondMessage::new(required_binary, optional_binary);
        second_args.insert(rs, full_message);
    }
    let len = second_args.len();
    let lua_table = lua_custom_plan_dispatch(lua, second_args, timeout, tier)
        .map_err(|e| SbroadError::DispatchError(format_smolstr!("{e}")))?;
    second_round_process(port, lua_table, len, row_len, max_rows)?;
    Ok(())
}

fn row_len(ex_plan: &ExecutionPlan) -> SqlResult<u32> {
    let ir_plan = ex_plan.get_ir_plan();
    let columns_len = ir_plan
        .get_row_list(ir_plan.get_relation_node(ir_plan.get_top()?)?.output())?
        .len();
    let len = u32::try_from(columns_len).map_err(|e| {
        SbroadError::DispatchError(format_smolstr!(
            "Failed to convert columns length {columns_len} to u32: {e}"
        ))
    })?;
    Ok(len)
}

enum FirstRoundResult {
    Completed,
    Missed(u64),
}

fn first_round_process<'lua, 'p>(
    port: &mut impl Port<'p>,
    table: Rc<IbufTable<'lua>>,
    table_len: usize,
    row_len: u32,
    deadline: Instant,
    max_rows: u64,
) -> SqlResult<FirstRoundResult> {
    let mut row_count: u64 = 0;
    let rs_ibufs = lua_decode_rs_ibufs(&table, table_len).map_err(|e| {
        SbroadError::DispatchError(format_smolstr!(
            "Failed to decode ibufs from DQL first round: {e}"
        ))
    })?;

    // First we should check that we don't have any MISS responses.
    // Otherwise we should forget ALL the data in ibufs and re-dispatch
    // to all replicasets. We can't just re-dispatch to the missed replicasets
    // as the buckets may be rebalanced meanwhile.
    for (rs, ibuf) in rs_ibufs.iter() {
        let mp = pcall_mp_process(ibuf.data()?).map_err(|_| {
            SbroadError::DispatchError(format_smolstr!(
                "Remote call on replicaset {rs} returned an error: {}",
                pcall_error(ibuf.data().unwrap_or(&[])),
            ))
        })?;
        let res = execute_read_response(mp).map_err(|e| {
            SbroadError::DispatchError(format_smolstr!(
                "Failed to decode first round response from replicaset {rs}: {e}, msgpack: {}",
                escape_bytes(mp),
            ))
        })?;
        match res {
            SqlExecute::Dql(_) => {}
            SqlExecute::Miss => {
                // We have at least one MISS, so we should re-dispatch.
                let timeout = deadline.duration_since(fiber::clock()).as_secs();
                return Ok(FirstRoundResult::Missed(timeout));
            }
            SqlExecute::Dml(_) => {
                return Err(SbroadError::DispatchError(
                    "Expected DQL or MISS response in the first round, got DML".into(),
                ))
            }
        }
    }

    // Great! All responses are DQL, so we can proceed to writing tuples to port.
    for (rs, ibuf) in rs_ibufs.into_iter() {
        let mp = pcall_mp_process(ibuf.data()?).map_err(|_| {
            SbroadError::DispatchError(format_smolstr!(
                "Remote call on replicaset {rs} returned an error: {}",
                pcall_error(ibuf.data().unwrap_or(&[])),
            ))
        })?;
        let res = execute_read_response(mp).map_err(|e| {
            SbroadError::DispatchError(format_smolstr!(
                "Failed to decode first round response from replicaset {rs}: {e}, msgpack: {}",
                escape_bytes(mp),
            ))
        })?;
        match res {
            SqlExecute::Dql(tuples) => {
                let ibuf_rows = port_write_tuples(port, tuples, max_rows, row_count, row_len, &rs)?;
                row_count += ibuf_rows;
            }
            _ => unreachable!("We have already checked that there are no MISS or DML responses"),
        }
    }
    Ok(FirstRoundResult::Completed)
}

fn second_round_process<'lua, 'p>(
    port: &mut impl Port<'p>,
    table: Rc<IbufTable<'lua>>,
    table_len: usize,
    row_len: u32,
    max_rows: u64,
) -> SqlResult<()> {
    let rs_ibufs = lua_decode_rs_ibufs(&table, table_len).map_err(|e| {
        SbroadError::DispatchError(format_smolstr!(
            "Failed to decode ibufs from DQL second round: {e}"
        ))
    })?;
    let mut row_count: u64 = 0;
    for (rs, ibuf) in rs_ibufs.into_iter() {
        let mp = pcall_mp_process(ibuf.data()?).map_err(|_| {
            SbroadError::DispatchError(format_smolstr!(
                "Remote call on replicaset {rs} returned an error: {}",
                pcall_error(ibuf.data().unwrap_or(&[])),
            ))
        })?;
        let res = execute_read_response(mp).map_err(|e| {
            SbroadError::DispatchError(format_smolstr!(
                "Failed to decode second round response from replicaset {rs}: {e}, msgpack: {}",
                escape_bytes(mp),
            ))
        })?;
        match res {
            SqlExecute::Dql(tuples) => {
                let ibuf_rows = port_write_tuples(port, tuples, max_rows, row_count, row_len, &rs)?;
                row_count += ibuf_rows;
            }
            SqlExecute::Miss => {
                return Err(SbroadError::DispatchError(format_smolstr!(
                    "Expected DQL response from replicaset {rs}, got MISS"
                )))
            }
            SqlExecute::Dml(_) => {
                return Err(SbroadError::DispatchError(format_smolstr!(
                    "Expected DQL response from replicaset {rs}, got DML"
                )))
            }
        }
    }
    Ok(())
}

#[inline(always)]
fn port_write_tuples<'tuples, 'p>(
    port: &mut impl Port<'p>,
    tuples: TupleIter<'tuples>,
    max_rows: u64,
    mut row_count: u64,
    row_len: u32,
    rs: &str,
) -> SqlResult<u64> {
    for mp in tuples {
        let mp = mp.map_err(|e| {
            SbroadError::DispatchError(format_smolstr!(
                "Failed to decode tuple from replicaset {rs}: {e}"
            ))
        })?;
        row_count += 1;
        if max_rows > 0 && row_count > max_rows {
            return Err(SbroadError::DispatchError(format_smolstr!(
                "Exceeded maximum number of rows ({max_rows}) in virtual table: {row_count}"
            )));
        }

        port_append_mp(port, mp, row_len).map_err(|e| {
            SbroadError::DispatchError(format_smolstr!(
                "Failed to append tuple from replicaset {rs} to port: {e}"
            ))
        })?;
    }
    Ok(row_count)
}

fn port_append_mp<'p>(port: &mut impl Port<'p>, mp: &[u8], row_len: u32) -> IoResult<()> {
    let mut cur = Cursor::new(mp);
    let len = read_array_len(&mut cur).map_err(IoError::other)?;
    if len > row_len {
        return Err(IoError::other(format!(
            "Expected array of length at most {row_len}, got {len}",
        )));
    }
    if len < row_len {
        // When msgpack has been formed from Lua dump callback in the
        // executor's port, its last NULLs are omitted. We will need
        // to append nils to the end of the array.
        let extra_nils = row_len - len;
        let mut buf = Vec::with_capacity(5 + mp.len() + extra_nils as usize);
        write_array_len(&mut buf, len + extra_nils).map_err(IoError::other)?;
        buf.extend_from_slice(&mp[cur.position() as usize..]);
        buf.resize(buf.len() + extra_nils as usize, 0xc0); // nils
        port.add_mp(buf.as_slice());
        return Ok(());
    }
    port.add_mp(mp);
    Ok(())
}

fn replicasets_from_buckets(
    lua: &LuaThread,
    buckets: &Buckets,
    tier: Option<&str>,
) -> SqlResult<Vec<String>> {
    let iter = match buckets {
        Buckets::Any => {
            return Err(SbroadError::DispatchError(
                "there is no sense to trnaslate 'any' buckets into replicasets".into(),
            ));
        }
        Buckets::All => return Ok(Vec::new()),
        Buckets::Filtered(list) => list.iter(),
    };
    let mut replicasets: Vec<Rc<String>> = Vec::new();
    // Make sure that only replicasets owns reference to its Rc elements.
    {
        let mut seen: AHashSet<Rc<String>> = AHashSet::new();

        for id in iter {
            let rs: String = bucket_into_rs(lua, *id, tier).map_err(|e| {
                SbroadError::DispatchError(format_smolstr!(
                    "Failed to get replicaset from bucket {id}: {e}"
                ))
            })?;

            if seen.contains(&rs) {
                continue;
            }
            let rc: Rc<String> = Rc::<String>::from(rs);
            seen.insert(rc.clone());
            replicasets.push(rc);
        }
    }

    let replicasets: Vec<String> = replicasets
        .into_iter()
        .map(|rc| Rc::try_unwrap(rc).expect("Extra Rc is alive"))
        .collect();

    Ok(replicasets)
}

fn buckets_by_replicasets(
    lua: &LuaThread,
    buckets: &Buckets,
    max_buckets: u64,
    tier: Option<&str>,
) -> SqlResult<Vec<(String, Vec<u64>)>> {
    enum BucketIter<'a> {
        All(std::ops::RangeInclusive<u64>),
        Filtered(std::collections::hash_set::Iter<'a, u64>),
    }

    impl Iterator for BucketIter<'_> {
        type Item = u64;
        fn next(&mut self) -> Option<u64> {
            match self {
                BucketIter::All(r) => r.next(),
                BucketIter::Filtered(it) => it.next().copied(),
            }
        }
    }

    let iter = match buckets {
        Buckets::Any => {
            return Err(SbroadError::DispatchError(
                "there is no sense to group 'any' buckets by replicasets".into(),
            ));
        }
        Buckets::All => BucketIter::All(1..=max_buckets),
        Buckets::Filtered(list) => BucketIter::Filtered(list.iter()),
    };
    let mut map: AHashMap<String, Vec<u64>> = AHashMap::new();
    for id in iter {
        let rs: String = bucket_into_rs(lua, id, tier).map_err(|e| {
            SbroadError::DispatchError(format_smolstr!(
                "Failed to get replicaset from bucket {id}: {e}"
            ))
        })?;
        map.entry(rs).or_default().push(id);
    }
    Ok(map.into_iter().collect())
}

fn single_plan_dispatch_dml<'lua, 'p>(
    port: &mut impl Port<'p>,
    lua: &'lua LuaThread,
    mut ex_plan: ExecutionPlan,
    replicasets: &[String],
    timeout: u64,
    tier: Option<&str>,
) -> SqlResult<()> {
    let required_binary = build_required_binary(&mut ex_plan)?;
    let optional_binary = build_optional_binary(ex_plan)?;
    let message = SecondMessage::new(required_binary, optional_binary);
    let lua_table = lua_single_plan_dispatch(lua, message, replicasets, timeout, tier)
        .map_err(|e| SbroadError::DispatchError(format_smolstr!("{e}")))?;
    // TODO: all buckets will allocate nothing, because it is empty
    dml_process(port, lua_table, replicasets.len())?;
    Ok(())
}

fn custom_plan_dispatch_dml<'lua, 'p>(
    port: &mut impl Port<'p>,
    lua: &'lua LuaThread,
    ex_plan: ExecutionPlan,
    rs_buckets: Vec<(String, Vec<u64>)>,
    timeout: u64,
    tier: Option<&str>,
) -> SqlResult<()> {
    let rs_plan = prepare_rs_to_ir_map(&rs_buckets, ex_plan)?;
    let mut args = HashMap::with_capacity(rs_plan.len());
    for (rs, mut ex_plan) in rs_plan {
        let required_binary = build_required_binary(&mut ex_plan)?;
        let optional_binary = build_optional_binary(ex_plan)?;
        let message = SecondMessage::new(required_binary, optional_binary);
        args.insert(rs, message);
    }
    let len = args.len();
    let lua_table = lua_custom_plan_dispatch(lua, args, timeout, tier)
        .map_err(|e| SbroadError::DispatchError(format_smolstr!("{e}")))?;
    dml_process(port, lua_table, len)?;
    Ok(())
}

fn dml_process<'lua, 'p>(
    port: &mut impl Port<'p>,
    table: Rc<IbufTable<'lua>>,
    length: usize,
) -> SqlResult<()> {
    let rs_ibufs = lua_decode_rs_ibufs(&table, length).map_err(|e| {
        SbroadError::DispatchError(format_smolstr!(
            "Failed to decode DML response from Lua: {e}"
        ))
    })?;
    let mut row_count = 0;
    for (rs, ibuf) in rs_ibufs.into_iter() {
        let mp = pcall_mp_process(ibuf.data()?).map_err(|_| {
            SbroadError::DispatchError(format_smolstr!(
                "Remote call on replicaset {rs} returned an error: {}",
                pcall_error(ibuf.data().unwrap_or(&[])),
            ))
        })?;
        let res = execute_read_response(mp).map_err(|e| {
            SbroadError::DispatchError(format_smolstr!(
                "Failed to decode DML response from replicaset {rs}: {e}, msgpack: {}",
                escape_bytes(mp),
            ))
        })?;
        match res {
            SqlExecute::Dql(_) => {
                return Err(SbroadError::DispatchError(format_smolstr!(
                    "Expected DML response from replicaset {rs}, got DQL"
                )))
            }
            SqlExecute::Miss => {
                return Err(SbroadError::DispatchError(format_smolstr!(
                    "Expected DML response from replicaset {rs}, got MISS"
                )))
            }
            SqlExecute::Dml(changed) => {
                row_count += changed;
            }
        }
    }
    let mut mp = [0_u8; 9];
    let pos = {
        let mut cur = Cursor::new(&mut mp[..]);
        write_uint(&mut cur, row_count).map_err(|e| {
            SbroadError::DispatchError(format_smolstr!("Failed to encode affected row count: {e}"))
        })?;
        cur.position() as usize
    };
    port.add_mp(&mp[..pos]);
    Ok(())
}

fn pcall_mp_process(mp: &[u8]) -> IoResult<&[u8]> {
    let mut cur = Cursor::new(mp);
    let len = read_array_len(&mut cur).map_err(IoError::other)?;
    if len != 2 {
        return Err(IoError::other(format!(
            "Expected array of length 2 from pcall result, got {len}",
        )));
    }
    let is_ok = read_bool(&mut cur).map_err(IoError::other)?;
    if !is_ok {
        return Err(IoError::other("Lua pcall returned an error"));
    }
    Ok(&mp[cur.position() as usize..])
}

fn pcall_error(mp: &[u8]) -> String {
    match msgpack_decode(mp) {
        Ok(s) => s,
        Err(_) => escape_bytes(mp).to_string(),
    }
}

fn msgpack_decode(bytes: &[u8]) -> Result<String, String> {
    let mut cur = Cursor::new(bytes);
    let v: rmpv::Value = rmpv::decode::read_value(&mut cur).map_err(|e| format!("{e}"))?;
    serde_json::to_string(&v).map_err(|e| format!("{e}"))
}
