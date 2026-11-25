//! Encoding and decoding of the router-to-storage protocol messages.
//!
//! `encode/*` builds a message the way the router does: a counting pass to
//! get the exact size, then a pass that writes into a buffer of that size.
//! `decode/*` reads a message the way the storage does and visits each row.
//!
//! Run: cargo bench -p sql-planner --bench protocol

use std::rc::Rc;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use smol_str::SmolStr;
use sql_executor::executor::engine::helpers::{
    init_insert_tuple_builder, materialize_constant_values, vtable_columns,
};
use sql_executor::executor::engine::mock::{PortMocked, RouterRuntimeMock};
use sql_executor::executor::engine::protocol::{
    build_dql_protocol_for_top, DqlProtocol, InsertCoreData, TupleInsertData,
};
use sql_executor::executor::engine::VersionMap;
use sql_executor::executor::vtable::VirtualTable;
use sql_executor::executor::ExecutingQuery;
use sql_executor::test_helpers::ExecutingQueryExt;
use sql_ir::ir::node::relational::Relational;
use sql_ir::ir::node::{Insert, Values};
use sql_ir::ir::relation::{Column, ColumnRole, SpaceEngine, Table};
use sql_ir::ir::types::{DerivedType, UnrestrictedType};
use sql_ir::ir::value::{try_parse_datetime, Value};
use sql_protocol::decode::{ProtocolMessage, ProtocolMessageIter};
use sql_protocol::dml::insert::{write_insert_packet, InsertResult};
use sql_protocol::dql::{write_dql_packet, DQLResult};
use sql_protocol::msgpack::ByteCounter;

/// A router with the table of `benchmark/batched-inserts` as `t`, and a
/// table `s` with the same columns. `bucket_id` follows the primary key,
/// as in a table that `CREATE TABLE` makes.
fn router() -> RouterRuntimeMock {
    let mut router = RouterRuntimeMock::new();
    for (id, name) in [(1025, "t"), (1026, "s")] {
        let column = |name, ty, role, is_nullable| {
            Column::new(name, DerivedType::new(ty), role, is_nullable)
        };
        let columns = vec![
            column("a", UnrestrictedType::Integer, ColumnRole::User, false),
            column(
                "bucket_id",
                UnrestrictedType::Integer,
                ColumnRole::Sharding,
                false,
            ),
            column("b", UnrestrictedType::Datetime, ColumnRole::User, true),
            column("c", UnrestrictedType::String, ColumnRole::User, true),
        ];
        let table = Table::new_sharded(id, name, columns, &["a"], &["a"], SpaceEngine::Memtx);
        router.add_table(table.unwrap());
    }
    router
}

/// Sets the schema versions that `PreparedStatement::parse` takes from a
/// router that provides them. The mock router does not.
fn set_schema_versions(query: &mut ExecutingQuery<RouterRuntimeMock>) {
    let ir = query.get_mut_exec_plan().get_mut_ir_plan();
    let tables: VersionMap = ir.relations.tables.values().map(|t| (t.id, 1)).collect();
    ir.table_version_map = Rc::new(tables);
    for version in Rc::make_mut(&mut ir.index_version_map).values_mut() {
        *version = 1;
    }
}

/// Rows like the ones of `benchmark/batched-inserts`: a key above
/// `u32::MAX` (9 bytes in msgpack, as `random(1, 1e17)` gives), a datetime
/// and a text of 84 characters.
fn rows(count: usize) -> impl Iterator<Item = [Value; 3]> {
    let date = try_parse_datetime("2025-11-27T12:21:32.750190+0300").unwrap();
    (0..count).map(move |i| {
        [
            Value::Integer(10_000_000_000_000_000 + i as i64 * 7_919),
            Value::Datetime(date),
            Value::String("x".repeat(84)),
        ]
    })
}

/// `INSERT INTO t VALUES (?, ?, ?), ...` up to the encoding, as the router
/// prepares it: `materialize_constant_values`, then `materialize_subtree` sets the
/// vtable of the motion and builds its bucket index, then the insert
/// branch of the dispatch code makes the pattern and the core data.
fn insert_data(count: usize) -> TupleInsertData {
    let router = router();
    let sql = format!(
        "INSERT INTO t VALUES {}",
        vec!["(?, ?, ?)"; count].join(", ")
    );
    let params = rows(count).flatten().collect();
    let mut query = ExecutingQuery::from_text_and_params(&router, &sql, params).unwrap();
    set_schema_versions(&mut query);

    let ir = query.get_exec_plan().get_ir_plan();
    let top = ir.get_top().unwrap();
    let Relational::Insert(Insert {
        relation,
        conflict_strategy,
        child,
        ..
    }) = ir.get_relation_node(top).unwrap()
    else {
        panic!("not an insert");
    };
    let (relation, conflict_strategy, motion) =
        (relation.clone(), conflict_strategy.clone(), *child);
    let values = ir.get_motion_child(motion).unwrap();
    let Relational::Values(Values { rows, .. }) = ir.get_relation_node(values).unwrap() else {
        panic!("not VALUES");
    };
    let rows = rows.clone();
    // The mock router returns the vtable of VALUES from its registry.
    let vtable =
        materialize_constant_values(query.get_mut_exec_plan().get_mut_ir_plan(), &rows).unwrap();
    router.add_virtual_table(values, vtable);
    let slices = query.get_exec_plan().get_ir_plan().clone_slices();
    query
        .materialize_subtree(slices, None::<&mut PortMocked>)
        .unwrap();

    let exec_plan = query.get_exec_plan();
    let ir = exec_plan.get_ir_plan();
    let vtable = exec_plan.get_motion_vtable(motion).unwrap();
    let pattern = init_insert_tuple_builder(ir, vtable.get_columns(), top).unwrap();
    let table = ir.relations.get(&relation).unwrap();
    let core = InsertCoreData {
        request_id: SmolStr::new(exec_plan.get_request_id()),
        space_id: table.id,
        space_version: ir.table_version_map[&table.id],
        conflict_policy: (&conflict_strategy).try_into().unwrap(),
    };
    TupleInsertData::new(core, vtable, pattern)
}

/// The two passes of `encode_tuple_with_reservation` in the dispatch code.
/// The router writes into a `TupleBuilder` and then makes a tuple; this
/// copy is not measured here.
fn encode_insert(data: &TupleInsertData) -> Vec<u8> {
    let mut counter = ByteCounter::default();
    write_insert_packet(&mut counter, data).unwrap();
    let mut buf = Vec::with_capacity(counter.bytes());
    write_insert_packet(&mut buf, data).unwrap();
    buf
}

/// A query with one motion, as the router sends it to all the buckets: the
/// subquery `q` comes as a vtable of `count` rows. After
/// `materialize_subtree` come the steps of `ExecutingQuery::dispatch` and
/// `single_plan_dispatch_dql`; `normalize_for_dispatch` does nothing for
/// `Buckets::All`.
fn dql_source(count: usize) -> DqlProtocol {
    let router = router();
    let sql = "SELECT t.a FROM t JOIN (SELECT a, b, c FROM s) AS q ON t.c = q.c WHERE t.a > ?";
    let mut query =
        ExecutingQuery::from_text_and_params(&router, sql, vec![Value::Integer(42)]).unwrap();
    set_schema_versions(&mut query);
    let ir = query.get_exec_plan().get_ir_plan();
    let [slice] = ir.slices().slices() else {
        panic!("one slice expected");
    };
    let [motion] = slice.positions() else {
        panic!("one motion expected");
    };
    let motion = *motion;
    let subtree = ir.get_motion_subtree_root(motion).unwrap();
    let mut vtable = VirtualTable::with_columns(vtable_columns(ir, subtree).unwrap());
    for row in rows(count) {
        vtable.add_tuple(row.to_vec());
    }
    router.add_virtual_table(motion, vtable);
    let slices = query.get_exec_plan().get_ir_plan().clone_slices();
    query
        .materialize_subtree(slices, None::<&mut PortMocked>)
        .unwrap();

    let mut exec_plan = query.get_exec_plan().clone();
    let top = exec_plan.get_ir_plan().get_top().unwrap();
    if let Some(target) = exec_plan.get_plan_id_target().unwrap() {
        exec_plan.set_plan_id(target).unwrap();
    }
    build_dql_protocol_for_top(Rc::new(exec_plan), top, 1).unwrap()
}

/// The same two passes as in `encode_insert`.
fn encode_dql(source: &DqlProtocol) -> Vec<u8> {
    let mut counter = ByteCounter::default();
    write_dql_packet(&mut counter, source).unwrap();
    let mut buf = Vec::with_capacity(counter.bytes());
    write_dql_packet(&mut buf, source).unwrap();
    buf
}

/// Read each field of an insert message and visit each row.
fn decode_insert(bytes: &[u8]) -> usize {
    let msg = ProtocolMessage::decode_from_bytes(bytes).unwrap();
    let ProtocolMessageIter::DmlInsert(iter) = msg.get_iter().unwrap() else {
        panic!("not an insert");
    };
    let mut total = msg.request_id.len();
    for field in iter {
        match field.unwrap() {
            InsertResult::TableId(id) => total += id as usize,
            InsertResult::TableVersion(version) => total += version as usize,
            InsertResult::ConflictPolicy(policy) => total += policy as usize,
            InsertResult::Tuples(tuples) => {
                for tuple in tuples {
                    total += tuple.unwrap().len();
                }
            }
        }
    }
    total
}

/// Read each field of a DQL message and visit each row of each virtual table.
fn decode_dql(bytes: &[u8]) -> usize {
    let msg = ProtocolMessage::decode_from_bytes(bytes).unwrap();
    let ProtocolMessageIter::Dql(iter) = msg.get_iter().unwrap() else {
        panic!("not a DQL");
    };
    let mut total = msg.request_id.len();
    for field in iter {
        match field.unwrap() {
            DQLResult::TableSchemaInfo(map) => {
                for entry in map {
                    total += entry.unwrap().1 as usize;
                }
            }
            DQLResult::IndexSchemaInfo(map) => {
                for entry in map {
                    total += entry.unwrap().1 as usize;
                }
            }
            DQLResult::PlanId(id) => total += (id & 0xff) as usize,
            DQLResult::SenderId(id) => total += id as usize,
            DQLResult::Vtables(vtables) => {
                for entry in vtables {
                    let (name, tuples) = entry.unwrap();
                    total += name.len();
                    for tuple in tuples {
                        total += tuple.unwrap().len();
                    }
                }
            }
            DQLResult::Options(options) => total += options.sql_motion_row_max as usize,
            DQLResult::Params(params) => total += params.len(),
        }
    }
    total
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode");
    for rows in [1, 64, 1000] {
        let data = insert_data(rows);
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_function(BenchmarkId::new("insert", rows), |b| {
            b.iter(|| black_box(encode_insert(black_box(&data))));
        });
    }
    for rows in [0, 200, 2000] {
        let source = dql_source(rows);
        group.throughput(Throughput::Elements(rows.max(1) as u64));
        group.bench_function(BenchmarkId::new("dql", rows), |b| {
            b.iter(|| black_box(encode_dql(black_box(&source))));
        });
    }
    group.finish();

    let mut group = c.benchmark_group("decode");
    for rows in [1, 64, 1000] {
        let bytes = encode_insert(&insert_data(rows));
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_function(BenchmarkId::new("insert", rows), |b| {
            b.iter(|| black_box(decode_insert(black_box(&bytes))));
        });
    }
    for rows in [0, 200, 2000] {
        let bytes = encode_dql(&dql_source(rows));
        group.throughput(Throughput::Elements(rows.max(1) as u64));
        group.bench_function(BenchmarkId::new("dql", rows), |b| {
            b.iter(|| black_box(decode_dql(black_box(&bytes))));
        });
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
