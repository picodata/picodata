use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use sbroad::executor::engine::mock::{RouterConfigurationMock, RouterRuntimeMock};
use sbroad::frontend::sql::ast::AbstractSyntaxTree;
use sbroad::frontend::Ast;

use pest::Parser;
use pest_derive::Parser;
use sbroad::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use sbroad::executor::Query;
use sbroad::ir::tree::Snapshot;
use sbroad::ir::value::Value;
use sbroad::ir::Plan;

#[derive(Parser)]
#[grammar = "../sbroad-core/src/frontend/sql/query.pest"]
struct ParseTree;

#[allow(clippy::too_many_lines)]
fn get_query_with_many_references() -> &'static str {
    r#"SELECT
    *
    FROM
        (
            SELECT
                "vehicleguid",
                "reestrid",
                "reestrstatus",
                "vehicleregno",
                "vehiclevin",
                "vehiclevin2",
                "vehiclechassisnum",
                "vehiclereleaseyear",
                "operationregdoctypename",
                "operationregdoc",
                "operationregdocissuedate",
                "operationregdoccomments",
                "vehicleptstypename",
                "vehicleptsnum",
                "vehicleptsissuedate",
                "vehicleptsissuer",
                "vehicleptscomments",
                "vehiclebodycolor",
                "vehiclebrand",
                "vehiclemodel",
                "vehiclebrandmodel",
                "vehiclebodynum",
                "vehiclecost",
                "vehiclegasequip",
                "vehicleproducername",
                "vehiclegrossmass",
                "vehiclemass",
                "vehiclesteeringwheeltypeid",
                "vehiclekpptype",
                "vehicletransmissiontype",
                "vehicletypename",
                "vehiclecategory",
                "vehicletypeunit",
                "vehicleecoclass",
                "vehiclespecfuncname",
                "vehicleenclosedvolume",
                "vehicleenginemodel",
                "vehicleenginenum",
                "vehicleenginepower",
                "vehicleenginepowerkw",
                "vehicleenginetype",
                "holdrestrictiondate",
                "approvalnum",
                "approvaldate",
                "approvaltype",
                "utilizationfeename",
                "customsdoc",
                "customsdocdate",
                "customsdocissue",
                "customsdocrestriction",
                "customscountryremovalid",
                "customscountryremovalname",
                "ownerorgname",
                "ownerinn",
                "ownerogrn",
                "ownerkpp",
                "ownerpersonlastname",
                "ownerpersonfirstname",
                "ownerpersonmiddlename",
                "ownerpersonbirthdate",
                "ownerbirthplace",
                "ownerpersonogrnip",
                "owneraddressindex",
                "owneraddressmundistrict",
                "owneraddresssettlement",
                "owneraddressstreet",
                "ownerpersoninn",
                "ownerpersondoccode",
                "ownerpersondocnum",
                "ownerpersondocdate",
                "operationname",
                "operationdate",
                "operationdepartmentname",
                "operationattorney",
                "operationlising",
                "holdertypeid",
                "holderpersondoccode",
                "holderpersondocnum",
                "holderpersondocdate",
                "holderpersondocissuer",
                "holderpersonlastname",
                "holderpersonfirstname",
                "holderpersonmiddlename",
                "holderpersonbirthdate",
                "holderpersonbirthregionid",
                "holderpersonsex",
                "holderpersonbirthplace",
                "holderpersoninn",
                "holderpersonsnils",
                "holderpersonogrnip",
                "holderaddressguid",
                "holderaddressregionid",
                "holderaddressregionname",
                "holderaddressdistrict",
                "holderaddressmundistrict",
                "holderaddresssettlement",
                "holderaddressstreet",
                "holderaddressbuilding",
                "holderaddressstructureid",
                "holderaddressstructurename",
                "holderaddressstructure"
            FROM
                "test__gibdd_db__vehicle_reg_and_res100_history"
            WHERE
                "sys_from" <= 332
                AND "sys_to" >= 332
            UNION
            ALL
            SELECT
                "vehicleguid",
                "reestrid",
                "reestrstatus",
                "vehicleregno",
                "vehiclevin",
                "vehiclevin2",
                "vehiclechassisnum",
                "vehiclereleaseyear",
                "operationregdoctypename",
                "operationregdoc",
                "operationregdocissuedate",
                "operationregdoccomments",
                "vehicleptstypename",
                "vehicleptsnum",
                "vehicleptsissuedate",
                "vehicleptsissuer",
                "vehicleptscomments",
                "vehiclebodycolor",
                "vehiclebrand",
                "vehiclemodel",
                "vehiclebrandmodel",
                "vehiclebodynum",
                "vehiclecost",
                "vehiclegasequip",
                "vehicleproducername",
                "vehiclegrossmass",
                "vehiclemass",
                "vehiclesteeringwheeltypeid",
                "vehiclekpptype",
                "vehicletransmissiontype",
                "vehicletypename",
                "vehiclecategory",
                "vehicletypeunit",
                "vehicleecoclass",
                "vehiclespecfuncname",
                "vehicleenclosedvolume",
                "vehicleenginemodel",
                "vehicleenginenum",
                "vehicleenginepower",
                "vehicleenginepowerkw",
                "vehicleenginetype",
                "holdrestrictiondate",
                "approvalnum",
                "approvaldate",
                "approvaltype",
                "utilizationfeename",
                "customsdoc",
                "customsdocdate",
                "customsdocissue",
                "customsdocrestriction",
                "customscountryremovalid",
                "customscountryremovalname",
                "ownerorgname",
                "ownerinn",
                "ownerogrn",
                "ownerkpp",
                "ownerpersonlastname",
                "ownerpersonfirstname",
                "ownerpersonmiddlename",
                "ownerpersonbirthdate",
                "ownerbirthplace",
                "ownerpersonogrnip",
                "owneraddressindex",
                "owneraddressmundistrict",
                "owneraddresssettlement",
                "owneraddressstreet",
                "ownerpersoninn",
                "ownerpersondoccode",
                "ownerpersondocnum",
                "ownerpersondocdate",
                "operationname",
                "operationdate",
                "operationdepartmentname",
                "operationattorney",
                "operationlising",
                "holdertypeid",
                "holderpersondoccode",
                "holderpersondocnum",
                "holderpersondocdate",
                "holderpersondocissuer",
                "holderpersonlastname",
                "holderpersonfirstname",
                "holderpersonmiddlename",
                "holderpersonbirthdate",
                "holderpersonbirthregionid",
                "holderpersonsex",
                "holderpersonbirthplace",
                "holderpersoninn",
                "holderpersonsnils",
                "holderpersonogrnip",
                "holderaddressguid",
                "holderaddressregionid",
                "holderaddressregionname",
                "holderaddressdistrict",
                "holderaddressmundistrict",
                "holderaddresssettlement",
                "holderaddressstreet",
                "holderaddressbuilding",
                "holderaddressstructureid",
                "holderaddressstructurename",
                "holderaddressstructure"
            FROM
                "test__gibdd_db__vehicle_reg_and_res100_actual"
            WHERE
                "sys_from" <= 1
        ) AS "t3"
    WHERE
        "reestrid" = ?"#
}

/// Note: every target query contains single parameter.
fn get_target_queries() -> Vec<&'static str> {
    vec![
        r#"
        SELECT *
FROM
    (SELECT "id", "sysFrom", "sys_op"
    FROM "test_space"
    WHERE "sysFrom" <= 1 AND "sys_op" >= 1
    UNION ALL
    SELECT "id", "sysFrom", "sys_op"
    FROM "test_space"
    WHERE "sysFrom" <= 1) AS "t3"
WHERE "id" = 1
        AND ("sysFrom" = ?
        AND "sys_op" > 1)
        "#,
        r#"
        SELECT *
FROM
    (SELECT "id", "sysFrom", "sys_op"
    FROM "test_space"
    WHERE "sysFrom" <= 1 AND "sys_op" >= 1
    UNION ALL
    SELECT "id", "sysFrom", "sys_op"
    FROM "test_space"
    WHERE "sysFrom" <= 1) AS "t3"
WHERE ("id" = 1
        OR ("id" = 1
        OR "id" = 1))
        AND (("sysFrom" = 1
        OR "sysFrom" = 1)
        AND "sys_op" > ?)
        "#,
        r#"
        SELECT *
FROM
    (SELECT "id", "sysFrom", "sys_op"
    FROM "test_space"
    WHERE "sysFrom" <= 1 AND "sys_op" >= 1
    UNION ALL
    SELECT "id", "sysFrom", "sys_op"
    FROM "test_space"
    WHERE "sysFrom" <= 1) AS "t3"
INNER JOIN
    (SELECT "identification_number", "sys_op", "product_code"
    FROM "hash_testing_hist"
    WHERE "identification_number" <= 0 AND "sys_op" >= 0
    UNION ALL
    SELECT "identification_number", "sys_op", "product_code"
    FROM "hash_testing_hist"
    WHERE "identification_number" <= 0) AS "t8"
ON "t3"."id" = "t8"."identification_number"
WHERE "t3"."sysFrom" = 1 AND "t3"."sys_op" = 2
AND ("t8"."sys_op" = ? AND ("t8"."identification_number" = 2 AND "t3"."sys_op" > 0))
        "#,
    ]
}

fn bench_pure_pest_parsing(crit: &mut Criterion) {
    let mut group = crit.benchmark_group("pure_pest_parsing");

    let many_references_query = get_query_with_many_references();
    group
        .throughput(Throughput::Bytes(many_references_query.len() as _))
        .bench_with_input("many_references", many_references_query, |b, query| {
            b.iter_with_large_drop(|| ParseTree::parse(Rule::Command, query).unwrap());
        });

    let target_queries = get_target_queries();
    for (index, target_query) in target_queries.iter().enumerate() {
        group
            .throughput(Throughput::Bytes(target_query.len() as _))
            .bench_with_input(
                BenchmarkId::new("target_query", index),
                target_query,
                |b, query| {
                    b.iter_with_large_drop(|| ParseTree::parse(Rule::Command, query).unwrap());
                },
            );
    }
}

fn parse(pattern: &str) -> Plan {
    let metadata = &RouterConfigurationMock::new();
    AbstractSyntaxTree::transform_into_plan(pattern, &[], metadata).unwrap()
}

fn bench_full_parsing(crit: &mut Criterion) {
    let mut group = crit.benchmark_group("full_parsing");

    let many_references_query = get_query_with_many_references();
    group
        .throughput(Throughput::Bytes(many_references_query.len() as _))
        .bench_with_input("many_references", many_references_query, |b, query| {
            b.iter_with_large_drop(|| parse(query));
        });

    let target_queries = get_target_queries();
    for (index, target_query) in target_queries.iter().enumerate() {
        group
            .throughput(Throughput::Bytes(target_query.len() as _))
            .bench_with_input(
                BenchmarkId::new("target_query", index),
                target_query,
                |b, query| {
                    b.iter_with_large_drop(|| parse(query));
                },
            );
    }
}

fn bench_take_subtree(crit: &mut Criterion) {
    let engine = RouterRuntimeMock::new();
    let param: u64 = 42;
    let params = vec![Value::from(param)];

    let target_query = get_query_with_many_references();
    let mut query = Query::new(&engine, target_query, params).unwrap();

    let plan = query.get_exec_plan().get_ir_plan();
    let top_id = plan.get_top().unwrap();

    crit.bench_function("getting_subtree", |b| {
        b.iter_with_large_drop(|| {
            black_box(&mut query)
                .get_mut_exec_plan()
                .take_subtree(black_box(top_id))
                .unwrap()
        })
    });
}

fn bench_serde_clone(crit: &mut Criterion) {
    let engine = RouterRuntimeMock::new();
    let param: u64 = 42;
    let params = vec![Value::from(param)];

    let target_query = get_query_with_many_references();
    let query = Query::new(&engine, target_query, params).unwrap();

    let plan = query.get_exec_plan().get_ir_plan();

    crit.bench_function("serializing_plan_many_references", |b| {
        b.iter_with_large_drop(|| bincode::serialize(black_box(plan)).unwrap())
    });

    let ser_bytes: &[u8] = &bincode::serialize(plan).unwrap();
    crit.bench_function("deserializing_plan_many_references", |b| {
        b.iter_with_large_drop(|| bincode::deserialize::<Plan>(black_box(ser_bytes)).unwrap())
    });

    crit.bench_function("cloning_plan_many_references", |b| {
        b.iter_with_large_drop(|| black_box(plan).clone())
    });
}

fn build_ir(pattern: &str, params: Vec<Value>, engine: &mut RouterRuntimeMock) {
    let query = Query::new(engine, pattern, params).unwrap();
    let top_id = query.get_exec_plan().get_ir_plan().get_top().unwrap();
    let plan = query.get_exec_plan();
    let sp = SyntaxPlan::new(plan, top_id, Snapshot::Oldest).unwrap();
    let ordered = OrderedSyntaxNodes::try_from(sp).unwrap();
    let nodes = ordered.to_syntax_data().unwrap();
    plan.to_sql(&nodes, "", None).unwrap();
}

/// Note: it's disabled, because currently one of target queries fails on execution.
fn bench_ir_build(crit: &mut Criterion) {
    let mut engine = RouterRuntimeMock::new();
    let mut param: u64 = 42;

    let target_queries = get_target_queries();
    let mut group = crit.benchmark_group("build_ir");
    for (index, target_query) in target_queries.iter().enumerate() {
        group
            .throughput(Throughput::Bytes(target_query.len() as _))
            .bench_with_input(BenchmarkId::new("query", index), *target_query, |b, i| {
                b.iter(|| {
                    let params = vec![Value::from(param)];
                    param += 1;
                    build_ir(i, params, &mut engine);
                })
            });
    }
}

criterion_group!(
    benches,
    bench_pure_pest_parsing,
    bench_full_parsing,
    bench_serde_clone
);
criterion_main!(benches);
