use criterion::{black_box, criterion_group, criterion_main, Criterion};
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

fn bench_pure_pest_parsing(c: &mut Criterion) {
    let many_references_query = get_query_with_many_references();
    c.bench_function("pure_pest_parsing_many_references", |b| {
        b.iter(|| black_box(ParseTree::parse(Rule::Command, many_references_query)));
    });

    let target_qureies = get_target_queries();
    for (index, query) in target_qureies.iter().enumerate() {
        let bench_name = format!("pure_pest_parsing_target_query{index}");
        c.bench_function(bench_name.as_str(), |b| {
            b.iter(|| black_box(ParseTree::parse(Rule::Command, query)));
        });
    }
}

fn parse(pattern: &str) -> Plan {
    let metadata = &RouterConfigurationMock::new();
    AbstractSyntaxTree::transform_into_plan(pattern, metadata).unwrap()
}

fn bench_full_parsing(c: &mut Criterion) {
    let many_references_query = get_query_with_many_references();
    c.bench_function("full_parsing_many_references", |b| {
        b.iter(|| black_box(parse(many_references_query)));
    });

    let target_queries = get_target_queries();
    for (index, target_query) in target_queries.iter().enumerate() {
        let bench_name = format!("full_parsing_target_query{index}");
        c.bench_function(bench_name.as_str(), |b| {
            b.iter(|| black_box(parse(target_query)))
        });
    }
}

fn bench_take_subtree(c: &mut Criterion) {
    let mut engine = RouterRuntimeMock::new();
    let param: u64 = 42;
    let params = vec![Value::from(param)];

    let target_query = get_query_with_many_references();
    let mut query = Query::new(&engine, target_query, params).unwrap();

    let plan = query.get_exec_plan().get_ir_plan();
    let top_id = plan.get_top().unwrap();

    let bench_name = "getting_subtree".to_string();
    c.bench_function(bench_name.as_str(), |b| {
        b.iter(|| {
            let _subtree = query.get_mut_exec_plan().take_subtree(top_id).unwrap();
        })
    });
}

fn bench_serde_clone(c: &mut Criterion) {
    let mut engine = RouterRuntimeMock::new();
    let param: u64 = 42;
    let params = vec![Value::from(param)];

    let target_query = get_query_with_many_references();
    let query = Query::new(&engine, target_query, params).unwrap();

    let plan = query.get_exec_plan().get_ir_plan();

    let bench_name = "serializing_plan_many_references".to_string();
    c.bench_function(bench_name.as_str(), |b| {
        b.iter(|| {
            let _ser_bytes = bincode::serialize(plan).unwrap();
        })
    });

    let bench_name = "deserializing_plan_many_references".to_string();
    let ser_bytes: &[u8] = &bincode::serialize(plan).unwrap();
    c.bench_function(bench_name.as_str(), |b| {
        b.iter(|| {
            let _deser_plan: Plan = bincode::deserialize(ser_bytes).unwrap();
        })
    });

    let bench_name = "cloning_plan_many_references".to_string();
    c.bench_function(bench_name.as_str(), |b| {
        b.iter(|| {
            let _new_plan = plan.clone();
        })
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
fn bench_ir_build(c: &mut Criterion) {
    let mut engine = RouterRuntimeMock::new();
    let mut param: u64 = 42;

    let target_queries = get_target_queries();
    for (index, target_query) in target_queries.iter().enumerate() {
        let bench_name = format!("building_ir_target_query{index}");
        c.bench_function(bench_name.as_str(), |b| {
            b.iter(|| {
                let params = vec![Value::from(param)];
                param += 1;
                build_ir(target_query, params, &mut engine);
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
