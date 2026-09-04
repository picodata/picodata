use crate::explain::buckets::{buckets_repr, BoundedBuckets};
use crate::explain::ir::LogicalExplain;
use pretty_assertions::assert_eq;
use sql_ir::ir::bucket::BucketSet;
use sql_ir::ir::bucket::Buckets;
use sql_ir::ir::helpers::RepeatableState;
use std::collections::HashSet;

use sql_executor::test_helpers::sql_to_optimized_ir;
use sql_ir::collection;

#[test]
fn simple_query_without_cond_plan() {
    let query =
        r#"SELECT "t"."identification_number" as "c1", "product_code" FROM "hash_testing" as "t""#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = LogicalExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r"
    projection (t.identification_number::int -> c1, t.product_code::string -> product_code)
      scan hash_testing -> t
    ");
}

#[test]
fn simple_query_with_cond_plan() {
    let query = r#"SELECT "t"."identification_number" as "c1", "product_code" FROM "hash_testing" as "t" WHERE "t"."identification_number" = 1 AND "t"."product_code" = '222'"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = LogicalExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r"
    projection (t.identification_number::int -> c1, t.product_code::string -> product_code)
      selection ((t.identification_number::int = 1::int and t.product_code::string = '222'::string))
        scan hash_testing -> t
    ");
}

#[test]
fn union_query_plan() {
    let query = r#"SELECT "t"."identification_number" as "c1", "product_code" FROM "hash_testing" as "t"
        UNION ALL
        SELECT "t2"."identification_number", "product_code" FROM "hash_testing_hist" as "t2""#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = LogicalExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r"
    union all
      projection (t.identification_number::int -> c1, t.product_code::string -> product_code)
        scan hash_testing -> t
      projection (t2.identification_number::int -> identification_number, t2.product_code::string -> product_code)
        scan hash_testing_hist -> t2
    ");
}

#[test]
fn union_subquery_plan() {
    let query = r#"SELECT * FROM (
SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "sys_op" > 0 and "sysFrom" < 0
UNION ALL
SELECT "id", "FIRST_NAME" FROM "test_space_hist" WHERE "sys_op" < 0
) as "t"
WHERE "id" = 1"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = LogicalExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r#"
    projection (t.id::int -> id, t."FIRST_NAME"::string -> "FIRST_NAME")
      selection (t.id::int = 1::int)
        scan t
          union all
            projection (test_space.id::int -> id, test_space."FIRST_NAME"::string -> "FIRST_NAME")
              selection ((test_space.sys_op::int > 0::int and test_space."sysFrom"::int < 0::int))
                scan test_space
            projection (test_space_hist.id::int -> id, test_space_hist."FIRST_NAME"::string -> "FIRST_NAME")
              selection (test_space_hist.sys_op::int < 0::int)
                scan test_space_hist
    "#);
}

#[test]
fn union_cond_subquery_plan() {
    let query = r#"SELECT * FROM (
SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "sys_op" > 0 and "sysFrom" < 0
UNION ALL
SELECT "id", "FIRST_NAME" FROM "test_space_hist" WHERE "sys_op" < 0
) as "t"
WHERE "id" IN (SELECT "id"
   FROM (
      SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "sys_op" > 0
      UNION ALL
      SELECT "id", "FIRST_NAME" FROM "test_space_hist" WHERE "sys_op" < 0
  ) as "t2"
  WHERE "t2"."id" = 4)
"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = LogicalExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r#"
    projection (t.id::int -> id, t."FIRST_NAME"::string -> "FIRST_NAME")
      selection (t.id::int in ROW($0))
        scan t
          union all
            projection (test_space.id::int -> id, test_space."FIRST_NAME"::string -> "FIRST_NAME")
              selection ((test_space.sys_op::int > 0::int and test_space."sysFrom"::int < 0::int))
                scan test_space
            projection (test_space_hist.id::int -> id, test_space_hist."FIRST_NAME"::string -> "FIRST_NAME")
              selection (test_space_hist.sys_op::int < 0::int)
                scan test_space_hist
    subquery $0:
      scan
        projection (t2.id::int -> id)
          selection (t2.id::int = 4::int)
            scan t2
              union all
                projection (test_space.id::int -> id, test_space."FIRST_NAME"::string -> "FIRST_NAME")
                  selection (test_space.sys_op::int > 0::int)
                    scan test_space
                projection (test_space_hist.id::int -> id, test_space_hist."FIRST_NAME"::string -> "FIRST_NAME")
                  selection (test_space_hist.sys_op::int < 0::int)
                    scan test_space_hist
    "#);
}

#[test]
fn explain_except1() {
    let query = r#"SELECT "product_code" as "pc" FROM "hash_testing" AS "t"
        EXCEPT DISTINCT
        SELECT "identification_number"::text FROM "hash_testing_hist""#;

    let plan = sql_to_optimized_ir(query, vec![]);
    let top = &plan.get_top().unwrap();
    let explain_tree = LogicalExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r"
    except
      projection (t.product_code::string -> pc)
        scan hash_testing -> t
      motion [policy: full, program: ReshardIfNeeded]
        projection (hash_testing_hist.identification_number::int::string -> col_1)
          scan hash_testing_hist
    ");
}

#[test]
fn motion_subquery_plan() {
    let query = r#"
    SELECT * FROM (
        SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "sys_op" > 0 and "sysFrom" < 0
        UNION ALL
        SELECT "id", "FIRST_NAME" FROM "test_space_hist" WHERE "sys_op" < 0
    ) as "t"
    WHERE
    "id" IN (SELECT "id"
        FROM (
            SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "sys_op" > 0
            UNION ALL
            SELECT "id", "FIRST_NAME" FROM "test_space_hist" WHERE "sys_op" < 0
        ) as "t2"
        WHERE "t2"."id" = 4)
    OR "id" IN (SELECT "identification_number"
        FROM "hash_testing"
        WHERE "identification_number" = 5 AND "product_code" = '123'
        )
"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = LogicalExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r#"
    projection (t.id::int -> id, t."FIRST_NAME"::string -> "FIRST_NAME")
      selection (t.id::int in ROW($1) or t.id::int in ROW($0))
        scan t
          union all
            projection (test_space.id::int -> id, test_space."FIRST_NAME"::string -> "FIRST_NAME")
              selection ((test_space.sys_op::int > 0::int and test_space."sysFrom"::int < 0::int))
                scan test_space
            projection (test_space_hist.id::int -> id, test_space_hist."FIRST_NAME"::string -> "FIRST_NAME")
              selection (test_space_hist.sys_op::int < 0::int)
                scan test_space_hist
    subquery $0:
      motion [policy: segment([ref(identification_number)]), program: ReshardIfNeeded]
        scan
          projection (hash_testing.identification_number::int -> identification_number)
            selection ((hash_testing.identification_number::int = 5::int and hash_testing.product_code::string = '123'::string))
              scan hash_testing
    subquery $1:
      scan
        projection (t2.id::int -> id)
          selection (t2.id::int = 4::int)
            scan t2
              union all
                projection (test_space.id::int -> id, test_space."FIRST_NAME"::string -> "FIRST_NAME")
                  selection (test_space.sys_op::int > 0::int)
                    scan test_space
                projection (test_space_hist.id::int -> id, test_space_hist."FIRST_NAME"::string -> "FIRST_NAME")
                  selection (test_space_hist.sys_op::int < 0::int)
                    scan test_space_hist
    "#);
}

#[test]
fn motion_join_plan() {
    let query = r#"SELECT "t1"."FIRST_NAME"
FROM (SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "id" = 3) as "t1"
    JOIN (SELECT "identification_number", "product_code" FROM "hash_testing") as "t2" ON "t1"."id"="t2"."identification_number"
WHERE "t2"."product_code" = '123'"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = LogicalExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r#"
    projection (t1."FIRST_NAME"::string -> "FIRST_NAME")
      selection (t2.product_code::string = '123'::string)
        join on (t1.id::int = t2.identification_number::int)
          scan t1
            projection (test_space.id::int -> id, test_space."FIRST_NAME"::string -> "FIRST_NAME")
              selection (test_space.id::int = 3::int)
                scan test_space
          motion [policy: segment([ref(identification_number)]), program: ReshardIfNeeded]
            scan t2
              projection (hash_testing.identification_number::int -> identification_number, hash_testing.product_code::string -> product_code)
                scan hash_testing
    "#);
}

#[test]
fn sq_join_plan() {
    let query = r#"SELECT "t1"."FIRST_NAME"
FROM (SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "id" = 3) as "t1"
    JOIN "hash_testing" ON "t1"."id"=(SELECT "identification_number" FROM "hash_testing")"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = LogicalExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r#"
    projection (t1."FIRST_NAME"::string -> "FIRST_NAME")
      join on (t1.id::int = ROW($0))
        scan t1
          projection (test_space.id::int -> id, test_space."FIRST_NAME"::string -> "FIRST_NAME")
            selection (test_space.id::int = 3::int)
              scan test_space
        motion [policy: full, program: ReshardIfNeeded]
          projection (hash_testing.identification_number::int -> identification_number, hash_testing.product_code::string -> product_code, hash_testing.product_units::bool -> product_units, hash_testing.sys_op::int -> sys_op, hash_testing.bucket_id::int -> bucket_id)
            scan hash_testing
    subquery $0:
      motion [policy: segment([ref(identification_number)]), program: ReshardIfNeeded]
        scan
          projection (hash_testing.identification_number::int -> identification_number)
            scan hash_testing
    "#);
}

#[test]
fn unary_condition_plan() {
    let query = r#"SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "id" IS NULL and "FIRST_NAME" IS NOT NULL"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = LogicalExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r#"
    projection (test_space.id::int -> id, test_space."FIRST_NAME"::string -> "FIRST_NAME")
      selection ((test_space.id::int is null and not test_space."FIRST_NAME"::string is null))
        scan test_space
    "#);
}

#[test]
fn insert_plan() {
    let query = r#"INSERT INTO "test_space" ("id", "FIRST_NAME") VALUES (1, '123')"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = LogicalExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r#"
    insert into test_space on conflict: fail
      motion [policy: segment([ref("COLUMN_1")]), program: ReshardIfNeeded]
        values
          value ROW(1::int, '123'::string)
    "#);
}

#[test]
fn multiply_insert_plan() {
    let query = r#"INSERT INTO "test_space" ("id", "FIRST_NAME") VALUES (1, '123'), (2, '456'), (3, '789')"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = LogicalExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r#"
    insert into test_space on conflict: fail
      motion [policy: segment([ref("COLUMN_1")]), program: ReshardIfNeeded]
        values
          value ROW(1::int, '123'::string)
          value ROW(2::int, '456'::string)
          value ROW(3::int, '789'::string)
    "#);
}

#[test]
fn insert_select_plan() {
    let query = r#"INSERT INTO "test_space" ("id", "FIRST_NAME")
SELECT "identification_number", "product_code" FROM "hash_testing""#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = LogicalExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r"
    insert into test_space on conflict: fail
      motion [policy: segment([ref(identification_number)]), program: ReshardIfNeeded]
        projection (hash_testing.identification_number::int -> identification_number, hash_testing.product_code::string -> product_code)
          scan hash_testing
    ");
}

#[test]
fn select_value_plan() {
    let query = r#"select * from (values (1))"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = LogicalExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r#"
    projection (unnamed_subquery."COLUMN_1"::int -> "COLUMN_1")
      scan unnamed_subquery
        motion [policy: full, program: ReshardIfNeeded]
          values
            value ROW(1::int)
    "#);
}

#[test]
fn select_cast_plan1() {
    let query = r#"SELECT CAST("id" as int) as "b" FROM "test_space""#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = LogicalExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r"
    projection (test_space.id::int::int -> b)
      scan test_space
    ");
}

#[test]
fn select_cast_plan2() {
    let query = r#"SELECT "id", "FIRST_NAME" FROM "test_space" WHERE CAST("id" as int) = 1"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = LogicalExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r#"
    projection (test_space.id::int -> id, test_space."FIRST_NAME"::string -> "FIRST_NAME")
      selection (test_space.id::int::int = 1::int)
        scan test_space
    "#);
}

#[test]
fn select_cast_plan_nested() {
    let query = r#"SELECT cast(trim("id"::text) as string) FROM "test_space""#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = LogicalExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r"
    projection (TRIM(test_space.id::int::string)::string -> col_1)
      scan test_space
    ");
}

#[test]
fn select_cast_plan_nested_where() {
    let query = r#"SELECT "id" FROM "test_space" WHERE cast(trim("id"::text) as string) = '1'"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = LogicalExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r"
    projection (test_space.id::int -> id)
      selection (TRIM(test_space.id::int::string)::string = '1'::string)
        scan test_space
    ");
}

#[test]
fn select_cast_plan_nested_where2() {
    let query = r#"SELECT "id" FROM "test_space" WHERE trim(cast(42 as string)) = '1'"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = LogicalExplain::new(&plan, *top).unwrap();

    insta::assert_snapshot!(explain_tree.to_string(), @r"
    projection (test_space.id::int -> id)
      selection (TRIM(42::int::string) = '1'::string)
        scan test_space
    ");
}

#[test]
fn check_buckets_repr() {
    let bc = 3000;
    assert_eq!("[1-3000]", buckets_repr(&Buckets::All, bc, false));
    assert_eq!("any", buckets_repr(&Buckets::Any, bc, false));
    assert_eq!(
        "[1-3]",
        buckets_repr(
            &Buckets::Filtered(BucketSet::Exact(collection!(1, 2, 3))),
            bc,
            false
        )
    );
    assert_eq!(
        "[1-3]",
        buckets_repr(
            &Buckets::Filtered(BucketSet::Exact(collection!(3, 2, 1))),
            bc,
            false
        )
    );
    assert_eq!(
        "[1, 2]",
        buckets_repr(
            &Buckets::Filtered(BucketSet::Exact(collection!(1, 2))),
            bc,
            false
        )
    );
    assert_eq!(
        "[1, 10, 11, 21-23]",
        buckets_repr(
            &Buckets::Filtered(BucketSet::Exact(collection!(1, 10, 11, 23, 22, 21))),
            bc,
            false
        )
    );
    assert_eq!(
        "[]",
        buckets_repr(
            &Buckets::Filtered(BucketSet::Exact(collection!())),
            bc,
            false
        )
    );
}

#[test]
fn check_buckets_repr_fmt() {
    let bc = 3000;
    assert_eq!("[1-3000]", buckets_repr(&Buckets::All, bc, true));
    assert_eq!("any", buckets_repr(&Buckets::Any, bc, true));
    assert_eq!(
        "[]",
        buckets_repr(
            &Buckets::Filtered(BucketSet::Exact(collection!())),
            bc,
            true
        )
    );

    // Lists that fit into a single line are left as is.
    let eight = collection!(219, 626, 799, 1410, 1860, 1934, 1958, 2564);
    assert_eq!(
        "[219, 626, 799, 1410, 1860, 1934, 1958, 2564]",
        buckets_repr(&Buckets::Filtered(BucketSet::Exact(eight)), bc, true)
    );

    // Narrow ranges keep filling the line as long as they fit into it.
    let twelve: HashSet<u64, RepeatableState> = (0..12).map(|i| i * 2 + 1).collect();
    assert_eq!(
        "[1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23]",
        buckets_repr(&Buckets::Filtered(BucketSet::Exact(twelve)), bc, true)
    );

    // Longer lists are split so that each line fills the format width.
    let many: HashSet<u64, RepeatableState> = (1..=23).map(|i| i * 100).collect();
    insta::assert_snapshot!(
        buckets_repr(&Buckets::Filtered(BucketSet::Exact(many)), bc, true),
        @r"
    [
      100, 200, 300, 400, 500, 600, 700, 800, 900,
      1000, 1100, 1200, 1300, 1400, 1500, 1600,
      1700, 1800, 1900, 2000, 2100, 2200, 2300
    ]
    "
    );

    // A range is never split across lines.
    let ranges: HashSet<u64, RepeatableState> = (0..12)
        .flat_map(|i| [i * 10, i * 10 + 1, i * 10 + 2])
        .collect();
    insta::assert_snapshot!(
        buckets_repr(&Buckets::Filtered(BucketSet::Exact(ranges)), bc, true),
        @r"
    [
      0-2, 10-12, 20-22, 30-32, 40-42, 50-52,
      60-62, 70-72, 80-82, 90-92, 100-102,
      110-112
    ]
    "
    );
}

/// Wide ranges leave room for fewer items per line than plain ids do.
/// Modelled on a cluster with 30000 buckets.
#[test]
fn check_buckets_repr_fmt_width() {
    let bc = 30000;

    // Ten wide ranges make up 131 characters, so the list is wrapped.
    let ten_wide: HashSet<u64, RepeatableState> = (0..10)
        .flat_map(|i| {
            let base = 10000 + i * 100;
            [base, base + 1, base + 2]
        })
        .collect();
    insta::assert_snapshot!(
        buckets_repr(&Buckets::Filtered(BucketSet::Exact(ten_wide)), bc, true),
        @r"
    [
      10000-10002, 10100-10102, 10200-10202,
      10300-10302, 10400-10402, 10500-10502,
      10600-10602, 10700-10702, 10800-10802,
      10900-10902
    ]
    "
    );

    // Plain five-digit ids are narrow enough to pack more per line.
    let wide_ids: HashSet<u64, RepeatableState> = (0..23).map(|i| 10000 + i * 137).collect();
    insta::assert_snapshot!(
        buckets_repr(&Buckets::Filtered(BucketSet::Exact(wide_ids)), bc, true),
        @r"
    [
      10000, 10137, 10274, 10411, 10548, 10685,
      10822, 10959, 11096, 11233, 11370, 11507,
      11644, 11781, 11918, 12055, 12192, 12329,
      12466, 12603, 12740, 12877, 13014
    ]
    "
    );
}

/// `EXPLAIN (BUCKETS)` output for a set built out of many ranges. A range
/// is wider than a plain id, so fewer of them fit on a line.
#[test]
fn check_buckets_output_many_ranges() {
    let bc = 30000;

    // 50 ranges of three contiguous buckets each.
    let ranges: HashSet<u64, RepeatableState> = (0..50)
        .flat_map(|i| {
            let base = 1000 + i * 100;
            [base, base + 1, base + 2]
        })
        .collect();
    let buckets = Buckets::Filtered(BucketSet::Exact(ranges));

    // Without FMT the whole list stays on one line, however long.
    insta::assert_snapshot!(
        BoundedBuckets::new(buckets.clone(), bc, false),
        @"buckets = [1000-1002, 1100-1102, 1200-1202, 1300-1302, 1400-1402, 1500-1502, 1600-1602, 1700-1702, 1800-1802, 1900-1902, 2000-2002, 2100-2102, 2200-2202, 2300-2302, 2400-2402, 2500-2502, 2600-2602, 2700-2702, 2800-2802, 2900-2902, 3000-3002, 3100-3102, 3200-3202, 3300-3302, 3400-3402, 3500-3502, 3600-3602, 3700-3702, 3800-3802, 3900-3902, 4000-4002, 4100-4102, 4200-4202, 4300-4302, 4400-4402, 4500-4502, 4600-4602, 4700-4702, 4800-4802, 4900-4902, 5000-5002, 5100-5102, 5200-5202, 5300-5302, 5400-5402, 5500-5502, 5600-5602, 5700-5702, 5800-5802, 5900-5902]"
    );

    // With FMT six nine-character ranges fit per line.
    insta::assert_snapshot!(
        BoundedBuckets::new(buckets, bc, true),
        @r"
    buckets = [
      1000-1002, 1100-1102, 1200-1202, 1300-1302,
      1400-1402, 1500-1502, 1600-1602, 1700-1702,
      1800-1802, 1900-1902, 2000-2002, 2100-2102,
      2200-2202, 2300-2302, 2400-2402, 2500-2502,
      2600-2602, 2700-2702, 2800-2802, 2900-2902,
      3000-3002, 3100-3102, 3200-3202, 3300-3302,
      3400-3402, 3500-3502, 3600-3602, 3700-3702,
      3800-3802, 3900-3902, 4000-4002, 4100-4102,
      4200-4202, 4300-4302, 4400-4402, 4500-4502,
      4600-4602, 4700-4702, 4800-4802, 4900-4902,
      5000-5002, 5100-5102, 5200-5202, 5300-5302,
      5400-5402, 5500-5502, 5600-5602, 5700-5702,
      5800-5802, 5900-5902
    ]
    "
    );
}

/// Singletons, adjacent pairs and ranges mixed together. Item widths differ
/// here, so the number packed onto a line varies with what happens to land
/// on it.
#[test]
fn check_buckets_output_mixed_widths() {
    let bc = 30000;

    let mixed: HashSet<u64, RepeatableState> = (0..24)
        .flat_map(|i| match i % 4 {
            0 => vec![100 + i],
            1 => vec![1000 + i * 100, 1001 + i * 100],
            2 => vec![10000 + i * 100, 10001 + i * 100, 10002 + i * 100],
            _ => vec![20000 + i * 7],
        })
        .collect();
    let buckets = Buckets::Filtered(BucketSet::Exact(mixed));

    insta::assert_snapshot!(
        BoundedBuckets::new(buckets.clone(), bc, false),
        @"buckets = [100, 104, 108, 112, 116, 120, 1100, 1101, 1500, 1501, 1900, 1901, 2300, 2301, 2700, 2701, 3100, 3101, 10200-10202, 10600-10602, 11000-11002, 11400-11402, 11800-11802, 12200-12202, 20021, 20049, 20077, 20105, 20133, 20161]"
    );

    // Every line is packed up to the format width.
    insta::assert_snapshot!(
        BoundedBuckets::new(buckets, bc, true),
        @r"
    buckets = [
      100, 104, 108, 112, 116, 120, 1100, 1101,
      1500, 1501, 1900, 1901, 2300, 2301,
      2700, 2701, 3100, 3101, 10200-10202,
      10600-10602, 11000-11002, 11400-11402,
      11800-11802, 12200-12202, 20021, 20049,
      20077, 20105, 20133, 20161
    ]
    "
    );

    // An upper bound is a single range, so FMT leaves it alone.
    insta::assert_snapshot!(
        BoundedBuckets::new(Buckets::All, bc, true),
        @"buckets <= [1-30000]"
    );
}
