use super::{explain, explain_with_params};
use sql_ir::ir::value::Value;

// Select.

#[test]
fn select_columns() {
    let sql = r#"explain (logical) SELECT "t"."identification_number" as "c1", "product_code" FROM "hash_testing" as "t""#;
    insta::assert_snapshot!(explain(sql), @r"
    projection (t.identification_number::int -> c1, t.product_code::string -> product_code)
      scan hash_testing -> t
    ");
}

#[test]
fn select_with_filter() {
    let sql = r#"explain (logical) SELECT "t"."identification_number" as "c1", "product_code" FROM "hash_testing" as "t" WHERE "t"."identification_number" = 1 AND "t"."product_code" = '222'"#;
    insta::assert_snapshot!(explain(sql), @r"
    projection (t.identification_number::int -> c1, t.product_code::string -> product_code)
      selection ((t.identification_number::int = 1::int and t.product_code::string = '222'::string))
        scan hash_testing -> t
    ");
}

#[test]
fn select_with_null_checks() {
    let sql = r#"explain (logical) SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "id" IS NULL and "FIRST_NAME" IS NOT NULL"#;
    insta::assert_snapshot!(explain(sql), @r#"
    projection (test_space.id::int -> id, test_space."FIRST_NAME"::string -> "FIRST_NAME")
      selection ((test_space.id::int is null and not test_space."FIRST_NAME"::string is null))
        scan test_space
    "#);
}

#[test]
fn select_from_values() {
    let sql = r#"explain (logical) select * from (values (1))"#;
    insta::assert_snapshot!(explain(sql), @r#"
    projection (unnamed_subquery."COLUMN_1"::int -> "COLUMN_1")
      scan unnamed_subquery
        motion [policy: full, program: ReshardIfNeeded]
          values
            value ROW(1::int)
    "#);
}

#[test]
fn join_values() {
    let sql = "explain (logical) select * from (values (1, 2), (3, 4)) join (values (5, 6), (7, 8)) on true";
    insta::assert_snapshot!(explain(sql), @r#"
    projection (unnamed_subquery."COLUMN_1"::int -> "COLUMN_1", unnamed_subquery."COLUMN_2"::int -> "COLUMN_2", unnamed_subquery_1."COLUMN_1"::int -> "COLUMN_1", unnamed_subquery_1."COLUMN_2"::int -> "COLUMN_2")
      join on (true::bool)
        scan unnamed_subquery
          motion [policy: full, program: ReshardIfNeeded]
            values
              value ROW(1::int, 2::int)
              value ROW(3::int, 4::int)
        scan unnamed_subquery_1
          motion [policy: full, program: ReshardIfNeeded]
            values
              value ROW(5::int, 6::int)
              value ROW(7::int, 8::int)
    "#);
}

// Set operations.

#[test]
fn union_all_in_subquery() {
    let sql = r#"explain (logical) SELECT * FROM (
SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "sys_op" > 0 and "sysFrom" < 0
UNION ALL
SELECT "id", "FIRST_NAME" FROM "test_space_hist" WHERE "sys_op" < 0
) as "t"
WHERE "id" = 1"#;
    insta::assert_snapshot!(explain(sql), @r#"
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
fn union_all_with_in_subquery() {
    let sql = r#"explain (logical) SELECT * FROM (
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
    insta::assert_snapshot!(explain(sql), @r#"
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
fn except() {
    let sql = r#"explain (logical) SELECT "product_code" as "pc" FROM "hash_testing" AS "t"
        EXCEPT DISTINCT
        SELECT "identification_number"::text FROM "hash_testing_hist""#;
    insta::assert_snapshot!(explain(sql), @r"
    except
      projection (t.product_code::string -> pc)
        scan hash_testing -> t
      motion [policy: full, program: ReshardIfNeeded]
        projection (hash_testing_hist.identification_number::int::string -> col_1)
          scan hash_testing_hist
    ");
}

// Joins and subqueries.

#[test]
fn join_subquery() {
    let sql = r#"explain (logical) select "a" from "t3" as "q1"
        inner join (select "t3"."a" as "a2", "t3"."b" as "b2" from "t3") as "q2"
        on "q1"."a" = "q2"."a2""#;
    insta::assert_snapshot!(explain(sql), @r"
    projection (q1.a::string -> a)
      join on (q1.a::string = q2.a2::string)
        scan t3 -> q1
        scan q2
          projection (t3.a::string -> a2, t3.b::int -> b2)
            scan t3
    ");
}

#[test]
fn self_join() {
    let sql = r#"explain (logical) select "q2"."a" from "t3" as "q1"
        inner join "t3" as "q2"
        on "q1"."a" = "q2"."a""#;
    insta::assert_snapshot!(explain(sql), @r"
    projection (q2.a::string -> a)
      join on (q1.a::string = q2.a::string)
        scan t3 -> q1
        scan t3 -> q2
    ");
}

#[test]
fn join_with_segment_motion() {
    let sql = r#"explain (logical) SELECT "t1"."FIRST_NAME"
FROM (SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "id" = 3) as "t1"
    JOIN (SELECT "identification_number", "product_code" FROM "hash_testing") as "t2" ON "t1"."id"="t2"."identification_number"
WHERE "t2"."product_code" = '123'"#;
    insta::assert_snapshot!(explain(sql), @r#"
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
fn join_on_scalar_subquery() {
    let sql = r#"explain (logical) SELECT "t1"."FIRST_NAME"
FROM (SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "id" = 3) as "t1"
    JOIN "hash_testing" ON "t1"."id"=(SELECT "identification_number" FROM "hash_testing")"#;
    insta::assert_snapshot!(explain(sql), @r#"
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
fn in_subqueries_with_motions() {
    let sql = r#"explain (logical)
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
    insta::assert_snapshot!(explain(sql), @r#"
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

// DML.

#[test]
fn insert_values() {
    let sql =
        r#"explain (logical) INSERT INTO "test_space" ("id", "FIRST_NAME") VALUES (1, '123')"#;
    insta::assert_snapshot!(explain(sql), @r#"
    insert into test_space on conflict: fail
      motion [policy: segment([ref("COLUMN_1")]), program: ReshardIfNeeded]
        values
          value ROW(1::int, '123'::string)
    "#);
}

#[test]
fn insert_multiple_rows() {
    let sql = r#"explain (logical) INSERT INTO "test_space" ("id", "FIRST_NAME") VALUES (1, '123'), (2, '456'), (3, '789')"#;
    insta::assert_snapshot!(explain(sql), @r#"
    insert into test_space on conflict: fail
      motion [policy: segment([ref("COLUMN_1")]), program: ReshardIfNeeded]
        values
          value ROW(1::int, '123'::string)
          value ROW(2::int, '456'::string)
          value ROW(3::int, '789'::string)
    "#);
}

#[test]
fn insert_select() {
    let sql = r#"explain (logical) INSERT INTO "test_space" ("id", "FIRST_NAME")
SELECT "identification_number", "product_code" FROM "hash_testing""#;
    insta::assert_snapshot!(explain(sql), @r"
    insert into test_space on conflict: fail
      motion [policy: segment([ref(identification_number)]), program: ReshardIfNeeded]
        projection (hash_testing.identification_number::int -> identification_number, hash_testing.product_code::string -> product_code)
          scan hash_testing
    ");
}

#[test]
fn delete_all() {
    let sql = r#"explain (logical) DELETE FROM "t1""#;
    insta::assert_snapshot!(explain(sql), @"delete from t1");
}

#[test]
fn delete_with_filter() {
    let sql = r#"explain (logical) DELETE FROM "t1" where "b" > 3"#;
    insta::assert_snapshot!(explain(sql), @r"
    delete from t1
      motion [policy: local, program: [PrimaryKey(0, 1), ReshardIfNeeded]]
        projection (t1.a::string -> pk_col_0, t1.b::int -> pk_col_1)
          selection (t1.b::int > 3::int)
            scan t1
    ");
}

#[test]
fn delete_with_in_subquery() {
    let sql = r#"explain (logical) DELETE FROM "t1" where "a" in (SELECT "b"::text from "t1")"#;
    insta::assert_snapshot!(explain(sql), @r"
    delete from t1
      motion [policy: local, program: [PrimaryKey(0, 1), ReshardIfNeeded]]
        projection (t1.a::string -> pk_col_0, t1.b::int -> pk_col_1)
          selection (t1.a::string in ROW($0))
            scan t1
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t1.b::int::string -> col_1)
            scan t1
    ");
}

// Casts.

#[test]
fn cast_in_projection() {
    let sql = r#"explain (logical) SELECT CAST("id" as int) as "b" FROM "test_space""#;
    insta::assert_snapshot!(explain(sql), @r"
    projection (test_space.id::int::int -> b)
      scan test_space
    ");
}

#[test]
fn cast_in_filter() {
    let sql = r#"explain (logical) SELECT "id", "FIRST_NAME" FROM "test_space" WHERE CAST("id" as int) = 1"#;
    insta::assert_snapshot!(explain(sql), @r#"
    projection (test_space.id::int -> id, test_space."FIRST_NAME"::string -> "FIRST_NAME")
      selection (test_space.id::int::int = 1::int)
        scan test_space
    "#);
}

#[test]
fn nested_cast_in_projection() {
    let sql = r#"explain (logical) SELECT cast(trim("id"::text) as string) FROM "test_space""#;
    insta::assert_snapshot!(explain(sql), @r"
    projection (TRIM(test_space.id::int::string)::string -> col_1)
      scan test_space
    ");
}

#[test]
fn nested_cast_in_filter() {
    let sql = r#"explain (logical) SELECT "id" FROM "test_space" WHERE cast(trim("id"::text) as string) = '1'"#;
    insta::assert_snapshot!(explain(sql), @r"
    projection (test_space.id::int -> id)
      selection (TRIM(test_space.id::int::string)::string = '1'::string)
        scan test_space
    ");
}

#[test]
fn cast_of_constant_in_filter() {
    let sql =
        r#"explain (logical) SELECT "id" FROM "test_space" WHERE trim(cast(42 as string)) = '1'"#;
    insta::assert_snapshot!(explain(sql), @r"
    projection (test_space.id::int -> id)
      selection (TRIM(42::int::string) = '1'::string)
        scan test_space
    ");
}

#[test]
fn constant_casts_folded_in_values() {
    let sql = r#"explain (logical) SELECT * FROM (VALUES (1::int, 2::decimal::integer, 'txt'::text::text::text))"#;
    insta::assert_snapshot!(explain(sql), @r#"
    projection (unnamed_subquery."COLUMN_1"::int -> "COLUMN_1", unnamed_subquery."COLUMN_2"::int -> "COLUMN_2", unnamed_subquery."COLUMN_3"::string -> "COLUMN_3")
      scan unnamed_subquery
        motion [policy: full, program: ReshardIfNeeded]
          values
            value ROW(1::int, 2::int, 'txt'::string)
    "#);
}

#[test]
fn concat_constants() {
    let sql = r#"explain (logical) SELECT CAST('1' as string) || 'hello' FROM "t1""#;
    insta::assert_snapshot!(explain(sql), @r"
    projection ('1'::string || 'hello'::string -> col_1)
      scan t1
    ");
}

#[test]
fn concat_chain_in_filter() {
    let sql =
        r#"explain (logical) SELECT "a" FROM "t1" WHERE CAST('1' as string) || "a" || '2' = '42'"#;
    insta::assert_snapshot!(explain(sql), @r"
    projection (t1.a::string -> a)
      selection (('1'::string || t1.a::string::string)::string || '2'::string = '42'::string)
        scan t1
    ");
}

/// Operands that are already casted to text don't get a second cast on top.
#[test]
fn concat_explicitly_casted_args() {
    let sql = r#"explain (logical) SELECT "a"::text || "b"::text FROM "t1""#;
    insta::assert_snapshot!(explain(sql), @r"
    projection (t1.a::string::string || t1.b::int::string -> col_1)
      scan t1
    ");
}

/// A cast to a non-text type still has to be casted to text.
#[test]
fn concat_arg_casted_to_non_text() {
    let sql = r#"explain (logical) SELECT "a" || "b"::int FROM "t1""#;
    insta::assert_snapshot!(explain(sql), @r"
    projection (t1.a::string::string || t1.b::int::int::string -> col_1)
      scan t1
    ");
}

// Aggregates.

#[test]
fn count_has_local_and_final_stages() {
    let sql = r#"explain (logical) select count(*) from t2"#;
    insta::assert_snapshot!(explain(sql), @r"
    projection (sum(count_1::int)::int -> col_1)
      motion [policy: full, program: ReshardIfNeeded]
        projection (count(*)::int -> count_1)
          scan t2
    ");
}

#[test]
fn sum_of_decimal() {
    let sql = r#"explain (logical) select sum(1.0)"#;
    insta::assert_snapshot!(explain(sql), @"projection (sum(1.0::decimal)::decimal -> col_1)");
}

#[test]
fn sum_of_int() {
    let sql = r#"explain (logical) select sum(1)"#;
    insta::assert_snapshot!(explain(sql), @"projection (sum(1::int)::decimal -> col_1)");
}

#[test]
fn sum_of_double() {
    let sql = r#"explain (logical) select sum(1::double)"#;
    insta::assert_snapshot!(explain(sql), @"projection (sum(1::double)::double -> col_1)");
}

#[test]
fn avg_of_decimal() {
    let sql = r#"explain (logical) select avg(1.0)"#;
    insta::assert_snapshot!(explain(sql), @"projection (avg(1.0::decimal)::decimal -> col_1)");
}

#[test]
fn avg_of_int() {
    let sql = r#"explain (logical) select avg(1)"#;
    insta::assert_snapshot!(explain(sql), @"projection (avg(1::int)::decimal -> col_1)");
}

#[test]
fn avg_of_double() {
    let sql = r#"explain (logical) select avg(1::double)"#;
    insta::assert_snapshot!(explain(sql), @"projection (avg(1::double)::double -> col_1)");
}

// Query OPTION clause.

#[test]
fn option_clause_with_literals() {
    let sql = "explain (logical) select 1 option (sql_vdbe_opcode_max = 1, sql_motion_row_max = 2)";
    insta::assert_snapshot!(explain(sql), @"projection (1::int -> col_1)");
}

#[test]
fn option_clause_with_parameters() {
    let sql =
        "explain (logical) select 1 option (sql_vdbe_opcode_max = $1, sql_motion_row_max = $2)";
    let params = vec![Value::Integer(14), Value::Integer(88)];
    insta::assert_snapshot!(explain_with_params(sql, params), @"projection (1::int -> col_1)");
}

#[test]
fn option_clause_in_dml() {
    let sql = "explain (logical) update t set c = 1 option (sql_vdbe_opcode_max = 1, sql_motion_row_max = 2)";
    insta::assert_snapshot!(explain(sql), @r"
    update t (c = col_0)
      motion [policy: local, program: ReshardIfNeeded]
        projection (1::int -> col_0, t.b::int -> col_1)
          scan t
    ");
}
