-- TEST: explain
-- SQL:
DROP TABLE IF EXISTS testing_space;
DROP TABLE IF EXISTS testing_space_hist;
DROP TABLE IF EXISTS space_simple_shard_key;
DROP TABLE IF EXISTS space_simple_shard_key_hist;
CREATE TABLE testing_space ("id" int primary key, "name" string, "product_units" int);
CREATE TABLE testing_space_hist ("id" int primary key, "name" string, "product_units" int);
CREATE TABLE space_simple_shard_key ("id" int primary key, "name" string, "sysOp" int);
CREATE TABLE space_simple_shard_key_hist ("id" int primary key, "name" string, "sysOp" int);
CREATE TABLE arithmetic_space (id int primary key, a int, b int, c int, d int, e int, f int, boolean_col bool, string_col string, number_col double);
CREATE TABLE arithmetic_space2 ("id" int primary key, a int, b int, c int, d int, e int, f int, boolean_col bool, string_col string, number_col double);
INSERT INTO "arithmetic_space"
("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
VALUES (1, 1, 1, 1, 1, 1, 1, true, '123', 1);
INSERT INTO "arithmetic_space2"
("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
VALUES (1, 1, 1, 1, 1, 1, 1, false, '123', 1);
INSERT INTO "testing_space" ("id", "name", "product_units") VALUES
            (1, '123', 1);
INSERT INTO "testing_space_hist" ("id", "name", "product_units") VALUES
            (1, '123', 5);
INSERT INTO "space_simple_shard_key" ("id", "name", "sysOp") VALUES (1, 'ok', 1), (10, null, 0);
INSERT INTO "space_simple_shard_key_hist" ("id", "name", "sysOp") VALUES (1, 'ok_hist', 3), (2, 'ok_hist_2', 1);

-- TEST: test_motion_explain
-- SQL:
EXPLAIN SELECT "id", "name" FROM "testing_space"
    WHERE "id" in (SELECT "id" FROM "space_simple_shard_key_hist" WHERE "sysOp" < 0);
-- EXPECTED:
projection ("testing_space"."id"::int -> "id", "testing_space"."name"::string -> "name")
    selection "testing_space"."id"::int in ROW($0)
        scan "testing_space"
subquery $0:
scan
            projection ("space_simple_shard_key_hist"."id"::int -> "id")
                selection "space_simple_shard_key_hist"."sysOp"::int < 0::int
                    scan "space_simple_shard_key_hist"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_join_explain
-- SQL:
EXPLAIN SELECT *
FROM
    (SELECT "id", "name" FROM "space_simple_shard_key" WHERE "sysOp" < 1
     UNION ALL
     SELECT "id", "name" FROM "space_simple_shard_key_hist" WHERE "sysOp" > 0) AS "t3"
INNER JOIN
    (SELECT "id" as "tid"  FROM "testing_space" where "id" <> 1) AS "t8"
    ON "t3"."id" = "t8"."tid"
WHERE "t3"."name" = '123';
-- EXPECTED:
projection ("t3"."id"::int -> "id", "t3"."name"::string -> "name", "t8"."tid"::int -> "tid")
    selection "t3"."name"::string = '123'::string
        join on "t3"."id"::int = "t8"."tid"::int
            scan "t3"
                union all
                    projection ("space_simple_shard_key"."id"::int -> "id", "space_simple_shard_key"."name"::string -> "name")
                        selection "space_simple_shard_key"."sysOp"::int < 1::int
                            scan "space_simple_shard_key"
                    projection ("space_simple_shard_key_hist"."id"::int -> "id", "space_simple_shard_key_hist"."name"::string -> "name")
                        selection "space_simple_shard_key_hist"."sysOp"::int > 0::int
                            scan "space_simple_shard_key_hist"
            scan "t8"
                projection ("testing_space"."id"::int -> "tid")
                    selection "testing_space"."id"::int <> 1::int
                        scan "testing_space"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_valid_explain
-- SQL:
EXPLAIN SELECT * FROM (
            SELECT "id", "name" FROM "space_simple_shard_key" WHERE "sysOp" < 0
            UNION ALL
            SELECT "id", "name" FROM
            "space_simple_shard_key_hist" WHERE "sysOp" > 0
        ) as "t1"
        WHERE "id" = 1;
-- EXPECTED:
projection ("t1"."id"::int -> "id", "t1"."name"::string -> "name")
    selection "t1"."id"::int = 1::int
        scan "t1"
            union all
                projection ("space_simple_shard_key"."id"::int -> "id", "space_simple_shard_key"."name"::string -> "name")
                    selection "space_simple_shard_key"."sysOp"::int < 0::int
                        scan "space_simple_shard_key"
                projection ("space_simple_shard_key_hist"."id"::int -> "id", "space_simple_shard_key_hist"."name"::string -> "name")
                    selection "space_simple_shard_key_hist"."sysOp"::int > 0::int
                        scan "space_simple_shard_key_hist"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1934]

-- TEST: test_explain_arithmetic_selection-1
-- SQL:
EXPLAIN select "id" from "arithmetic_space" where "a" + "b" = "b" + "a";
-- EXPECTED:
projection ("arithmetic_space"."id"::int -> "id")
    selection ("arithmetic_space"."a"::int + "arithmetic_space"."b"::int) = ("arithmetic_space"."b"::int + "arithmetic_space"."a"::int)
        scan "arithmetic_space"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_explain_arithmetic_selection-2
-- SQL:
EXPLAIN select "id" from "arithmetic_space" where "a" + "b" > 0 and "b" * "a" = 5;
-- EXPECTED:
projection ("arithmetic_space"."id"::int -> "id")
    selection (("arithmetic_space"."a"::int + "arithmetic_space"."b"::int) > 0::int) and (("arithmetic_space"."b"::int * "arithmetic_space"."a"::int) = 5::int)
        scan "arithmetic_space"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_explain_arithmetic_selection-3
-- SQL:
EXPLAIN SELECT *
FROM
    (SELECT "id", "a" FROM "arithmetic_space" WHERE "c" < 0
    UNION ALL
    SELECT "id", "a" FROM "arithmetic_space" WHERE "c" > 0) AS "t3"
INNER JOIN
    (SELECT "id" as "id1" FROM "arithmetic_space2" WHERE "c" < 0) AS "t8"
ON "t3"."id" + "t3"."a" * 2 = "t8"."id1" + 4
WHERE "t3"."id" = 2;
-- EXPECTED:
projection ("t3"."id"::int -> "id", "t3"."a"::int -> "a", "t8"."id1"::int -> "id1")
    selection "t3"."id"::int = 2::int
        join on ("t3"."id"::int + ("t3"."a"::int * 2::int)) = ("t8"."id1"::int + 4::int)
            scan "t3"
                union all
                    projection ("arithmetic_space"."id"::int -> "id", "arithmetic_space"."a"::int -> "a")
                        selection "arithmetic_space"."c"::int < 0::int
                            scan "arithmetic_space"
                    projection ("arithmetic_space"."id"::int -> "id", "arithmetic_space"."a"::int -> "a")
                        selection "arithmetic_space"."c"::int > 0::int
                            scan "arithmetic_space"
            motion [policy: full]
                scan "t8"
                    projection ("arithmetic_space2"."id"::int -> "id1")
                        selection "arithmetic_space2"."c"::int < 0::int
                            scan "arithmetic_space2"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_explain_arithmetic_selection-4
-- SQL:
EXPLAIN SELECT *
FROM
    (SELECT "id", "a" FROM "arithmetic_space" WHERE "c" + "a" < 0
    UNION ALL
    SELECT "id", "a" FROM "arithmetic_space" WHERE "c" > 0) AS "t3"
INNER JOIN
    (SELECT "id" as "id1" FROM "arithmetic_space2" WHERE "c" < 0) AS "t8"
ON "t3"."id" + "t3"."a" * 2 = "t8"."id1" + 4
WHERE "t3"."id" = 2;
-- EXPECTED:
projection ("t3"."id"::int -> "id", "t3"."a"::int -> "a", "t8"."id1"::int -> "id1")
    selection "t3"."id"::int = 2::int
        join on ("t3"."id"::int + ("t3"."a"::int * 2::int)) = ("t8"."id1"::int + 4::int)
            scan "t3"
                union all
                    projection ("arithmetic_space"."id"::int -> "id", "arithmetic_space"."a"::int -> "a")
                        selection ("arithmetic_space"."c"::int + "arithmetic_space"."a"::int) < 0::int
                            scan "arithmetic_space"
                    projection ("arithmetic_space"."id"::int -> "id", "arithmetic_space"."a"::int -> "a")
                        selection "arithmetic_space"."c"::int > 0::int
                            scan "arithmetic_space"
            motion [policy: full]
                scan "t8"
                    projection ("arithmetic_space2"."id"::int -> "id1")
                        selection "arithmetic_space2"."c"::int < 0::int
                            scan "arithmetic_space2"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_explain_arithmetic_projection-1
-- SQL:
EXPLAIN select "id" + 2 from "arithmetic_space";
-- EXPECTED:
projection ("arithmetic_space"."id"::int + 2::int -> "col_1")
    scan "arithmetic_space"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_explain_arithmetic_projection-2
-- SQL:
EXPLAIN select "a" + "b" * "c" from "arithmetic_space";
-- EXPECTED:
projection ("arithmetic_space"."a"::int + ("arithmetic_space"."b"::int * "arithmetic_space"."c"::int) -> "col_1")
    scan "arithmetic_space"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_explain_arithmetic_projection-3
-- SQL:
EXPLAIN select ("a" + "b") * "c" from "arithmetic_space";
-- EXPECTED:
projection (("arithmetic_space"."a"::int + "arithmetic_space"."b"::int) * "arithmetic_space"."c"::int -> "col_1")
    scan "arithmetic_space"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_explain_arithmetic_projection-4
-- SQL:
EXPLAIN select "a" > "b" from "arithmetic_space";
-- EXPECTED:
projection ("arithmetic_space"."a"::int > "arithmetic_space"."b"::int -> "col_1")
    scan "arithmetic_space"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_explain_arithmetic_projection-5
-- SQL:
EXPLAIN select "a" is null from "arithmetic_space";
-- EXPECTED:
projection ("arithmetic_space"."a"::int is null -> "col_1")
    scan "arithmetic_space"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_explain_trim-1
-- SQL:
EXPLAIN WITH t(a) AS (SELECT '1') SELECT * FROM t t1 WHERE t1.a = trim('');
-- EXPECTED:
projection ("t1"."a"::string -> "a")
    selection "t1"."a"::string = TRIM(''::string)
        scan cte t1($0)
subquery $0:
projection ("t"."col_1"::string -> "a")
                scan "t"
                    projection ('1'::string -> "col_1")
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = any

-- TEST: test_explain_trim-2
-- SQL:
EXPLAIN SELECT CASE WHEN TRUE THEN '1' ELSE TRIM('2') END;
-- EXPECTED:
projection (case when true::bool then '1'::string else TRIM('2'::string) end -> "col_1")
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = any

-- TEST: test_raw_explain
-- SQL:
EXPLAIN (RAW, FMT) SELECT "id" from testing_space UNION SELECT "id" from testing_space;
-- EXPECTED:
1. Query (STORAGE):
SELECT
  "testing_space"."id"
FROM
  "testing_space"
UNION
SELECT
  "testing_space"."id"
FROM
  "testing_space"
+----------+-------+------+-------------------------------------------------------+
| selectid | order | from | detail                                                |
+=================================================================================+
| 1        | 0     | 0    | SCAN TABLE testing_space (~1048576 rows)              |
|----------+-------+------+-------------------------------------------------------|
| 2        | 0     | 0    | SCAN TABLE testing_space (~1048576 rows)              |
|----------+-------+------+-------------------------------------------------------|
| 0        | 0     | 0    | COMPOUND SUBQUERIES 1 AND 2 USING TEMP B-TREE (UNION) |
+----------+-------+------+-------------------------------------------------------+
''

-- TEST: test_raw_explain-2
-- SQL:
EXPLAIN (RAW) SELECT * from testing_space WHERE "id" = 1;
-- EXPECTED:
1. Query (FILTERED STORAGE):
SELECT "testing_space"."id", "testing_space"."name", "testing_space"."product_units" FROM "testing_space" WHERE "testing_space"."id" = CAST(1 AS int)
+----------+-------+------+--------------------------------------------------------------+
| selectid | order | from | detail                                                       |
+========================================================================================+
| 0        | 0     | 0    | SEARCH TABLE testing_space USING PRIMARY KEY (id=?) (~1 row) |
+----------+-------+------+--------------------------------------------------------------+
''

-- TEST: test_raw_explain-3
-- SQL:
EXPLAIN (RAW) SELECT * from testing_space WHERE "id" = 1 ORDER BY 1 LIMIT 1;
-- EXPECTED:
1. Query (FILTERED STORAGE):
SELECT "testing_space"."id", "testing_space"."name", "testing_space"."product_units" FROM "testing_space" WHERE "testing_space"."id" = CAST(1 AS int)
+----------+-------+------+--------------------------------------------------------------+
| selectid | order | from | detail                                                       |
+========================================================================================+
| 0        | 0     | 0    | SEARCH TABLE testing_space USING PRIMARY KEY (id=?) (~1 row) |
+----------+-------+------+--------------------------------------------------------------+
''
2. Query (ROUTER):
SELECT "COL_0" as "id", "COL_1" as "name", "COL_2" as "product_units" FROM ( SELECT "COL_0", "COL_1", "COL_2" FROM "TMP_2890830351568476674_0136" ) ORDER BY 1 LIMIT 1
+----------+-------+------+---------------------------------------------------------+
| selectid | order | from | detail                                                  |
+===================================================================================+
| 0        | 0     | 0    | SCAN TABLE TMP_2890830351568476674_0136 (~1048576 rows) |
|----------+-------+------+---------------------------------------------------------|
| 0        | 0     | 0    | USE TEMP B-TREE FOR ORDER BY                            |
+----------+-------+------+---------------------------------------------------------+
''

-- TEST: test_raw_explain-4
-- SQL:
EXPLAIN (RAW) SELECT * from testing_space WHERE "id" = 1 GROUP BY 1, 2, 3 ORDER BY 1 LIMIT 1;
-- EXPECTED:
1. Query (FILTERED STORAGE):
SELECT "testing_space"."id" as "gr_expr_1", "testing_space"."name" as "gr_expr_2", "testing_space"."product_units" as "gr_expr_3" FROM "testing_space" WHERE "testing_space"."id" = CAST(1 AS int) GROUP BY "testing_space"."id", "testing_space"."name", "testing_space"."product_units"
+----------+-------+------+--------------------------------------------------------------+
| selectid | order | from | detail                                                       |
+========================================================================================+
| 0        | 0     | 0    | SEARCH TABLE testing_space USING PRIMARY KEY (id=?) (~1 row) |
+----------+-------+------+--------------------------------------------------------------+
''
2. Query (ROUTER):
SELECT "id", "name", "product_units" FROM ( SELECT "COL_0" as "id", "COL_1" as "name", "COL_2" as "product_units" FROM ( SELECT "COL_0", "COL_1", "COL_2" FROM "TMP_16437131905997475581_0136" ) GROUP BY "COL_0", "COL_1", "COL_2" ) ORDER BY 1 LIMIT 1
+----------+-------+------+----------------------------------------------------------+
| selectid | order | from | detail                                                   |
+====================================================================================+
| 0        | 0     | 0    | SCAN TABLE TMP_16437131905997475581_0136 (~1048576 rows) |
|----------+-------+------+----------------------------------------------------------|
| 0        | 0     | 0    | USE TEMP B-TREE FOR GROUP BY                             |
|----------+-------+------+----------------------------------------------------------|
| 0        | 0     | 0    | USE TEMP B-TREE FOR ORDER BY                             |
+----------+-------+------+----------------------------------------------------------+
''

-- TEST: test_raw_explain-5
-- SQL:
EXPLAIN (RAW, FMT) SELECT * from testing_space JOIN testing_space ON true GROUP BY 1, 2, 3, 4, 5, 6 ORDER BY 1 LIMIT 1;
-- EXPECTED:
1. Query (STORAGE):
SELECT
  "testing_space"."id",
  "testing_space"."bucket_id",
  "testing_space"."name",
  "testing_space"."product_units"
FROM
  "testing_space"
+----------+-------+------+------------------------------------------+
| selectid | order | from | detail                                   |
+====================================================================+
| 0        | 0     | 0    | SCAN TABLE testing_space (~1048576 rows) |
+----------+-------+------+------------------------------------------+
''
2. Query (STORAGE):
SELECT
  "testing_space"."id" as "gr_expr_1",
  "testing_space"."name" as "gr_expr_2",
  "testing_space"."product_units" as "gr_expr_3",
  "testing_space"."COL_0" as "gr_expr_4",
  "testing_space"."COL_2" as "gr_expr_5",
  "testing_space"."COL_3" as "gr_expr_6"
FROM
  "testing_space"
  INNER JOIN (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3"
    FROM
      "TMP_12096455301584828679_0136"
  ) as "testing_space" ON CAST(true AS bool)
GROUP BY
  "testing_space"."id",
  "testing_space"."name",
  "testing_space"."product_units",
  "testing_space"."COL_0",
  "testing_space"."COL_2",
  "testing_space"."COL_3"
+----------+-------+------+----------------------------------------------------------+
| selectid | order | from | detail                                                   |
+====================================================================================+
| 0        | 0     | 0    | SCAN TABLE testing_space (~1048576 rows)                 |
|----------+-------+------+----------------------------------------------------------|
| 0        | 1     | 1    | SCAN TABLE TMP_12096455301584828679_0136 (~1048576 rows) |
|----------+-------+------+----------------------------------------------------------|
| 0        | 0     | 0    | USE TEMP B-TREE FOR GROUP BY                             |
+----------+-------+------+----------------------------------------------------------+
''
3. Query (ROUTER):
SELECT
  "id",
  "name",
  "product_units",
  "id",
  "name",
  "product_units"
FROM
  (
    SELECT
      "COL_0" as "id",
      "COL_1" as "name",
      "COL_2" as "product_units",
      "COL_3" as "id",
      "COL_4" as "name",
      "COL_5" as "product_units"
    FROM
      (
        SELECT
          "COL_0",
          "COL_1",
          "COL_2",
          "COL_3",
          "COL_4",
          "COL_5"
        FROM
          "TMP_13902639316269636821_0136"
      )
    GROUP BY
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4",
      "COL_5"
  )
ORDER BY
'  1'
LIMIT
'  1'
+----------+-------+------+----------------------------------------------------------+
| selectid | order | from | detail                                                   |
+====================================================================================+
| 0        | 0     | 0    | SCAN TABLE TMP_13902639316269636821_0136 (~1048576 rows) |
|----------+-------+------+----------------------------------------------------------|
| 0        | 0     | 0    | USE TEMP B-TREE FOR GROUP BY                             |
|----------+-------+------+----------------------------------------------------------|
| 0        | 0     | 0    | USE TEMP B-TREE FOR ORDER BY                             |
+----------+-------+------+----------------------------------------------------------+
''

-- TEST: test_raw_explain-6
-- SQL:
EXPLAIN (RAW) INSERT INTO testing_space VALUES (1, '1', 1);
-- ERROR:
sbroad: unsupported plan: EXPLAIN QUERY PLAN is not supported for DML queries
