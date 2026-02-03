-- TEST: explain
-- SQL:
DROP TABLE IF EXISTS testing_space;
DROP TABLE IF EXISTS testing_space_hist;
DROP TABLE IF EXISTS space_simple_shard_key;
DROP TABLE IF EXISTS space_simple_shard_key_hist;
DROP TABLE IF EXISTS testing_space_global;
CREATE TABLE testing_space ("id" int primary key, "name" string, "product_units" int);
CREATE TABLE testing_space_hist ("id" int primary key, "name" string, "product_units" int);
CREATE TABLE space_simple_shard_key ("id" int primary key, "name" string, "sysOp" int);
CREATE TABLE space_simple_shard_key_hist ("id" int primary key, "name" string, "sysOp" int);
CREATE TABLE arithmetic_space (id int primary key, a int, b int, c int, d int, e int, f int, boolean_col bool, string_col string, number_col double);
CREATE TABLE arithmetic_space2 ("id" int primary key, a int, b int, c int, d int, e int, f int, boolean_col bool, string_col string, number_col double);
CREATE TABLE testing_space_global ("id" int primary key, "name" string, "product_units" int) DISTRIBUTED GLOBALLY;
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
            motion [policy: full, program: ReshardIfNeeded]
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
            motion [policy: full, program: ReshardIfNeeded]
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
SELECT "COL_0" as "id", "COL_1" as "name", "COL_2" as "product_units" FROM ( SELECT "COL_0", "COL_1", "COL_2" FROM "TMP_18137326403914987828_0136" ) ORDER BY 1 LIMIT 1
+----------+-------+------+----------------------------------------------------------+
| selectid | order | from | detail                                                   |
+====================================================================================+
| 0        | 0     | 0    | SCAN TABLE TMP_18137326403914987828_0136 (~1048576 rows) |
|----------+-------+------+----------------------------------------------------------|
| 0        | 0     | 0    | USE TEMP B-TREE FOR ORDER BY                             |
+----------+-------+------+----------------------------------------------------------+
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
SELECT "id", "name", "product_units" FROM ( SELECT "COL_0" as "id", "COL_1" as "name", "COL_2" as "product_units" FROM ( SELECT "COL_0", "COL_1", "COL_2" FROM "TMP_5632449124882167792_0136" ) GROUP BY "COL_0", "COL_1", "COL_2" ) ORDER BY 1 LIMIT 1
+----------+-------+------+---------------------------------------------------------+
| selectid | order | from | detail                                                  |
+===================================================================================+
| 0        | 0     | 0    | SCAN TABLE TMP_5632449124882167792_0136 (~1048576 rows) |
|----------+-------+------+---------------------------------------------------------|
| 0        | 0     | 0    | USE TEMP B-TREE FOR GROUP BY                            |
|----------+-------+------+---------------------------------------------------------|
| 0        | 0     | 0    | USE TEMP B-TREE FOR ORDER BY                            |
+----------+-------+------+---------------------------------------------------------+
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
      "TMP_14686513118906948204_0136"
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
| 0        | 1     | 1    | SCAN TABLE TMP_14686513118906948204_0136 (~1048576 rows) |
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
          "TMP_14693860875729634426_0136"
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
| 0        | 0     | 0    | SCAN TABLE TMP_14693860875729634426_0136 (~1048576 rows) |
|----------+-------+------+----------------------------------------------------------|
| 0        | 0     | 0    | USE TEMP B-TREE FOR GROUP BY                             |
|----------+-------+------+----------------------------------------------------------|
| 0        | 0     | 0    | USE TEMP B-TREE FOR ORDER BY                             |
+----------+-------+------+----------------------------------------------------------+
''

-- TEST: test_raw_explain-6
-- SQL:
EXPLAIN (RAW) INSERT INTO testing_space VALUES (1, '1', 1);
-- EXPECTED:
1. Query (ROUTER):
VALUES ( CAST(1 AS int), CAST('1' AS string), CAST(1 AS int) )
+----------+-------+------+--------+
| selectid | order | from | detail |
+==================================+
+----------+-------+------+--------+
''

-- TEST: test_raw_explain-7
-- SQL:
EXPLAIN (RAW) DELETE FROM testing_space WHERE "product_units" < 10 AND "name" = 'beluga';
-- EXPECTED:
1. Query (STORAGE):
SELECT "testing_space"."id" as "pk_col_0" FROM "testing_space" WHERE ( "testing_space"."product_units" < CAST(10 AS int) ) and ("testing_space"."name" = CAST('beluga' AS string))
+----------+-------+------+-----------------------------------------+
| selectid | order | from | detail                                  |
+===================================================================+
| 0        | 0     | 0    | SCAN TABLE testing_space (~262144 rows) |
+----------+-------+------+-----------------------------------------+
''

-- TEST: test_raw_explain-8
-- SQL:
EXPLAIN (RAW) DELETE FROM testing_space WHERE "name" IN (SELECT "name" FROM testing_space_hist WHERE "product_units" > 10);
-- EXPECTED:
1. Query (STORAGE):
SELECT "testing_space_hist"."name" FROM "testing_space_hist" WHERE "testing_space_hist"."product_units" > CAST(10 AS int)
+----------+-------+------+----------------------------------------------+
| selectid | order | from | detail                                       |
+========================================================================+
| 0        | 0     | 0    | SCAN TABLE testing_space_hist (~983040 rows) |
+----------+-------+------+----------------------------------------------+
''
2. Query (STORAGE):
SELECT "testing_space"."id" as "pk_col_0" FROM "testing_space" WHERE "testing_space"."name" in ( SELECT "COL_0" FROM "TMP_14062130367842554903_0136" )
+----------+-------+------+----------------------------------------------------------+
| selectid | order | from | detail                                                   |
+====================================================================================+
| 0        | 0     | 0    | SCAN TABLE testing_space (~983040 rows)                  |
|----------+-------+------+----------------------------------------------------------|
| 0        | 0     | 0    | EXECUTE LIST SUBQUERY 1                                  |
|----------+-------+------+----------------------------------------------------------|
| 1        | 0     | 0    | SCAN TABLE TMP_14062130367842554903_0136 (~1048576 rows) |
+----------+-------+------+----------------------------------------------------------+
''

-- TEST: test_raw_explain-9
-- SQL:
EXPLAIN (RAW) DELETE FROM testing_space;
-- EXPECTED:

-- TEST: test_raw_explain-10
-- SQL:
EXPLAIN (RAW) DELETE FROM testing_space WHERE id IN ( SELECT id FROM testing_space_hist GROUP BY id HAVING SUM(product_units) = 0 );
-- EXPECTED:
1. Query (STORAGE):
SELECT "testing_space_hist"."id" as "gr_expr_1", sum ( CAST ("testing_space_hist"."product_units" as int) ) as "sum_1" FROM "testing_space_hist" GROUP BY "testing_space_hist"."id"
+----------+-------+------+-----------------------------------------------+
| selectid | order | from | detail                                        |
+=========================================================================+
| 0        | 0     | 0    | SCAN TABLE testing_space_hist (~1048576 rows) |
+----------+-------+------+-----------------------------------------------+
''
2. Query (ROUTER):
SELECT "COL_0" as "id" FROM ( SELECT "COL_0", "COL_1" FROM "TMP_17893045443290476123_0136" ) GROUP BY "COL_0" HAVING sum ("COL_1") = CAST(0 AS int)
+----------+-------+------+----------------------------------------------------------+
| selectid | order | from | detail                                                   |
+====================================================================================+
| 0        | 0     | 0    | SCAN TABLE TMP_17893045443290476123_0136 (~1048576 rows) |
|----------+-------+------+----------------------------------------------------------|
| 0        | 0     | 0    | USE TEMP B-TREE FOR GROUP BY                             |
+----------+-------+------+----------------------------------------------------------+
''
3. Query (STORAGE):
SELECT "testing_space"."id" as "pk_col_0" FROM "testing_space" WHERE "testing_space"."id" in ( SELECT "COL_0" FROM "TMP_16911589622850435461_0136" )
+----------+-------+------+----------------------------------------------------------------+
| selectid | order | from | detail                                                         |
+==========================================================================================+
| 0        | 0     | 0    | SEARCH TABLE testing_space USING PRIMARY KEY (id=?) (~24 rows) |
|----------+-------+------+----------------------------------------------------------------|
| 0        | 0     | 0    | EXECUTE LIST SUBQUERY 1                                        |
|----------+-------+------+----------------------------------------------------------------|
| 1        | 0     | 0    | SCAN TABLE TMP_16911589622850435461_0136 (~1048576 rows)       |
+----------+-------+------+----------------------------------------------------------------+
''

-- TEST: test_raw_explain-11
-- SQL:
EXPLAIN (RAW) UPDATE testing_space SET product_units = 0 WHERE id IN ( SELECT id FROM testing_space_hist WHERE product_units < 0 );
-- EXPECTED:
1. Query (STORAGE):
SELECT CAST(0 AS int) as "col_0", "testing_space"."id" as "col_1" FROM "testing_space" WHERE "testing_space"."id" in ( SELECT "testing_space_hist"."id" FROM "testing_space_hist" WHERE "testing_space_hist"."product_units" < CAST(0 AS int) )
+----------+-------+------+----------------------------------------------------------------+
| selectid | order | from | detail                                                         |
+==========================================================================================+
| 0        | 0     | 0    | SEARCH TABLE testing_space USING PRIMARY KEY (id=?) (~24 rows) |
|----------+-------+------+----------------------------------------------------------------|
| 0        | 0     | 0    | EXECUTE LIST SUBQUERY 1                                        |
|----------+-------+------+----------------------------------------------------------------|
| 1        | 0     | 0    | SCAN TABLE testing_space_hist (~983040 rows)                   |
+----------+-------+------+----------------------------------------------------------------+
''

-- TEST: test_raw_explain-12
-- SQL:
EXPLAIN (RAW) UPDATE testing_space SET product_units = -1 WHERE id IN ( SELECT id FROM testing_space_hist GROUP BY id HAVING SUM(product_units) = 0 );
-- EXPECTED:
1. Query (STORAGE):
SELECT "testing_space_hist"."id" as "gr_expr_1", sum ( CAST ("testing_space_hist"."product_units" as int) ) as "sum_1" FROM "testing_space_hist" GROUP BY "testing_space_hist"."id"
+----------+-------+------+-----------------------------------------------+
| selectid | order | from | detail                                        |
+=========================================================================+
| 0        | 0     | 0    | SCAN TABLE testing_space_hist (~1048576 rows) |
+----------+-------+------+-----------------------------------------------+
''
2. Query (ROUTER):
SELECT "COL_0" as "id" FROM ( SELECT "COL_0", "COL_1" FROM "TMP_17893045443290476123_0136" ) GROUP BY "COL_0" HAVING sum ("COL_1") = CAST(0 AS int)
+----------+-------+------+----------------------------------------------------------+
| selectid | order | from | detail                                                   |
+====================================================================================+
| 0        | 0     | 0    | SCAN TABLE TMP_17893045443290476123_0136 (~1048576 rows) |
|----------+-------+------+----------------------------------------------------------|
| 0        | 0     | 0    | USE TEMP B-TREE FOR GROUP BY                             |
+----------+-------+------+----------------------------------------------------------+
''
3. Query (STORAGE):
SELECT CAST(-1 AS int) as "col_0", "testing_space"."id" as "col_1" FROM "testing_space" WHERE "testing_space"."id" in ( SELECT "COL_0" FROM "TMP_3985030934273899384_0136" )
+----------+-------+------+----------------------------------------------------------------+
| selectid | order | from | detail                                                         |
+==========================================================================================+
| 0        | 0     | 0    | SEARCH TABLE testing_space USING PRIMARY KEY (id=?) (~24 rows) |
|----------+-------+------+----------------------------------------------------------------|
| 0        | 0     | 0    | EXECUTE LIST SUBQUERY 1                                        |
|----------+-------+------+----------------------------------------------------------------|
| 1        | 0     | 0    | SCAN TABLE TMP_3985030934273899384_0136 (~1048576 rows)        |
+----------+-------+------+----------------------------------------------------------------+
''

-- TEST: test_raw_explain-13
-- SQL:
EXPLAIN (RAW) DELETE FROM testing_space WHERE id IN (10, 15, 42);
-- EXPECTED:
1. Query (FILTERED STORAGE):
SELECT "testing_space"."id" as "pk_col_0" FROM "testing_space" WHERE "testing_space"."id" in ( CAST(10 AS int), CAST(15 AS int), CAST(42 AS int) )
+----------+-------+------+---------------------------------------------------------------+
| selectid | order | from | detail                                                        |
+=========================================================================================+
| 0        | 0     | 0    | SEARCH TABLE testing_space USING PRIMARY KEY (id=?) (~3 rows) |
|----------+-------+------+---------------------------------------------------------------|
| 0        | 0     | 0    | EXECUTE LIST SUBQUERY 1                                       |
+----------+-------+------+---------------------------------------------------------------+
''

-- TEST: test_raw_explain-14
-- SQL:
EXPLAIN (RAW) UPDATE testing_space SET product_units = product_units + 10 WHERE id IN (10, 15, 42);
-- EXPECTED:
1. Query (FILTERED STORAGE):
SELECT "testing_space"."product_units" + CAST(10 AS int) as "col_0", "testing_space"."id" as "col_1" FROM "testing_space" WHERE "testing_space"."id" in ( CAST(10 AS int), CAST(15 AS int), CAST(42 AS int) )
+----------+-------+------+---------------------------------------------------------------+
| selectid | order | from | detail                                                        |
+=========================================================================================+
| 0        | 0     | 0    | SEARCH TABLE testing_space USING PRIMARY KEY (id=?) (~3 rows) |
|----------+-------+------+---------------------------------------------------------------|
| 0        | 0     | 0    | EXECUTE LIST SUBQUERY 1                                       |
+----------+-------+------+---------------------------------------------------------------+
''

-- TEST: test_raw_explain-15
-- SQL:
EXPLAIN (RAW) INSERT INTO testing_space SELECT * FROM testing_space WHERE id = 42;
-- EXPECTED:
1. Query (FILTERED STORAGE):
SELECT "testing_space"."id", "testing_space"."name", "testing_space"."product_units" FROM "testing_space" WHERE "testing_space"."id" = CAST(42 AS int)
+----------+-------+------+--------------------------------------------------------------+
| selectid | order | from | detail                                                       |
+========================================================================================+
| 0        | 0     | 0    | SEARCH TABLE testing_space USING PRIMARY KEY (id=?) (~1 row) |
+----------+-------+------+--------------------------------------------------------------+
''

-- TEST: test_raw_explain-16
-- SQL:
EXPLAIN (RAW,FMT) INSERT INTO testing_space SELECT * FROM testing_space WHERE id = 42;
-- EXPECTED:
1. Query (FILTERED STORAGE):
SELECT
  "testing_space"."id",
  "testing_space"."name",
  "testing_space"."product_units"
FROM
  "testing_space"
WHERE
  "testing_space"."id" = CAST(42 AS int)
+----------+-------+------+--------------------------------------------------------------+
| selectid | order | from | detail                                                       |
+========================================================================================+
| 0        | 0     | 0    | SEARCH TABLE testing_space USING PRIMARY KEY (id=?) (~1 row) |
+----------+-------+------+--------------------------------------------------------------+
''

-- TEST: test_raw_explain-17
-- SQL:
EXPLAIN (RAW) INSERT INTO testing_space VALUES (42, 'beluga', (SELECT 1000));
-- EXPECTED:
1. Query (ROUTER):
VALUES ( CAST(42 AS int), CAST('beluga' AS string), ( SELECT CAST(1000 AS int) as "col_1") )
+----------+-------+------+---------------------------+
| selectid | order | from | detail                    |
+=====================================================+
| 0        | 0     | 0    | EXECUTE SCALAR SUBQUERY 1 |
+----------+-------+------+---------------------------+
''

-- TEST: test_raw_explain-18
-- SQL:
EXPLAIN (RAW) DELETE FROM testing_space WHERE product_units IN ( SELECT id FROM testing_space );
-- EXPECTED:
1. Query (STORAGE):
SELECT "testing_space"."id" FROM "testing_space"
+----------+-------+------+------------------------------------------+
| selectid | order | from | detail                                   |
+====================================================================+
| 0        | 0     | 0    | SCAN TABLE testing_space (~1048576 rows) |
+----------+-------+------+------------------------------------------+
''
2. Query (STORAGE):
SELECT "testing_space"."id" as "pk_col_0" FROM "testing_space" WHERE "testing_space"."product_units" in ( SELECT "COL_0" FROM "TMP_10080260809890159792_0136" )
+----------+-------+------+----------------------------------------------------------+
| selectid | order | from | detail                                                   |
+====================================================================================+
| 0        | 0     | 0    | SCAN TABLE testing_space (~983040 rows)                  |
|----------+-------+------+----------------------------------------------------------|
| 0        | 0     | 0    | EXECUTE LIST SUBQUERY 1                                  |
|----------+-------+------+----------------------------------------------------------|
| 1        | 0     | 0    | SCAN TABLE TMP_10080260809890159792_0136 (~1048576 rows) |
+----------+-------+------+----------------------------------------------------------+
''

-- TEST: test_raw_explain-19
-- SQL:
EXPLAIN (RAW) INSERT INTO testing_space WITH testing_space AS (SELECT * FROM testing_space) SELECT * FROM testing_space;
-- EXPECTED:
1. Query (STORAGE):
SELECT "testing_space"."id", "testing_space"."name", "testing_space"."product_units" FROM "testing_space"
+----------+-------+------+------------------------------------------+
| selectid | order | from | detail                                   |
+====================================================================+
| 0        | 0     | 0    | SCAN TABLE testing_space (~1048576 rows) |
+----------+-------+------+------------------------------------------+
''
2. Query (ROUTER):
SELECT * FROM ( SELECT "COL_0", "COL_1", "COL_2" FROM "TMP_3577100120548201710_0136" ) as "testing_space"
+----------+-------+------+---------------------------------------------------------+
| selectid | order | from | detail                                                  |
+===================================================================================+
| 0        | 0     | 0    | SCAN TABLE TMP_3577100120548201710_0136 (~1048576 rows) |
+----------+-------+------+---------------------------------------------------------+
''

-- TEST: test_raw_explain-20
-- SQL:
EXPLAIN (RAW) INSERT INTO testing_space SELECT 1, '1', 1;
-- EXPECTED:
1. Query (ROUTER):
SELECT CAST(1 AS int) as "col_1", CAST('1' AS string) as "col_2", CAST(1 AS int) as "col_3"
+----------+-------+------+--------+
| selectid | order | from | detail |
+==================================+
+----------+-------+------+--------+
''

-- TEST: test_raw_explain-21
-- SQL:
EXPLAIN (RAW) SELECT * FROM testing_space_global WHERE (product_units > 10 AND name LIKE 'sosisky_') ORDER BY product_units DESC LIMIT 10;
-- EXPECTED:
1. Query (ROUTER):
SELECT "id", "name", "product_units" FROM ( SELECT * FROM "testing_space_global" WHERE ( "testing_space_global"."product_units" > CAST(10 AS int) ) and ( "testing_space_global"."name" LIKE CAST('sosisky_' AS string) ESCAPE CAST('\' AS string) ) ) ORDER BY "product_units" DESC LIMIT 10
+----------+-------+------+------------------------------------------------+
| selectid | order | from | detail                                         |
+==========================================================================+
| 0        | 0     | 0    | SCAN TABLE testing_space_global (~917504 rows) |
|----------+-------+------+------------------------------------------------|
| 0        | 0     | 0    | USE TEMP B-TREE FOR ORDER BY                   |
+----------+-------+------+------------------------------------------------+
''

-- TEST: test_raw_explain-22
-- SQL:
EXPLAIN (RAW) SELECT g.name, COUNT(*) AS global_rows, SUM(g.product_units) AS global_units, SUM(l.product_units) AS local_units FROM testing_space_global g JOIN testing_space l ON g.name = l.name WHERE g.product_units > 5 GROUP BY g.name HAVING SUM(l.product_units) > SUM(g.product_units) ORDER BY global_units DESC LIMIT 10;
-- EXPECTED:
1. Query (STORAGE):
SELECT "g"."name" as "gr_expr_1", count (*) as "count_1", sum (CAST ("l"."product_units" as int)) as "sum_3", sum (CAST ("g"."product_units" as int)) as "sum_2" FROM "testing_space_global" as "g" INNER JOIN "testing_space" as "l" ON "g"."name" = "l"."name" WHERE "g"."product_units" > CAST(5 AS int) GROUP BY "g"."name"
+----------+-------+------+-----------------------------------------------------+
| selectid | order | from | detail                                              |
+===============================================================================+
| 0        | 0     | 0    | SCAN TABLE testing_space_global AS g (~983040 rows) |
|----------+-------+------+-----------------------------------------------------|
| 0        | 1     | 1    | SCAN TABLE testing_space AS l (~1048576 rows)       |
|----------+-------+------+-----------------------------------------------------|
| 0        | 0     | 0    | USE TEMP B-TREE FOR GROUP BY                        |
+----------+-------+------+-----------------------------------------------------+
''
2. Query (ROUTER):
SELECT "name", "global_rows", "global_units", "local_units" FROM ( SELECT "COL_0" as "name", sum ("COL_1") as "global_rows", sum ("COL_3") as "global_units", sum ("COL_2") as "local_units" FROM ( SELECT "COL_0", "COL_1", "COL_2", "COL_3" FROM "TMP_13612427720739559236_0136" ) GROUP BY "COL_0" HAVING sum ("COL_2") > sum ("COL_3") ) ORDER BY "global_units" DESC LIMIT 10
+----------+-------+------+----------------------------------------------------------+
| selectid | order | from | detail                                                   |
+====================================================================================+
| 0        | 0     | 0    | SCAN TABLE TMP_13612427720739559236_0136 (~1048576 rows) |
|----------+-------+------+----------------------------------------------------------|
| 0        | 0     | 0    | USE TEMP B-TREE FOR GROUP BY                             |
|----------+-------+------+----------------------------------------------------------|
| 0        | 0     | 0    | USE TEMP B-TREE FOR ORDER BY                             |
+----------+-------+------+----------------------------------------------------------+
''

-- TEST: test_raw_explain-23
-- SQL:
EXPLAIN (RAW,FMT) DELETE FROM testing_space_global WHERE id = 15 OR product_units < 10;
-- EXPECTED:
1. Query (ROUTER):
SELECT
  "testing_space_global"."id" as "pk_col_0"
FROM
  "testing_space_global"
WHERE
  ("testing_space_global"."id" = CAST(15 AS int))
  or (
    "testing_space_global"."product_units" < CAST(10 AS int)
  )
+----------+-------+------+------------------------------------------------+
| selectid | order | from | detail                                         |
+==========================================================================+
| 0        | 0     | 0    | SCAN TABLE testing_space_global (~983040 rows) |
+----------+-------+------+------------------------------------------------+
''

-- TEST: test_raw_explain-24
-- SQL:
EXPLAIN (RAW) INSERT INTO testing_space_global VALUES ((SELECT 1), (SELECT name FROM testing_space ORDER BY name LIMIT 1), 42 + 67);
-- EXPECTED:
1. Query (STORAGE):
SELECT "testing_space"."name" FROM "testing_space"
+----------+-------+------+------------------------------------------+
| selectid | order | from | detail                                   |
+====================================================================+
| 0        | 0     | 0    | SCAN TABLE testing_space (~1048576 rows) |
+----------+-------+------+------------------------------------------+
''
2. Query (ROUTER):
SELECT "COL_0" as "name" FROM ( SELECT "COL_0" FROM "TMP_16740879327537574584_0136" ) ORDER BY "COL_0" LIMIT 1
+----------+-------+------+----------------------------------------------------------+
| selectid | order | from | detail                                                   |
+====================================================================================+
| 0        | 0     | 0    | SCAN TABLE TMP_16740879327537574584_0136 (~1048576 rows) |
|----------+-------+------+----------------------------------------------------------|
| 0        | 0     | 0    | USE TEMP B-TREE FOR ORDER BY                             |
+----------+-------+------+----------------------------------------------------------+
''
3. Query (ROUTER):
VALUES ( ( SELECT CAST(1 AS int) as "col_1"), ( SELECT "COL_0" FROM "TMP_13455265481172171935_0136" ), CAST(42 AS int) + CAST(67 AS int) )
+----------+-------+------+----------------------------------------------------------+
| selectid | order | from | detail                                                   |
+====================================================================================+
| 0        | 0     | 0    | EXECUTE SCALAR SUBQUERY 1                                |
|----------+-------+------+----------------------------------------------------------|
| 0        | 0     | 0    | EXECUTE SCALAR SUBQUERY 2                                |
|----------+-------+------+----------------------------------------------------------|
| 2        | 0     | 0    | SCAN TABLE TMP_13455265481172171935_0136 (~1048576 rows) |
+----------+-------+------+----------------------------------------------------------+
''

-- TEST: test_raw_explain-25
-- SQL:
EXPLAIN (RAW) UPDATE testing_space_global SET name = upper(name);
-- EXPECTED:
1. Query (ROUTER):
SELECT upper (CAST ("testing_space_global"."name" as string)) as "col_0", "testing_space_global"."id" as "col_1" FROM "testing_space_global"
+----------+-------+------+-------------------------------------------------+
| selectid | order | from | detail                                          |
+===========================================================================+
| 0        | 0     | 0    | SCAN TABLE testing_space_global (~1048576 rows) |
+----------+-------+------+-------------------------------------------------+
''
