-- TEST: initialization
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t (a INT PRIMARY KEY, b INT, c INT);
DROP TABLE IF EXISTS g;
CREATE TABLE g (a INT PRIMARY KEY, b INT, c INT) DISTRIBUTED GLOBALLY;
DROP TABLE IF EXISTS t2;
CREATE TABLE t2 (a INT PRIMARY KEY, b INT, c INT);
INSERT INTO t VALUES(1, -1, -1);
INSERT INTO t VALUES(2, 2, 2);
INSERT INTO t VALUES(3, 3, 3);
INSERT INTO g VALUES(1, 1, 1);
INSERT INTO g VALUES(2, 2, 2);
INSERT INTO g VALUES(3, 3, 3);
INSERT INTO t2 VALUES(3, 3, 3), (1, 4, 5), (6, 7, 1), (4, 3, 4), (2, 1, 3);

-- TEST: one-sharded-one-global-simple-filter
-- SQL:
SELECT 66 FROM g WHERE (0) <= a EXCEPT SELECT 88 FROM t;
-- EXPECTED:
66

-- TEST: one-sharded-one-global-filter-subquery
-- SQL:
SELECT * FROM g WHERE (select 0) <= a EXCEPT SELECT * FROM t;
-- EXPECTED:
1, 1, 1

-- TEST: one-sharded-one-global-always-true-filter-subquery
-- SQL:
SELECT * FROM g WHERE (select true) EXCEPT SELECT * FROM t;
-- EXPECTED:
1, 1, 1

-- TEST: one-sharded-one-global-filter-subquery-ok
-- SQL:
SELECT * FROM g WHERE (select 1) < a EXCEPT SELECT * FROM t EXCEPT SELECT * FROM t;

-- TEST: one-sharded-one-global-filter-subquery-agg
-- SQL:
SELECT a FROM g WHERE (SELECT min(c) FROM g) <= a
EXCEPT SELECT e.c FROM t d JOIN g e ON e.b = d.c GROUP BY e.c
EXCEPT SELECT e.c FROM t d JOIN g e ON e.a = d.b GROUP BY e.c;
-- EXPECTED:
1

-- TEST: one-sharded-one-global-intersect-order
-- SQL:
SELECT a FROM g UNION ALL SELECT b FROM g EXCEPT SELECT c FROM t;
-- EXPECTED:
1

-- TEST: global-shared-union-except
-- SQL:
SELECT a FROM g UNION SELECT b FROM g EXCEPT SELECT c FROM t;
-- EXPECTED:
1

-- TEST: scalar-union-except
-- SQL:
SELECT 1 UNION SELECT 0 EXCEPT SELECT 1 from t;
-- EXPECTED:
0

-- TEST: group-by-having
-- SQL:
SELECT sum(t.a) FROM t GROUP BY (SELECT true) HAVING (SELECT true);
-- EXPECTED:
6

-- TEST: explain-raw-huge
-- SQL:
EXPLAIN(RAW) WITH d AS (SELECT t.b  FROM t2 e JOIN t ON t.b = e.a ),
f AS (SELECT t2.b , e.a FROM t e JOIN t ON e.a = t.b JOIN t2 ON true GROUP BY a, b)
SELECT (SELECT (SELECT 4 FROM f) UNION SELECT b FROM t2 EXCEPT SELECT 7 FROM t2);
-- EXPECTED:
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "t"."a", "t"."bucket_id", "t"."b", "t"."c" FROM "t"
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
''
╭──────────────────────────╮
│ 2. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "t2"."a", "t2"."bucket_id", "t2"."b", "t2"."c" FROM "t2"
''
plan:
    [0] SCAN TABLE t2 (~1048576 rows)
''
╭─────────────────────────────────╮
│ 3. Query (DYN-FILTERED STORAGE) │
╰─────────────────────────────────╯
''
SELECT "e"."a" as "gr_expr_1", "t2"."COL_2" as "gr_expr_2" FROM "t" as "e" INNER JOIN ( SELECT "COL_0", "COL_1", "COL_2", "COL_3" FROM "_tmp_1408043656183011107_0136" ) as "t" ON "e"."a" = "t"."COL_2" INNER JOIN ( SELECT "COL_0", "COL_1", "COL_2", "COL_3" FROM "_tmp_1408043656183011107_1136" ) as "t2" ON CAST(true AS bool) GROUP BY "e"."a", "t2"."COL_2"
''
plan:
    [0] SCAN TABLE _tmp_1408043656183011107_0136 (~1048576 rows)
        [0] SCAN TABLE _tmp_1408043656183011107_1136 (~1048576 rows)
            [0] SEARCH TABLE t AS e USING PRIMARY KEY (a=?) (~1 row)
    [0] USE TEMP B-TREE FOR GROUP BY
''
╭───────────────────╮
│ 4. Query (ROUTER) │
╰───────────────────╯
''
SELECT CAST(4 AS int) as "col_1" FROM ( SELECT "COL_1" as "b", "COL_0" as "a" FROM ( SELECT "COL_0", "COL_1" FROM "_tmp_12093674245356452270_2136" ) GROUP BY "COL_0", "COL_1" ) as "f"
''
plan:
    [0] SCAN TABLE _tmp_12093674245356452270_2136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
╭────────────────────────────────────────╮
│ 5. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT ( SELECT "COL_0" FROM "_tmp_10006618095261174462_3136" ) as "col_1" UNION SELECT "t2"."b" FROM "t2"
''
plan:
    [1] EXECUTE SCALAR SUBQUERY 2
    [2] SCAN TABLE _tmp_10006618095261174462_3136 (~1048576 rows)
    [3] SCAN TABLE t2 (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 3 USING TEMP B-TREE (UNION)
''
╭────────────────────────────────────────╮
│ 6. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT ( SELECT "COL_0" FROM "_tmp_10006618095261174462_3136" ) as "col_1" UNION SELECT "t2"."b" FROM "t2"
''
plan:
    [1] EXECUTE SCALAR SUBQUERY 2
    [2] SCAN TABLE _tmp_10006618095261174462_3136 (~1048576 rows)
    [3] SCAN TABLE t2 (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 3 USING TEMP B-TREE (UNION)
''
╭──────────────────────────╮
│ 7. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "COL_0" FROM "_tmp_13071539120297931311_11136" INTERSECT SELECT CAST(7 AS int) as "col_1" FROM "t2"
''
plan:
    [1] SCAN TABLE _tmp_13071539120297931311_11136 (~1048576 rows)
    [2] SCAN TABLE t2 (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 2 USING TEMP B-TREE (INTERSECT)
''
╭───────────────────╮
│ 8. Query (ROUTER) │
╰───────────────────╯
''
SELECT ( SELECT "COL_0" FROM "_tmp_4357191812955235924_5136" EXCEPT SELECT "COL_0" FROM "_tmp_4357191812955235924_12136" ) as "col_1"
''
plan:
    [0] EXECUTE SCALAR SUBQUERY 1
    [2] SCAN TABLE _tmp_4357191812955235924_5136 (~1048576 rows)
    [3] SCAN TABLE _tmp_4357191812955235924_12136 (~1048576 rows)
    [1] COMPOUND SUBQUERIES 2 AND 3 USING TEMP B-TREE (EXCEPT)

-- TEST: explain-raw-intersect-without-braces
-- SQL:
EXPLAIN (RAW) SELECT 1 from t UNION SELECT 0 EXCEPT SELECT 1 from t;
-- EXPECTED:
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT CAST(1 AS int) as "col_1" FROM "t" UNION SELECT CAST(0 AS int) as "col_1"
''
plan:
    [1] SCAN TABLE t (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 2 USING TEMP B-TREE (UNION)
''
╭────────────────────────────────────────╮
│ 2. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT CAST(1 AS int) as "col_1" FROM "t" UNION SELECT CAST(0 AS int) as "col_1"
''
plan:
    [1] SCAN TABLE t (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 2 USING TEMP B-TREE (UNION)
''
╭──────────────────────────╮
│ 3. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "COL_0" FROM "_tmp_4307261431969392776_3136" INTERSECT SELECT CAST(1 AS int) as "col_1" FROM "t"
''
plan:
    [1] SCAN TABLE _tmp_4307261431969392776_3136 (~1048576 rows)
    [2] SCAN TABLE t (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 2 USING TEMP B-TREE (INTERSECT)
''
╭───────────────────╮
│ 4. Query (ROUTER) │
╰───────────────────╯
''
SELECT "COL_0" FROM "_tmp_4327442981240398295_1136" EXCEPT SELECT "COL_0" FROM "_tmp_4327442981240398295_4136"
''
plan:
    [1] SCAN TABLE _tmp_4327442981240398295_1136 (~1048576 rows)
    [2] SCAN TABLE _tmp_4327442981240398295_4136 (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 2 USING TEMP B-TREE (EXCEPT)

-- TEST: explain-raw-scalar-union-except
-- SQL:
EXPLAIN (RAW) SELECT 1 from t UNION SELECT 0 EXCEPT SELECT 1 from t;
-- EXPECTED:
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT CAST(1 AS int) as "col_1" FROM "t" UNION SELECT CAST(0 AS int) as "col_1"
''
plan:
    [1] SCAN TABLE t (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 2 USING TEMP B-TREE (UNION)
''
╭────────────────────────────────────────╮
│ 2. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT CAST(1 AS int) as "col_1" FROM "t" UNION SELECT CAST(0 AS int) as "col_1"
''
plan:
    [1] SCAN TABLE t (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 2 USING TEMP B-TREE (UNION)
''
╭──────────────────────────╮
│ 3. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "COL_0" FROM "_tmp_4307261431969392776_3136" INTERSECT SELECT CAST(1 AS int) as "col_1" FROM "t"
''
plan:
    [1] SCAN TABLE _tmp_4307261431969392776_3136 (~1048576 rows)
    [2] SCAN TABLE t (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 2 USING TEMP B-TREE (INTERSECT)
''
╭───────────────────╮
│ 4. Query (ROUTER) │
╰───────────────────╯
''
SELECT "COL_0" FROM "_tmp_4327442981240398295_1136" EXCEPT SELECT "COL_0" FROM "_tmp_4327442981240398295_4136"
''
plan:
    [1] SCAN TABLE _tmp_4327442981240398295_1136 (~1048576 rows)
    [2] SCAN TABLE _tmp_4327442981240398295_4136 (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 2 USING TEMP B-TREE (EXCEPT)

-- TEST: one-sharded-one-global-intersect-order-union-all-chain
-- SQL:
SELECT c FROM g UNION ALL SELECT a FROM g UNION ALL SELECT b FROM g
EXCEPT SELECT c FROM t;
-- EXPECTED:
1

-- TEST: one-sharded-one-global-intersect-order-multiple-columns
-- SQL:
SELECT a, b FROM g UNION ALL SELECT b, c FROM g EXCEPT SELECT a, b FROM t;
-- EXPECTED:
1, 1

-- TEST: one-sharded-one-global-intersect-order-group-by
-- SQL:
SELECT a FROM g GROUP BY a UNION ALL SELECT b FROM g GROUP BY b
EXCEPT SELECT c FROM t;
-- EXPECTED:
1

-- TEST: one-sharded-one-global-intersect-order-filter-subquery
-- SQL:
SELECT a FROM g WHERE (SELECT 0) <= a UNION ALL SELECT b FROM g
EXCEPT SELECT c FROM t;
-- EXPECTED:
1

-- TEST: one-sharded-one-global-intersect-order-chained-except
-- SQL:
SELECT a FROM g UNION ALL SELECT b FROM g
EXCEPT SELECT c FROM t
EXCEPT SELECT b FROM t;
-- EXPECTED:
1

-- TEST: one-sharded-one-global-compound-sharded-side
-- SQL:
SELECT a FROM g EXCEPT SELECT * FROM (SELECT b FROM t UNION ALL SELECT c FROM t) AS s;
-- EXPECTED:
1
