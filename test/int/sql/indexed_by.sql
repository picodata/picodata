-- TEST: indexed-by
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t ("a" int primary key, "b" int, "c" text);
CREATE INDEX aaa on t (a, b, c);
INSERT INTO "t" VALUES
            (1, 1, 'aaa'),
            (2, 2, 'bbb');
CREATE TABLE s ("d" int primary key, "e" int, "f" int);
CREATE INDEX bbb on s (e, f);
INSERT INTO "s" VALUES
            (3, 3, 3),
            (4, 4, 4);


-- TEST: indexed-by-1
-- SQL:
SELECT a FROM t INDEXED BY aaa WHERE true;
-- EXPECTED:
1, 2

-- TEST: indexed-by-2
-- SQL:
SELECT * FROM t INDEXED BY aa WHERE true;
-- ERROR:
sbroad: index aa not found

-- TEST: indexed-by-3
-- SQL:
explain (raw) SELECT a FROM t INDEXED BY aaa WHERE true;
-- EXPECTED:
1. Query (STORAGE):
SELECT "t"."a" FROM "t" INDEXED BY "aaa" WHERE CAST(true AS bool)
+----------+-------+------+-------------------------------------------------------+
| selectid | order | from | detail                                                |
+=================================================================================+
| 0        | 0     | 0    | SCAN TABLE t USING COVERING INDEX aaa (~1048576 rows) |
+----------+-------+------+-------------------------------------------------------+
''

-- TEST: indexed-by-4
-- SQL:
explain (raw) SELECT a FROM t WHERE true;
-- EXPECTED:
1. Query (STORAGE):
SELECT "t"."a" FROM "t" WHERE CAST(true AS bool)
+----------+-------+------+------------------------------+
| selectid | order | from | detail                       |
+========================================================+
| 0        | 0     | 0    | SCAN TABLE t (~1048576 rows) |
+----------+-------+------+------------------------------+
''

-- TEST: indexed-by-5
-- SQL:
explain (raw) SELECT a FROM t INDEXED BY aaa WHERE a > 10 UNION SELECT d from s INDEXED by bbb WHERE f < -5;
-- EXPECTED:
1. Query (STORAGE):
SELECT "t"."a" FROM "t" INDEXED BY "aaa" WHERE "t"."a" > CAST(10 AS int) UNION SELECT "s"."d" FROM "s" INDEXED BY "bbb" WHERE "s"."f" < CAST(-5 AS int)
+----------+-------+------+--------------------------------------------------------------+
| selectid | order | from | detail                                                       |
+========================================================================================+
| 1        | 0     | 0    | SEARCH TABLE t USING COVERING INDEX aaa (a>?) (~262144 rows) |
|----------+-------+------+--------------------------------------------------------------|
| 2        | 0     | 0    | SCAN TABLE s USING COVERING INDEX bbb (~983040 rows)         |
|----------+-------+------+--------------------------------------------------------------|
| 0        | 0     | 0    | COMPOUND SUBQUERIES 1 AND 2 USING TEMP B-TREE (UNION)        |
+----------+-------+------+--------------------------------------------------------------+
''

-- TEST: indexed-by-6
-- SQL:
explain (raw) SELECT * FROM (SELECT * FROM t INDEXED BY aaa WHERE a > 10) JOIN (SELECT d from s INDEXED by bbb WHERE f < -5) ON true;
-- EXPECTED:
1. Query (STORAGE):
SELECT "s"."d" FROM "s" INDEXED BY "bbb" WHERE "s"."f" < CAST(-5 AS int)
+----------+-------+------+------------------------------------------------------+
| selectid | order | from | detail                                               |
+================================================================================+
| 0        | 0     | 0    | SCAN TABLE s USING COVERING INDEX bbb (~983040 rows) |
+----------+-------+------+------------------------------------------------------+
''
2. Query (STORAGE):
SELECT * FROM ( SELECT "t"."a", "t"."b", "t"."c" FROM "t" INDEXED BY "aaa" WHERE "t"."a" > CAST(10 AS int) ) as "unnamed_subquery" INNER JOIN ( SELECT "COL_0" FROM "TMP_12548177466147962059_0136" ) as "unnamed_subquery_1" ON CAST(true AS bool)
+----------+-------+------+--------------------------------------------------------------+
| selectid | order | from | detail                                                       |
+========================================================================================+
| 0        | 0     | 0    | SEARCH TABLE t USING COVERING INDEX aaa (a>?) (~262144 rows) |
|----------+-------+------+--------------------------------------------------------------|
| 0        | 1     | 1    | SCAN TABLE TMP_12548177466147962059_0136 (~1048576 rows)     |
+----------+-------+------+--------------------------------------------------------------+
''

-- TEST: indexed-by-7
-- SQL:
explain SELECT a FROM t INDEXED BY aaa WHERE true;
-- EXPECTED:
projection ("t"."a"::int -> "a")
    selection true::bool
        scan "t" (indexed by "aaa")
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: indexed-by-8
-- SQL:
explain SELECT a FROM t AS ttt INDEXED BY aaa WHERE true;
-- EXPECTED:
projection ("ttt"."a"::int -> "a")
    selection true::bool
        scan "t" -> "ttt" (indexed by "aaa")
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: indexed-by-9
-- SQL:
SELECT * FROM (SELECT * FROM t INDEXED BY aaa) INDEXED BY aaa WHERE true;
-- ERROR:
invalid index: INDEXED BY clause is only supported for tables

-- TEST: indexed-by-10
-- SQL:
WITH cte(a) AS (SELECT a FROM t) SELECT * FROM cte INDEXED BY abc;
-- ERROR:
invalid index: INDEXED BY clause is only supported for tables

-- TEST: indexed-by-10
-- SQL:
explain DELETE FROM t INDEXED by aaa WHERE true
-- EXPECTED:
delete "t"
    motion [policy: local, program: [PrimaryKey(0), ReshardIfNeeded]]
        projection ("t"."a"::int -> "pk_col_0")
            selection true::bool
                scan "t" (indexed by "aaa")
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: indexed-by-11
-- SQL:
explain UPDATE t INDEXED BY aaa SET b = d FROM s INDEXED BY bbb WHERE TRUE
-- EXPECTED:
update "t"
"b" = "col_0"
    motion [policy: local, program: ReshardIfNeeded]
        projection ("s"."d"::int -> "col_0", "t"."a"::int -> "col_1")
            join on true::bool
                scan "t" (indexed by "aaa")
                motion [policy: full, program: ReshardIfNeeded]
                    projection ("s"."d"::int -> "d", "s"."bucket_id"::int -> "bucket_id", "s"."e"::int -> "e", "s"."f"::int -> "f")
                        scan "s" (indexed by "bbb")
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: indexed-by-12
-- SQL:
SELECT * FROM t INDEXED BY aaa;
-- EXPECTED:
1, 1, 'aaa', 2, 2, 'bbb'

-- TEST: indexed-by-13
-- SQL:
DELETE FROM t INDEXED BY aaa WHERE FALSE;

-- TEST: indexed-by-14
-- SQL:
UPDATE t INDEXED BY aaa SET b = d FROM s INDEXED BY bbb WHERE FALSE;

-- TEST: indexed-by-15
-- SQL:
ALTER INDEX aaa RENAME to aaaa;

-- TEST: indexed-by-16
-- SQL:
SELECT * FROM t INDEXED BY aaa;
-- ERROR:
sbroad: index aaa not found

-- TEST: indexed-by-17
-- SQL:
DELETE FROM t INDEXED BY aaa WHERE FALSE;
-- ERROR:
sbroad: index aaa not found

-- TEST: indexed-by-18
-- SQL:
UPDATE t INDEXED BY aaa SET b = d FROM s INDEXED BY bbb WHERE FALSE;
-- ERROR:
sbroad: index aaa not found

-- TEST: indexed-by-13
-- SQL:
DELETE FROM t INDEXED BY aaaa;
