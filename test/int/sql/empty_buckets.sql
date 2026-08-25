-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- Queries with an empty set of calculated buckets should be executed using
-- `Buckets::Any`, e.g. `SELECT 1 UNION ALL SELECT a FROM t WHERE false` must
-- return a single row.

-- TEST: initialization
-- SQL:
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS g;
CREATE TABLE t (a INT PRIMARY KEY, b INT);
CREATE TABLE t2 (a INT PRIMARY KEY, b INT);
CREATE TABLE g (a INT PRIMARY KEY, b INT) DISTRIBUTED GLOBALLY;
INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO t2 VALUES (1, 1), (2, 2), (3, 3);
INSERT INTO g VALUES (7, 70), (8, 80);

-- TEST: constant-union-all-false-filter
-- SQL:
SELECT 1 UNION ALL SELECT a FROM t WHERE false;
-- EXPECTED:
1

-- TEST: false-filter-union-all-constant
-- SQL:
SELECT a FROM t WHERE false UNION ALL SELECT 1;
-- EXPECTED:
1

-- TEST: constant-union-all-null-filter
-- SQL:
SELECT 1 UNION ALL SELECT a FROM t WHERE null;
-- EXPECTED:
1

-- TEST: constant-union-all-conflicting-sharding-key
-- SQL:
SELECT 1 UNION ALL SELECT a FROM t WHERE a = 1 AND a = 2;
-- EXPECTED:
1

-- TEST: constant-union-all-conflicting-sharding-key-with-segment-motion
-- SQL:
SELECT 1 UNION ALL SELECT a FROM t WHERE a = 1 AND a = 2 AND a IN (SELECT b FROM t2);
-- EXPECTED:
1

-- TEST: global-union-all-conflicting-sharding-key-with-segment-motion
-- SQL:
SELECT a FROM g UNION ALL SELECT a FROM t WHERE a = 1 AND a = 2 AND a IN (SELECT b FROM t2);
-- UNORDERED:
7, 8

-- TEST: constant-union-all-single-bucket-with-segment-motion
-- SQL:
SELECT 100 UNION ALL SELECT a FROM t WHERE a = 1 AND a IN (SELECT b FROM t2);
-- UNORDERED:
1, 100

-- TEST: constant-union-all-empty-arm-twice
-- SQL:
SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT a FROM t WHERE false;
-- UNORDERED:
1, 2

-- TEST: values-union-all-false-filter
-- SQL:
SELECT * FROM (VALUES (1)) UNION ALL SELECT a FROM t WHERE false;
-- EXPECTED:
1

-- TEST: global-union-all-false-filter
-- SQL:
SELECT a FROM g UNION ALL SELECT a FROM t WHERE false;
-- UNORDERED:
7, 8

-- TEST: subquery-over-constant-union-all-false-filter
-- SQL:
SELECT * FROM (SELECT 1 AS a UNION ALL SELECT a FROM t WHERE false) AS u;
-- EXPECTED:
1

-- TEST: constant-union-all-single-bucket
-- SQL:
SELECT 100 UNION ALL SELECT a FROM t WHERE a = 1;
-- UNORDERED:
1, 100

-- TEST: constant-union-all-whole-table
-- SQL:
SELECT 100 UNION ALL SELECT a FROM t;
-- UNORDERED:
1, 2, 3, 100

-- TEST: false-filter-union-all-false-filter
-- SQL:
SELECT a FROM t WHERE false UNION ALL SELECT a FROM t WHERE false;
-- EXPECTED:

-- TEST: sharded-union-all-false-filter
-- SQL:
SELECT a FROM t UNION ALL SELECT a FROM t WHERE false;
-- UNORDERED:
1, 2, 3

-- TEST: constant-except-false-filter
-- SQL:
SELECT 1 EXCEPT SELECT a FROM t WHERE false;
-- EXPECTED:
1

-- TEST: false-filter-except-constant
-- SQL:
SELECT a FROM t WHERE false EXCEPT SELECT 1;
-- EXPECTED:

-- TEST: constant-union-false-filter
-- SQL:
SELECT 1 UNION SELECT a FROM t WHERE false;
-- EXPECTED:
1

-- TEST: constant-union-all-false-filter-order-by
-- SQL:
SELECT 1 UNION ALL SELECT a FROM t WHERE false ORDER BY 1;
-- EXPECTED:
1

-- TEST: constant-union-all-false-filter-limit
-- SQL:
SELECT 1 UNION ALL SELECT a FROM t WHERE false LIMIT 1;
-- EXPECTED:
1

-- TEST: explain-constant-union-all-false-filter
-- SQL:
EXPLAIN SELECT 1 UNION ALL SELECT a FROM t WHERE false;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
union all
  motion [policy: local, program: SerializeAsEmptyTable(true)]
    projection (1::int -> col_1)
  projection (t.a::int -> a)
    selection (false::bool)
      scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = any

-- TEST: explain-raw-constant-union-all-false-filter
-- SQL:
EXPLAIN (RAW, BUCKETS) SELECT 1 UNION ALL SELECT a FROM t WHERE false;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭───────────────────╮
│ 1. Query (ROUTER) │
╰───────────────────╯
''
SELECT CAST(1 AS int) as "col_1" UNION ALL SELECT "t"."a" FROM "t" WHERE CAST(false AS bool)
''
plan:
    [2] SCAN TABLE t (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = any

-- TEST: explain-raw-global-union-all-false-filter
-- SQL:
EXPLAIN (RAW, BUCKETS) SELECT a FROM g UNION ALL SELECT a FROM t WHERE false;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭───────────────────╮
│ 1. Query (ROUTER) │
╰───────────────────╯
''
SELECT "g"."a" FROM "g" UNION ALL SELECT "t"."a" FROM "t" WHERE CAST(false AS bool)
''
plan:
    [1] SCAN TABLE g (~1048576 rows)
    [2] SCAN TABLE t (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = any

-- TEST: explain-raw-global-union-all-sharded
-- SKIP_FOR: 1rsX1
-- SQL:
EXPLAIN (RAW, BUCKETS) SELECT a FROM g UNION ALL SELECT a FROM t;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────────────────────╮
│ 1.1. Query (CONST-FILTERED STORAGE, 1/2) │
╰──────────────────────────────────────────╯
''
SELECT "g"."a" FROM "g" UNION ALL SELECT "t"."a" FROM "t"
''
plan:
    [1] SCAN TABLE g (~1048576 rows)
    [2] SCAN TABLE t (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets <= [1-3000]
''
╭──────────────────────────────────────────╮
│ 1.2. Query (CONST-FILTERED STORAGE, 1/2) │
╰──────────────────────────────────────────╯
''
select cast(null as int) as "a" where false UNION ALL SELECT "t"."a" FROM "t"
''
plan:
    [2] SCAN TABLE t (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets <= [1-3000]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: explain-raw-global-join-sharded
-- SQL:
EXPLAIN (RAW, BUCKETS) SELECT s.a,g.a FROM g JOIN (SELECT a FROM t WHERE false) s ON g.a = s.a;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭───────────────────╮
│ 1. Query (ROUTER) │
╰───────────────────╯
''
SELECT "s"."a", "g"."a" FROM "g" INNER JOIN ( SELECT "t"."a" FROM "t" WHERE CAST(false AS bool) ) as "s" ON "g"."a" = "s"."a"
''
plan:
    [0] SCAN TABLE g (~1048576 rows)
        [0] SEARCH TABLE t USING PRIMARY KEY (a=?) (~1 row)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = any

-- TEST: explain-raw-sharded-join-global
-- SQL:
EXPLAIN (RAW, BUCKETS) SELECT s.a,g.a FROM g JOIN (SELECT a FROM t WHERE false) s ON s.a = g.a;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭───────────────────╮
│ 1. Query (ROUTER) │
╰───────────────────╯
''
SELECT "s"."a", "g"."a" FROM "g" INNER JOIN ( SELECT "t"."a" FROM "t" WHERE CAST(false AS bool) ) as "s" ON "s"."a" = "g"."a"
''
plan:
    [0] SCAN TABLE g (~1048576 rows)
        [0] SEARCH TABLE t USING PRIMARY KEY (a=?) (~1 row)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = any

-- TEST: explain-raw-global-join-global
-- SQL:
EXPLAIN (RAW, BUCKETS) SELECT * FROM g JOIN (SELECT * FROM t WHERE false) t ON g.a = g.a;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭───────────────────╮
│ 1. Query (ROUTER) │
╰───────────────────╯
''
SELECT * FROM "g" INNER JOIN ( SELECT "t"."a", "t"."b" FROM "t" WHERE CAST(false AS bool) ) as "t" ON "g"."a" = "g"."a"
''
plan:
    [0] SCAN TABLE g (~983040 rows)
        [0] SCAN TABLE t (~1048576 rows)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = any

-- TEST: explain-raw-sharded-join-sharded
-- SQL:
EXPLAIN (RAW, BUCKETS) SELECT * FROM g JOIN (SELECT * FROM t WHERE false) t ON t.a = t.a;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭───────────────────╮
│ 1. Query (ROUTER) │
╰───────────────────╯
''
SELECT * FROM "g" INNER JOIN ( SELECT "t"."a", "t"."b" FROM "t" WHERE CAST(false AS bool) ) as "t" ON "t"."a" = "t"."a"
''
plan:
    [0] SCAN TABLE t (~983040 rows)
        [0] SCAN TABLE g (~1048576 rows)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = any
