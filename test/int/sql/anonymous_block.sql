-- TEST: init
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t (pk INT PRIMARY KEY, a INT, b INT);
CREATE INDEX ta ON t(a);
INSERT INTO t VALUES (1,1,1), (2,2,2), (3,3,3), (4,4,4);
DROP TABLE IF EXISTS g;
CREATE TABLE g (pk INT PRIMARY KEY, a INT, b INT) DISTRIBUTED GLOBALLY;
INSERT INTO g VALUES (1,1,1);

-- TEST: return query-1
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT a FROM t WHERE a = 1;
END $$;
-- ERROR:
transaction cannot be executed on all buckets

-- TEST: return query-2
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT a + 1 FROM t WHERE pk = 1;
  RETURN QUERY SELECT b + 2 FROM t WHERE pk = 1;
END $$;
-- EXPECTED:
2,
3,

-- TEST: return query-3
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT 1,2 UNION ALL SELECT 1,2;
END $$;
-- EXPECTED:
1,2,
1,2,

-- TEST: return query-4
-- SQL:
DO $$
BEGIN
  RETURN QUERY VALUES (1,2), (2,3);
  RETURN QUERY SELECT 3,4;
END $$;
-- EXPECTED:
1,2,
2,3,
3,4,

-- TEST: return query-5
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT 2;
  RETURN QUERY SELECT b + 2 FROM t WHERE pk = 1;
  RETURN QUERY SELECT 4;
END $$;
-- EXPECTED:
2,
3,
4,

-- TEST: return query-different-buckets
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT a FROM t WHERE pk = 1;
  RETURN QUERY SELECT b FROM t WHERE pk = 2;
END $$;
-- ERROR:
transaction queries have different buckets: \[1934\] and \[1410\]

-- TEST: updates-1
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT * FROM t WHERE pk = 1;
  UPDATE t SET a = a + 1 WHERE pk = 1;
  UPDATE t SET a = a + 1 WHERE pk = 1;
END $$;
-- EXPECTED:
1, 1, 1

-- TEST: updates-1-ensure-updated
-- SQL:
SELECT * FROM t WHERE pk = 1;
-- EXPECTED:
1, 3, 1

-- TEST: updates-2
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT * FROM t WHERE pk = 2;
  UPDATE t SET a = a + 1 WHERE pk = 2;
  UPDATE t SET a = a * 2 WHERE pk = 2;
END $$;
-- EXPECTED:
2, 2, 2

-- TEST: updates-2-ensure-updated
-- SQL:
SELECT * FROM t WHERE pk = 2;
-- EXPECTED:
2, 6, 2

-- TEST: updates-no-return-query
-- SQL:
DO $$
BEGIN
  UPDATE t SET a = a + 1 WHERE pk = 3;
  UPDATE t SET a = a * 2 WHERE pk = 3;
END $$;

-- TEST: updates-no-return-query-check
-- SQL:
SELECT * FROM t WHERE pk = 3;
-- EXPECTED:
3, 8, 3

-- TEST: update-2-columns
-- SQL:
DO $$
BEGIN
  UPDATE t SET a = coalesce(42, 42), b = (2 + 2)::int WHERE pk = 3;
END $$;

-- TEST: update-2-columns-check
-- SQL:
SELECT * FROM t WHERE pk = 3;
-- EXPECTED:
3, 42, 4

-- TEST: update-indexed-by
-- SQL:
DO $$
BEGIN
  UPDATE t INDEXED BY ta SET b = 3::int + 3::int, a = coalesce(42 + 1, null) WHERE pk = 3;
END $$;

-- TEST: update-indexed-by-check
-- SQL:
SELECT * FROM t WHERE pk = 3;
-- EXPECTED:
3, 43, 6

-- TEST: update-index-not-found
-- SQL:
DO $$
BEGIN
  UPDATE t INDEXED BY ahahhahha SET b = 3::int + 3::int, a = coalesce(42 + 1, null) WHERE pk = 3;
END $$;
-- ERROR:
index ahahhahha not found

-- TEST: trigger-block-rollback
-- SQL:
DO $$
BEGIN
  UPDATE t SET a = 42 WHERE pk = 4;
  UPDATE t SET a = a / 0 WHERE pk = 4;
END $$;
-- ERROR:
division by zero

-- TEST: ensure-block-rollbacked
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT a FROM t WHERE pk = 4;
END $$;
-- EXPECTED:
4

-- TEST: localtimestamp-substitution-works
-- SQL:
do $$ begin return query select localtimestamp = localtimestamp from t where pk = 1; end $$;
-- EXPECTED:
true

-- TEST: const-folding-init
-- SQL:
DROP TABLE IF EXISTS t2;
CREATE TABLE t2 (pk BOOL PRIMARY KEY, a INT, b INT);
INSERT INTO t2 VALUES (false, 0, 0), (true, 1, 1);

-- TEST: const-folding-works
-- SQL:
do $$ begin return query select * from t2 where pk = (1 = 1); end $$;
-- EXPECTED:
true, 1, 1

-- TEST: return-query-types-cannot-be-matched-1
-- SQL:
DO $$ BEGIN
  RETURN QUERY SELECT 1;
  RETURN QUERY SELECT '1';
END $$;
-- ERROR:
RETURN QUERY types cannot be matched \(\[int\] and \[string\]\)

-- TEST: return-query-types-cannot-be-matched-2
-- SQL:
DO $$ BEGIN
  RETURN QUERY SELECT 1;
  RETURN QUERY SELECT '1', null;
END $$;
-- ERROR:
RETURN QUERY types cannot be matched \(\[int\] and \[string\, unknown]\)

-- TEST: can-read-system-table
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT name FROM _pico_table where name = 't';
END $$;
-- EXPECTED:
't'

-- TEST: cannot-modify-system-table
-- SQL:
DO $$
BEGIN
  UPDATE _pico_table SET name = 'lame';
END $$;
-- ERROR:
cannot modify system table _pico_table within transaction

-- TEST: can-read-global-table-1
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT * FROM g;
END $$;
-- EXPECTED:
1,1,1

-- TEST: can-read-global-table-2
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT * FROM g ORDER BY 1 LIMIT 1;
END $$;
-- EXPECTED:
1,1,1

-- TEST: can-read-global-and-sharded-table
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT * FROM g ORDER BY 1 LIMIT 1;
  RETURN QUERY SELECT * FROM t WHERE pk = 4;
END $$;
-- EXPECTED:
1,1,1,
4,4,4,

-- TEST: cannot-update-global-table
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT * FROM g ORDER BY 1 LIMIT 1;
  UPDATE g SET b = a WHERE b = 1;
END $$;
-- ERROR:
cannot modify global table g within transaction

-- TEST: cannot-delete-from-global-table
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT * FROM g ORDER BY 1 LIMIT 1;
  DELETE FROM g WHERE b = 1;
END $$;
-- ERROR:
cannot modify global table g within transaction

-- TEST: block-query-stmt-must-be-dml-error
-- SQL:
DO $$
BEGIN
  SELECT * FROM _pico_table;
END $$;
-- ERROR:
QUERY statements must execute DML queries, use RETURN QUERY to return rows

-- TEST: let-and-return-query-stmts-must-come-before-dml
-- SQL:
DO $$
BEGIN
  UPDATE t SET a = a + 1 WHERE pk = 2;
  RETURN QUERY SELECT * FROM _pico_table;
END $$;
-- ERROR:
QUERY statements must follow LET and RETURN QUERY statements

-- TEST: insert-single-row
-- SQL:
DO $$
BEGIN
  INSERT INTO t VALUES (10, 100, 100);
END $$;

-- TEST: insert-single-row-check
-- SQL:
SELECT * FROM t WHERE pk = 10;
-- EXPECTED:
10, 100, 100

-- TEST: insert-with-explicit-columns
-- SQL:
DO $$
BEGIN
  INSERT INTO t (pk, a, b) VALUES (11, 101, 111);
END $$;

-- TEST: insert-with-explicit-columns-check
-- SQL:
SELECT * FROM t WHERE pk = 11;
-- EXPECTED:
11, 101, 111

-- TEST: insert-multi-row-same-bucket
-- SQL:
DO $$
BEGIN
  INSERT INTO t VALUES (12, 1, 1), (12, 2, 2) ON CONFLICT DO REPLACE;
END $$;

-- TEST: insert-multi-row-same-bucket-check
-- SQL:
SELECT * FROM t WHERE pk = 12;
-- EXPECTED:
12, 2, 2

-- TEST: insert-with-arithmetic-non-key
-- SQL:
DO $$
BEGIN
  INSERT INTO t VALUES (13, 1 + 2, 4 * 5);
END $$;

-- TEST: insert-with-arithmetic-non-key-check
-- SQL:
SELECT * FROM t WHERE pk = 13;
-- EXPECTED:
13, 3, 20

-- TEST: insert-with-update-same-bucket
-- SQL:
DO $$
BEGIN
  INSERT INTO t VALUES (14, 1, 1);
  UPDATE t SET a = 99 WHERE pk = 14;
END $$;

-- TEST: insert-with-update-same-bucket-check
-- SQL:
SELECT * FROM t WHERE pk = 14;
-- EXPECTED:
14, 99, 1

-- TEST: insert-on-conflict-do-replace
-- SQL:
DO $$
BEGIN
  INSERT INTO t VALUES (15, 1, 1);
  INSERT INTO t VALUES (15, 2, 2) ON CONFLICT DO REPLACE;
END $$;

-- TEST: insert-on-conflict-do-replace-check
-- SQL:
SELECT * FROM t WHERE pk = 15;
-- EXPECTED:
15, 2, 2

-- TEST: insert-on-conflict-do-nothing
-- SQL:
DO $$
BEGIN
  INSERT INTO t VALUES (16, 1, 1);
  INSERT INTO t VALUES (16, 2, 2) ON CONFLICT DO NOTHING;
END $$;

-- TEST: insert-on-conflict-do-nothing-check
-- SQL:
SELECT * FROM t WHERE pk = 16;
-- EXPECTED:
16, 1, 1

-- TEST: insert-different-buckets-error
-- SQL:
DO $$
BEGIN
  INSERT INTO t VALUES (1, 1, 1), (2, 2, 2) ON CONFLICT DO REPLACE;
END $$;
-- ERROR:
transaction can only be executed on a single bucket

-- TEST: insert-and-query-different-buckets-error
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT * FROM t WHERE pk = 1;
  INSERT INTO t VALUES (2, 1, 1) ON CONFLICT DO REPLACE;
END $$;
-- ERROR:
transaction queries have different buckets

-- TEST: insert-non-constant-sharding-key-error
-- SQL:
DO $$
BEGIN
  INSERT INTO t VALUES (1 + 1, 1, 1);
END $$;
-- ERROR:
INSERT in transaction requires constant or parameter values for sharding-key columns

-- TEST: insert-into-global-table-error
-- SQL:
DO $$
BEGIN
  INSERT INTO g VALUES (5, 5, 5);
END $$;
-- ERROR:
cannot modify global table g within transaction

-- TEST: insert-different-pks-same-bucket
-- SQL:
DO $$
BEGIN
  INSERT INTO t VALUES (433, 1, 1), (1618, 2, 2);
END $$;

-- TEST: insert-different-pks-same-bucket-check
-- SQL:
SELECT * FROM t WHERE pk = 433 OR pk = 1618;
-- UNORDERED:
433, 1, 1,
1618, 2, 2

-- TEST: insert-with-function-call-non-key
-- SQL:
DO $$
BEGIN
  INSERT INTO t VALUES (300, COALESCE(NULL, 99), ABS(-7));
END $$;

-- TEST: insert-with-function-call-non-key-check
-- SQL:
SELECT * FROM t WHERE pk = 300;
-- EXPECTED:
300, 99, 7

-- TEST: insert-cast-constant-from-dk
-- SQL:
DO $$
BEGIN
  INSERT INTO t VALUES (400::int, 99, 7);
END $$;

-- TEST: insert-cast-constant-from-dk-check
-- SQL:
SELECT * FROM t WHERE pk = 400;
-- EXPECTED:
400, 99, 7

-- TEST: explain-insert
-- SQL:
EXPLAIN (raw)
DO $$
BEGIN
  INSERT INTO t VALUES (400, 99 + 1, 1 + 1);
END $$;
-- EXPECTED:
1. Query (FILTERED STORAGE):
''
INSERT INTO "t" ("pk", "a", "b", "bucket_id") VALUES ( CAST(400 AS int), CAST(99 AS int) + CAST(1 AS int), CAST(1 AS int) + CAST(1 AS int), 590 )
''
plan:
    [0] TRIVIAL

-- TEST: insert-with-subquery-in-values-error
-- SQL:
DO $$
BEGIN
  INSERT INTO t VALUES (400, (SELECT 1), 1);
END $$;
-- ERROR:
INSERT in transaction does not support subqueries in VALUES

-- TEST: insert-from-select-error
-- SQL:
DO $$
BEGIN
  INSERT INTO t SELECT pk + 100, a, b FROM t WHERE pk = 1;
END $$;
-- ERROR:
INSERT query has motions which are not allowed in transactions

-- TEST: block-with-motions
-- SQL:
DO $$ BEGIN RETURN QUERY SELECT * FROM t WHERE pk = 1 LIMIT 1; END $$;
-- EXPECTED:
1,3,1

-- TEST: block-with-subquery
-- SQL:
DO $$ BEGIN RETURN QUERY SELECT pk FROM (SELECT * FROM t WHERE pk = 1) s ORDER BY pk LIMIT 1; END $$;
-- EXPECTED:
1

-- TEST: block-with-cte
-- SQL:
DO $$ BEGIN RETURN QUERY WITH cte AS (SELECT * FROM t WHERE pk = 1) SELECT pk FROM cte ORDER BY pk LIMIT 1; END $$;
-- EXPECTED:
1

-- TEST: multibucket-block-1
-- SQL:
DO $$ BEGIN RETURN QUERY SELECT * FROM t WHERE pk = 1 AND pk = 2; END $$;
-- ERROR:
transaction can only be executed on a single bucket, got \[\]

-- TEST: multibucket-block-2
-- SQL:
DO $$ BEGIN RETURN QUERY SELECT * FROM t WHERE pk = 1 OR pk = 2; END $$;
-- ERROR:
transaction can only be executed on a single bucket, got \[1410, 1934\]

-- TEST: block-with-sql_vdbe_opcode_max-0
-- SQL:
do $$ BEGIN RETURN QUERY select * from t where pk = 1; END $$ option (sql_vdbe_opcode_max = 0);
-- EXPECTED:
1, 3, 1

-- TEST: block-with-sql_vdbe_opcode_max-1
-- SQL:
do $$ BEGIN RETURN QUERY select * from t where pk = 1; END $$ option (sql_vdbe_opcode_max = 1);
-- ERROR:
Reached a limit on max executed vdbe opcodes. Limit: 1

-- TEST: block-delete-1
-- SQL:
do $$ BEGIN DELETE FROM t WHERE pk = 1; END $$;

-- TEST: block-delete-1-check
-- SQL:
SELECT * FROM t WHERE pk = 1;

-- TEST: block-delete-2
-- SQL:
do $$ BEGIN DELETE FROM t; END $$;
-- ERROR:
transaction cannot be executed on all buckets

-- TEST: block-delete-with-return-query
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT * FROM t WHERE pk = 2;
  DELETE FROM t WHERE pk = 2;
END $$;
-- EXPECTED:
2, 6, 2

-- TEST: block-delete-with-return-query-check
-- SQL:
SELECT * FROM t WHERE pk = 2;

-- TEST: explain-block-delete-with-return-query
-- SQL:
EXPLAIN (raw)
DO $$ BEGIN
  RETURN QUERY SELECT * FROM t WHERE pk = 2;
  DELETE FROM t WHERE pk = 2;
END $$;
-- EXPECTED:
1. Return query (FILTERED STORAGE):
''
SELECT "t"."pk", "t"."a", "t"."b" FROM "t" WHERE "t"."pk" = CAST(2 AS int)
''
plan:
    [0] SEARCH TABLE t USING PRIMARY KEY (pk=?) (~1 row)
''
2. Query (FILTERED STORAGE):
''
DELETE FROM "t" WHERE "t"."pk" = CAST(2 AS int)
''
plan:
    [0] SEARCH TABLE t USING PRIMARY KEY (pk=?) (~1 row)

-- TEST: block-delete-with-subquery-1
-- SQL:
do $$ BEGIN DELETE FROM t WHERE pk = (SELECT 1); END $$;
-- ERROR:
DELETE in transaction cannot have subqueries

-- TEST: update-with-subquery-1
-- SQL:
do $$ BEGIN UPDATE t SET b = (SELECT 1) WHERE pk = 1; END $$;
-- ERROR:
UPDATE in transaction cannot have subqueries

-- TEST: update-with-subquery-2
-- SQL:
do $$ BEGIN UPDATE t SET b = 1 WHERE pk = 1 AND a = (SELECT 1); END $$;
-- ERROR:
UPDATE in transaction cannot have subqueries

-- TEST: update-with-subquery-3
-- SQL:
do $$ BEGIN UPDATE t SET b = 1 FROM (SELECT 1) WHERE pk = 1; END $$;
-- ERROR:
UPDATE in transaction cannot have subqueries
