-- TEST: init
-- SQL:
CREATE TABLE g (id INT PRIMARY KEY) DISTRIBUTED GLOBALLY;
CREATE TABLE t (a INT PRIMARY KEY);


-- TEST: limit-should-come-after-union
-- SQL:
SELECT max(id) FROM g HAVING true UNION SELECT 1;
-- ERROR:
Failed to compile SQL statement: LIMIT clause should come after UNION not before

-- TEST: explain-limit-should-come-after-union
-- SQL:
EXPLAIN (RAW) SELECT max(id) FROM g HAVING true UNION SELECT 1;
-- EXPECTED:
╭───────────────────╮
│ 1. Query (ROUTER) │
╰───────────────────╯
''
SELECT max (CAST ("g"."id" as int)) as "col_1" FROM "g" HAVING CAST(true AS bool) UNION SELECT CAST(1 AS int) as "col_1"
''
plan:
Failed to compile SQL statement: LIMIT clause should come after UNION not before

-- TEST: misuse-of-aggregate-max
-- SQL:
SELECT * FROM g WHERE MAX(id) = 5;
-- ERROR:
Failed to compile SQL statement: misuse of aggregate function MAX()

-- TEST: explain-misuse-of-aggregate-max
-- SQL:
EXPLAIN (RAW) SELECT * FROM g WHERE MAX(id) = 5;
-- EXPECTED:
╭───────────────────╮
│ 1. Query (ROUTER) │
╰───────────────────╯
''
SELECT * FROM "g" WHERE max (CAST ("g"."id" as int)) = CAST(5 AS int)
''
plan:
Failed to compile SQL statement: misuse of aggregate function MAX()

-- TEST: execution-of-empty-query
-- SQL:
WITH a AS (SELECT 1 FROM t limit 1) SELECT 1 FROM t JOIN a ON (values (true));
-- ERROR:
Failed to compile SQL statement: Failed to execute an empty SQL statement

-- TEST: explain-execution-of-empty-query
-- SQL:
EXPLAIN (RAW) WITH a AS (SELECT 1 FROM t limit 1) SELECT 1 FROM t JOIN a ON (values (true));
-- EXPECTED:
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT CAST(1 AS int) as "col_1" FROM "t" LIMIT 1
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
VALUES (CAST(true AS bool))
''
plan:
    [0] TRIVIAL
''
╭───────────────────╮
│ 3. Query (ROUTER) │
╰───────────────────╯
''
SELECT "COL_0" FROM "_tmp_7934279834277496778_0136" LIMIT 1
''
plan:
    [0] SCAN TABLE _tmp_7934279834277496778_0136 (~1048576 rows)
''
╭───────────────────╮
│ 4. Query (ROUTER) │
╰───────────────────╯
''
''
''
plan:
Failed to compile SQL statement: Failed to execute an empty SQL statement
''
╭──────────────────────────╮
│ 5. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT CAST(1 AS int) as "col_1" FROM "t" INNER JOIN ( SELECT "COL_0" FROM "_tmp_7002119783055804240_2136" ) as "a" ON ( SELECT "COL_0" FROM "_tmp_7002119783055804240_3136" )
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
    [0] EXECUTE SCALAR SUBQUERY 1
    [1] SCAN TABLE _tmp_7002119783055804240_3136 (~1048576 rows)
        [0] SCAN TABLE _tmp_7002119783055804240_2136 (~1048576 rows)


-- TEST: intersect-without-braces
-- SQL:
SELECT 1 from t UNION SELECT 0 EXCEPT SELECT 1 from t;
-- ERROR:
Query 3 from EXPLAIN \(RAW\): Failed to compile SQL statement: Syntax error at line 1 near '\(\'

-- TEST: explain-intersect-without-braces
-- SQL:
EXPLAIN (RAW) SELECT 1 from t UNION SELECT 0 EXCEPT SELECT 1 from t;
-- EXPECTED:
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT CAST(1 AS int) as "col_1" FROM "t" UNION select cast(null as int) as "col_1" where false
''
plan:
    [1] SCAN TABLE t (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 2 USING TEMP B-TREE (UNION)
''
╭──────────────────────────╮
│ 2. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT CAST(1 AS int) as "col_1" FROM "t" UNION select cast(null as int) as "col_1" where false
''
plan:
    [1] SCAN TABLE t (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 2 USING TEMP B-TREE (UNION)
''
╭──────────────────────────╮
│ 3. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT CAST(1 AS int) as "col_1" FROM "t" INTERSECT ( SELECT "COL_0" FROM "_tmp_9162583575142203342_3136" )
''
plan:
Failed to compile SQL statement: Syntax error at line 1 near '('
''
╭───────────────────╮
│ 4. Query (ROUTER) │
╰───────────────────╯
''
SELECT "COL_0" FROM "_tmp_15197742904724328793_1136" EXCEPT SELECT "COL_0" FROM "_tmp_15197742904724328793_4136"
''
plan:
    [1] SCAN TABLE _tmp_15197742904724328793_1136 (~1048576 rows)
    [2] SCAN TABLE _tmp_15197742904724328793_4136 (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 2 USING TEMP B-TREE (EXCEPT)
