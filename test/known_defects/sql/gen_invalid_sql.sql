-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

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
SELECT max ("g"."id"::int) as "col_1" FROM "g" HAVING true::bool UNION SELECT 1::int as "col_1"
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
SELECT * FROM "g" WHERE max ("g"."id"::int) = 5::int
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
SELECT 1::int as "col_1" FROM "t" LIMIT 1
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
VALUES (true::bool)
''
plan:
    [0] TRIVIAL
''
╭───────────────────╮
│ 3. Query (ROUTER) │
╰───────────────────╯
''
SELECT "COL_0" FROM "_tmp_6594175747005374850_0136" LIMIT 1
''
plan:
    [0] SCAN TABLE _tmp_6594175747005374850_0136 (~1048576 rows)
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
SELECT 1::int as "col_1" FROM "t" INNER JOIN "_tmp_1202446835763270938_2136" as "a" ON ( SELECT "COL_0" FROM "_tmp_1202446835763270938_3136" )
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
    [0] EXECUTE SCALAR SUBQUERY 1
    [1] SCAN TABLE _tmp_1202446835763270938_3136 (~1048576 rows)
        [0] SCAN TABLE _tmp_1202446835763270938_2136 AS a (~1048576 rows)
