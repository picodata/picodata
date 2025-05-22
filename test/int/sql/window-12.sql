-- TEST: window12
-- SQL:
DROP TABLE IF EXISTS t6;
CREATE TABLE t6(x INT PRIMARY KEY, y INT);
INSERT INTO t6 VALUES(1, 2);
INSERT INTO t6 VALUES(3, 4);
INSERT INTO t6 VALUES(5, 6);
INSERT INTO t6 VALUES(7, 8);
INSERT INTO t6 VALUES(9, 10);

-- TEST: window12-1.1
-- SQL:
SELECT 1 + (sum(x) OVER (ORDER BY x))::int FROM t6;
-- EXPECTED:
2, 5, 10, 17, 26

-- TEST: window12-1.2
-- SQL:
SELECT (sum(y) OVER (ORDER BY x))::int + (sum(x) OVER (ORDER BY x))::int FROM t6;
-- EXPECTED:
3, 10, 21, 36, 55

-- TEST: window12-1.3
-- SQL:
SELECT (sum(x) OVER (ORDER BY x))::int * (count(y) OVER (ORDER BY x))::int FROM t6;
-- EXPECTED:
1, 8, 27, 64, 125

-- TEST: window12-2.1
-- SQL:
select count(x) over (partition by 1 + false) from t6;
-- ERROR:
sbroad: could not resolve operator overload for +(unsigned, bool)

-- TEST: window12-2.2
-- SQL:
SELECT count(x) OVER (PARTITION BY 'hello' - 3) FROM t6;
-- ERROR:
sbroad: could not resolve operator overload for -(text, unsigned)

-- TEST: window12-2.3
-- SQL:
SELECT count(*) OVER (ORDER BY current_date || 5) FROM t6;
-- ERROR:
sbroad: could not resolve operator overload for ||(datetime, unsigned)

-- TEST: window12-2.4
-- SQL:
SELECT count(*) OVER (ORDER BY '2025‑01‑01'::datetime - 'abc') FROM t6;
-- ERROR:
sbroad: could not resolve operator overload for -(datetime, text)

-- TEST: window12-2.5
-- SQL:
SELECT avg(x) FILTER (WHERE x > '2025‑01‑01'::datetime) OVER () FROM t6;
-- ERROR:
sbroad: could not resolve operator overload for >(int, datetime)

-- TEST: window12-2.6
-- SQL:
SELECT  sum(x)  OVER ( ORDER BY x ROWS BETWEEN 1 + '1' PRECEDING AND 1 FOLLOWING) from t6
-- ERROR:
sbroad: could not resolve operator overload for +(unsigned, text)

-- TEST: window12-3.1
-- SQL:
SELECT
    row_number() OVER win,
    sum(y) OVER win,
    max(x) OVER win
FROM t6
WINDOW win AS (
    ORDER BY x + (SELECT 1)
);
-- EXPECTED:
1, 2, 1, 2, 6, 3, 3, 12, 5, 4, 20, 7, 5, 30, 9

-- TEST: window12-3.2
-- SQL:
SELECT
    row_number() OVER win,
    sum(y) OVER win,
    max(x) OVER win
FROM t6
WINDOW win AS (
    ORDER BY x + (SELECT 1) + (SELECT 2)
);
-- EXPECTED:
1, 2, 1, 2, 6, 3, 3, 12, 5, 4, 20, 7, 5, 30, 9

-- TEST: window12-3.3
-- SQL:
SELECT
    row_number() OVER win,
    sum(y) OVER win AS b,
    max(x) OVER win AS c
FROM t6
WINDOW win AS (
    PARTITION BY x + (SELECT 1)
)
ORDER BY b, c;
-- EXPECTED:
1, 2, 1, 1, 4, 3, 1, 6, 5, 1, 8, 7, 1, 10, 9

-- TEST: window12-3.4
-- SQL:
WITH t AS (
    SELECT count(*) OVER win
    FROM (SELECT 1)
    WINDOW win AS (PARTITION BY (SELECT 1))
)
SELECT count(*) OVER ()
FROM (SELECT 1)
WINDOW win AS (ORDER BY 1);
-- EXPECTED:
1

-- TEST: window12-3.5
-- SQL:
EXPLAIN
SELECT 
    avg(x) OVER win1,
    sum(x) OVER win
FROM t6
WINDOW
    win AS (ORDER BY y + 2 * (SELECT 111) + (SELECT 2)),
    win1 AS (PARTITION BY x + (SELECT 3));
-- EXPECTED:
projection (avg("x"::integer) over win1 -> "col_1", sum("x"::integer) over win -> "col_2")
    windows: win1 as (partition by (ROW("x"::integer) + ROW($0)) ), win as (order by ((ROW("y"::integer) + (ROW(2::unsigned) * ROW($2))) + ROW($1)) )
        motion [policy: full]
            projection ("t6"."x"::integer -> "x", "t6"."bucket_id"::unsigned -> "bucket_id", "t6"."y"::integer -> "y")
                scan "t6"
subquery $0:
scan
        projection (3::unsigned -> "col_1")
subquery $1:
scan
        projection (2::unsigned -> "col_1")
subquery $2:
scan
        projection (111::unsigned -> "col_1")
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: window12-3.6
-- SQL:
EXPLAIN
SELECT 
    row_number() OVER win2,
    sum(y) OVER win2,
    max(x) OVER win3
FROM t6
WINDOW
    win1 AS (ORDER BY x + (SELECT 1)),
    win2 AS (PARTITION BY x + (SELECT 2)),
    win3 AS (ORDER BY x + (
        SELECT count(*) OVER win_nested
        FROM (SELECT * FROM t6 LIMIT 1)
        WINDOW win_nested AS (
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        )
    )::int);
-- EXPECTED:
projection (row_number() over win2 -> "col_1", sum("y"::integer) over win2 -> "col_2", max("x"::integer) over win3 -> "col_3")
    windows: win2 as (partition by (ROW("x"::integer) + ROW($1)) ), win3 as (order by (ROW("x"::integer) + ROW(ROW($0)::int)) )
        motion [policy: full]
            projection ("t6"."x"::integer -> "x", "t6"."bucket_id"::unsigned -> "bucket_id", "t6"."y"::integer -> "y")
                scan "t6"
subquery $0:
motion [policy: full]
        scan
            projection (count(*::integer) over win_nested -> "col_1")
                windows: win_nested as (rows between current row and unbounded following)
                    scan
                        limit 1
                            motion [policy: full]
                                limit 1
                                    projection ("t6"."x"::integer -> "x", "t6"."y"::integer -> "y")
                                        scan "t6"
subquery $1:
scan
        projection (2::unsigned -> "col_1")
subquery $2:
scan
        projection (1::unsigned -> "col_1")
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]
