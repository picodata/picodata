-- TEST: window12-init-1
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
sbroad: could not resolve operator overload for \+\(int, bool\)

-- TEST: window12-2.2
-- SQL:
SELECT count(x) OVER (PARTITION BY 'hello'::text - 3) FROM t6;
-- ERROR:
sbroad: could not resolve operator overload for -\(text, int\)

-- TEST: window12-2.3
-- SQL:
SELECT count(*) OVER (ORDER BY current_date || 5) FROM t6;
-- ERROR:
sbroad: could not resolve operator overload for ||(datetime, int)

-- TEST: window12-2.4
-- SQL:
SELECT count(*) OVER (ORDER BY '2025‑01‑01'::datetime - 'abc') FROM t6;
-- ERROR:
sbroad: could not resolve operator overload for -\(datetime, text\)

-- TEST: window12-2.5
-- SQL:
SELECT avg(x) FILTER (WHERE x > '2025‑01‑01'::datetime) OVER () FROM t6;
-- ERROR:
sbroad: could not resolve operator overload for >\(int, datetime\)

-- TEST: window12-2.6
-- SQL:
SELECT  sum(x)  OVER ( ORDER BY x ROWS BETWEEN 1 + false PRECEDING AND 1 FOLLOWING) from t6;
-- ERROR:
sbroad: could not resolve operator overload for \+\(int, bool\)

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
projection (avg("x"::int::int) over (partition by ("x"::int + ROW($0)) ) -> "col_1", sum("x"::int::int) over (order by (("y"::int + (2::int * ROW($2))) + ROW($1)) ) -> "col_2")
    motion [policy: full, program: ReshardIfNeeded]
        projection ("t6"."x"::int -> "x", "t6"."y"::int -> "y")
            scan "t6"
subquery $0:
scan
        projection (3::int -> "col_1")
subquery $1:
scan
        projection (2::int -> "col_1")
subquery $2:
scan
        projection (111::int -> "col_1")
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
projection (row_number() over (partition by ("x"::int + ROW($1)) ) -> "col_1", sum("y"::int::int) over (partition by ("x"::int + ROW($1)) ) -> "col_2", max("x"::int::int) over (order by ("x"::int + ROW($0)::int) ) -> "col_3")
    motion [policy: full, program: ReshardIfNeeded]
        projection ("t6"."x"::int -> "x", "t6"."y"::int -> "y")
            scan "t6"
subquery $0:
motion [policy: full, program: ReshardIfNeeded]
        scan
            projection (count(*::int) over (rows between current row and unbounded following) -> "col_1")
                scan "unnamed_subquery"
                    limit 1
                        motion [policy: full, program: ReshardIfNeeded]
                            limit 1
                                projection ("t6"."x"::int -> "x", "t6"."y"::int -> "y")
                                    scan "t6"
subquery $1:
scan
        projection (2::int -> "col_1")
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: window12-3.7
-- SQL:
explain select 1 from t6 window w as (partition by (select 1 from t6 window w as ()));
-- EXPECTED:
projection (1::int -> "col_1")
    scan "t6"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: window12-4-init
-- SQL:
DROP TABLE IF EXISTS t_win;
CREATE TABLE t_win(a INT PRIMARY KEY, b INT, c INT);
INSERT INTO t_win VALUES(1, 10, 100);
INSERT INTO t_win VALUES(2, 20, 200);
INSERT INTO t_win VALUES(3, 30, 300);
INSERT INTO t_win VALUES(4, 40, 400);
INSERT INTO t_win VALUES(5, 50, 500);

-- TEST: window12-4.1
-- SQL:
SELECT max(a) OVER w FROM t_win WINDOW w AS () 
EXCEPT 
SELECT max(1) OVER w ORDER BY 1;
-- ERROR:
sbroad: invalid expression: Window with name w not found

-- TEST: window12-4.2
-- SQL:
SELECT * FROM (
    SELECT row_number() OVER w FROM t_win WINDOW w AS (ORDER BY a)
) AS sub
WHERE EXISTS (
    SELECT 1 FROM t_win WINDOW w AS (ORDER BY b DESC)
);
-- EXPECTED:
1, 2, 3, 4, 5

-- TEST: window12-4.3
-- SQL:
SELECT a, (SELECT max(b) OVER w FROM t_win) 
FROM t_win 
WINDOW w AS (PARTITION BY a);
-- ERROR:
sbroad: invalid expression: Window with name w not found

-- TEST: window12-4.4
-- SQL:
SELECT count(*) OVER w 
FROM t_win AS w 
WINDOW w AS (ORDER BY w.a);
-- EXPECTED:
1, 2, 3, 4, 5

-- TEST: window12-4.5
-- SQL:
SELECT sum(a) OVER w FROM t_win WINDOW w AS (ORDER BY a)
UNION ALL
SELECT sum(b) OVER w FROM t_win WINDOW w AS (ORDER BY b DESC);
-- EXPECTED:
1, 3, 6, 10, 15, 50, 90, 120, 140, 150

-- TEST: window12-4.6
-- SQL:
SELECT count(*) OVER w 
FROM t_win 
WINDOW w AS (PARTITION BY (SELECT max(a) OVER w2 FROM t_win WINDOW w2 AS ()));
-- ERROR:
Failed to execute SQL statement: Expression subquery returned more than 1 row

-- TEST: window12-4.7
-- SQL:
WITH cte AS (
    SELECT a, sum(b) OVER w AS sum_b 
    FROM t_win 
    WINDOW w AS (PARTITION BY a)
)
SELECT *, avg(sum_b) OVER w 
FROM cte 
WINDOW w AS (ORDER BY a);
-- EXPECTED:
1, 10, 10.0, 2, 20, 15.0, 3, 30, 20.0, 4, 40, 25.0, 5, 50, 30.0

-- TEST: window12-4.8
-- SQL:
SELECT 1 FROM t_win 
WINDOW w AS (PARTITION BY (SELECT 1 FROM t_win WINDOW w AS ()));
-- EXPECTED:
1, 1, 1, 1, 1

-- TEST: window12-4.9
-- SQL:
SELECT 
    row_number() OVER w1,
    row_number() OVER w2 
FROM t_win 
WINDOW w1 AS (ORDER BY a),
       w2 AS (ORDER BY a);
-- EXPECTED:
1, 1, 2, 2, 3, 3, 4, 4, 5, 5

-- TEST: window12-4.10
-- SQL:
WITH cte AS (
    SELECT * FROM t_win WINDOW w AS (ORDER BY a)
)
SELECT sum(a) OVER w FROM cte;
-- ERROR:
sbroad: invalid expression: Window with name w not found

-- TEST: window12-4.11
-- SQL:
SELECT DISTINCT a, count(*) OVER w 
FROM t_win 
GROUP BY a 
WINDOW w AS (ORDER BY a);
-- EXPECTED:
1, 1, 2, 2, 3, 3, 4, 4, 5, 5

-- TEST: window12-4.12
-- SQL:
SELECT 
    CASE 
        WHEN row_number() OVER w = 1 THEN 'first'
        WHEN row_number() OVER w = count(*) OVER w THEN 'last'
        ELSE 'middle'
    END 
FROM t_win 
WINDOW w AS (ORDER BY a);
-- EXPECTED:
'first', 'last', 'last', 'last', 'last'

-- TEST: window12-4.13
-- SQL:
SELECT sum(a) OVER w as s FROM t_win WINDOW w AS (ORDER BY a ASC)
UNION
SELECT sum(a) OVER w as s FROM t_win WINDOW w AS (ORDER BY a DESC)
ORDER BY s;
-- EXPECTED:
1, 3, 5, 6, 9, 10, 12, 14, 15

-- TEST: window12-4.14
-- SQL:
SELECT row_number() OVER user 
FROM t_win 
WINDOW user AS (ORDER BY a);
-- EXPECTED:
1, 2, 3, 4, 5

-- TEST: window12-4.15
-- SQL:
SELECT max(1) over w FROM t_win
WINDOW w AS (PARTITION BY (SELECT a from t_win limit 1));
-- EXPECTED:
1, 1, 1, 1, 1

-- TEST: window12-5-init
-- SQL:
DROP TABLE IF EXISTS t1;
CREATE TABLE t1(a INT PRIMARY KEY, b INT, c INT);
DROP TABLE IF EXISTS t2;
CREATE TABLE t2(a INT PRIMARY KEY, b INT, c INT);
DROP TABLE IF EXISTS g1;
CREATE TABLE g1(a INT PRIMARY KEY, b INT, c INT);

-- TEST: window12-5.1
-- SQL:
select t1.a from
	t1
	left join t2 on (
		select a from t1 limit 1
	) > 0
	group by t1.a
;
-- EXPECTED:

-- TEST: window12-5.2
-- SQL:
SELECT sum(-13) AS c0, 11 AS c1, t3.b AS c2, t3.c AS c3 FROM t2 AS t0 LEFT JOIN t1 AS t1 ON t0.c > t1.b JOIN t1 AS t2 ON t2.c = t1.c JOIN t2 AS t3 ON (SELECT t2.a AS c0 FROM t1 AS t0 LEFT JOIN g1 AS t1 ON t0.b = t1.a JOIN g1 AS t2 ON t1.a = t2.a LEFT JOIN t2 AS t3 ON t3.a = t2.a WHERE t0.b = t0.a AND t2.c >= t2.a UNION SELECT max(t0.a) AS c0 FROM t1 AS t0 LEFT JOIN g1 AS t1 ON t0.b = t1.b JOIN g1 AS t2 ON t2.c = t1.c WHERE t1.b <> t1.b OR t1.c < t1.c HAVING 15 < avg(t0.c) OR count(t0.c) = 5961600 WINDOW win0 AS (PARTITION BY t1.a, t2.a ORDER BY t1.a), win1 AS (PARTITION BY t0.a ORDER BY t1.c DESC, t1.c DESC), win2 AS (PARTITION BY (SELECT t0.a AS c0 FROM t2 AS t0 WHERE t0.c < t0.a OR t0.a = t0.b OR t0.a NOT BETWEEN t0.c AND t0.a ORDER BY c0 DESC LIMIT 1), count(t0.b), max(9), t0.a ORDER BY t0.b ASC, t0.c ASC, t0.a ASC, t2.c ASC, t0.a ASC) ORDER BY c0 ASC LIMIT 1) < t3.b WHERE t0.a = t1.a OR t3.b <= t3.b GROUP BY t3.c, t3.b HAVING max(DISTINCT t3.a) = 11 ORDER BY c3, c1 ASC, c0 DESC, c2 ASC;
-- EXPECTED:
