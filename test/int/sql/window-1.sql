-- TEST: window1
-- SQL:
DROP TABLE IF EXISTS t1;
CREATE TABLE t1(a INT PRIMARY KEY, b INT, c INT, d INT);
INSERT INTO t1 VALUES(1, 2, 3, 4);
INSERT INTO t1 VALUES(5, 6, 7, 8);
INSERT INTO t1 VALUES(9, 10, 11, 12);

-- TEST: window1-1.1
-- SQL:
SELECT COL_1 FROM (SELECT a, sum(b) OVER () FROM t1 ORDER BY a);
-- EXPECTED:
18, 18, 18

-- TEST: window1-1.2
-- SQL:
SELECT a, sum(b) OVER () FROM t1 ORDER BY a;
-- EXPECTED:
1, 18, 5, 18, 9, 18

-- TEST: window1-1.5
-- SQL:
SELECT a, sum(b) OVER (PARTITION BY c) FROM t1 ORDER BY a;
-- EXPECTED:
1, 2, 5, 6, 9, 10

-- TEST: window1-2.1
-- SQL:
SELECT COL_1 FROM (SELECT a, sum(b) OVER (PARTITION BY c) FROM t1 ORDER BY a);
-- EXPECTED:
2, 6, 10

-- TEST: window1-2.2
-- SQL:
SELECT COL_1 FROM (SELECT a, sum(b) OVER (ORDER BY c) FROM t1 ORDER BY a);
-- EXPECTED:
2, 8, 18

-- TEST: window1-2.3
-- SQL:
SELECT COL_1 FROM (SELECT a, sum(b) OVER (PARTITION BY d ORDER BY c) FROM t1 ORDER BY a);
-- EXPECTED:
2, 6, 10

-- TEST: window1-2.4
-- SQL:
SELECT COL_1 FROM (SELECT a, sum(b) FILTER (WHERE a>0) OVER (PARTITION BY d ORDER BY c)
FROM t1 ORDER BY a);
-- EXPECTED:
2, 6, 10

-- TEST: window1-2.5
-- SQL:
SELECT COL_1 FROM (SELECT a, sum(b) OVER (ORDER BY c RANGE UNBOUNDED PRECEDING)
FROM t1 ORDER BY a);
-- EXPECTED:
2, 8, 18

-- TEST: window1-2.6
-- SQL:
SELECT COL_1 FROM (SELECT a, sum(b) OVER (ORDER BY c ROWS 45 PRECEDING) FROM t1
ORDER BY a);
-- EXPECTED:
2, 8, 18

-- TEST: window1-2.7
-- SQL:
SELECT COL_1 FROM (SELECT a, sum(b) OVER (ORDER BY c RANGE CURRENT ROW) FROM t1
ORDER BY a);
-- EXPECTED:
2, 6, 10

-- TEST: window1-2.8
-- SQL:
SELECT COL_1 FROM (SELECT a, sum(b) OVER (ORDER BY c RANGE BETWEEN UNBOUNDED PRECEDING
AND CURRENT ROW) FROM t1 ORDER BY a);
-- EXPECTED:
2, 8, 18

-- TEST: window1-2.9
-- SQL:
SELECT COL_1 FROM (SELECT a, sum(b) OVER (ORDER BY c ROWS BETWEEN UNBOUNDED PRECEDING
AND UNBOUNDED FOLLOWING) FROM t1 ORDER BY a);
-- EXPECTED:
18, 18, 18

-- TEST: window1-3.1
-- SQL:
SELECT * FROM t1 WHERE sum(b) OVER ();
-- ERROR:
filter expression is not a trivalent expression

-- TEST: window1-3.2
-- SQL:
SELECT * FROM t1 GROUP BY sum(b) OVER ();
-- ERROR:
misuse of window function SUM()

-- TEST: window1-3.3
-- SQL:
SELECT * FROM t1 GROUP BY a HAVING sum(b) OVER ();
-- ERROR:
filter expression is not a trivalent expression

-- TEST: window1-4.1
-- SQL:
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (
	a int primary key,
	b int,
	c int
) distributed by (b);
INSERT INTO t1 VALUES (1, 2, 1), (2, 2, 2), (3, 3, 3), (4, 3, 4), (5, 4, 5);
DROP TABLE IF EXISTS t2;
CREATE TABLE t2 (
	a int primary key,
	b int,
	c int
) distributed by (b);
INSERT INTO t2 VALUES (1, 3, 1), (2, 3, 2), (3, 2, 3), (4, 2, 4), (5, 4, 5);

-- TEST: window1-4.2
-- SQL:
EXPLAIN (RAW, FMT)
SELECT
	t1.b,
	t1.c,
	t2.c,
	ROW_NUMBER() OVER (
		PARTITION BY
			t1.b
		ORDER BY
			t1.c
	) AS rn
FROM
	t1 JOIN t2 ON t1.b = t2.b;
-- EXPECTED:
1. Query (STORAGE):
SELECT
  "t1"."b",
  "t1"."c",
  "t2"."c",
  row_number () OVER (
    PARTITION BY
      "t1"."b"
    ORDER BY
      "t1"."c" ASC
  ) as "rn"
FROM
  "t1"
  INNER JOIN "t2" ON "t1"."b" = "t2"."b"
+----------+-------+------+-------------------------------+
| selectid | order | from | detail                        |
+=========================================================+
| 1        | 0     | 0    | SCAN TABLE t1 (~1048576 rows) |
|----------+-------+------+-------------------------------|
| 1        | 1     | 1    | SCAN TABLE t2 (~1048576 rows) |
|----------+-------+------+-------------------------------|
| 1        | 0     | 0    | USE TEMP B-TREE FOR ORDER BY  |
|----------+-------+------+-------------------------------|
| 0        | 0     | 0    | SCAN SUBQUERY 1 (~1 row)      |
+----------+-------+------+-------------------------------+
''