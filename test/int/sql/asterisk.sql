-- TEST: init
-- SQL:
CREATE TABLE t1 (a INT PRIMARY KEY, b INT) DISTRIBUTED BY (b);
CREATE TABLE t2 (a INT PRIMARY KEY, b INT);
INSERT INTO t1 VALUES (1, 2), (2, 3), (3, 4), (4, 9), (5, 5);
INSERT INTO t2 VALUES (9, 8), (7, 6), (2, 1);

-- TEST: cte-row-number-order-by
-- SQL:
WITH base AS (SELECT a FROM t1 WHERE b = 9)
SELECT *, ROW_NUMBER() OVER(ORDER BY a) AS CURSOR FROM base;
-- EXPECTED:
4, 1

-- TEST: cte-row-number
-- SQL:
WITH base AS (SELECT a FROM t1 WHERE b = 9)
SELECT *, ROW_NUMBER() OVER() AS CURSOR FROM base;
-- EXPECTED:
4, 1

-- TEST: cte-simple
-- SQL:
WITH base AS (SELECT a FROM t1 WHERE b = 9)
SELECT * FROM base;
-- EXPECTED:
4

-- TEST: cte-dot-simple
-- SQL:
WITH base AS (SELECT a FROM t1 WHERE b = 9)
SELECT base.* FROM base;
-- EXPECTED:
4

-- TEST: cte-dot-row-number-order-by
-- SQL:
WITH base AS (SELECT a FROM t1 WHERE b = 9)
SELECT base.*, ROW_NUMBER() OVER(ORDER BY a) AS CURSOR FROM base;
-- EXPECTED:
4, 1

-- TEST: cte-dot-row-number
-- SQL:
WITH base AS (SELECT a FROM t1 WHERE b = 9)
SELECT base.*, ROW_NUMBER() OVER() AS CURSOR FROM base;
-- EXPECTED:
4, 1

-- TEST: cte-multi-col-dot-row-number
-- SQL:
WITH base AS (SELECT a, b FROM t1 WHERE b = 9)
SELECT base.*, ROW_NUMBER() OVER() AS CURSOR FROM base;
-- EXPECTED:
4, 9, 1

-- TEST: cte-multi-col-dot-row-number-order-by
-- SQL:
WITH base AS (SELECT a, b FROM t1 WHERE b = 9)
SELECT base.*, ROW_NUMBER() OVER(ORDER BY a) AS CURSOR FROM base;
-- EXPECTED:
4, 9, 1

-- TEST: cte-renamed-col-dot-row-number
-- SQL:
WITH base(x) AS (SELECT a FROM t1 WHERE b = 9)
SELECT base.*, ROW_NUMBER() OVER() AS CURSOR FROM base;
-- EXPECTED:
4, 1

-- TEST: cte-renamed-col-dot-row-number-order-by
-- SQL:
WITH base(x) AS (SELECT a FROM t1 WHERE b = 9)
SELECT base.*, ROW_NUMBER() OVER(ORDER BY x) AS CURSOR FROM base;
-- EXPECTED:
4, 1

-- TEST: cte-dot-and-expression
-- SQL:
WITH base AS (SELECT a FROM t1 WHERE b = 9)
SELECT base.*, base.a + 10 AS SHIFTED, ROW_NUMBER() OVER() AS CURSOR FROM base;
-- EXPECTED:
4, 14, 1

-- TEST: cte-dot-partition-by-non-shard-key
-- SQL:
WITH base AS (SELECT a, b FROM t1 WHERE b = 9)
SELECT base.*, ROW_NUMBER() OVER(PARTITION BY a) AS CURSOR FROM base;
-- EXPECTED:
4, 9, 1

-- TEST: cte-dot-multiple-windows
-- SQL:
WITH base AS (SELECT a FROM t1 WHERE b = 9)
SELECT base.*, ROW_NUMBER() OVER() AS R1, ROW_NUMBER() OVER(ORDER BY a) AS R2 FROM base;
-- EXPECTED:
4, 1, 1

-- TEST: subquery-dot-simple
-- SQL:
SELECT s.* FROM (SELECT a FROM t1 WHERE b = 9) AS s;
-- EXPECTED:
4

-- TEST: subquery-dot-row-number
-- SQL:
SELECT s.*, ROW_NUMBER() OVER() AS CURSOR
FROM (SELECT a FROM t1 WHERE b = 9) AS s;
-- EXPECTED:
4, 1

-- TEST: subquery-dot-row-number-order-by
-- SQL:
SELECT s.*, ROW_NUMBER() OVER(ORDER BY a) AS CURSOR
FROM (SELECT a FROM t1 WHERE b = 9) AS s;
-- EXPECTED:
4, 1

-- TEST: subquery-multi-col-dot-row-number
-- SQL:
SELECT s.*, ROW_NUMBER() OVER() AS CURSOR
FROM (SELECT a, b FROM t1 WHERE b = 9) AS s;
-- EXPECTED:
4, 9, 1

-- TEST: cte-dot-other-1
-- SQL:
WITH base AS (SELECT a FROM t1 WHERE b = 9)
SELECT t1.* FROM base;
-- ERROR:
sbroad: table 't1' not found

-- TEST: cte-dot-other-2
-- SQL:
WITH base AS (SELECT a FROM t1 WHERE b = 9)
SELECT t2.* FROM base;
-- ERROR:
sbroad: table 't2' not found

-- TEST: cte-join-table-dot-row-number
-- SQL:
WITH base AS (SELECT a FROM t1 WHERE b = 9)
SELECT base.*, t1.b, ROW_NUMBER() OVER() AS CURSOR
FROM base JOIN t1 ON base.a = t1.a;
-- EXPECTED:
4, 9, 1

-- TEST: cte-self-join-dot-row-number
-- SQL:
WITH base AS (SELECT a FROM t1 WHERE b = 9)
SELECT b1.*, b2.*, ROW_NUMBER() OVER() AS CURSOR
FROM base AS b1 JOIN base AS b2 ON b1.a = b2.a;
-- EXPECTED:
4, 4, 1

-- TEST: subquery-join-table-dot-row-number
-- SQL:
SELECT s.*, t1.b, ROW_NUMBER() OVER() AS CURSOR
FROM (SELECT a FROM t1 WHERE b = 9) AS s JOIN t1 ON s.a = t1.a;
-- EXPECTED:
4, 9, 1

-- TEST: cte-join-table-multi-dot-row-number
-- SQL:
WITH base AS (SELECT a FROM t1 WHERE b = 9)
SELECT base.*, t1.*, ROW_NUMBER() OVER() AS CURSOR
FROM base JOIN t1 ON base.a = t1.a;
-- EXPECTED:
4, 4, 9, 1

-- TEST: table-join-table-dot-row-number
-- SQL:
SELECT t1.*, ROW_NUMBER() OVER() AS CURSOR
FROM t1 JOIN t2 ON t1.a = t2.a;
-- EXPECTED:
2, 3, 1

-- TEST: table-join-table-multi-dot-row-number
-- SQL:
SELECT t1.*, t2.*, ROW_NUMBER() OVER() AS CURSOR
FROM t1 JOIN t2 ON t1.a = t2.a;
-- EXPECTED:
2, 3, 2, 1, 1

-- TEST: subquery-join-subquery-dot-row-number
-- SQL:
SELECT s1.*, s2.b, ROW_NUMBER() OVER() AS CURSOR
FROM (SELECT a FROM t1 WHERE a = 2) AS s1
JOIN (SELECT a, b FROM t2) AS s2 ON s1.a = s2.a;
-- EXPECTED:
2, 1, 1

-- TEST: filter-dot-row-number
-- SQL:
SELECT t1.*, ROW_NUMBER() OVER() AS CURSOR FROM t1 WHERE a = 4;
-- EXPECTED:
4, 9, 1
