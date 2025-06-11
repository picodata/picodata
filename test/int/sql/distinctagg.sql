-- TEST: init
-- SQL:
CREATE TABLE t1(pk INT PRIMARY KEY, a INT ,b INT ,c INT);
INSERT INTO t1 VALUES(1, 1,2,3);
INSERT INTO t1 VALUES(2, 1,3,4);
INSERT INTO t1 VALUES(3, 1,3,5);

-- TEST: distinctagg-1.1
-- SQL:
SELECT count(distinct a),
        count(distinct b),
        count(distinct c),
        count(a) FROM t1;
-- EXPECTED:
1, 2, 3, 3

-- TEST: distinctagg-1.2
-- SQL:
SELECT b, count(distinct c) FROM t1 GROUP BY b
-- EXPECTED:
2, 1, 3, 2

-- TEST: init
-- SQL:
INSERT INTO t1 SELECT pk * 10, a+1, b+3, c+5 FROM t1;
INSERT INTO t1 SELECT pk * 101, a+2, b+6, c+10 FROM t1;
INSERT INTO t1 SELECT pk * 1002, a+4, b+12, c+20 FROM t1;

-- TEST: distinctagg-1.3
-- SQL:
SELECT count(*), count(distinct a), count(distinct b) FROM t1
-- EXPECTED:
24, 8, 16

-- TEST: distinctagg-1.4
-- SQL:
SELECT a, count(distinct c) FROM t1 GROUP BY a ORDER BY a
-- EXPECTED:
1, 3, 2, 3, 3, 3, 4, 3, 5, 3, 6, 3, 7, 3, 8, 3

-- TEST: distinctagg-2.1
-- SQL:
SELECT count(distinct) FROM t1;
-- ERROR:
expected Expr or CountAsterisk

-- TEST: distinctagg-2.2
-- SQL:
SELECT string_agg(distinct a,b) FROM t1;
-- ERROR:
GROUP_CONCAT aggregate function has only one argument. Got: 2 arguments

-- TEST: distinctagg-3.0
-- SQL:
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1(pk INT PRIMARY KEY, a INT, b INT, c INT);
CREATE TABLE t2(d TEXT PRIMARY KEY, e TEXT , f TEXT);
INSERT INTO t1 VALUES (1, 1, 1, 1);
INSERT INTO t1 VALUES (2, 2, 2, 2);
INSERT INTO t1 VALUES (3, 3, 3, 3);
INSERT INTO t1 VALUES (4, 4, 1, 4);
INSERT INTO t1 VALUES (5, 5, 2, 1);
INSERT INTO t1 VALUES (6, 5, 3, 2);
INSERT INTO t1 VALUES (7, 4, 1, 3);
INSERT INTO t1 VALUES (8, 3, 2, 4);
INSERT INTO t1 VALUES (9, 2, 3, 1);
INSERT INTO t1 VALUES (10, 1, 1, 2);
INSERT INTO t2 VALUES('a', 'a', 'a');
INSERT INTO t2 VALUES('b', 'b', 'b');
INSERT INTO t2 VALUES('c', 'c', 'c');

-- TEST: distinctagg-3.1
-- SQL:
SELECT count(DISTINCT a) FROM t1;
-- EXPECTED:
5

-- TEST: distinctagg-3.2
-- SQL:
SELECT count(DISTINCT b) FROM t1;
-- EXPECTED:
3

-- TEST: distinctagg-3.3
-- SQL:
SELECT count(DISTINCT c) FROM t1;
-- EXPECTED:
4

-- TEST: distinctagg-3.4
-- SQL:
SELECT count(DISTINCT c) FROM t1 WHERE b=3;
-- EXPECTED:
3

-- TEST: distinctagg-3.6
-- SQL:
SELECT count(DISTINCT a) FROM t1 JOIN t2 ON TRUE
-- EXPECTED:
5

-- TEST: distinctagg-3.7
-- SQL:
SELECT count(DISTINCT a) FROM t2 JOIN t1 ON TRUE
-- EXPECTED:
5

-- TEST: distinctagg-3.8
-- SQL:
SELECT count(DISTINCT a+b) FROM t1 JOIN t2 ON TRUE JOIN t2 ON TRUE JOIN t2 ON TRUE
-- EXPECTED:
6

-- TEST: distinctagg-3.9
-- SQL:
SELECT count(DISTINCT c) FROM t1 WHERE c=2
-- EXPECTED:
1

-- TEST: distinctagg-3.10
-- SQL:
SELECT a, count(DISTINCT b) FROM t1 GROUP BY a;
-- UNORDERED:
1, 1,
2, 2,
3, 2,
4, 1,
5, 2

-- TEST: distinctagg-4.0
-- SQL:
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;
CREATE TABLE t1(pk INT PRIMARY KEY, a INT NULL, b TEXT , c INT NULL);
INSERT INTO t1 VALUES(1, 1, 'A', 1);
INSERT INTO t1 VALUES(2, 1, 'A', 1);
INSERT INTO t1 VALUES(3, 2, 'A', 2);
INSERT INTO t1 VALUES(4, 2, 'A', 2);
INSERT INTO t1 VALUES(5, 1, 'B', 1);
INSERT INTO t1 VALUES(6, 2, 'B', 2);
INSERT INTO t1 VALUES(7, 3, 'B', 3);
INSERT INTO t1 VALUES(8, NULL, 'B', NULL);
INSERT INTO t1 VALUES(9, NULL, 'C', NULL);
INSERT INTO t1 VALUES(10, 4, 'D', 4);
CREATE TABLE t2(pk INT PRIMARY KEY, d INT, e INT, f TEXT);
INSERT INTO t2 VALUES(1, 1, 1, 'a');
INSERT INTO t2 VALUES(2, 1, 1, 'a');
INSERT INTO t2 VALUES(3, 1, 2, 'a');
INSERT INTO t2 VALUES(4, 1, 2, 'a');
INSERT INTO t2 VALUES(5, 1, 2, 'b');
INSERT INTO t2 VALUES(6, 1, 3, 'b');
INSERT INTO t2 VALUES(7, 1, 3, 'a');
INSERT INTO t2 VALUES(8, 1, 3, 'b');
INSERT INTO t2 VALUES(9, 2, 3, 'x');
INSERT INTO t2 VALUES(10, 2, 3, 'y');
INSERT INTO t2 VALUES(11, 2, 3, 'z');
CREATE TABLE t3(x INT PRIMARY KEY, y INT, z INT);
INSERT INTO t3 VALUES(1,1,1);
INSERT INTO t3 VALUES(2,2,2);
CREATE TABLE t4(pk INT PRIMARY KEY, a INT);
INSERT INTO t4 VALUES(1, 1), (2, 2), (3, 2), (4, 3), (5, 1);

-- TEST: distinctagg-4.1
-- SQL:
SELECT count(DISTINCT c) AS count FROM t1 GROUP BY b
-- UNORDERED:
0,
1,
2,
3

-- TEST: distinctagg-4.2
-- SQL:
SELECT count(DISTINCT a) AS count FROM t1 GROUP BY b
-- UNORDERED:
0,
1,
2,
3


-- TEST: distinctagg-4.4
-- SQL:
SELECT count(DISTINCT f) FROM t2 GROUP BY d, e
-- UNORDERED:
1,
2,
2,
3

-- TEST: distinctagg-4.5
-- SQL:
SELECT count(DISTINCT f) FROM t2 GROUP BY d
-- UNORDERED:
2, 3

-- TEST: distinctagg-4.6
-- TBD: https://git.picodata.io/core/picodata/-/issues/1928
SELECT count(DISTINCT f) FROM t2 WHERE d IS 1 GROUP BY e
-- EXPECTED:
1, 2, 2

-- TEST: distinctagg-4.7
-- SQL:
SELECT count(DISTINCT a) FROM t1
-- EXPECTED:
4

-- TEST: distinctagg-4.8
-- SQL:
SELECT count(DISTINCT a) FROM t4
-- EXPECTED:
3

-- TEST: distinctagg-5.1
-- SQL:
SELECT count(*) FROM t3;
-- EXPECTED:
2

-- TEST: distinctagg-5.2
-- SQL:
SELECT count(*) FROM t1;
-- EXPECTED:
10

-- TEST: distinctagg-5.3
-- SQL:
SELECT count(DISTINCT a) FROM t1 JOIN t3 ON TRUE;
-- EXPECTED:
4

-- TEST: distinctagg-5.4
-- SQL:
SELECT count(DISTINCT a) FROM t1 LEFT JOIN t3 ON TRUE;
-- EXPECTED:
4

-- TEST: distinctagg-5.5
-- SQL:
SELECT count(DISTINCT a) FROM t1 LEFT JOIN t3 ON TRUE WHERE t3.x=1;
-- EXPECTED:
4

-- TEST: distinctagg-5.6
-- SQL:
SELECT count(DISTINCT a) FROM t1 LEFT JOIN t3 ON TRUE WHERE t3.x=0;
-- EXPECTED:
0

-- TEST: distinctagg-5.7
-- SQL:
SELECT count(DISTINCT a) FROM t1 LEFT JOIN t3 ON (t3.x=0);
-- EXPECTED:
4

-- TEST: distinctagg-5.8
-- SQL:
SELECT count(DISTINCT x) FROM t1 LEFT JOIN t3 ON TRUE;
-- EXPECTED:
2

-- TEST: distinctagg-5.9
-- SQL:
SELECT count(DISTINCT x) FROM t1 LEFT JOIN t3 ON TRUE WHERE t3.x=1;
-- EXPECTED:
1

-- TEST: distinctagg-5.10
-- SQL:
SELECT count(DISTINCT x) FROM t1 LEFT JOIN t3 ON TRUE WHERE t3.x=0;
-- EXPECTED:
0

-- TEST: distinctagg-5.11
-- SQL:
SELECT count(DISTINCT x) FROM t1 LEFT JOIN t3 ON (t3.x=0);
-- EXPECTED:
0

-- TEST: init
-- SQL:
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1(a INT PRIMARY KEY, b INT);
CREATE TABLE t2(c INT PRIMARY KEY, d INT);
INSERT INTO t1 VALUES(123,456);
INSERT INTO t2 VALUES(123,456);

-- TEST: distinctagg-6.0
-- SQL:
SELECT count(DISTINCT c) FROM t1 LEFT JOIN t2 ON TRUE;
-- EXPECTED:
1
