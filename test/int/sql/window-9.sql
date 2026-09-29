-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- TEST: window9
-- SQL:
DROP TABLE IF EXISTS t4;
CREATE TABLE t4(a INTEGER PRIMARY KEY, b TEXT, c INTEGER);
INSERT INTO t4 VALUES(1, 'A', 9);
INSERT INTO t4 VALUES(2, 'B', 3);
INSERT INTO t4 VALUES(3, 'C', 2);
INSERT INTO t4 VALUES(4, 'D', 10);
INSERT INTO t4 VALUES(5, 'E', 5);
INSERT INTO t4 VALUES(6, 'F', 1);
INSERT INTO t4 VALUES(7, 'G', 1);
INSERT INTO t4 VALUES(8, 'H', 2);
INSERT INTO t4 VALUES(9, 'I', 10);
INSERT INTO t4 VALUES(10, 'J', 4);

-- TEST: window9-2.4.1
-- SQL:
SELECT group_concat(b, '.') OVER (
ORDER BY a ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
) FROM t4;
-- EXPECTED:
A.B.C.D.E.F.G.H.I.J,
B.C.D.E.F.G.H.I.J,
C.D.E.F.G.H.I.J,
D.E.F.G.H.I.J,
E.F.G.H.I.J,
F.G.H.I.J,
G.H.I.J,
H.I.J,
I.J,
J

-- TEST: window9-8.0
-- SQL:
DROP TABLE IF EXISTS t1;
CREATE TABLE t1(id INT PRIMARY KEY, a INT, b INT);
INSERT INTO t1 VALUES(1, 1, 2), (2, 3, 4);
DROP TABLE IF EXISTS t8;
CREATE TABLE t8(id INT PRIMARY KEY, t INT, total INT);
INSERT INTO t8 VALUES(1, 0, 0), (2, 10, 1);

-- TEST: window9-8.1.1
-- SQL:
SELECT min(sum(a)) OVER () FROM t1;
-- EXPECTED:
Decimal('4')

-- TEST: window9-8.1.2
-- SQL:
SELECT min(sum(a)) OVER () FROM t1 GROUP BY a;
-- EXPECTED:
Decimal('1'), Decimal('1')

-- TEST: window9-8.2.1
-- SQL:
SELECT sum(min(t)) OVER () FROM t8;
-- EXPECTED:
Decimal('0')

-- TEST: window9-8.2.2
-- SQL:
SELECT sum(max(t)) OVER () FROM t8;
-- EXPECTED:
Decimal('10')

-- TEST: window9-8.2.3
-- SQL:
SELECT sum(min(t)) OVER () FROM t8 GROUP BY total;
-- EXPECTED:
Decimal('10'), Decimal('10')

-- TEST: window9-8.2.4
-- SQL:
SELECT sum(max(t)) OVER () FROM t8 GROUP BY total;
-- EXPECTED:
Decimal('10'), Decimal('10')

-- TEST: window9-8.3.1
-- SQL:
SELECT sum(count(*)) OVER () FROM t1;
-- EXPECTED:
Decimal('2')

-- TEST: window9-8.3.2
-- SQL:
SELECT max(avg(a)) OVER () FROM t1;
-- EXPECTED:
Decimal('2')

-- TEST: window9-8.3.3
-- SQL:
SELECT sum(total(a)) OVER () FROM t1;
-- EXPECTED:
4.0

-- TEST: window9-8.4.1
-- SQL:
SELECT sum(min(a)) FILTER (WHERE min(a) > 0) OVER () FROM t1;
-- EXPECTED:
Decimal('1')

-- TEST: window9-8.4.2
-- SQL:
SELECT count(sum(a)) OVER (PARTITION BY b) FROM t1 GROUP BY b;
-- EXPECTED:
1, 1

-- TEST: window9-8.4.3
-- SQL:
SELECT sum(min(a)) OVER (ORDER BY b) FROM t1 GROUP BY b;
-- EXPECTED:
Decimal('1'), Decimal('4')

-- TEST: window9-8.5
-- SQL:
SELECT sum(min(a)) FROM t1;
-- ERROR:
aggregate functions inside aggregate function are not allowed
