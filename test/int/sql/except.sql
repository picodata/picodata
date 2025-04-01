-- TEST: initialization
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t (a INT PRIMARY KEY, b INT, c INT);
DROP TABLE IF EXISTS g;
CREATE TABLE g (a INT PRIMARY KEY, b INT, c INT) DISTRIBUTED GLOBALLY;
INSERT INTO t VALUES(1, -1, -1);
INSERT INTO t VALUES(2, 2, 2);
INSERT INTO t VALUES(3, 3, 3);
INSERT INTO g VALUES(1, 1, 1);
INSERT INTO g VALUES(2, 2, 2);
INSERT INTO g VALUES(3, 3, 3);

-- TEST: one-sharded-one-global-simple-filter
-- SQL:
SELECT 66 FROM g WHERE (0) <= a EXCEPT SELECT 88 FROM t;
-- EXPECTED:
66

-- TEST: one-sharded-one-global-filter-subquery
-- SQL:
SELECT * FROM g WHERE (select 0) <= a EXCEPT SELECT * FROM t;
-- EXPECTED:
1, 1, 1

-- TEST: one-sharded-one-global-always-true-filter-subquery
-- SQL:
SELECT * FROM g WHERE (select true) EXCEPT SELECT * FROM t;
-- EXPECTED:
1, 1, 1

-- TEST: one-sharded-one-global-filter-subquery-ok
-- SQL:
SELECT * FROM g WHERE (select 1) < a EXCEPT SELECT * FROM t EXCEPT SELECT * FROM t;

-- TEST: one-sharded-one-global-filter-subquery-agg
-- SQL:
SELECT a FROM g WHERE (SELECT min(c) FROM g) <= a
EXCEPT SELECT e.c FROM t d JOIN g e ON e.b = d.c GROUP BY e.c
EXCEPT SELECT e.c FROM t d JOIN g e ON e.a = d.b GROUP BY e.c;
-- EXPECTED:
1
