-- TEST: initialization
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t(x INT PRIMARY KEY, y TEXT, z DOUBLE);
INSERT INTO t VALUES(-1, '1', -1.1);
INSERT INTO t VALUES(2, '2', 2.0);
INSERT INTO t VALUES(3, '3', NULL);
INSERT INTO t VALUES(4, '4', NULL);
INSERT INTO t VALUES(5, '5', 5.5);

-- TEST: select-1-col
-- SQL:
SELECT x FROM t ORDER BY x;
-- EXPECTED:
-1,
2,
3,
4,
5

-- TEST: select-2-col
-- SQL:
SELECT x, y FROM t ORDER BY 1;
-- EXPECTED:
-1, '1', 2, '2', 3, '3', 4, '4', 5, '5'

-- TEST: select-double
-- SQL:
SELECT z FROM t ORDER BY 1;
-- EXPECTED:
NULL, , -1.1, 2.0, 5.5

-- TEST: insert-ok
-- SQL:
INSERT INTO t VALUES (6, '6', 6.0);

-- TEST: insert-error
-- SQL:
INSERT INTO t VALUES (6, '6', 6.0);
-- ERROR:
Duplicate key exists in unique index
