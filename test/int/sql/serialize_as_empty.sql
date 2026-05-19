-- TEST: init
-- SQL:
CREATE TABLE t1 (a INT, b INT, c INT, PRIMARY KEY (b));
CREATE TABLE t (a INT PRIMARY KEY);
CREATE TABLE g (b int PRIMARY KEY) DISTRIBUTED GLOBALLY;

-- TEST: explain-union-all
-- SQL:
EXPLAIN (raw) WITH t2 AS (SELECT * FROM t1 WHERE true)
SELECT * FROM t1 WHERE b = 1 UNION ALL SELECT * FROM t2 WHERE a = 1;
-- ERROR:
failed to build query: constants and parameters count differ

-- TEST: union-all
-- SQL:
SELECT * FROM t UNION ALL SELECT * FROM g WHERE true;
-- ERROR:
failed to build query: constants and parameters count differ
