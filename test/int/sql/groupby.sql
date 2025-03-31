-- TEST: initialization
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t(a INT PRIMARY KEY, b INT);
INSERT INTO t VALUES(1, 1);
INSERT INTO t VALUES(2, 1);
INSERT INTO t VALUES(3, 2);
INSERT INTO t VALUES(4, 3);

-- TEST: reference-under-case-expression
-- SQL:
SELECT CASE a WHEN 1 THEN 42 WHEN 2 THEN 69 ELSE 0 END AS c FROM t ORDER BY c;
-- EXPECTED:
0,
0,
42,
69

-- TEST: reference-under-when-without-case-expression
-- SQL:
SELECT CASE WHEN a <= 2 THEN true ELSE false END AS c FROM t ORDER BY c;
-- EXPECTED:
false,
false,
true,
true

-- TEST: reference-under-else-without-case-expression
-- SQL:
SELECT CASE WHEN false THEN 42::INT ELSE a END AS c FROM t ORDER BY c;
-- EXPECTED:
1,
2,
3,
4

-- TEST: reference-under-when-without-case-expression-and-else
-- SQL:
SELECT CASE WHEN a <= 4 THEN 42 END AS c FROM t ORDER BY c;
-- EXPECTED:
42,
42,
42,
42

-- TEST: case-under-where-clause
-- SQL:
SELECT * FROM t WHERE CASE WHEN true THEN 5::INT END = 5;
-- EXPECTED:
1, 1, 2, 1, 3, 2, 4, 3

-- TEST: case-under-where-clause-subtree
-- SQL:
SELECT * FROM t WHERE true and CASE WHEN true THEN 5::INT END = 5;
-- EXPECTED:
1, 1, 2, 1, 3, 2, 4, 3
