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
SELECT * FROM t WHERE CASE WHEN true THEN 5::INT END = 5 ORDER BY 1;
-- EXPECTED:
1, 1, 2, 1, 3, 2, 4, 3

-- TEST: case-under-where-clause-subtree
-- SQL:
SELECT * FROM t WHERE true and CASE WHEN true THEN 5::INT END = 5 ORDER BY 1;
-- EXPECTED:
1, 1, 2, 1, 3, 2, 4, 3

-- TEST: not-in-simple
-- SQL:
SELECT a FROM t WHERE a NOT IN (1, 3) ORDER BY 1;
-- EXPECTED:
2,
4

-- TEST: not-in-redundant
-- SQL:
SELECT a FROM t WHERE a NOT IN (1, 2) AND TRUE ORDER BY 1;
-- EXPECTED:
3,
4

-- TEST: not-in-under-join
-- SQL:
SELECT a FROM t JOIN (SELECT b from t) new ON t.b = new.b AND a NOT IN (1, 2) AND TRUE ORDER BY 1;
-- EXPECTED:
3,
4

-- TEST: not-in-simple
-- SQL:
SELECT a FROM t WHERE a NOT IN (1, 3) ORDER BY 1;
-- EXPECTED:
2,
4

-- TEST: not-in-redundant
-- SQL:
SELECT a FROM t WHERE a NOT IN (1, 2) AND TRUE ORDER BY 1;
-- EXPECTED:
3,
4

-- TEST: not-in-under-join
-- SQL:
SELECT a FROM t JOIN (SELECT b from t) new ON t.b = new.b AND a NOT IN (1, 2) AND TRUE ORDER BY 1;
-- EXPECTED:
3,
4

-- TEST: parentheses-under-cast-with-not
-- SQL:
SELECT (NOT TRUE)::TEXT
-- EXPECTED:
'FALSE'

-- TEST: parentheses-under-cast-with-concat
-- SQL:
SELECT ('1' || '2')::INT
-- EXPECTED:
12

-- TEST: parentheses-under-is-null
-- SQL:
SELECT (TRUE OR FALSE) IS NULL
-- EXPECTED:
false

-- TEST: parentheses-under-arithmetic
-- SQL:
SELECT 1 + (2 < 3)
-- ERROR:
unsigned and boolean are not supported for arithmetic

-- TEST: parentheses-under-arithmetic-with-not
-- SQL:
SELECT (NOT 1) + NULL
-- ERROR:
can not convert integer(1) to boolean

-- TEST: parentheses-under-arithmetic-with-between
-- SQL:
SELECT 1 + (1 BETWEEN 1 AND 1)
-- ERROR:
unsigned and boolean are not supported for arithmetic

-- TEST: parentheses-under-concat
-- SQL:
SELECT (NOT 1) || '1'
-- ERROR:
can not convert integer(1) to boolean

-- TEST: parentheses-under-divide
-- SQL:
SELECT 8 / (4 / 2)
-- EXPECTED:
4

-- TEST: parentheses-under-subtract
-- SQL:
SELECT 2 - (4 - 8)
-- EXPECTED:
6

-- TEST: parentheses-under-multiply
-- SQL:
SELECT 2 * (3 + 5)
-- EXPECTED:
16

-- TEST: parentheses-under-bool
-- SQL:
SELECT 1 = (2 = FALSE)
-- ERROR:
can not convert boolean(FALSE) to number

-- TEST: parentheses-under-like
-- SQL:
SELECT (NOT NULL) LIKE 'a'
-- ERROR:
wrong arguments for function LIKE

-- TEST: parentheses-under-not-with-and
-- SQL:
SELECT NOT (FALSE AND TRUE)
-- EXPECTED:
true

-- TEST: parentheses-under-not-with-or
-- SQL:
SELECT NOT (TRUE OR TRUE)
-- EXPECTED:
false

-- TEST: parentheses-under-and
-- SQL:
SELECT FALSE AND (FALSE OR TRUE)
-- EXPECTED:
false