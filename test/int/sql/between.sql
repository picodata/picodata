-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- TEST: initialization
-- SQL:
DROP TABLE IF EXISTS gt;
DROP TABLE IF EXISTS gt2;
DROP TABLE IF EXISTS st;
DROP TABLE IF EXISTS st2;
DROP TABLE IF EXISTS stn;
CREATE TABLE gt (a INT PRIMARY KEY) DISTRIBUTED GLOBALLY;
CREATE TABLE gt2 (a INT PRIMARY KEY) DISTRIBUTED GLOBALLY;
CREATE TABLE st (a INT PRIMARY KEY);
CREATE TABLE st2 (a INT PRIMARY KEY);
CREATE TABLE stn (a INT PRIMARY KEY, b INT);
INSERT INTO gt VALUES (1);
INSERT INTO gt2 VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
INSERT INTO st VALUES (1);
INSERT INTO st2 VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
INSERT INTO stn VALUES (1, NULL), (2, 2);

-- TEST: between-subquery
-- SQL:
SELECT a FROM gt WHERE (SELECT a FROM gt) BETWEEN 0 AND 1;
-- EXPECTED:
1

-- TEST: between-subquery-outside
-- SQL:
SELECT count(*) FROM gt WHERE (SELECT a FROM gt) BETWEEN 2 AND 3;
-- EXPECTED:
0

-- TEST: not-between-subquery
-- SQL:
SELECT count(*) FROM gt WHERE (SELECT a FROM gt) NOT BETWEEN 0 AND 1;
-- EXPECTED:
0

-- TEST: not-between-subquery-outside
-- SQL:
SELECT a FROM gt WHERE (SELECT a FROM gt) NOT BETWEEN 2 AND 3;
-- EXPECTED:
1

-- TEST: between-subquery-no-rows
-- SQL:
SELECT count(*) FROM gt WHERE (SELECT a FROM gt WHERE a = 100) BETWEEN 0 AND 1;
-- EXPECTED:
0

-- TEST: not-between-subquery-no-rows
-- SQL:
SELECT count(*) FROM gt WHERE (SELECT a FROM gt WHERE a = 100) NOT BETWEEN 0 AND 1;
-- EXPECTED:
0

-- TEST: between-subquery-null-lower-inside
-- SQL:
SELECT count(*) FROM gt WHERE (SELECT a FROM gt) BETWEEN NULL AND 2;
-- EXPECTED:
0

-- TEST: between-subquery-null-lower-outside
-- SQL:
SELECT count(*) FROM gt WHERE (SELECT a FROM gt) NOT BETWEEN NULL AND 0;
-- EXPECTED:
1

-- TEST: between-subquery-null-upper-outside
-- SQL:
SELECT count(*) FROM gt WHERE (SELECT a FROM gt) NOT BETWEEN 2 AND NULL;
-- EXPECTED:
1

-- TEST: between-in-output
-- SQL:
SELECT (SELECT a FROM gt) BETWEEN 0 AND 1 FROM gt;
-- EXPECTED:
true

-- TEST: between-compared
-- SQL:
SELECT a FROM gt WHERE ((SELECT a FROM gt) BETWEEN 0 AND 1) = true;
-- EXPECTED:
1

-- TEST: between-comparison-on-the-left
-- SQL:
SELECT a FROM gt WHERE (a = 1) = (a + 1 BETWEEN 2 AND 2);
-- EXPECTED:
1

-- TEST: between-expression
-- SQL:
SELECT a FROM gt WHERE a + 1 BETWEEN 2 AND 2;
-- EXPECTED:
1

-- TEST: between-expression-with-parameters-of-its-own
-- SQL:
SELECT a FROM gt WHERE (SELECT a + 10 FROM gt) BETWEEN 11 AND 11;
-- EXPECTED:
1

-- TEST: between-case
-- SQL:
SELECT a FROM gt WHERE CASE WHEN a = 1 THEN 1 ELSE 0 END BETWEEN 1 AND 1;
-- EXPECTED:
1

-- TEST: between-with-other-conditions
-- SQL:
SELECT a FROM gt WHERE a = 1 AND (SELECT a FROM gt) BETWEEN 0 AND 1 OR a = 2;
-- EXPECTED:
1

-- TEST: between-column
-- SQL:
SELECT a FROM gt WHERE a BETWEEN 0 AND 1;
-- EXPECTED:
1

-- TEST: comparisons-written-by-hand
-- SQL:
SELECT a FROM gt WHERE (SELECT a FROM gt) >= 0 AND (SELECT a FROM gt) <= 1;
-- EXPECTED:
1

-- TEST: between-sharded
-- SQL:
SELECT a FROM st WHERE (SELECT a FROM st) BETWEEN 0 AND 1;
-- EXPECTED:
1

-- TEST: between-sharded-on-one-bucket
-- SQL:
SELECT a FROM st WHERE a = 1 AND (SELECT a FROM st WHERE a = 1) BETWEEN 0 AND 1;
-- EXPECTED:
1

-- TEST: between-without-scan-on-one-bucket
-- SQL:
SELECT (SELECT a FROM st WHERE a = 1) BETWEEN 0 AND 1;
-- EXPECTED:
true

-- TEST: between-in-join
-- SQL:
SELECT x.a FROM st x JOIN gt y ON x.a = y.a AND (SELECT a FROM gt) BETWEEN 0 AND 1;
-- EXPECTED:
1

-- TEST: between-sharded-subquery-in-join
-- SQL:
SELECT count(*) FROM st2 x JOIN st2 y
ON x.a = y.a AND x.a BETWEEN 1 AND (SELECT a FROM st2 WHERE a = 10);
-- EXPECTED:
10

-- TEST: between-sharded-subquery-in-left-join
-- SQL:
SELECT count(y.a) FROM st2 x LEFT JOIN st2 y
ON x.a = y.a AND x.a BETWEEN 1 AND (SELECT a FROM st2 WHERE a = 10);
-- EXPECTED:
10

-- TEST: between-sharded-subquery-in-left-join-of-global
-- SQL:
SELECT count(y.a) FROM gt2 x LEFT JOIN st2 y
ON x.a = y.a AND y.a BETWEEN 1 AND (SELECT a FROM st2 WHERE a = 10);
-- EXPECTED:
10

-- TEST: not-between-sharded-subquery-in-join
-- SQL:
SELECT count(*) FROM st2 x JOIN st2 y
ON x.a = y.a AND x.a NOT BETWEEN 0 AND (SELECT a FROM st2 WHERE a = 10) - 5;
-- EXPECTED:
5

-- TEST: not-in-under-between-sees-a-null-of-another-storage
-- SQL:
SELECT count(*) FROM st2 WHERE (NOT (a IN (SELECT b FROM stn) OR a = 0)) BETWEEN true AND true;
-- EXPECTED:
0

-- TEST: nested-between-global
-- SQL:
SELECT a FROM gt WHERE (SELECT a FROM gt WHERE (SELECT a FROM gt WHERE
(SELECT a FROM gt WHERE (SELECT a FROM gt WHERE (SELECT a FROM gt WHERE
(SELECT a FROM gt WHERE (SELECT a FROM gt WHERE (SELECT a FROM gt WHERE
(SELECT a FROM gt WHERE (SELECT a FROM gt WHERE (SELECT a FROM gt WHERE (SELECT a FROM gt)
BETWEEN 0 AND 1) BETWEEN 0 AND 1) BETWEEN 0 AND 1) BETWEEN 0 AND 1) BETWEEN 0 AND 1)
BETWEEN 0 AND 1) BETWEEN 0 AND 1) BETWEEN 0 AND 1) BETWEEN 0 AND 1) BETWEEN 0 AND 1)
BETWEEN 0 AND 1) BETWEEN 0 AND 1;
-- EXPECTED:
1

-- TEST: nested-between-sharded
-- SQL:
SELECT a FROM st WHERE (SELECT a FROM st WHERE (SELECT a FROM st WHERE
(SELECT a FROM st WHERE (SELECT a FROM st WHERE (SELECT a FROM st WHERE
(SELECT a FROM st WHERE (SELECT a FROM st WHERE (SELECT a FROM st WHERE
(SELECT a FROM st WHERE (SELECT a FROM st WHERE (SELECT a FROM st WHERE (SELECT a FROM st)
BETWEEN 0 AND 1) BETWEEN 0 AND 1) BETWEEN 0 AND 1) BETWEEN 0 AND 1) BETWEEN 0 AND 1)
BETWEEN 0 AND 1) BETWEEN 0 AND 1) BETWEEN 0 AND 1) BETWEEN 0 AND 1) BETWEEN 0 AND 1)
BETWEEN 0 AND 1) BETWEEN 0 AND 1;
-- EXPECTED:
1

-- TEST: between-expression-group-by
-- SQL:
SELECT a + 1 BETWEEN 0 AND 5 AS b FROM st GROUP BY a + 1 BETWEEN 0 AND 5;
-- EXPECTED:
true

-- TEST: between-expression-group-by-alias
-- SQL:
SELECT a + 1 BETWEEN 0 AND 5 AS b FROM st GROUP BY b;
-- EXPECTED:
true

-- TEST: between-expression-over-group-by
-- SQL:
SELECT a + 1 BETWEEN 0 AND 5 AS b FROM st GROUP BY a;
-- EXPECTED:
true

-- TEST: between-expression-having
-- SQL:
SELECT a FROM st GROUP BY a HAVING a + 1 BETWEEN 0 AND 5;
-- EXPECTED:
1

-- TEST: between-aggregate
-- SQL:
SELECT count(*) + 1 BETWEEN 0 AND 5 AS b FROM st;
-- EXPECTED:
true

-- TEST: between-aggregate-having
-- SQL:
SELECT a FROM st GROUP BY a HAVING count(*) + 1 BETWEEN 0 AND 5;
-- EXPECTED:
1

-- TEST: between-distinct
-- SQL:
SELECT DISTINCT a + 1 BETWEEN 0 AND 5 AS b FROM st;
-- EXPECTED:
true

-- TEST: between-order-by
-- SQL:
SELECT a FROM st ORDER BY a + 1 BETWEEN 0 AND 5;
-- EXPECTED:
1

-- TEST: between-not
-- SQL:
SELECT count(*) FROM gt WHERE NOT (a + 1 BETWEEN 0 AND 5);
-- EXPECTED:
0

-- TEST: not-between-expression
-- SQL:
SELECT a FROM gt WHERE a + 1 NOT BETWEEN 5 AND 6;
-- EXPECTED:
1

-- TEST: not-between-expression-inside
-- SQL:
SELECT count(*) FROM gt WHERE a + 1 NOT BETWEEN 0 AND 5;
-- EXPECTED:
0

-- TEST: not-between-expression-null-operand
-- SQL:
SELECT count(*) FROM gt WHERE a + NULL::int NOT BETWEEN 0 AND 5;
-- EXPECTED:
0

-- TEST: not-between-expression-null-bound-true
-- SQL:
SELECT a FROM gt WHERE a + 1 NOT BETWEEN NULL::int AND 1;
-- EXPECTED:
1

-- TEST: not-between-expression-null-bound-unknown
-- SQL:
SELECT count(*) FROM gt WHERE a + 1 NOT BETWEEN NULL::int AND 5;
-- EXPECTED:
0

-- TEST: not-between-in-output
-- SQL:
SELECT a + 1 NOT BETWEEN 5 AND 6 FROM gt;
-- EXPECTED:
true

-- TEST: not-between-sharded
-- SQL:
SELECT a FROM st WHERE a + 1 NOT BETWEEN 5 AND 6;
-- EXPECTED:
1

-- TEST: not-between-with-a-true-constant-half
-- SQL:
SELECT count(*) FROM st2 WHERE 10 NOT BETWEEN 1 AND a;
-- EXPECTED:
9

-- TEST: not-between-nested-global
-- SQL:
SELECT a FROM gt WHERE ((a = 1) NOT BETWEEN false AND false) NOT BETWEEN false AND false;
-- EXPECTED:
1

-- TEST: between-or
-- SQL:
SELECT a FROM gt WHERE a = 2 OR a + 1 BETWEEN 0 AND 5;
-- EXPECTED:
1

-- TEST: nested-between-over-expression-global
-- SQL:
SELECT a FROM gt WHERE CASE WHEN CASE WHEN CASE WHEN CASE WHEN CASE WHEN CASE WHEN
CASE WHEN CASE WHEN CASE WHEN CASE WHEN CASE WHEN CASE WHEN CASE WHEN CASE WHEN
a = 1 THEN 1 ELSE 0 END BETWEEN 0 AND 1 THEN 1 ELSE 0 END BETWEEN 0 AND 1
THEN 1 ELSE 0 END BETWEEN 0 AND 1 THEN 1 ELSE 0 END BETWEEN 0 AND 1 THEN 1 ELSE 0 END
BETWEEN 0 AND 1 THEN 1 ELSE 0 END BETWEEN 0 AND 1 THEN 1 ELSE 0 END BETWEEN 0 AND 1
THEN 1 ELSE 0 END BETWEEN 0 AND 1 THEN 1 ELSE 0 END BETWEEN 0 AND 1 THEN 1 ELSE 0 END
BETWEEN 0 AND 1 THEN 1 ELSE 0 END BETWEEN 0 AND 1 THEN 1 ELSE 0 END BETWEEN 0 AND 1
THEN 1 ELSE 0 END BETWEEN 0 AND 1 THEN 1 ELSE 0 END BETWEEN 0 AND 1;
-- EXPECTED:
1

-- TEST: nested-between-over-expression-sharded
-- SQL:
SELECT a FROM st WHERE CASE WHEN CASE WHEN CASE WHEN CASE WHEN CASE WHEN CASE WHEN
CASE WHEN CASE WHEN CASE WHEN CASE WHEN CASE WHEN CASE WHEN CASE WHEN CASE WHEN
a = 1 THEN 1 ELSE 0 END BETWEEN 0 AND 1 THEN 1 ELSE 0 END BETWEEN 0 AND 1
THEN 1 ELSE 0 END BETWEEN 0 AND 1 THEN 1 ELSE 0 END BETWEEN 0 AND 1 THEN 1 ELSE 0 END
BETWEEN 0 AND 1 THEN 1 ELSE 0 END BETWEEN 0 AND 1 THEN 1 ELSE 0 END BETWEEN 0 AND 1
THEN 1 ELSE 0 END BETWEEN 0 AND 1 THEN 1 ELSE 0 END BETWEEN 0 AND 1 THEN 1 ELSE 0 END
BETWEEN 0 AND 1 THEN 1 ELSE 0 END BETWEEN 0 AND 1 THEN 1 ELSE 0 END BETWEEN 0 AND 1
THEN 1 ELSE 0 END BETWEEN 0 AND 1 THEN 1 ELSE 0 END BETWEEN 0 AND 1;
-- EXPECTED:
1
