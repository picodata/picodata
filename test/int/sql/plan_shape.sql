-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- The effective subtree shape of a prepared statement is cached across
-- executions. Each query below runs several times with values that give a
-- different plan structure after binding, so a stale shape would give a
-- wrong result.

-- TEST: initialization
-- SQL:
DROP TABLE IF EXISTS ps;
CREATE TABLE ps (a INT PRIMARY KEY, b INT);
INSERT INTO ps VALUES (1, 10), (2, 20), (3, 10);

-- TEST: folded-and-true
-- SQL:
SELECT a FROM ps WHERE ? AND a = 1;
-- PARAMS:
true
-- EXPECTED:
1

-- TEST: folded-and-false
-- SQL:
SELECT a FROM ps WHERE ? AND a = 1;
-- PARAMS:
false
-- EXPECTED:

-- TEST: not-folded-and
-- SQL:
SELECT a FROM ps WHERE ? AND a = 1;
-- PARAMS:
null
-- EXPECTED:

-- TEST: folded-or-false
-- SQL:
SELECT a FROM ps WHERE ? OR a = 1;
-- PARAMS:
false
-- EXPECTED:
1

-- TEST: folded-or-true
-- SQL:
SELECT a FROM ps WHERE ? OR a = 1;
-- PARAMS:
true
-- UNORDERED:
1, 2, 3

-- TEST: cast-folded
-- SQL:
SELECT a FROM ps WHERE a = CAST(? AS INT);
-- PARAMS:
'2'
-- EXPECTED:
2

-- TEST: cast-not-folded
-- SQL:
SELECT a FROM ps WHERE a = CAST(? AS INT);
-- PARAMS:
'x'
-- ERROR:
.*

-- TEST: cast-folded-again
-- SQL:
SELECT a FROM ps WHERE a = CAST(? AS INT);
-- PARAMS:
'3'
-- EXPECTED:
3

-- TEST: motion-first
-- SQL:
SELECT b, count(*) FROM ps WHERE a > ? GROUP BY b;
-- PARAMS:
0
-- UNORDERED:
10, 2, 20, 1

-- TEST: motion-second
-- SQL:
SELECT b, count(*) FROM ps WHERE a > ? GROUP BY b;
-- PARAMS:
1
-- UNORDERED:
10, 1, 20, 1
