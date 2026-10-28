-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- TEST: is-true-false-setup
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t (id INT PRIMARY KEY, b BOOL) DISTRIBUTED BY (id);
INSERT INTO t VALUES (1, true), (2, false), (3, NULL);

-- TEST: is-true-false-literals
-- SQL:
SELECT
    true IS TRUE, true IS NOT TRUE, true IS FALSE, true IS NOT FALSE,
    false IS TRUE, false IS NOT TRUE, false IS FALSE, false IS NOT FALSE,
    NULL IS TRUE, NULL IS NOT TRUE, NULL IS FALSE, NULL IS NOT FALSE;
-- EXPECTED:
true, false, false, true,
false, true, true, false,
false, true, false, true

-- TEST: is-true-false-column-in-projection
-- SQL:
SELECT id, b IS TRUE, b IS NOT TRUE, b IS FALSE, b IS NOT FALSE FROM t;
-- UNORDERED:
1, true, false, false, true,
2, false, true, true, false,
3, false, true, false, true

-- TEST: is-true-in-filter
-- SQL:
SELECT id FROM t WHERE b IS TRUE;
-- EXPECTED:
1

-- TEST: is-not-true-in-filter
-- SQL:
SELECT id FROM t WHERE b IS NOT TRUE;
-- UNORDERED:
2, 3

-- TEST: is-false-in-filter
-- SQL:
SELECT id FROM t WHERE b IS FALSE;
-- EXPECTED:
2

-- TEST: is-not-false-in-filter
-- SQL:
SELECT id FROM t WHERE b IS NOT FALSE;
-- UNORDERED:
1, 3

-- TEST: is-true-under-not-in-filter
-- SQL:
SELECT id FROM t WHERE NOT (b IS TRUE) AND NOT (b IS FALSE);
-- EXPECTED:
3

-- TEST: is-true-over-comparison-with-null
-- SQL:
SELECT id FROM t WHERE (b = true) IS NOT TRUE;
-- UNORDERED:
2, 3

-- TEST: is-true-folded-in-filter
-- SQL:
SELECT id FROM t WHERE NULL IS NOT TRUE AND NOT (NULL IS FALSE);
-- UNORDERED:
1, 2, 3

-- TEST: is-true-in-join-condition
-- SQL:
SELECT t1.id, t2.id FROM t AS t1 JOIN t AS t2 ON t1.id = t2.id AND t2.b IS NOT FALSE;
-- UNORDERED:
1, 1,
3, 3

-- TEST: is-true-in-group-by
-- SQL:
SELECT b IS TRUE, count(*) FROM t GROUP BY b IS TRUE;
-- UNORDERED:
true, 1,
false, 2

-- TEST: is-true-with-null-parameter
-- SQL:
SELECT $1::bool IS TRUE, $1::bool IS NOT FALSE;
-- PARAMS:
None
-- EXPECTED:
false, true

-- TEST: is-true-requires-boolean
-- SQL:
SELECT 1 IS TRUE;
-- ERROR:
argument of IS TRUE must be type boolean, not type int

-- TEST: is-true-in-explain
-- SQL:
EXPLAIN SELECT id FROM t WHERE b IS NOT TRUE;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t.id::int -> id)
  selection (not t.b::bool is true)
    scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]
