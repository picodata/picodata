-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- TEST: block-bool-param-setup
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t (a INT PRIMARY KEY, b INT);
INSERT INTO t VALUES (1, 10), (2, 20);

-- TEST: block-bool-param-or-branch
-- SQL:
DO $$ BEGIN UPDATE t SET b = b + 1 WHERE a = $1 OR $2; END $$;
-- PARAMS:
1, false
-- ERROR:
transaction cannot be executed on all buckets

-- TEST: block-bool-param-negated-or-branch
-- SQL:
DO $$ BEGIN RETURN QUERY SELECT b FROM t WHERE a = $1 OR NOT $2; END $$;
-- PARAMS:
1, true
-- ERROR:
transaction cannot be executed on all buckets

-- TEST: block-bool-param-selects-key
-- SQL:
DO $$ BEGIN UPDATE t SET b = b + 1 WHERE (a = $1 AND $3) OR (a = $2 AND NOT $3); END $$;
-- PARAMS:
1, 2, true
-- ERROR:
transaction can only be executed on a single bucket, got \[\d+, \d+\]

-- TEST: block-cast-param-or-branch
-- SQL:
DO $$ BEGIN UPDATE t SET b = b + 1 WHERE a = $1::int OR $2; END $$;
-- PARAMS:
1, false
-- ERROR:
transaction cannot be executed on all buckets

-- TEST: block-bool-param-check
-- SQL:
SELECT a, b FROM t ORDER BY a;
-- EXPECTED:
1, 10,
2, 20
