-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- https://git.picodata.io/core/picodata/-/work_items/3367

-- TEST: having-only-aggregate-setup
-- SQL:
CREATE TABLE t (a INT PRIMARY KEY, b BOOLEAN);
INSERT INTO t VALUES (1, TRUE), (2, FALSE);

-- TEST: having-only-aggregate
-- SQL:
SELECT a FROM t GROUP BY a HAVING max(b);
-- ERROR:
aggregate max as the whole HAVING condition not implemented

-- TEST: having-aggregate-in-expression
-- SQL:
SELECT a FROM t GROUP BY a HAVING max(b) = true;
-- EXPECTED:
1
