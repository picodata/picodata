-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- A GROUP BY alias is parsed into an untyped placeholder reference, so the
-- frontend types it as NULL and misses type errors: indexing an int alias
-- should fail with "cannot index expression of type int", but instead the
-- query reaches the storage and dies at SQL compilation. To be fixed by the
-- parser rewrite.

-- TEST: group-by-alias-placeholder-typing
-- SQL:
CREATE TABLE t1 (a INT PRIMARY KEY, b INT ARRAY);

-- TEST: group-by-alias-placeholder-typing.2
-- SQL:
SELECT a AS x, count(*) FROM t1 GROUP BY x, x[1];
-- ERROR:
Selecting is only possible from map and array values
