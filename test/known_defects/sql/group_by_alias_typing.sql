-- TEST: group-by-alias-placeholder-typing
-- SQL:
CREATE TABLE t1 (a INT PRIMARY KEY, b INT ARRAY);

-- TEST: group-by-alias-placeholder-typing.2
-- SQL:
SELECT a AS x, count(*) FROM t1 GROUP BY x, x[1];
-- ERROR:
Selecting is only possible from map and array values
