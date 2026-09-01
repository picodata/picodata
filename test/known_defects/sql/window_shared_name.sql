-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- TEST: shared-named-window-setup
-- SQL:
CREATE TABLE t (a INT PRIMARY KEY, b INT);
INSERT INTO t VALUES (1, 10), (2, 20);

-- TEST: shared-named-window
-- SQL:
SELECT SUM(0) OVER w, SUM(0) OVER w FROM t WINDOW w AS (ORDER BY b);
-- ERROR:
sbroad: invalid expression: reference at position 0 is not among the columns the projection keeps

-- TEST: shared-named-window-partition-by
-- SQL:
SELECT SUM(0) OVER w, SUM(0) OVER w FROM t WINDOW w AS (PARTITION BY b);
-- ERROR:
sbroad: invalid expression: reference at position 0 is not among the columns the projection keeps
