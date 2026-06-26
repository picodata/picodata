-- TEST: initialization
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t(a INT PRIMARY KEY, b INT);
INSERT INTO t VALUES(1, 1);
INSERT INTO t VALUES(2, 1);
INSERT INTO t VALUES(3, 2);
INSERT INTO t VALUES(4, 3);

-- TEST: null-noop-or-groups
-- SQL:
SELECT a FROM t
WHERE (a = 1 OR a = 2)
  AND (null is null OR (null is not null AND b = 1))
  AND (null is null OR (null is not null AND b = 2))
  AND (null is null OR (null is not null AND b = 3))
  AND (null is null OR (null is not null AND b = 4))
  AND (null is null OR (null is not null AND b = 5))
  AND (null is null OR (null is not null AND b = 6))
  AND (null is null OR (null is not null AND b = 7))
  AND (null is null OR (null is not null AND b = 8))
  AND (null is null OR (null is not null AND b = 9))
  AND (null is null OR (null is not null AND b = 10))
  AND (null is null OR (null is not null AND b = 11))
ORDER BY a;
-- EXPECTED:
1, 2

-- TEST: nonfoldable-or-groups
-- SQL:
SELECT a FROM t
WHERE (a = 1 OR a = 10)
  AND (a = 1 OR a = 11)
  AND (a = 1 OR a = 12)
  AND (a = 1 OR a = 13)
  AND (a = 1 OR a = 14)
  AND (a = 1 OR a = 15)
  AND (a = 1 OR a = 16)
  AND (a = 1 OR a = 17)
  AND (a = 1 OR a = 18)
  AND (a = 1 OR a = 19)
  AND (a = 1 OR a = 20)
  AND (a = 1 OR a = 21)
ORDER BY a;
-- EXPECTED:
1
