-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

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

-- TEST: or-under-and-in-comparison
-- SQL:
SELECT a FROM t WHERE ((a = 1 OR a = 2) AND a > 0 AND a < 4) = false ORDER BY a;
-- EXPECTED:
3, 4

-- TEST: or-groups-past-dnf-limit-under-or
-- SQL:
SELECT a FROM t
WHERE ((a = 1 OR b = 10)
    AND (a = 1 OR b = 11)
    AND (a = 1 OR b = 12)
    AND (a = 1 OR b = 13)
    AND (a = 1 OR b = 14)
    AND (a = 1 OR b = 15)
    AND (a = 1 OR b = 16)
    AND (a = 1 OR b = 17)
    AND (a = 1 OR b = 18)
    AND (a = 1 OR b = 19)
    AND b > 0)
  OR a = 4
ORDER BY a;
-- EXPECTED:
1, 4

-- TEST: join-on-or-under-and-in-comparison
-- SQL:
SELECT DISTINCT x.a FROM t x JOIN t y
ON ((x.a = 1 OR x.a = 2) AND x.a > 0 AND x.a < 4) = (y.a < 9)
ORDER BY x.a;
-- EXPECTED:
1, 2

-- TEST: update-or-under-and-in-comparison-1
-- SQL:
UPDATE t SET b = 99 WHERE ((a = 1 OR a = 2) AND a > 0 AND a < 4) = (a < 9);

-- TEST: update-or-under-and-in-comparison-2
-- SQL:
SELECT a, b FROM t ORDER BY a;
-- EXPECTED:
1, 99, 2, 99, 3, 2, 4, 3

-- TEST: delete-or-under-and-in-comparison-1
-- SQL:
DELETE FROM t WHERE ((a = 1 OR a = 2) AND a > 0 AND a < 4) = (a < 9);

-- TEST: delete-or-under-and-in-comparison-2
-- SQL:
SELECT a FROM t ORDER BY a;
-- EXPECTED:
3, 4
