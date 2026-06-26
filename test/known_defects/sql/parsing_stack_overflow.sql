-- TEST: initialization
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t(a INT PRIMARY KEY, b INT);
INSERT INTO t VALUES(1, 1);
INSERT INTO t VALUES(2, 1);
INSERT INTO t VALUES(3, 2);
INSERT INTO t VALUES(4, 3);

-- TEST: null-noop-or-groups-long
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
  AND (null is null OR (null is not null AND b = 12))
  AND (null is null OR (null is not null AND b = 13))
  AND (null is null OR (null is not null AND b = 14))
  AND (null is null OR (null is not null AND b = 15))
  AND (null is null OR (null is not null AND b = 16))
  AND (null is null OR (null is not null AND b = 17))
  AND (null is null OR (null is not null AND b = 18))
  AND (null is null OR (null is not null AND b = 19))
  AND (null is null OR (null is not null AND b = 20))
  AND (null is null OR (null is not null AND b = 21))
  AND (null is null OR (null is not null AND b = 22))
  AND (null is null OR (null is not null AND b = 23))
  AND (null is null OR (null is not null AND b = 24))
  AND (null is null OR (null is not null AND b = 25))
  AND (null is null OR (null is not null AND b = 26))
  AND (null is null OR (null is not null AND b = 27))
  AND (null is null OR (null is not null AND b = 28))
  AND (null is null OR (null is not null AND b = 29))
  AND (null is null OR (null is not null AND b = 30))
ORDER BY a;
-- EXPECTED:
1, 2
