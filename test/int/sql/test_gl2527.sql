-- TEST: test
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t (
    id     INT,
    parent INT,
    sharding INT,
    PRIMARY KEY (id, sharding)
)
USING memtx
DISTRIBUTED BY (sharding);
INSERT INTO t
VALUES (1, 1, 1),
        (2, 2, 4),
        (3, 3, 3),
        (4, 4, 8),
        (5, 5, 5),
        (6, 6, 12),
        (7, 7, 7),
        (8, 8, 16),
        (9, 9, 9),
        (10, 10, 20);


-- TEST: query-1
-- SQL:
WITH fa AS (
  SELECT id, parent, sharding
  FROM t
  WHERE sharding = 1
  ORDER BY id
)
SELECT count(*)
FROM fa
LEFT JOIN t p
  ON p.id = fa.parent
 AND p.sharding = fa.sharding
LEFT JOIN t g
  ON g.id = p.parent
 AND g.sharding = p.sharding;
-- EXPECTED:
1

-- TEST: query-2
-- SQL:
WITH fa AS (
  SELECT id, parent, sharding
  FROM t
  WHERE sharding = 1
)
SELECT count(*)
FROM fa
LEFT JOIN t p
  ON p.id = fa.parent
 AND p.sharding = fa.sharding
LEFT JOIN t g
  ON g.id = p.parent
 AND g.sharding = p.sharding;
-- EXPECTED:
1

-- TEST: query-3
-- SQL:
explain (raw) WITH fa AS (
  SELECT id, parent, sharding
  FROM t
  WHERE sharding = 1
)
SELECT count(*)
FROM fa
LEFT JOIN t p
  ON p.id = fa.parent
 AND p.sharding = fa.sharding
LEFT JOIN t g
  ON g.id = p.parent
 AND g.sharding = p.sharding;
-- EXPECTED:
1. Query (STORAGE):
SELECT count (*) as "count_1" FROM ( SELECT "t"."id", "t"."parent", "t"."sharding" FROM "t" WHERE "t"."sharding" = CAST(1 AS int) ) as "fa" LEFT JOIN "t" as "p" ON ("p"."id" = "fa"."parent") and ("p"."sharding" = "fa"."sharding") LEFT JOIN "t" as "g" ON ("g"."id" = "p"."parent") and ("g"."sharding" = "p"."sharding")
+----------+-------+------+----------------------------------------------------------------------+
| selectid | order | from | detail                                                               |
+================================================================================================+
| 0        | 0     | 0    | SCAN TABLE t (~262144 rows)                                          |
|----------+-------+------+----------------------------------------------------------------------|
| 0        | 1     | 1    | SEARCH TABLE t AS p USING PRIMARY KEY (id=? AND sharding=?) (~1 row) |
|----------+-------+------+----------------------------------------------------------------------|
| 0        | 2     | 2    | SEARCH TABLE t AS g USING PRIMARY KEY (id=? AND sharding=?) (~1 row) |
+----------+-------+------+----------------------------------------------------------------------+
''
2. Query (ROUTER):
SELECT sum ("COL_0") as "col_1" FROM ( SELECT "COL_0" FROM "TMP_11641976947517713853_0136" )
+----------+-------+------+----------------------------------------------------------+
| selectid | order | from | detail                                                   |
+====================================================================================+
| 0        | 0     | 0    | SCAN TABLE TMP_11641976947517713853_0136 (~1048576 rows) |
+----------+-------+------+----------------------------------------------------------+
''
