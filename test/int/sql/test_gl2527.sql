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
1. Query (FILTERED STORAGE):
SELECT "t"."id", "t"."parent", "t"."sharding" FROM "t" WHERE "t"."sharding" = CAST(1 AS int)
+----------+-------+------+-----------------------------+
| selectid | order | from | detail                      |
+=======================================================+
| 0        | 0     | 0    | SCAN TABLE t (~262144 rows) |
+----------+-------+------+-----------------------------+
''
2. Query (STORAGE):
SELECT "fa"."COL_0" as "id", "fa"."COL_1" as "parent", "fa"."COL_2" as "sharding", "p"."id", "p"."parent", "p"."sharding", "p"."bucket_id" FROM ( SELECT "COL_0", "COL_1", "COL_2" FROM "TMP_1139005679461301870_0136" ) as "fa" INNER JOIN "t" as "p" ON ("p"."id" = "fa"."COL_1") and ("p"."sharding" = "fa"."COL_2")
+----------+-------+------+----------------------------------------------------------------------+
| selectid | order | from | detail                                                               |
+================================================================================================+
| 0        | 0     | 0    | SCAN TABLE TMP_1139005679461301870_0136 (~1048576 rows)              |
|----------+-------+------+----------------------------------------------------------------------|
| 0        | 1     | 1    | SEARCH TABLE t AS p USING PRIMARY KEY (id=? AND sharding=?) (~1 row) |
+----------+-------+------+----------------------------------------------------------------------+
''
3. Query (STORAGE):
SELECT "unnamed_join"."COL_0" as "id", "unnamed_join"."COL_1" as "parent", "unnamed_join"."COL_2" as "sharding", "unnamed_join"."COL_3" as "id", "unnamed_join"."COL_4" as "parent", "unnamed_join"."COL_5" as "sharding", "unnamed_join"."COL_6" as "bucket_id", "g"."id", "g"."parent", "g"."sharding", "g"."bucket_id" FROM ( SELECT "COL_0", "COL_1", "COL_2", "COL_3", "COL_4", "COL_5", "COL_6" FROM "TMP_14290416481197473476_0136" ) as "unnamed_join" INNER JOIN "t" as "g" ON ("g"."id" = "unnamed_join"."COL_4") and ("g"."sharding" = "unnamed_join"."COL_5")
+----------+-------+------+----------------------------------------------------------------------+
| selectid | order | from | detail                                                               |
+================================================================================================+
| 0        | 0     | 0    | SCAN TABLE TMP_14290416481197473476_0136 (~1048576 rows)             |
|----------+-------+------+----------------------------------------------------------------------|
| 0        | 1     | 1    | SEARCH TABLE t AS g USING PRIMARY KEY (id=? AND sharding=?) (~1 row) |
+----------+-------+------+----------------------------------------------------------------------+
''
4. Query (ROUTER):
SELECT count (*) as "col_1" FROM ( SELECT "COL_0", "COL_1", "COL_2", "COL_3", "COL_4", "COL_5", "COL_6", "COL_7", "COL_8", "COL_9", "COL_10" FROM "TMP_16717083892382772147_0136" ) as "unnamed_join_1"
+----------+-------+------+--------------------------------------------+
| selectid | order | from | detail                                     |
+======================================================================+
| 0        | 0     | 0    | B+tree count TMP_16717083892382772147_0136 |
+----------+-------+------+--------------------------------------------+
''
