-- TEST: initialization
-- SQL:
CREATE TABLE t ("id" INT PRIMARY KEY, "a" INT);
INSERT INTO t VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);

-- TEST: explain-1
-- SQL:
EXPLAIN (RAW) WITH cte1 (a) AS (SELECT "a" FROM "t" WHERE "id" = 1),
cte2 (b) AS (SELECT * FROM cte1 UNION ALL SELECT "a" FROM "t" WHERE "id" = 2)
SELECT b FROM cte2;
-- EXPECTED:
1. Query (FILTERED STORAGE):
SELECT "cte2"."a" as "b" FROM ( SELECT * FROM ( SELECT "t"."a" FROM "t" WHERE "t"."id" = CAST(1 AS int) ) as "cte1" UNION ALL SELECT "t"."a" FROM "t" WHERE "t"."id" = CAST(2 AS int) ) as "cte2"
+----------+-------+------+--------------------------------------------------+
| selectid | order | from | detail                                           |
+============================================================================+
| 1        | 0     | 0    | SEARCH TABLE t USING PRIMARY KEY (id=?) (~1 row) |
|----------+-------+------+--------------------------------------------------|
| 2        | 0     | 0    | SEARCH TABLE t USING PRIMARY KEY (id=?) (~1 row) |
|----------+-------+------+--------------------------------------------------|
| 0        | 0     | 0    | COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)          |
+----------+-------+------+--------------------------------------------------+
''
2. Query (ROUTER):
SELECT "cte2"."COL_0" as "b" FROM ( SELECT "COL_0" FROM "TMP_1285800704210173889_0136" ) as "cte2"
+----------+-------+------+---------------------------------------------------------+
| selectid | order | from | detail                                                  |
+===================================================================================+
| 0        | 0     | 0    | SCAN TABLE TMP_1285800704210173889_0136 (~1048576 rows) |
+----------+-------+------+---------------------------------------------------------+
''

-- TEST: cte-1
-- SQL:
WITH cte1 (a) AS (SELECT "a" FROM "t" WHERE "id" = 1),
cte2 (b) AS (SELECT * FROM cte1 UNION ALL SELECT "a" FROM "t" WHERE "id" = 2)
SELECT b FROM cte2;
-- UNORDERED:
1, 2

-- TEST: cte-union-multi-col
-- SQL:
WITH cte1 (x, y) AS (SELECT "id", "a" FROM "t" WHERE "id" = 1),
cte2 (p, q) AS (SELECT * FROM cte1 UNION ALL SELECT "id", "a" FROM "t" WHERE "id" = 3)
SELECT p, q FROM cte2;
-- UNORDERED:
1, 1, 3, 3

-- TEST: cte-union-not-all
-- SQL:
WITH cte1 (a) AS (SELECT "a" FROM "t" WHERE "id" <= 2),
cte2 (b) AS (SELECT * FROM cte1 UNION SELECT "a" FROM "t" WHERE "id" <= 3)
SELECT b FROM cte2;
-- UNORDERED:
1, 2, 3

-- TEST: cte-union-three-branches
-- SQL:
WITH cte1 (a) AS (SELECT "a" FROM "t" WHERE "id" = 1),
cte2 (b) AS (
    SELECT * FROM cte1
    UNION ALL SELECT "a" FROM "t" WHERE "id" = 2
    UNION ALL SELECT "a" FROM "t" WHERE "id" = 3
)
SELECT b FROM cte2;
-- UNORDERED:
1, 2, 3

-- TEST: cte-union-both-cte
-- SQL:
WITH cte1 (a) AS (SELECT "a" FROM "t" WHERE "id" = 4),
cte2 (a) AS (SELECT "a" FROM "t" WHERE "id" = 5),
cte3 (b) AS (SELECT * FROM cte1 UNION ALL SELECT * FROM cte2)
SELECT b FROM cte3;
-- UNORDERED:
4, 5

-- TEST: cte-union-cte-reuse
-- SQL:
WITH cte1 (a) AS (SELECT "a" FROM "t" WHERE "id" = 1)
SELECT * FROM cte1 UNION ALL SELECT * FROM cte1;
-- EXPECTED:
1, 1

-- TEST: cte-union-with-expression
-- SQL:
WITH cte1 (a) AS (SELECT "a" + 10 FROM "t" WHERE "id" = 1),
cte2 (b) AS (SELECT * FROM cte1 UNION ALL SELECT "a" + 20 FROM "t" WHERE "id" = 2)
SELECT b FROM cte2;
-- UNORDERED:
11, 22

-- TEST: cte-values
-- SQL:
WITH cte1(a) as (VALUES(1)),
cte2(a) as (SELECT a1.a FROM cte1 a1 JOIN "t" ON true UNION SELECT * FROM cte1 a2)
SELECT * FROM cte2;
-- EXPECTED:
1

-- TEST: cte-range-union-all
-- SQL:
WITH cte1 (a) AS (SELECT "a" FROM "t" WHERE "id" <= 2),
cte2 (b) AS (SELECT * FROM cte1 UNION ALL SELECT "a" FROM "t" WHERE "id" <= 3)
SELECT b FROM cte2;
-- UNORDERED:
1, 2, 1, 2, 3

-- TEST: cte-range-union
-- SQL:
WITH cte1 (a) AS (SELECT "a" FROM "t" WHERE "id" <= 2),
cte2 (b) AS (SELECT * FROM cte1 UNION SELECT "a" FROM "t" WHERE "id" <= 3)
SELECT b FROM cte2;
-- UNORDERED:
1, 2, 3

-- TEST: cte-range-multi-col-union-all
-- SQL:
WITH cte1 (x, y) AS (SELECT "id", "a" FROM "t" WHERE "id" > 3),
cte2 (p, q) AS (SELECT * FROM cte1 UNION ALL SELECT "id", "a" FROM "t" WHERE "id" <= 3)
SELECT p, q FROM cte2;
-- UNORDERED:
4, 4, 5, 5, 1, 1, 2, 2, 3, 3

-- TEST: cte-range-multi-col-union
-- SQL:
WITH cte1 (x, y) AS (SELECT "id", "a" FROM "t" WHERE "id" > 3),
cte2 (p, q) AS (SELECT * FROM cte1 UNION SELECT "id", "a" FROM "t" WHERE "id" >= 4)
SELECT p, q FROM cte2;
-- UNORDERED:
4, 4, 5, 5

-- TEST: cte-fullscan-union-all
-- SQL:
WITH cte1 (a) AS (SELECT "a" FROM "t"),
cte2 (b) AS (SELECT * FROM cte1 UNION ALL SELECT "a" FROM "t" WHERE "id" > 3)
SELECT b FROM cte2;
-- UNORDERED:
1, 2, 3, 4, 5, 4, 5

-- TEST: cte-fullscan-union
-- SQL:
WITH cte1 (a) AS (SELECT "a" FROM "t"),
cte2 (b) AS (SELECT * FROM cte1 UNION SELECT "a" FROM "t" WHERE "id" > 3)
SELECT b FROM cte2;
-- UNORDERED:
1, 2, 3, 4, 5

-- TEST: cte-in-union-all
-- SQL:
WITH cte1 (a) AS (SELECT "a" FROM "t" WHERE "id" IN (1, 3, 5)),
cte2 (b) AS (SELECT * FROM cte1 UNION ALL SELECT "a" FROM "t" WHERE "id" IN (2, 4))
SELECT b FROM cte2;
-- UNORDERED:
1, 3, 5, 2, 4

-- TEST: cte-in-union
-- SQL:
WITH cte1 (a) AS (SELECT "a" FROM "t" WHERE "id" IN (1, 3, 5)),
cte2 (b) AS (SELECT * FROM cte1 UNION SELECT "a" FROM "t" WHERE "id" IN (2, 4))
SELECT b FROM cte2;
-- UNORDERED:
1, 2, 3, 4, 5

-- TEST: cte-range-three-branches-union-all
-- SQL:
WITH cte1 (a) AS (SELECT "a" FROM "t" WHERE "id" <= 2),
cte2 (b) AS (
    SELECT * FROM cte1
    UNION ALL SELECT "a" FROM "t" WHERE "id" > 2 AND "id" <= 4
    UNION ALL SELECT "a" FROM "t" WHERE "id" > 4
)
SELECT b FROM cte2;
-- UNORDERED:
1, 2, 3, 4, 5

-- TEST: cte-range-both-cte-union-all
-- SQL:
WITH cte1 (a) AS (SELECT "a" FROM "t" WHERE "id" <= 2),
cte2 (a) AS (SELECT "a" FROM "t" WHERE "id" > 3),
cte3 (b) AS (SELECT * FROM cte1 UNION ALL SELECT * FROM cte2)
SELECT b FROM cte3;
-- UNORDERED:
1, 2, 4, 5

-- TEST: cte-range-both-cte-union
-- SQL:
WITH cte1 (a) AS (SELECT "a" FROM "t" WHERE "id" <= 2),
cte2 (a) AS (SELECT "a" FROM "t" WHERE "id" > 3),
cte3 (b) AS (SELECT * FROM cte1 UNION SELECT * FROM cte2)
SELECT b FROM cte3;
-- UNORDERED:
1, 2, 4, 5

-- TEST: cte-range-reuse-union-all
-- SQL:
WITH cte1 (a) AS (SELECT "a" FROM "t" WHERE "id" > 3)
SELECT * FROM cte1 UNION ALL SELECT * FROM cte1;
-- UNORDERED:
4, 5, 4, 5

-- TEST: cte-range-with-expr-union-all
-- SQL:
WITH cte1 (a) AS (SELECT "a" + 10 FROM "t" WHERE "id" <= 3),
cte2 (b) AS (SELECT * FROM cte1 UNION ALL SELECT "a" + 20 FROM "t" WHERE "id" > 3)
SELECT b FROM cte2;
-- UNORDERED:
11, 12, 13, 24, 25
