-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- TEST: initialization
-- SQL:
CREATE TABLE t ("id" INT PRIMARY KEY, "a" INT);
INSERT INTO t VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);

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
-- UNORDERED:
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

-- TEST: cte-chain-reused
-- SQL:
WITH c0 (a) AS (SELECT "a" FROM "t" WHERE "a" = 100),
c1 (a) AS (SELECT a FROM c0 UNION ALL SELECT a FROM c0),
c2 (a) AS (SELECT a FROM c1 UNION ALL SELECT a FROM c1),
c3 (a) AS (SELECT a FROM c2 UNION ALL SELECT a FROM c2),
c4 (a) AS (SELECT a FROM c3 UNION ALL SELECT a FROM c3),
c5 (a) AS (SELECT a FROM c4 UNION ALL SELECT a FROM c4),
c6 (a) AS (SELECT a FROM c5 UNION ALL SELECT a FROM c5),
c7 (a) AS (SELECT a FROM c6 UNION ALL SELECT a FROM c6),
c8 (a) AS (SELECT a FROM c7 UNION ALL SELECT a FROM c7),
c9 (a) AS (SELECT a FROM c8 UNION ALL SELECT a FROM c8),
c10 (a) AS (SELECT a FROM c9 UNION ALL SELECT a FROM c9),
c11 (a) AS (SELECT a FROM c10 UNION ALL SELECT a FROM c10),
c12 (a) AS (SELECT a FROM c11 UNION ALL SELECT a FROM c11),
c13 (a) AS (SELECT a FROM c12 UNION ALL SELECT a FROM c12),
c14 (a) AS (SELECT a FROM c13 UNION ALL SELECT a FROM c13),
c15 (a) AS (SELECT a FROM c14 UNION ALL SELECT a FROM c14),
c16 (a) AS (SELECT a FROM c15 UNION ALL SELECT a FROM c15),
c17 (a) AS (SELECT a FROM c16 UNION ALL SELECT a FROM c16),
c18 (a) AS (SELECT a FROM c17 UNION ALL SELECT a FROM c17),
c19 (a) AS (SELECT a FROM c18 UNION ALL SELECT a FROM c18),
c20 (a) AS (SELECT a FROM c19 UNION ALL SELECT a FROM c19),
c21 (a) AS (SELECT a FROM c20 UNION ALL SELECT a FROM c20),
c22 (a) AS (SELECT a FROM c21 UNION ALL SELECT a FROM c21),
c23 (a) AS (SELECT a FROM c22 UNION ALL SELECT a FROM c22),
c24 (a) AS (SELECT a FROM c23 UNION ALL SELECT a FROM c23),
c25 (a) AS (SELECT a FROM c24 UNION ALL SELECT a FROM c24),
c26 (a) AS (SELECT a FROM c25 UNION ALL SELECT a FROM c25),
c27 (a) AS (SELECT a FROM c26 UNION ALL SELECT a FROM c26),
c28 (a) AS (SELECT a FROM c27 UNION ALL SELECT a FROM c27),
c29 (a) AS (SELECT a FROM c28 UNION ALL SELECT a FROM c28),
c30 (a) AS (SELECT a FROM c29 UNION ALL SELECT a FROM c29),
c31 (a) AS (SELECT a FROM c30 UNION ALL SELECT a FROM c30),
c32 (a) AS (SELECT a FROM c31 UNION ALL SELECT a FROM c31)
SELECT count(*) FROM c32;
-- EXPECTED:
0
