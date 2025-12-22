-- TEST: test_limit
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t ("id" int primary key, "a" int);
INSERT INTO "t"("id", "a")
VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);

-- TEST: limit1
-- SQL:
SELECT "a" FROM "t" LIMIT 2;
-- EXPECTED:
1, 2

-- TEST: limit2
-- SQL:
SELECT "id" FROM "t" ORDER BY "id" LIMIT 2;
-- EXPECTED:
1, 2

-- TEST: limit3
-- SQL:
SELECT count(*) FROM "t" GROUP BY "id" LIMIT 3;
-- EXPECTED:
1, 1, 1

-- TEST: limit4
-- SQL:
WITH cte (b) AS (SELECT "a" FROM "t" ORDER BY "a" LIMIT 2)
        SELECT b FROM cte
        UNION ALL
        SELECT b FROM cte;
-- EXPECTED:
1, 2, 1, 2

-- TEST: limit5
-- SQL:
WITH cte (b) AS (SELECT "a" FROM "t" ORDER BY "a" LIMIT 2)
        SELECT b FROM cte
        UNION ALL
        SELECT b FROM cte
        LIMIT 1;
-- EXPECTED:
1

-- TEST: limit6
-- SQL:
SELECT "a" FROM (SELECT "a" FROM "t" LIMIT 1);
-- EXPECTED:
1

-- TEST: limit_pushdown-1.0
-- SQL:
DROP TABLE IF EXISTS "lpd";
CREATE TABLE "lpd" ("id" int primary key, "n" int) DISTRIBUTED BY ("id");
INSERT INTO "lpd" VALUES
    (1, 30), (2, 10), (3, 20), (4, 5), (5, 5),
    (6, 10), (7, 30), (8, 1), (9, 1), (10, 20);

-- TEST: limit_pushdown-1.1
-- SQL:
SELECT count(*) FROM (SELECT DISTINCT "id", "n" FROM "lpd" LIMIT 5);
-- EXPECTED:
5

-- TEST: limit_pushdown-1.2
-- SQL:
SELECT "id", "n" FROM "lpd" ORDER BY "n", "id" LIMIT 5;
-- EXPECTED:
8, 1, 9, 1, 4, 5, 5, 5, 2, 10
