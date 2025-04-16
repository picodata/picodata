-- TEST: test_limit
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t ("id" int primary key, "a" int);
INSERT INTO "t"("id", "a")
VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);

-- TEST: limit1
-- SQL:
SELECT "a" FROM "t" LIMIT 2
-- EXPECTED:
1, 2

-- TEST: limit2
-- SQL:
SELECT "id" FROM "t" ORDER BY "id" LIMIT 2
-- EXPECTED:
1, 2

-- TEST: limit3
-- SQL:
SELECT count(*) FROM "t" GROUP BY "id" LIMIT 3
-- EXPECTED:
1, 1, 1

-- TEST: limit4
-- SQL:
WITH cte (b) AS (SELECT "a" FROM "t" ORDER BY "a" LIMIT 2)
        SELECT b FROM cte
        UNION ALL
        SELECT b FROM cte
-- EXPECTED:
1, 2, 1, 2

-- TEST: limit5
-- SQL:
WITH cte (b) AS (SELECT "a" FROM "t" ORDER BY "a" LIMIT 2)
        SELECT b FROM cte
        UNION ALL
        SELECT b FROM cte
        LIMIT 1
-- EXPECTED:
1

-- TEST: limit2
-- SQL:
SELECT "a" FROM (SELECT "a" FROM "t" LIMIT 1)
-- EXPECTED:
1
