-- TEST: test_cte
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t ("id" int primary key, "a" int);
INSERT INTO "t"("id", "a")
VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);

-- TEST: cte1
-- SQL:
WITH cte (b) AS (SELECT "a" FROM "t" WHERE "id" > 3)
        SELECT b FROM cte
-- EXPECTED:
4, 5

-- TEST: cte2
-- SQL:
WITH cte1 (b) AS (SELECT "a" FROM "t" WHERE "id" > 3),
             cte2 AS (SELECT b FROM cte1)
        SELECT * FROM cte2
-- EXPECTED:
4, 5

-- TEST: cte3
-- SQL:
WITH cte (b) AS (SELECT "a" FROM "t" WHERE "id" > 3)
        SELECT b FROM cte
        UNION ALL
        SELECT b FROM cte
-- EXPECTED:
4, 5, 4, 5

-- TEST: cte4
-- SQL:
WITH cte (b) AS (SELECT "a" FROM "t" WHERE "id" = 1 OR "id" = 2)
    SELECT cte.b, "t"."a" FROM cte JOIN "t" ON cte.b = "t"."id"
-- EXPECTED:
1, 1, 2, 2

-- TEST: cte5
-- SQL:
WITH cte (b) AS (SELECT "a" FROM "t" WHERE "id" = 1 OR "id" = 2)
        SELECT cte.b, "t"."a" FROM cte LEFT JOIN "t" ON cte.b = "t"."id"
-- EXPECTED:
1, 1, 2, 2

-- TEST: cte6
-- SQL:
WITH cte (b) AS (SELECT "a" FROM "t" WHERE "id" = 1 OR "id" = 2),
        r (a) as (SELECT cte.b FROM cte LEFT JOIN "t" ON cte.b = "t"."id")
        select b from cte where b in (select a from r)
-- EXPECTED:
1, 2

-- TEST: cte7
-- SQL:
WITH cte (b) AS (SELECT "a" FROM "t" WHERE "id" > 3)
        SELECT count(b) FROM cte
-- EXPECTED:
2

-- TEST: cte8
-- SQL:
WITH cte (b) AS (SELECT "a" FROM "t" WHERE "id" IN (1, 2, 3))
        SELECT * FROM "t" WHERE "a" IN (SELECT b FROM cte)
-- EXPECTED:
1, 1, 2, 2, 3, 3

-- TEST: cte9
-- SQL:
WITH cte (b) AS (VALUES (1), (2), (3))
        SELECT b FROM cte
-- EXPECTED:
1, 2, 3

-- TEST: cte10
-- SQL:
WITH c1 (a) AS (VALUES (1), (2)),
        c2 AS (SELECT * FROM c1 UNION SELECT * FROM c1)
SELECT a FROM c2
-- EXPECTED:
1, 2

-- TEST: cte11
-- SQL:
WITH cte1 (a) AS (SELECT "a" FROM "t" WHERE "id" = 1),
        cte2 (b) AS (SELECT * FROM cte1 UNION ALL SELECT "a" FROM "t" WHERE "id" = 2)
        SELECT b FROM cte2
-- EXPECTED:
1, 2

-- TEST: cte12
-- SQL:
WITH cte (c) AS (
    SELECT t1."a" FROM "t" t1
    JOIN "t" t2 ON t1."a" = t2."id"
    WHERE t1."id" = 1
)
SELECT c FROM cte
-- EXPECTED:
1

-- TEST: cte13
-- SQL:
WITH cte (b) AS (SELECT "a" FROM "t" WHERE "id" > 3 ORDER BY "a" DESC)
    SELECT b FROM cte
-- EXPECTED:
5, 4

-- TEST: cte14
-- SQL:
WITH cte (b) AS (SELECT "a" FROM "t" WHERE "id" > 3)
SELECT t.c FROM (SELECT count(*) as c FROM cte c1 JOIN cte c2 ON true) t
JOIN cte ON true
-- EXPECTED:
4, 4

-- TEST: cte15
-- SQL:
WITH cte (b) AS (SELECT "id" FROM "t" WHERE "id" = 1)
        SELECT t.c FROM (SELECT count(*) as c FROM cte c1 JOIN cte c2 ON true) t
        JOIN cte ON true
-- EXPECTED:
1

-- TEST: cte16
-- SQL:
WITH cte (b) AS (VALUES (1))
SELECT t.c FROM (SELECT count(*) as c FROM cte c1 JOIN cte c2 ON true) t
JOIN cte ON true
-- EXPECTED:
1

-- TEST: cte17
-- SQL:
WITH cte1(a) as (VALUES(1)),
cte2(a) as (SELECT a1.a FROM cte1 a1 JOIN "t" ON true UNION SELECT * FROM cte1 a2)
SELECT * FROM cte2
-- EXPECTED:
1

-- TEST: cte18
-- SQL:
WITH cte1(a) as (VALUES(1)),
cte2(a) as (
    SELECT a1.a FROM cte1 a1 JOIN "t" ON a1.a = "id"
    UNION ALL
    SELECT * FROM cte1 a2
    UNION ALL
    SELECT * FROM cte1 a3
)
SELECT * FROM cte2
-- EXPECTED:
1, 1, 1
