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

-- TEST: init
-- SQL:
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1(a INT PRIMARY KEY, b INT, c INT);
CREATE TABLE t2(a INT PRIMARY KEY, b INT, c INT);

-- TEST: cte-direct-child-of-motion-is-not-removed-on-take-subtree-1
-- SQL:
WITH d AS (
    SELECT e.a f, g.c , h.a
    FROM t2 h JOIN t1 g ON g.b = h.a
    JOIN t2 e ON e.c >= e.c
    ORDER BY 2
)
SELECT i.f  FROM t1 h
JOIN t2 g ON h.a = g.c
JOIN d  ON g.c = 2 JOIN d i ON i.a >= i.f;
-- EXPECTED:

-- TEST: cte-direct-child-of-motion-is-not-removed-on-take-subtree-2
-- SQL:
WITH a AS (SELECT 1) SELECT 2 FROM a UNION SELECT 3 FROM a LEFT JOIN t1 ON TRUE;
-- EXPECTED:
2, 3

-- TEST: init_2
-- SQL:
DROP TABLE IF EXISTS t1;
CREATE TABLE t1( a int primary key, b int, c int);
INSERT INTO "t1"("a", "b", "c")
VALUES (3, 4, 5);

-- TEST: cte-with-values-correct-columns-names-1
-- SQL:
WITH i(g) AS (
    VALUES(
        (SELECT d.g FROM t1 JOIN (SELECT 7 AS g) AS d ON true)
    ),
    (1)
)
SELECT i.g FROM i;
-- EXPECTED:
7, 1

-- TEST: cte-with-values-correct-columns-names-2
-- SQL:
WITH i(g) AS (
    VALUES(
        (SELECT d.g FROM t1 JOIN (SELECT 7 AS g) AS d ON true)
    )
)
SELECT i.g FROM i;
-- EXPECTED:
7

-- TEST: cte-with-values-correct-columns-names-3
-- SQL:
WITH i(g) AS (
    VALUES (1), (
        (SELECT d.g FROM t1 JOIN (SELECT 7 AS g) AS d ON true)
    )
)
SELECT i.g FROM i;
-- EXPECTED:
1, 7

-- TEST: cte-with-values-correct-columns-names-4
-- SQL:
WITH i(g) AS (
    VALUES (1), (
        (SELECT d.g FROM t1 JOIN (select (values (5)) as g) d ON true))
    )
SELECT i.g FROM i;
-- EXPECTED:
1, 5

-- TEST: cte-with-values-correct-columns-names-5
-- SQL:
WITH i(g) AS (
    VALUES (
        (SELECT d.g FROM t1 JOIN (select (values (5)) as g) d ON true))
        , (1)
    )
SELECT i.g FROM i;
-- EXPECTED:
5, 1

-- TEST: cte-with-values-correct-columns-names-6
-- SQL:
WITH i(g, t) AS (
    VALUES(
        (SELECT d.g FROM t1 JOIN (SELECT 7 AS g) AS d ON true), 2
    ),
    (1, 5)
)
SELECT * FROM i;
-- EXPECTED:
7, 2, 1, 5

-- TEST: cte-with-values-correct-columns-names-7
-- SQL:
WITH d AS (
    SELECT 
        1 AS g, 
        t1.b
    FROM t1 AS f
    JOIN t1 ON f.a >= f.a
),
i(e, g, h) AS (
    VALUES
        (
            0, 
            (
                SELECT j.g
                FROM t1
                JOIN d ON c = 5
                JOIN d AS j ON TRUE
            ), 
            1
        ),
        (7, 1, 1)
)
SELECT 
    k.g
FROM d
JOIN i AS k ON TRUE;
-- EXPECTED:
1, 1

-- TEST: cte-with-values-correct-columns-names-8
-- SQL:
WITH table1(a, b, c) AS (
    VALUES (1, 1, 1)
),
table2(a, b, d) AS (
    VALUES(1, 1, 2)
),
res_table AS (
    SELECT
        t2.a,
        t1.c,
        t2.d
    FROM table1 t1
    JOIN table2 t2 ON t1.a = t2.a AND t1.b = t2.b
)
SELECT * from res_table;
-- EXPECTED:
1, 1, 2

-- TEST: cte-with-values-correct-columns-names-9
-- SQL:
WITH cte1 (a) AS (
    SELECT "a"
    FROM "t"
    WHERE "id" = 1
),
cte2 (b) AS (
    SELECT *
    FROM cte1
    UNION ALL
    SELECT "a"
    FROM "t"
    WHERE "id" = 2
)
SELECT b
FROM cte2;
-- EXPECTED:
1, 2

-- TEST: cte-with-values-correct-columns-names-10
-- SQL:
WITH c (d, e) AS (
    VALUES (1, 4)
),
f AS (
    SELECT g.b AS e,
           g.a AS h
    FROM t1 AS g
),
i AS (
    SELECT h AS d,
           e
    FROM f
),
j (d, e) AS (
    VALUES (0, (
        SELECT 0
        FROM c
    ))
)
SELECT 8
FROM f AS g
JOIN i AS t1 ON g.e = t1.e
JOIN j AS k ON true;
-- EXPECTED:
8

-- TEST: cte-with-values-correct-columns-names-11
-- SQL:
WITH d (e) AS (
    VALUES (8)
),
g AS (
    SELECT (
        SELECT d.e
        FROM d
        EXCEPT
        SELECT 0
        FROM t1
        EXCEPT
        SELECT 8
        FROM t1
    )
)
SELECT 1
FROM g
-- EXPECTED:
1