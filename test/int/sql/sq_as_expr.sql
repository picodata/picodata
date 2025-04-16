-- TEST: sq_as_expr
-- SQL:
DROP TABLE IF EXISTS testing_space;
DROP TABLE IF EXISTS null_t;
CREATE TABLE testing_space ("id" int primary key, "name" string, "product_units" int);
CREATE TABLE null_t ("na" int primary key, "nb" int, "nc" int);
INSERT INTO "testing_space" ("id", "name", "product_units") VALUES
            (1, '123', 1),
            (2, '1', 1),
            (3, '1', 1),
            (4, '2', 2),
            (5, '123', 2),
            (6, '2', 4);
INSERT INTO "null_t"
("na", "nb", "nc")
VALUES 
    (1, null, 1),
    (2, null, null),
    (3, null, 3),
    (4, 1, 2),
    (5, null, 1);

-- TEST: test_under_projection-1
-- SQL:
SELECT (VALUES (1)) FROM "testing_space" WHERE "id" = 1
-- EXPECTED:
1

-- TEST: test_under_projection-2
-- SQL:
SELECT (VALUES (1)), (VALUES (2)) FROM "testing_space" WHERE "id" = 1
-- EXPECTED:
1, 2

-- TEST: test_under_projection-3
-- SQL:
SELECT (VALUES ((VALUES (3)))) FROM "testing_space" WHERE "id" = 1
-- EXPECTED:
3

-- TEST: test_under_projection-4
-- SQL:
SELECT (SELECT "id" FROM "testing_space" WHERE "id" = 1) + "id" FROM "testing_space" WHERE "id" in (1, 2, 3)
-- EXPECTED:
2, 3, 4


-- TEST: test_under_selection-1
-- SQL:
SELECT "id" FROM "testing_space" WHERE "id" = (VALUES (1))
-- EXPECTED:
1

-- TEST: test_under_selection-2
-- SQL:
SELECT "id" FROM "testing_space" WHERE "id" = (VALUES (1)) + (VALUES (3)) / (VALUES (2))
-- EXPECTED:
2

-- TEST: test_under_group_by-1
-- SQL:
SELECT count(*) FROM "testing_space" GROUP BY "product_units" + (VALUES (1))
-- EXPECTED:
3, 2, 1

-- TEST: test_under_group_by-2
-- SQL:
SELECT sum("id") + 1, count(*)
FROM "testing_space"
GROUP BY "product_units" + (VALUES (1))
HAVING sum("id") + (VALUES (1)) > 7
-- EXPECTED:
10, 2

-- TEST: test_under_order_by-1
-- SQL:
SELECT "name", "id" FROM "testing_space" ORDER BY "name" || (VALUES ('a'))
-- EXPECTED:
'123', 1,
'123', 5, 
'1', 2,
'1', 3,
'2', 4,
'2', 6 

-- TEST: test_under_cte-1
-- SQL:
WITH "my_cte" ("first") AS (VALUES (cast(1 as string)), ((SELECT "name" FROM "testing_space" WHERE "id" = 1)))
SELECT "first" FROM "my_cte"
-- EXPECTED:
'1', '123'

-- TEST: test_under_join-1
-- SQL:
SELECT "id" FROM "testing_space" JOIN "null_t" ON
(SELECT true FROM "null_t" WHERE "na" = 1) AND "product_units" = "na" AND "name" != (VALUES ('123'))
-- EXPECTED:
2, 3, 4, 6

-- TEST: test_under_insert-1.1
-- SQL:
INSERT INTO "testing_space"
          VALUES
          (
            (VALUES (11)),
            (VALUES ('111')) || (VALUES ('222')),
            (SELECT 42 FROM "testing_space" WHERE "id" = 1)
          ),
          (
            (SELECT 42 FROM "testing_space" WHERE "id" = 1),
            'aba',
            33)

-- TEST: test_under_insert-1.2
-- SQL:
SELECT * FROM "testing_space"
-- EXPECTED:
1, '123', 1,
2, '1', 1,
3, '1', 1,
4, '2', 2,
5, '123', 2,
6, '2', 4,
11, '111222', 42,
42, 'aba', 33

-- TEST: test_under_insert-1.3
-- SQL:
DELETE FROM "testing_space";
INSERT INTO "testing_space" ("id", "name", "product_units") VALUES
            (1, '123', 1),
            (2, '1', 1),
            (3, '1', 1),
            (4, '2', 2),
            (5, '123', 2),
            (6, '2', 4);

-- TEST: test_under_update-1.1
-- SQL:
update "testing_space"
set "name" = (SELECT "name" FROM "testing_space" WHERE "product_units" = 4), "product_units" = (VALUES (42))

-- TEST: test_under_update-1.2
-- SQL:
SELECT * FROM "testing_space"
-- EXPECTED:
1, '2', 42,
2, '2', 42,
3, '2', 42,
4, '2', 42,
5, '2', 42,
6, '2', 42

-- TEST: test_under_update-1.3
-- SQL:
DELETE FROM "testing_space";
INSERT INTO "testing_space" ("id", "name", "product_units") VALUES
            (1, '123', 1),
            (2, '1', 1),
            (3, '1', 1),
            (4, '2', 2),
            (5, '123', 2),
            (6, '2', 4);