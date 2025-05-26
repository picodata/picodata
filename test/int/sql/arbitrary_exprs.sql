-- TEST: test_arbitrary_expr
-- SQL:
DROP TABLE IF EXISTS arithmetic_space;
CREATE TABLE arithmetic_space (id int primary key, a int, b int, c int, d int, e int, f int, boolean_col bool, string_col string, number_col double);
INSERT INTO "arithmetic_space"
("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
VALUES (1, 1, 2, 3, -1, -1, -1, true, '123', 1),
        (2, 2, 4, 6, -2, -2, -2, true, '123', 2),
        (3, 3, 6, 9, -3, -3, -3, true, '123', 3),
        (4, 4, 8, 12, -4, -4, -4, true, '123', 4),
        (5, 5, 10, 15, -5, -5, -5, true, '123', 5),
        (6, 6, 12, 18, -6, -6, -6, true, '123', 6),
        (7, 7, 14, 21, -7, -7, -7, true, '123', 7),
        (8, 8, 16, 24, -8, -8, -8, true, '123', 8),
        (9, 9, 18, 27, -9, -9, -9, true, '123', 9),
        (10, 10, 20, 30, -10, -10, -10, true, '123', 10);

-- TEST: test_arbitrary_invalid-1
-- SQL:
select "id" + 1 as "alias" > a from "arithmetic_space"
-- ERROR:
rule parsing error

-- TEST: test_arbitrary_invalid-2
-- SQL:
'select id" + 1 as "alias" > "a" is not null from "arithmetic_space"'
-- ERROR:
rule parsing error

-- TEST: test_arbitrary_invalid-3
-- SQL:
select "a" + "b" and true from "arithmetic_space"
-- ERROR:
could not resolve operator overload for and\(int, bool\)

-- TEST: test_arbitrary_invalid-4
-- SQL:
SELECT
    CASE "id"
        WHEN 1 THEN 'first'::text
        ELSE 42
    END "case_result"
FROM "arithmetic_space"
-- ERROR:
CASE/THEN types text and int cannot be matched

-- TEST: test_arbitrary_valid-1
-- SQL:
select "id" from "arithmetic_space"
-- EXPECTED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_arbitrary_valid-2
-- SQL:
select "id" - 9 > 0 from "arithmetic_space"
-- EXPECTED:
False, False, False, False, False, False, False, False, False, True

-- TEST: test_arbitrary_valid-3
-- SQL:
select "id" + "b" > "id" + "b", "id" + "b" > "id" + "b" as "cmp" from "arithmetic_space"
-- EXPECTED:
False, False, False, False, False, False, False,
False, False, False, False, False, False, False,
False, False, False, False, False, False,

-- TEST: test_arbitrary_valid-4
-- SQL:
select 0 = "id" + "f", 0 = "id" + "f" as "cmp" from "arithmetic_space"
-- EXPECTED:
True, True, True, True, True, True,
True, True, True, True, True, True,
True, True, True, True, True, True,
True, True

-- TEST: test_arbitrary_valid-5
-- SQL:
select 1 > 0, 1 > 0 as "cmp" from "arithmetic_space"
-- EXPECTED:
True, True, True, True, True, True,
True, True, True, True, True, True,
True, True, True, True, True, True,
True, True

-- TEST: test_arbitrary_valid-6
-- SQL:
select
    "id" between "id" - 1 and "id" * 4,
    "id" between "id" - 1 and "id" * 4 as "between"
from
    "arithmetic_space"
-- EXPECTED:
True, True, True, True, True, True,
True, True, True, True, True, True,
True, True, True, True, True, True,
True, True

-- TEST: test_arbitrary_valid-7
-- SQL:
select
    "id" between "id" - 1 and "id" * 4,
    "id" between "id" - 1 and "id" * 4 as "between"
from
    "arithmetic_space"
-- EXPECTED:
True, True, True, True, True, True,
True, True, True, True, True, True,
True, True, True, True, True, True,
True, True

-- TEST: test_arbitrary_valid-8
-- SQL:
SELECT "COLUMN_1" FROM (VALUES (1))
-- EXPECTED:
1

-- TEST: test_arbitrary_valid-9
-- SQL:
SELECT CAST("COLUMN_1" as int) FROM (VALUES (1))
-- EXPECTED:
1

-- TEST: test_arbitrary_valid-10
-- SQL:
SELECT "COLUMN_1" as "колонка" FROM (VALUES (1))
-- EXPECTED:
1

-- TEST: test_arbitrary_valid-11
-- SQL:
SELECT
    CASE "id"
        WHEN 1 THEN 'first'
        WHEN 2 THEN 'second'
        ELSE '42'
    END "case_result"
FROM "arithmetic_space"
-- EXPECTED:
'first', 'second', '42', '42', '42', '42', '42', '42', '42', '42'

-- TEST: test_arbitrary_valid-12
-- SQL:
SELECT
    "id",
    "val",
    CASE "id"
        WHEN 5 THEN 'five'
        WHEN "val" THEN 'equal'
    END "case_result"
FROM "arithmetic_space"
INNER JOIN
(SELECT "COLUMN_2" as "val" FROM (VALUES (1), (2))) AS "values"
ON true
-- EXPECTED:
1, 1, 'equal', 2, 1, None,
3, 1, None, 4, 1, None,
5, 1, 'five', 6, 1, None,
7, 1, None, 8, 1, None, 9,
1, None, 10, 1, None, 1,
2, None, 2, 2, 'equal', 3,
2, None, 4, 2, None, 5,
2, 'five', 6, 2, None, 7,
2, None, 8, 2, None,
9, 2, None, 10, 2, None

-- TEST: test_arbitrary_valid-13
-- SQL:
SELECT
    "id",
    CASE
        WHEN "id" = 7 THEN 'first'
        WHEN "id" / 2 < 4 THEN 'second'
    END "case_result"
FROM "arithmetic_space"
-- EXPECTED:
1, 'second',
2, 'second',
3, 'second',
4, 'second',
5, 'second',
6, 'second',
7, 'first',
8, null,
9, null,
10, null,

-- TEST: test_arbitrary_valid-14
-- SQL:
SELECT
    "id",
    CASE
        WHEN false THEN 0
        WHEN "id" < 3 THEN 1
        WHEN "id" > 3 AND "id" < 8 THEN 2
        ELSE
            CASE
                WHEN "id" = 8 THEN 3
                WHEN "id" = 9 THEN 4
                ELSE 0
            END
    END
FROM "arithmetic_space"
-- EXPECTED:
1, 1, 2, 1, 3, 0, 4, 2, 5, 2, 6, 2, 7, 2, 8, 3, 9, 4, 10, 0

-- TEST: test_values-1
-- SQL:
VALUES (8, 8, null), (9, 9, 'hello')
-- EXPECTED:
8, 8, null, 9, 9, 'hello'

-- TEST: test_values-2
-- SQL:
VALUES (9, 9, 'hello'), (8, 8, null)
-- EXPECTED:
9, 9, 'hello', 8, 8, null
