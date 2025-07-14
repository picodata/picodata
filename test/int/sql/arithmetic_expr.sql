-- TEST: test_arithmetic_expr
-- SQL:
DROP TABLE IF EXISTS arithmetic_space;
DROP TABLE IF EXISTS arithmetic_space2;
CREATE TABLE arithmetic_space (id int primary key, a int, b int, c int, d int, e int, f int, boolean_col bool, string_col string, number_col double);
CREATE TABLE arithmetic_space2 (id int primary key, a int, b int, c int, d int, e int, f int, boolean_col bool, string_col string, number_col double);
INSERT INTO "arithmetic_space"
("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
VALUES (1, 1, 2, 3, 1, 1, 1, true, '123', 4.6),
        (2, 2, 4, 6, 2, 2, 2, true, '123', 4.6),
        (3, 3, 6, 9, 3, 3, 3, true, '123', 4.6),
        (4, 4, 8, 12, 4, 4, 4, true, '123', 4.6),
        (5, 5, 10, 15, 5, 5, 5, true, '123', 4.6),
        (6, 6, 12, 18, 6, 6, 6, true, '123', 4.6),
        (7, 7, 14, 21, 7, 7, 7, true, '123', 4.6),
        (8, 8, 16, 24, 8, 8, 8, true, '123', 4.6),
        (9, 9, 18, 27, 9, 9, 9, true, '123', 4.6),
        (10, 10, 20, 30, 10, 10, 10, true, '123', 4.6);
INSERT INTO "arithmetic_space2"
("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
VALUES (1, 1, 1, 1, 1, 1, 1, false, '123', 4.599999),
        (2, 2, 2, 2, 2, 2, 2, false, '123', 4.599999),
        (3, 3, 3, 3, 3, 3, 3, false, '123', 4.599999),
        (4, 4, 4, 4, 4, 4, 4, false, '123', 4.599999),
        (5, 5, 5, 5, 5, 5, 5, false, '123', 4.599999),
        (6, 6, 6, 6, 6, 6, 6, false, '123', 4.599999),
        (7, 7, 7, 7, 7, 7, 7, false, '123', 4.599999),
        (8, 8, 8, 8, 8, 8, 8, false, '123', 4.599999),
        (9, 9, 9, 9, 9, 9, 9, false, '123', 4.599999),
        (10, 10, 10, 10, 10, 10, 10, false, '123', 4.599999);


-- TEST: test-arithmetic-modulo-1
-- SQL:
select "id" from "arithmetic_space" where "id" % 2 > 0
-- EXPECTED:
1, 3, 5, 7, 9

-- TEST: test-arithmetic-modulo-2
-- SQL:
select "id" % 2 from "arithmetic_space"
-- EXPECTED:
1, 0, 1, 0, 1, 0, 1, 0, 1, 0

-- TEST: test_arithmetic_invalid1-2
-- SQL:
select "id" from "arithmetic_space" where "id" ^ 2 > 0
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid1-3
-- SQL:
select "id" from "arithmetic_space" where "id" ++ 2 > 0
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid1-4
-- SQL:
select "id" from "arithmetic_space" where "id" ** 2 > 0
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid1-5
-- SQL:
select "id" from "arithmetic_space" where "id" // 2 > 0
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid1-6
-- SQL:
select "id" from "arithmetic_space" where "id" ** 2 > 0
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid1-7
-- SQL:
select "id" from "arithmetic_space" where "id" +- 2 > 0
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid1-8
-- SQL:
select "id" from "arithmetic_space" where "id" +* 2 > 0
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid1-9
-- SQL:
select "id" from "arithmetic_space" where "boolean_col" + "boolean_col" > 0
-- ERROR:
could not resolve operator overload for \+\(bool, bool\)

-- TEST: test_arithmetic_invalid1-10
-- SQL:
select "id" from "arithmetic_space" where "string_col" + "string_col" > 0
-- ERROR:
could not resolve operator overload for \+\(text, text\)

-- TEST: test_arithmetic_invalid2-1
-- SQL:
select "id" as "alias1" + "a" as "alias2" from "arithmetic_space"
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid2-2
-- SQL:
select ("id" + "a") as "alias1" + "b" as "alias2" from "arithmetic_space"
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid2-4
-- SQL:
select "id" ^ 2 from "arithmetic_space"
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid2-5
-- SQL:
select "id" ++ 2 from "arithmetic_space"
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid2-6
-- SQL:
select "id" ** 2 from "arithmetic_space"
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid2-7
-- SQL:
select "id" // 2 from "arithmetic_space"
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid2-8
-- SQL:
select "id" ** 2 from "arithmetic_space"
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid2-9
-- SQL:
select "id" +- 2 from "arithmetic_space"
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid2-10
-- SQL:
select "id" +* 2 from "arithmetic_space"
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid2-11
-- SQL:
select "boolean_col" + "boolean_col" from "arithmetic_space"
-- ERROR:
could not resolve operator overload for \+\(bool, bool\)

-- TEST: test_arithmetic_invalid2-12
-- SQL:
select "string_col" + "string_col" from "arithmetic_space"
-- ERROR:
could not resolve operator overload for \+\(text, text\)

-- TEST: test_arithmetic_valid-1
-- SQL:
select "id" from "arithmetic_space"
-- EXPECTED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_arithmetic_valid-2
-- SQL:
select "id" from "arithmetic_space" where 2 + 2 = 4
-- EXPECTED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_arithmetic_valid-3
-- SQL:
select "id" from "arithmetic_space"
where
    "id" + "id" > 0 and "id" + "id" + "id" > 0
    or ("id" * "id" > 0 and "id" * "id" * "id" > 0)
    or ("id" - "id" < 0 and "id" - "id" - "id" < 0)
    or ("id" / "id" > 0 and "id" / "id" / "id" > 0)
-- EXPECTED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_arithmetic_valid-4
-- SQL:
select "id" from "arithmetic_space"
where
    "id" + "id" * "id" + "id" >= 0
    and "id" - "id" * "id" - "id" <= 0
    and "id" + "id" / "id" + "id" >= 0
    and "id" - "id" / "id" - "id" <= 0
-- EXPECTED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_arithmetic_with_bool-1
-- SQL:
select "id" from "arithmetic_space"
-- EXPECTED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_arithmetic_with_bool-2
-- SQL:
select "id" from "arithmetic_space"
where "id" + "a" >= 0
    and "id" + "b" <= 12
    and "id" + "d" > 0
    and "id" + "e" < 8
    and "id" + "d" = 2
    and "id" + "a" != 3
-- EXPECTED:
1

-- TEST: test_arithmetic_with_bool-3
-- SQL:
select "id" from "arithmetic_space"
where "id" + "a" >= "id" * 2
    and "id" + "c" <= "id" * 4
    and "id" + "b" > "id" * "a"
    and "id" + "a" < "id" + 3
    and "id" + "a" = 2
    and "id" + "a" != 4
-- EXPECTED:
1

-- TEST: test_arithmetic_with_bool-4
-- SQL:
select "id" from "arithmetic_space"
where "id" + "a" >= "id"
    and "id" + "b" <= "c"
    and "id" + "d" > "e"
    and "id" + "f" < "c"
    and "id" + "a" = "b"
    and "id" + "a" != "c"
-- EXPECTED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_arithmetic_with_bool-5
-- SQL:
select "id" from "arithmetic_space"
where 12 >= "id" + "a"
    and 4 <= "id" + "d"
    and 12 > "id" + "e"
    and 4 < "id" + "f"
    and 20 = "id" + "c"
    and 9 != "id" + "b"
-- EXPECTED:
5

-- TEST: test_arithmetic_with_bool-6
-- SQL:
select "id" from "arithmetic_space"
where "c" >= "id" + "b"
    and "b" <= "id" + "c"
    and "c" > "id" + "a"
    and "id" < "a" + "e"
    and "b" = "id" + "f"
    and "c" != "id" + "a"
-- EXPECTED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_selection_simple_arithmetic-1
-- SQL:
select "id" from "arithmetic_space"
-- EXPECTED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_selection_simple_arithmetic-2
-- SQL:
select "id" from "arithmetic_space" where "id" + 1 > 8
-- EXPECTED:
8, 9, 10

-- TEST: test_selection_simple_arithmetic-3
-- SQL:
select "id" from "arithmetic_space" where "id" between "id" - 1 and "id" * 4
-- EXPECTED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_selection_simple_arithmetic-4
-- SQL:
select "id" from "arithmetic_space"
        where ("id" > "a" * 2 or "id" * 2 > 10) and "id" - 6 != 0
-- EXPECTED:
7, 8, 9, 10

-- TEST: test_associativity-1
-- SQL:
select "id" from "arithmetic_space"
-- EXPECTED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_associativity-2
-- SQL:
select "id" from "arithmetic_space" where "a" + ("b" + "c") = ("a" + "b") + "c"
-- EXPECTED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_associativity-3
-- SQL:
select "id" from "arithmetic_space" where "a" * ("b" * "c") = ("a" * "b") * "c"
-- EXPECTED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_associativity-4
-- SQL:
select "id" from "arithmetic_space" where ("a" - "b") - "c" = "a" - "b" - "c"
-- EXPECTED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_associativity-5
-- SQL:
select "id" from "arithmetic_space" where "a" - ("b" - "c" ) = "a" - "b" - "c"
-- EXPECTED:

-- TEST: test_associativity-6
-- SQL:
select "id" from "arithmetic_space" where
    (cast("a" as decimal) / cast("b" as decimal)) / cast("c" as decimal) =
    cast("a" as decimal) / cast("b" as decimal) / cast("c" as decimal)
-- EXPECTED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_associativity-7
-- SQL:
select "id" from "arithmetic_space" where
    cast("a" as decimal) / (cast("b" as decimal) / cast("c" as decimal)) =
    (cast("a" as decimal) / cast("b" as decimal)) / cast("c" as decimal)
-- EXPECTED:


-- TEST: test_commutativity-1
-- SQL:
select "id" from "arithmetic_space"
-- EXPECTED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_commutativity-2
-- SQL:
select "id" from "arithmetic_space" where "a" + "b" = "b" + "a"
-- EXPECTED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_commutativity-3
-- SQL:
select "id" from "arithmetic_space" where "a" * "b" = "b" * "a"
-- EXPECTED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_commutativity-4
-- SQL:
select "id" from "arithmetic_space" where "a" - "b" = "b" - "a"
except
select "id" from "arithmetic_space" where "a" = "b"
-- EXPECTED:

-- TEST: test_commutativity-5
-- SQL:
select "id" from "arithmetic_space"
    where cast("b" as decimal) / cast("a" as decimal) = cast("a" as decimal) / cast("b" as decimal)
except
select "id" from "arithmetic_space"
    where "a" = "b" or "a" = -1 * "b"
-- EXPECTED:


-- TEST: test_distributivity-1
-- SQL:
select "id" from "arithmetic_space"
-- EXPECTED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_distributivity-2
-- SQL:
select "id" from "arithmetic_space" where
    "a"  * ("b" + "c") = "a" * "b" + "a" * "c"
-- EXPECTED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_distributivity-3
-- SQL:
select "id" from "arithmetic_space" where
    ("a" + "b") * "c" = "a" * "c" + "b" * "c"
-- EXPECTED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_distributivity-4
-- SQL:
select "id" from "arithmetic_space" where
    (cast("a" as decimal) + cast("b" as decimal)) / cast("c" as decimal) =
    cast("a" as decimal) / cast("c" as decimal) + cast("b" as decimal) / cast("c" as decimal)
-- EXPECTED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_distributivity-5
-- SQL:
select "id" from "arithmetic_space" where
cast("a" as decimal) / (cast("b" as decimal) + cast("c" as decimal)) =
cast("a" as decimal) / cast("b" as decimal) + cast("a" as decimal) / cast("c" as decimal)
-- EXPECTED:


-- TEST: test_arithmetic_in_parens-1
-- SQL:
select "c" from "arithmetic_space" where "a" + "b" > 1
-- EXPECTED:
3, 6, 9, 12, 15, 18, 21, 24, 27, 30

-- TEST: test_arithmetic_in_parens-2
-- SQL:
select "c" from "arithmetic_space" where ("a" + "b" > 1)
-- EXPECTED:
3, 6, 9, 12, 15, 18, 21, 24, 27, 30

-- TEST: test_arithmetic_in_subquery-1
-- SQL:
select "id" from "arithmetic_space"
-- EXPECTED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_arithmetic_in_subquery-2
-- SQL:
select "id" from "arithmetic_space"
where exists (select (1 + 2) * 3 / 4 from "arithmetic_space" where (1 * 2) / (8 / 4) = "id")
-- EXPECTED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_arithmetic_in_subquery-3
-- SQL:
select "id" from "arithmetic_space"
where exists (select * from "arithmetic_space" where 1 * 1 = 2)
-- EXPECTED:


-- TEST: test_arithmetic_in_subquery-4
-- SQL:
select "id" from "arithmetic_space"
where "id" in (select 2 * 3 from "arithmetic_space")
-- EXPECTED:
6

-- TEST: test_arithmetic_in_subquery-5
-- SQL:
select "id" from "arithmetic_space"
where "id" in (
    select 1 + 0 from "arithmetic_space" where exists (
        select 1 * (2 + 3) from (select * from (values (1)))
    )
)
-- EXPECTED:
1

-- TEST: test_join_simple_arithmetic
-- SQL:
SELECT "t3"."id", "t3"."a", "t8"."b"
FROM
    (SELECT "id", "a"
        FROM "arithmetic_space"
        WHERE "c" < 0
    UNION ALL
        SELECT "id", "a"
        FROM "arithmetic_space"
        WHERE "c" > 0) AS "t3"
INNER JOIN
    (SELECT "id" as "id1", "b"
        FROM "arithmetic_space2"
        WHERE "b" < 0
    UNION ALL
    SELECT "id" as "id1", "b"
        FROM "arithmetic_space2"
        WHERE "b" > 0) AS "t8"
ON "t3"."id" + "t3"."a" * 2 = "t8"."id1" + "t8"."b"
WHERE "t3"."id" = 2
-- EXPECTED:
2, 2, 3

-- TEST: test_projection_selection_join-1
-- SQL:
SELECT
    "t3"."id",
    "t3"."a",
    "t8"."b",
    "t3"."id" + "t3"."a" + "t8"."b" as "sum"
FROM
    (SELECT "id", "a"
        FROM "arithmetic_space"
        WHERE "c" < 0
    UNION ALL
        SELECT "id", "a"
        FROM "arithmetic_space"
        WHERE "c" > 0) AS "t3"
INNER JOIN
    (SELECT "id" as "id1", "b"
        FROM "arithmetic_space2"
        WHERE "b" < 0
    UNION ALL
    SELECT "id" as "id1", "b"
        FROM "arithmetic_space2"
        WHERE "b" > 0) AS "t8"
ON "t3"."id" + "t3"."a" * 2 = "t8"."id1" + "t8"."b"
-- EXPECTED:
2, 2, 3, 7, 4, 4, 6, 14, 6, 6, 9, 21

-- TEST: test_projection_selection_join-2
-- SQL:
SELECT "t3"."id", "t3"."a", "t8"."b", "t3"."id" * "t3"."a" * "t8"."b" + 1 as "mul"
FROM
    (SELECT "id", "a"
        FROM "arithmetic_space"
        WHERE "c" < 0
    UNION ALL
        SELECT "id", "a"
        FROM "arithmetic_space"
        WHERE "c" > 0) AS "t3"
INNER JOIN
    (SELECT "id" as "id1", "b"
        FROM "arithmetic_space2"
        WHERE "b" < 0
    UNION ALL
    SELECT "id" as "id1", "b"
        FROM "arithmetic_space2"
        WHERE "b" > 0) AS "t8"
ON "t3"."id" + "t3"."a" * 2 = "t8"."id1" + "t8"."b"
-- EXPECTED:
2, 2, 3, 13, 4, 4, 6, 97, 6, 6, 9, 325

-- TEST: test_arithmetic_in_parens-1
-- SQL:
select t1.a1, t2.a2 from (select "a" as a1 from "arithmetic_space") as t1
inner join (select "c" as a2 from "arithmetic_space2") as t2
on t1.a1 = t2.a2 * 2
-- EXPECTED:
2, 1, 4, 2, 6, 3, 8, 4, 10, 5

-- TEST: test_arithmetic_in_parens-2
-- SQL:
select t1.a1, t2.a2 from (select "a" as a1 from "arithmetic_space") as t1
inner join (select "c" as a2 from "arithmetic_space2") as t2
on (t1.a1 = t2.a2 * 2)
-- EXPECTED:
2, 1, 4, 2, 6, 3, 8, 4, 10, 5

-- TEST: test_alias-1
-- SQL:
select "id", "id" + "a", "id" * "a" , "a" from "arithmetic_space"
-- EXPECTED:
1, 2, 1, 1, 2, 4, 4, 2, 3, 6, 9, 3, 4, 8, 16, 4,
5, 10, 25, 5, 6, 12, 36, 6, 7, 14, 49, 7, 8, 16,
64, 8, 9, 18, 81, 9, 10, 20, 100, 10

-- TEST: test_alias-2
-- SQL:
select "id", "id" + "a" as "sum", "id" * "a" as "mul", "a" from "arithmetic_space"
-- EXPECTED:
1, 2, 1, 1, 2, 4, 4, 2, 3, 6, 9, 3, 4, 8, 16, 4,
5, 10, 25, 5, 6, 12, 36, 6, 7, 14, 49, 7, 8, 16,
64, 8, 9, 18, 81, 9, 10, 20, 100, 10

-- TEST: test_associativity-1
-- SQL:
select "id", "a" + ("b" + "c") from "arithmetic_space"
-- EXPECTED:
1, 6, 2, 12, 3, 18, 4, 24, 5, 30, 6, 36, 7, 42, 8, 48, 9, 54, 10, 60

-- TEST: test_associativity-2
-- SQL:
select "id", ("a" + "b") + "c" from "arithmetic_space"
-- EXPECTED:
1, 6, 2, 12, 3, 18, 4, 24, 5, 30, 6, 36, 7, 42, 8, 48, 9, 54, 10, 60

-- TEST: test_associativity-3
-- SQL:
select "id", "a" * ("b" * "c") from "arithmetic_space"
-- EXPECTED:
1, 6, 2, 48, 3, 162, 4, 384, 5, 750,
6, 1296, 7, 2058, 8, 3072, 9, 4374, 10, 6000

-- TEST: test_associativity-4
-- SQL:
select "id", ("a" * "b") * "c" from "arithmetic_space"
-- EXPECTED:
1, 6, 2, 48, 3, 162, 4, 384, 5, 750,
6, 1296, 7, 2058, 8, 3072, 9, 4374, 10, 6000

-- TEST: test_associativity-5
-- SQL:
select "id", "a" - "b" - "c" from "arithmetic_space"
-- EXPECTED:
1, -4, 2, -8, 3, -12, 4, -16, 5, -20, 6,
-24, 7, -28, 8, -32, 9, -36, 10, -40


-- TEST: test_associativity-6
-- SQL:
select "id", ("a" - "b") - "c" from "arithmetic_space"
-- EXPECTED:
1, -4, 2, -8, 3, -12, 4, -16, 5, -20, 6,
-24, 7, -28, 8, -32, 9, -36, 10, -40

-- TEST: test_associativity-7
-- SQL:
select "id", "a" - ("b" - "c" ) from "arithmetic_space"
-- EXPECTED:
1, 2, 2, 4, 3, 6, 4, 8, 5, 10, 6, 12, 7, 14, 8, 16, 9, 18, 10, 20

-- TEST: test_associativity-8
-- SQL:
select "id", cast("a" as decimal) / cast("b" as decimal) / cast("c" as decimal) from "arithmetic_space"
-- EXPECTED:
1, Decimal('0.16666666666666666666666666666666666667'),
2, Decimal('0.08333333333333333333333333333333333333'),
3, Decimal('0.05555555555555555555555555555555555556'),
4, Decimal('0.04166666666666666666666666666666666667'),
5, Decimal('0.03333333333333333333333333333333333333'),
6, Decimal('0.02777777777777777777777777777777777778'),
7, Decimal('0.02380952380952380952380952380952380952'),
8, Decimal('0.02083333333333333333333333333333333333'),
9, Decimal('0.01851851851851851851851851851851851852'),
10, Decimal('0.01666666666666666666666666666666666667')

-- TEST: test_associativity-9
-- SQL:
select "id", (cast("a" as decimal) / cast("b" as decimal)) / cast("c" as decimal) from "arithmetic_space"
-- EXPECTED:
1, Decimal('0.16666666666666666666666666666666666667'),
2, Decimal('0.08333333333333333333333333333333333333'),
3, Decimal('0.05555555555555555555555555555555555556'),
4, Decimal('0.04166666666666666666666666666666666667'),
5, Decimal('0.03333333333333333333333333333333333333'),
6, Decimal('0.02777777777777777777777777777777777778'),
7, Decimal('0.02380952380952380952380952380952380952'),
8, Decimal('0.02083333333333333333333333333333333333'),
9, Decimal('0.01851851851851851851851851851851851852'),
10, Decimal('0.01666666666666666666666666666666666667')

-- TEST: test_associativity-10
-- SQL:
select "id", cast("a" as decimal) / (cast("b" as decimal) / cast("c" as decimal)) from "arithmetic_space"
-- EXPECTED:
1, Decimal('1.5'),
2, Decimal('3.0'),
3, Decimal('4.5'),
4, Decimal('6.0'),
5, Decimal('7.5'),
6, Decimal('9.0'),
7, Decimal('10.5'),
8, Decimal('12.0'),
9, Decimal('13.5'),
10, Decimal('15.0')

-- TEST: test_commutativity-1
-- SQL:
select "id", "a" + "b" from "arithmetic_space"
-- EXPECTED:
1, 3, 2, 6, 3, 9, 4, 12, 5, 15, 6, 18, 7, 21, 8, 24, 9, 27, 10, 30

-- TEST: test_commutativity-2
-- SQL:
select "id", "b" + "a" from "arithmetic_space"
-- EXPECTED:
1, 3, 2, 6, 3, 9, 4, 12, 5, 15, 6, 18, 7, 21, 8, 24, 9, 27, 10, 30

-- TEST: test_commutativity-3
-- SQL:
select "id", "a" * "b" from "arithmetic_space"
-- EXPECTED:
1, 2, 2, 8, 3, 18, 4, 32, 5, 50, 6, 72, 7, 98, 8, 128, 9, 162, 10, 200

-- TEST: test_commutativity-4
-- SQL:
select "id", "b" * "a" from "arithmetic_space"
-- EXPECTED:
1, 2, 2, 8, 3, 18, 4, 32, 5, 50, 6, 72, 7, 98, 8, 128, 9, 162, 10, 200

-- TEST: test_commutativity-5
-- SQL:
select "id", "a" - "b" from "arithmetic_space" where "a" != "b"
-- EXPECTED:
1, -1, 2, -2, 3, -3, 4, -4, 5, -5, 6, -6, 7, -7, 8, -8, 9, -9, 10, -10

-- TEST: test_commutativity-6
-- SQL:
select "id", "b" - "a" from "arithmetic_space" where "a" != "b"
-- EXPECTED:
1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10

-- TEST: test_commutativity-7
-- SQL:
select "id", cast("b" as decimal) / cast("a" as decimal) from "arithmetic_space"
    where "a" != "b" or "a" != -1 * "b"
-- EXPECTED:
1, Decimal('2'),
2, Decimal('2'),
3, Decimal('2'),
4, Decimal('2'),
5, Decimal('2'),
6, Decimal('2'),
7, Decimal('2'),
8, Decimal('2'),
9, Decimal('2'),
10, Decimal('2')

-- TEST: test_commutativity-8
-- SQL:
select "id", cast("a" as decimal) / cast("b" as decimal) from "arithmetic_space"
    where "a" != "b" or "a" != -1 * "b"
-- EXPECTED:
1, Decimal('0.5'),
2, Decimal('0.5'),
3, Decimal('0.5'),
4, Decimal('0.5'),
5, Decimal('0.5'),
6, Decimal('0.5'),
7, Decimal('0.5'),
8, Decimal('0.5'),
9, Decimal('0.5'),
10, Decimal('0.5')

-- TEST: test_distributivity-1
-- SQL:
select "id", "a" * "b" + "a" * "c" from "arithmetic_space"
-- EXPECTED:
1, 5, 2, 20, 3, 45, 4, 80, 5, 125, 6, 180, 7, 245, 8, 320, 9, 405, 10, 500

-- TEST: test_distributivity-2
-- SQL:
select "id", "a" * ("b" + "c") from "arithmetic_space"
-- EXPECTED:
1, 5, 2, 20, 3, 45, 4, 80, 5, 125, 6, 180, 7, 245, 8, 320, 9, 405, 10, 500

-- TEST: test_distributivity-3
-- SQL:
select "id", ("b" + "c") * "a" from "arithmetic_space"
-- EXPECTED:
1, 5, 2, 20, 3, 45, 4, 80, 5, 125, 6, 180, 7, 245, 8, 320, 9, 405, 10, 500

-- TEST: test_distributivity-4
-- SQL:
select
    "id",
    cast("a" as decimal) / cast("c" as decimal) + cast("b" as decimal) / cast("c" as decimal)
from "arithmetic_space"
-- EXPECTED:
1, Decimal('1.0'),
2, Decimal('1.0'),
3, Decimal('1.0'),
4, Decimal('1.0'),
5, Decimal('1.0'),
6, Decimal('1.0'),
7, Decimal('1.0'),
8, Decimal('1.0'),
9, Decimal('1.0'),
10, Decimal('1.0')

-- TEST: test_distributivity-5
-- SQL:
select
    "id",
    (cast("a" as decimal) + cast("b" as decimal)) / cast("c" as decimal)
from "arithmetic_space"
-- EXPECTED:
1, Decimal('1'),
2, Decimal('1'),
3, Decimal('1'),
4, Decimal('1'),
5, Decimal('1'),
6, Decimal('1'),
7, Decimal('1'),
8, Decimal('1'),
9, Decimal('1'),
10, Decimal('1')

-- TEST: test_distributivity-6
-- SQL:
select
    "id",
    cast("a" as decimal) / cast("b" as decimal) + cast("a" as decimal) / cast("c" as decimal)
from "arithmetic_space"
-- EXPECTED:
1, Decimal('0.83333333333333333333333333333333333333'),
2, Decimal('0.83333333333333333333333333333333333333'),
3, Decimal('0.83333333333333333333333333333333333333'),
4, Decimal('0.83333333333333333333333333333333333333'),
5, Decimal('0.83333333333333333333333333333333333333'),
6, Decimal('0.83333333333333333333333333333333333333'),
7, Decimal('0.83333333333333333333333333333333333333'),
8, Decimal('0.83333333333333333333333333333333333333'),
9, Decimal('0.83333333333333333333333333333333333333'),
10, Decimal('0.83333333333333333333333333333333333333')

-- TEST: test_distributivity-7
-- SQL:
select
    "id",
    cast("a" as decimal) / (cast("b" as decimal) + cast("c" as decimal))
from "arithmetic_space"
-- EXPECTED:
1, Decimal('0.2'),
2, Decimal('0.2'),
3, Decimal('0.2'),
4, Decimal('0.2'),
5, Decimal('0.2'),
6, Decimal('0.2'),
7, Decimal('0.2'),
8, Decimal('0.2'),
9, Decimal('0.2'),
10, Decimal('0.2')

-- TEST: test_arithmetic_in_parens-1
-- SQL:
select "a"+"b" from "arithmetic_space"
-- EXPECTED:
3, 6, 9, 12, 15, 18, 21, 24, 27, 30

-- TEST: test_arithmetic_in_parens-2
-- SQL:
select ("a"+"b") from "arithmetic_space"
-- EXPECTED:
3, 6, 9, 12, 15, 18, 21, 24, 27, 30

-- TEST: modulo-precedence-1
-- SQL:
SELECT 3 * 7 % 3
-- EXPECTED:
0

-- TEST: modulo-precedence-2
-- SQL:
SELECT 21 / 7 % 3
-- EXPECTED:
0

-- TEST: modulo-precedence-3
-- SQL:
SELECT 3 + 7 % 3
-- EXPECTED:
4

-- TEST: modulo-precedence-4
-- SQL:
SELECT 3 - 7 % 3
-- EXPECTED:
2

-- TEST: modulo-precedence-5
-- SQL:
SELECT 7 % 3 * 3
-- EXPECTED:
3

-- TEST: modulo-precedence-6
-- SQL:
SELECT 7 % 3 / 3
-- EXPECTED:
0

-- TEST: modulo-int
-- SQL:
SELECT -7 % 3
-- EXPECTED:
-1

-- TEST: modulo-unsigned
-- SQL:
SELECT 7 % 3
-- EXPECTED:
1

-- TEST: modulo-numeric-1
-- SQL:
SELECT 7.0 % 3
-- ERROR:
could not resolve operator overload for %\(numeric, unsigned\)

-- TEST: modulo-numeric-2
-- SQL:
SELECT 7 % 3.0
-- ERROR:
could not resolve operator overload for %\(unsigned, numeric\)

-- TEST: modulo-numeric-3
-- SQL:
SELECT 7.0 % 3.0
-- ERROR:
could not resolve operator overload for %\(numeric, numeric\)
