-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

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
select "id" from "arithmetic_space" where "id" % 2 > 0;
-- UNORDERED:
1, 3, 5, 7, 9

-- TEST: test-arithmetic-modulo-2
-- SQL:
select "id" % 2 from "arithmetic_space";
-- UNORDERED:
1, 0, 1, 0, 1, 0, 1, 0, 1, 0

-- TEST: test_arithmetic_invalid1-2
-- SQL:
select "id" from "arithmetic_space" where "id" ^ 2 > 0;
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid1-3
-- SQL:
select "id" from "arithmetic_space" where "id" ++ 2 > 0;
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid1-4
-- SQL:
select "id" from "arithmetic_space" where "id" ** 2 > 0;
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid1-5
-- SQL:
select "id" from "arithmetic_space" where "id" // 2 > 0;
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid1-6
-- SQL:
select "id" from "arithmetic_space" where "id" ** 2 > 0;
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid1-7
-- SQL:
select "id" from "arithmetic_space" where "id" +- 2 > 0;
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid1-8
-- SQL:
select "id" from "arithmetic_space" where "id" +* 2 > 0;
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid1-9
-- SQL:
select "id" from "arithmetic_space" where "boolean_col" + "boolean_col" > 0;
-- ERROR:
could not resolve operator overload for \+\(bool, bool\)

-- TEST: test_arithmetic_invalid1-10
-- SQL:
select "id" from "arithmetic_space" where "string_col" + "string_col" > 0;
-- ERROR:
could not resolve operator overload for \+\(text, text\)

-- TEST: test_arithmetic_invalid2-1
-- SQL:
select "id" as "alias1" + "a" as "alias2" from "arithmetic_space";
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid2-2
-- SQL:
select ("id" + "a") as "alias1" + "b" as "alias2" from "arithmetic_space";
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid2-4
-- SQL:
select "id" ^ 2 from "arithmetic_space";
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid2-5
-- SQL:
select "id" ++ 2 from "arithmetic_space";
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid2-6
-- SQL:
select "id" ** 2 from "arithmetic_space";
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid2-7
-- SQL:
select "id" // 2 from "arithmetic_space";
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid2-8
-- SQL:
select "id" ** 2 from "arithmetic_space";
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid2-9
-- SQL:
select "id" +- 2 from "arithmetic_space";
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid2-10
-- SQL:
select "id" +* 2 from "arithmetic_space";
-- ERROR:
rule parsing error

-- TEST: test_arithmetic_invalid2-11
-- SQL:
select "boolean_col" + "boolean_col" from "arithmetic_space";
-- ERROR:
could not resolve operator overload for \+\(bool, bool\)

-- TEST: test_arithmetic_invalid2-12
-- SQL:
select "string_col" + "string_col" from "arithmetic_space";
-- ERROR:
could not resolve operator overload for \+\(text, text\)

-- TEST: test_arithmetic_valid-1
-- SQL:
select "id" from "arithmetic_space";
-- UNORDERED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_arithmetic_valid-2
-- SQL:
select "id" from "arithmetic_space" where 2 + 2 = 4;
-- UNORDERED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_arithmetic_valid-3
-- SQL:
select "id" from "arithmetic_space"
where
    "id" + "id" > 0 and "id" + "id" + "id" > 0
    or ("id" * "id" > 0 and "id" * "id" * "id" > 0)
    or ("id" - "id" < 0 and "id" - "id" - "id" < 0)
    or ("id" / "id" > 0 and "id" / "id" / "id" > 0);
-- UNORDERED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_arithmetic_valid-4
-- SQL:
select "id" from "arithmetic_space"
where
    "id" + "id" * "id" + "id" >= 0
    and "id" - "id" * "id" - "id" <= 0
    and "id" + "id" / "id" + "id" >= 0
    and "id" - "id" / "id" - "id" <= 0;
-- UNORDERED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_arithmetic_with_bool-1
-- SQL:
select "id" from "arithmetic_space";
-- UNORDERED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_arithmetic_with_bool-2
-- SQL:
select "id" from "arithmetic_space"
where "id" + "a" >= 0
    and "id" + "b" <= 12
    and "id" + "d" > 0
    and "id" + "e" < 8
    and "id" + "d" = 2
    and "id" + "a" != 3;
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
    and "id" + "a" != 4;
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
    and "id" + "a" != "c";
-- UNORDERED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_arithmetic_with_bool-5
-- SQL:
select "id" from "arithmetic_space"
where 12 >= "id" + "a"
    and 4 <= "id" + "d"
    and 12 > "id" + "e"
    and 4 < "id" + "f"
    and 20 = "id" + "c"
    and 9 != "id" + "b";
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
    and "c" != "id" + "a";
-- UNORDERED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_selection_simple_arithmetic-1
-- SQL:
select "id" from "arithmetic_space";
-- UNORDERED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_selection_simple_arithmetic-2
-- SQL:
select "id" from "arithmetic_space" where "id" + 1 > 8;
-- UNORDERED:
8, 9, 10

-- TEST: test_selection_simple_arithmetic-3
-- SQL:
select "id" from "arithmetic_space" where "id" between "id" - 1 and "id" * 4;
-- UNORDERED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_selection_simple_arithmetic-4
-- SQL:
select "id" from "arithmetic_space"
        where ("id" > "a" * 2 or "id" * 2 > 10) and "id" - 6 != 0;
-- UNORDERED:
7, 8, 9, 10

-- TEST: test_associativity-1
-- SQL:
select "id" from "arithmetic_space";
-- UNORDERED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_associativity-2
-- SQL:
select "id" from "arithmetic_space" where "a" + ("b" + "c") = ("a" + "b") + "c";
-- UNORDERED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_associativity-3
-- SQL:
select "id" from "arithmetic_space" where "a" * ("b" * "c") = ("a" * "b") * "c";
-- UNORDERED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_associativity-4
-- SQL:
select "id" from "arithmetic_space" where ("a" - "b") - "c" = "a" - "b" - "c";
-- UNORDERED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_associativity-5
-- SQL:
select "id" from "arithmetic_space" where "a" - ("b" - "c" ) = "a" - "b" - "c";
-- EXPECTED:

-- TEST: test_associativity-6
-- SQL:
select "id" from "arithmetic_space" where
    (cast("a" as decimal) / cast("b" as decimal)) / cast("c" as decimal) =
    cast("a" as decimal) / cast("b" as decimal) / cast("c" as decimal);
-- UNORDERED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_associativity-7
-- SQL:
select "id" from "arithmetic_space" where
    cast("a" as decimal) / (cast("b" as decimal) / cast("c" as decimal)) =
    (cast("a" as decimal) / cast("b" as decimal)) / cast("c" as decimal);
-- EXPECTED:


-- TEST: test_commutativity-1
-- SQL:
select "id" from "arithmetic_space";
-- UNORDERED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_commutativity-2
-- SQL:
select "id" from "arithmetic_space" where "a" + "b" = "b" + "a";
-- UNORDERED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_commutativity-3
-- SQL:
select "id" from "arithmetic_space" where "a" * "b" = "b" * "a";
-- UNORDERED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_commutativity-4
-- SQL:
select "id" from "arithmetic_space" where "a" - "b" = "b" - "a"
except
select "id" from "arithmetic_space" where "a" = "b";
-- EXPECTED:

-- TEST: test_commutativity-5
-- SQL:
select "id" from "arithmetic_space"
    where cast("b" as decimal) / cast("a" as decimal) = cast("a" as decimal) / cast("b" as decimal)
except
select "id" from "arithmetic_space"
    where "a" = "b" or "a" = -1 * "b";
-- EXPECTED:


-- TEST: test_distributivity-1
-- SQL:
select "id" from "arithmetic_space";
-- UNORDERED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_distributivity-2
-- SQL:
select "id" from "arithmetic_space" where
    "a"  * ("b" + "c") = "a" * "b" + "a" * "c";
-- UNORDERED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_distributivity-3
-- SQL:
select "id" from "arithmetic_space" where
    ("a" + "b") * "c" = "a" * "c" + "b" * "c";
-- UNORDERED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_distributivity-4
-- SQL:
select "id" from "arithmetic_space" where
    (cast("a" as decimal) + cast("b" as decimal)) / cast("c" as decimal) =
    cast("a" as decimal) / cast("c" as decimal) + cast("b" as decimal) / cast("c" as decimal);
-- UNORDERED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_distributivity-5
-- SQL:
select "id" from "arithmetic_space" where
cast("a" as decimal) / (cast("b" as decimal) + cast("c" as decimal)) =
cast("a" as decimal) / cast("b" as decimal) + cast("a" as decimal) / cast("c" as decimal);
-- EXPECTED:


-- TEST: test_arithmetic_in_parens-1
-- SQL:
select "c" from "arithmetic_space" where "a" + "b" > 1;
-- UNORDERED:
3, 6, 9, 12, 15, 18, 21, 24, 27, 30

-- TEST: test_arithmetic_in_parens-2
-- SQL:
select "c" from "arithmetic_space" where ("a" + "b" > 1);
-- UNORDERED:
3, 6, 9, 12, 15, 18, 21, 24, 27, 30

-- TEST: test_arithmetic_in_subquery-1
-- SQL:
select "id" from "arithmetic_space";
-- UNORDERED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_arithmetic_in_subquery-2
-- SQL:
select "id" from "arithmetic_space"
where exists (select (1 + 2) * 3 / 4 from "arithmetic_space" where (1 * 2) / (8 / 4) = "id");
-- UNORDERED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_arithmetic_in_subquery-3
-- SQL:
select "id" from "arithmetic_space"
where exists (select * from "arithmetic_space" where 1 * 1 = 2);
-- EXPECTED:


-- TEST: test_arithmetic_in_subquery-4
-- SQL:
select "id" from "arithmetic_space"
where "id" in (select 2 * 3 from "arithmetic_space");
-- EXPECTED:
6

-- TEST: test_arithmetic_in_subquery-5
-- SQL:
select "id" from "arithmetic_space"
where "id" in (
    select 1 + 0 from "arithmetic_space" where exists (
        select 1 * (2 + 3) from (select * from (values (1)))
    )
);
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
WHERE "t3"."id" = 2;
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
ON "t3"."id" + "t3"."a" * 2 = "t8"."id1" + "t8"."b";
-- UNORDERED:
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
ON "t3"."id" + "t3"."a" * 2 = "t8"."id1" + "t8"."b";
-- UNORDERED:
2, 2, 3, 13, 4, 4, 6, 97, 6, 6, 9, 325

-- TEST: test_arithmetic_in_parens-1
-- SQL:
select t1.a1, t2.a2 from (select "a" as a1 from "arithmetic_space") as t1
inner join (select "c" as a2 from "arithmetic_space2") as t2
on t1.a1 = t2.a2 * 2;
-- UNORDERED:
2, 1, 4, 2, 6, 3, 8, 4, 10, 5

-- TEST: test_arithmetic_in_parens-2
-- SQL:
select t1.a1, t2.a2 from (select "a" as a1 from "arithmetic_space") as t1
inner join (select "c" as a2 from "arithmetic_space2") as t2
on (t1.a1 = t2.a2 * 2);
-- UNORDERED:
2, 1, 4, 2, 6, 3, 8, 4, 10, 5

-- TEST: test_alias-1
-- SQL:
select "id", "id" + "a", "id" * "a" , "a" from "arithmetic_space";
-- UNORDERED:
1, 2, 1, 1, 2, 4, 4, 2, 3, 6, 9, 3, 4, 8, 16, 4,
5, 10, 25, 5, 6, 12, 36, 6, 7, 14, 49, 7, 8, 16,
64, 8, 9, 18, 81, 9, 10, 20, 100, 10

-- TEST: test_alias-2
-- SQL:
select "id", "id" + "a" as "sum", "id" * "a" as "mul", "a" from "arithmetic_space";
-- UNORDERED:
1, 2, 1, 1, 2, 4, 4, 2, 3, 6, 9, 3, 4, 8, 16, 4,
5, 10, 25, 5, 6, 12, 36, 6, 7, 14, 49, 7, 8, 16,
64, 8, 9, 18, 81, 9, 10, 20, 100, 10

-- TEST: test_associativity-1
-- SQL:
select "id", "a" + ("b" + "c") from "arithmetic_space";
-- UNORDERED:
1, 6, 2, 12, 3, 18, 4, 24, 5, 30, 6, 36, 7, 42, 8, 48, 9, 54, 10, 60

-- TEST: test_associativity-2
-- SQL:
select "id", ("a" + "b") + "c" from "arithmetic_space";
-- UNORDERED:
1, 6, 2, 12, 3, 18, 4, 24, 5, 30, 6, 36, 7, 42, 8, 48, 9, 54, 10, 60

-- TEST: test_associativity-3
-- SQL:
select "id", "a" * ("b" * "c") from "arithmetic_space";
-- UNORDERED:
1, 6, 2, 48, 3, 162, 4, 384, 5, 750,
6, 1296, 7, 2058, 8, 3072, 9, 4374, 10, 6000

-- TEST: test_associativity-4
-- SQL:
select "id", ("a" * "b") * "c" from "arithmetic_space";
-- UNORDERED:
1, 6, 2, 48, 3, 162, 4, 384, 5, 750,
6, 1296, 7, 2058, 8, 3072, 9, 4374, 10, 6000

-- TEST: test_associativity-5
-- SQL:
select "id", "a" - "b" - "c" from "arithmetic_space";
-- UNORDERED:
1, -4, 2, -8, 3, -12, 4, -16, 5, -20, 6,
-24, 7, -28, 8, -32, 9, -36, 10, -40


-- TEST: test_associativity-6
-- SQL:
select "id", ("a" - "b") - "c" from "arithmetic_space";
-- UNORDERED:
1, -4, 2, -8, 3, -12, 4, -16, 5, -20, 6,
-24, 7, -28, 8, -32, 9, -36, 10, -40

-- TEST: test_associativity-7
-- SQL:
select "id", "a" - ("b" - "c" ) from "arithmetic_space";
-- UNORDERED:
1, 2, 2, 4, 3, 6, 4, 8, 5, 10, 6, 12, 7, 14, 8, 16, 9, 18, 10, 20

-- TEST: test_associativity-8
-- SQL:
select "id", cast("a" as decimal) / cast("b" as decimal) / cast("c" as decimal) from "arithmetic_space";
-- UNORDERED:
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
select "id", (cast("a" as decimal) / cast("b" as decimal)) / cast("c" as decimal) from "arithmetic_space";
-- UNORDERED:
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
select "id", cast("a" as decimal) / (cast("b" as decimal) / cast("c" as decimal)) from "arithmetic_space";
-- UNORDERED:
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
select "id", "a" + "b" from "arithmetic_space";
-- UNORDERED:
1, 3, 2, 6, 3, 9, 4, 12, 5, 15, 6, 18, 7, 21, 8, 24, 9, 27, 10, 30

-- TEST: test_commutativity-2
-- SQL:
select "id", "b" + "a" from "arithmetic_space";
-- UNORDERED:
1, 3, 2, 6, 3, 9, 4, 12, 5, 15, 6, 18, 7, 21, 8, 24, 9, 27, 10, 30

-- TEST: test_commutativity-3
-- SQL:
select "id", "a" * "b" from "arithmetic_space";
-- UNORDERED:
1, 2, 2, 8, 3, 18, 4, 32, 5, 50, 6, 72, 7, 98, 8, 128, 9, 162, 10, 200

-- TEST: test_commutativity-4
-- SQL:
select "id", "b" * "a" from "arithmetic_space";
-- UNORDERED:
1, 2, 2, 8, 3, 18, 4, 32, 5, 50, 6, 72, 7, 98, 8, 128, 9, 162, 10, 200

-- TEST: test_commutativity-5
-- SQL:
select "id", "a" - "b" from "arithmetic_space" where "a" != "b";
-- UNORDERED:
1, -1, 2, -2, 3, -3, 4, -4, 5, -5, 6, -6, 7, -7, 8, -8, 9, -9, 10, -10

-- TEST: test_commutativity-6
-- SQL:
select "id", "b" - "a" from "arithmetic_space" where "a" != "b";
-- UNORDERED:
1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10

-- TEST: test_commutativity-7
-- SQL:
select "id", cast("b" as decimal) / cast("a" as decimal) from "arithmetic_space"
    where "a" != "b" or "a" != -1 * "b";
-- UNORDERED:
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
    where "a" != "b" or "a" != -1 * "b";
-- UNORDERED:
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
select "id", "a" * "b" + "a" * "c" from "arithmetic_space";
-- UNORDERED:
1, 5, 2, 20, 3, 45, 4, 80, 5, 125, 6, 180, 7, 245, 8, 320, 9, 405, 10, 500

-- TEST: test_distributivity-2
-- SQL:
select "id", "a" * ("b" + "c") from "arithmetic_space";
-- UNORDERED:
1, 5, 2, 20, 3, 45, 4, 80, 5, 125, 6, 180, 7, 245, 8, 320, 9, 405, 10, 500

-- TEST: test_distributivity-3
-- SQL:
select "id", ("b" + "c") * "a" from "arithmetic_space";
-- UNORDERED:
1, 5, 2, 20, 3, 45, 4, 80, 5, 125, 6, 180, 7, 245, 8, 320, 9, 405, 10, 500

-- TEST: test_distributivity-4
-- SQL:
select
    "id",
    cast("a" as decimal) / cast("c" as decimal) + cast("b" as decimal) / cast("c" as decimal)
from "arithmetic_space";
-- UNORDERED:
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
from "arithmetic_space";
-- UNORDERED:
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
from "arithmetic_space";
-- UNORDERED:
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
from "arithmetic_space";
-- UNORDERED:
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
select "a"+"b" from "arithmetic_space";
-- UNORDERED:
3, 6, 9, 12, 15, 18, 21, 24, 27, 30

-- TEST: test_arithmetic_in_parens-2
-- SQL:
select ("a"+"b") from "arithmetic_space";
-- UNORDERED:
3, 6, 9, 12, 15, 18, 21, 24, 27, 30

-- TEST: modulo-precedence-1
-- SQL:
SELECT 3 * 7 % 3;
-- EXPECTED:
0

-- TEST: modulo-precedence-2
-- SQL:
SELECT 21 / 7 % 3;
-- EXPECTED:
0

-- TEST: modulo-precedence-3
-- SQL:
SELECT 3 + 7 % 3;
-- EXPECTED:
4

-- TEST: modulo-precedence-4
-- SQL:
SELECT 3 - 7 % 3;
-- EXPECTED:
2

-- TEST: modulo-precedence-5
-- SQL:
SELECT 7 % 3 * 3;
-- EXPECTED:
3

-- TEST: modulo-precedence-6
-- SQL:
SELECT 7 % 3 / 3;
-- EXPECTED:
0

-- TEST: modulo-int
-- SQL:
SELECT -7 % 3;
-- EXPECTED:
-1

-- TEST: modulo-unsigned
-- SQL:
SELECT 7 % 3;
-- EXPECTED:
1

-- TEST: modulo-numeric-1
-- SQL:
SELECT 7.0 % 3;
-- ERROR:
could not resolve operator overload for %\(numeric, int\)

-- TEST: modulo-numeric-2
-- SQL:
SELECT 7 % 3.0;
-- ERROR:
could not resolve operator overload for %\(int, numeric\)

-- TEST: modulo-numeric-3
-- SQL:
SELECT 7.0 % 3.0;
-- ERROR:
could not resolve operator overload for %\(numeric, numeric\)

-- TEST: num-division-1
-- SQL:
SELECT 5 / 2;
-- EXPECTED:
2

-- TEST: num-division-2
-- SQL:
SELECT 5 / 2::double;
-- EXPECTED:
2.5

-- TEST: num-division-3
-- SQL:
SELECT 5 / 2::decimal;
-- EXPECTED:
2.5

-- TEST: num-division-4
-- SQL:
SELECT 5::double / 2::decimal;
-- EXPECTED:
2.5

-- TEST: num-division-5
-- SQL:
SELECT 5::decimal / 2;
-- EXPECTED:
2.5

-- TEST: num-division-6
-- SQL:
SELECT 5.0 / 2.0;
-- EXPECTED:
2.5

-- TEST: num-division-7
-- SQL:
SELECT 5.0::int / 2.0::int;
-- EXPECTED:
2

-- TEST: num-division-8
-- SQL:
SELECT 5 / CASE 1 WHEN 2 THEN 3.0 ELSE 2 END;
-- EXPECTED:
2.5

-- TEST: num-division-9
-- SQL:
SELECT 5 / -2;
-- EXPECTED:
-2

-- TEST: num-addition-subquery-type-unification-1
-- SQL:
SELECT 1 + (SELECT '2');
-- ERROR:
could not resolve operator overload for \+\(int, text\)

-- TEST: concat-precedence-0
-- SQL:
DROP TABLE IF EXISTS arithmetic_space;
CREATE TABLE arithmetic_space (int_col int primary key, double_col double, numeric_col numeric, bool_col bool, datetime_col datetime, uuid_col uuid, string_col string);
INSERT INTO arithmetic_space
VALUES (1, 1.1, 2.2, true, '2003-07-08T19:13:00+03:00'::datetime, '11111111-1111-1111-1111-111111111111'::uuid, '123'),
        (2, 2.1, 4.4, false, '2009-06-10T13:31:00+03:00'::datetime, '22222222-2222-2222-2222-222222222222'::uuid, '123'),
        (3, 3.1, 6.6, true, '2016-12-22T15:51:00+03:00'::datetime, '33333333-3333-3333-3333-333333333333'::uuid, '123');

-- TEST: concat-precedence-1
-- SQL:
SELECT 'x' || 52 + 152;
-- EXPECTED:
'x204'

-- TEST: concat-precedence-2
-- SQL:
SELECT 152 + 52 || 'x';
-- EXPECTED:
'204x'

-- TEST: concat-precedence-3
-- SQL:
SELECT 'x' || 0.99;
-- EXPECTED:
'x0.99'

-- TEST: concat-precedence-4
-- SQL:
SELECT 0.99 || 'x';
-- EXPECTED:
'0.99x'

-- TEST: concat-precedence-5
-- SQL:
SELECT 'x' || 0.8 + 0.7 + 2003;
-- EXPECTED:
'x2004.5'

-- TEST: concat-precedence-6
-- SQL:
SELECT 0.8 + 0.7 + 2003 || 'x';
-- EXPECTED:
'2004.5x'

-- TEST: concat-precedence-7
-- SQL:
SELECT 'HELL ' || 'bd2eff94-9c4f-4526-a3b6-3c379b7e2c4a'::uuid;
-- EXPECTED:
'HELL bd2eff94-9c4f-4526-a3b6-3c379b7e2c4a'

-- TEST: concat-precedence-8
-- SQL:
SELECT 'bd2eff94-9c4f-4526-a3b6-3c379b7e2c4a'::uuid || ' HELL';
-- EXPECTED:
'bd2eff94-9c4f-4526-a3b6-3c379b7e2c4a HELL'

-- TEST: concat-precedence-9
-- SQL:
SELECT 'HELL EXISTS: ' || (SELECT FALSE)::string;
-- EXPECTED:
'HELL EXISTS: FALSE'

-- TEST: concat-precedence-10
-- SQL:
SELECT 'HELL EXISTS ' || (SELECT TRUE) || 'LIE';
-- EXPECTED:
'HELL EXISTS TRUELIE'

-- TEST: concat-precedence-11
-- SQL:
SELECT int_col AS alias FROM arithmetic_space GROUP BY 'x' || alias;
-- ERROR:
column "int_col" is not found in grouping expressions!

-- TEST: concat-precedence-12
-- SQL:
SELECT 'x' || int_col AS alias FROM arithmetic_space GROUP BY alias;
-- UNORDERED:
'x1', 'x2', 'x3'

-- TEST: concat-precedence-13
-- SQL:
SELECT 'SHOULD FAIL' || ARRAY[3., 2, 1];
-- ERROR:
could not resolve operator overload for \|\|\(text, numeric\[\]\)

-- TEST: concat-precedence-14
-- SQL:
SELECT 'TOO TIGHT `IS` PRECEDENCE FIXED IN https://git.picodata.io/core/picodata/-/issues/1876: ' || int_col IS NULL FROM arithmetic_space LIMIT 1;
-- EXPECTED:
'TOO TIGHT `IS` PRECEDENCE FIXED IN https://git.picodata.io/core/picodata/-/issues/1876: FALSE'

-- TEST: concat-precedence-15
-- SQL:
SELECT 1 || 2;
-- ERROR:
could not resolve operator overload for \|\|\(int, int\)

-- TEST: concat-precedence-16
-- SQL:
SELECT 'x' || NULL;
-- EXPECTED:
NULL

-- TEST: concat-precedence-17
-- SQL:
SELECT NULL || 'x';
-- EXPECTED:
NULL

-- TEST: concat-precedence-18
-- SQL:
SELECT 'x' || $1;
-- PARAMS:
5
-- EXPECTED:
'x5'

-- TEST: concat-precedence-19
-- SQL:
SELECT 'x' || $1;
-- PARAMS:
nil
-- EXPECTED:
NULL

-- TEST: concat-precedence-20
-- SKIP_FOR: 2rsX1
-- SQL:
SELECT bool_col || 'y' FROM arithmetic_space LIMIT 1;
-- EXPECTED:
'TRUEy'

-- TEST: concat-precedence-21
-- SKIP_FOR: 2rsX1
-- SQL:
SELECT 'y' || double_col FROM arithmetic_space LIMIT 1;
-- EXPECTED:
'y1.1'

-- TEST: concat-precedence-22
-- SQL:
SELECT int_col || 'x' as ax FROM arithmetic_space ORDER BY ax;
-- EXPECTED:
'1x', '2x', '3x'

-- TEST: concat-precedence-23
-- SQL:
SELECT int_col + $1, $1 || 'x' FROM arithmetic_space;
-- PARAMS:
2
-- UNORDERED:
3, '2x', 4, '2x', 5, '2x'

-- TEST: concat-precedence-24
-- SQL:
SELECT int_col AS alias FROM arithmetic_space GROUP BY alias, alias || 'x';
-- UNORDERED:
1, 2, 3

-- TEST: concat-precedence-25
-- SQL:
SELECT 'x' || 2 * 3;
-- EXPECTED:
'x6'

-- TEST: concat-precedence-26
-- SQL:
SELECT 2 * 3 || 'x';
-- EXPECTED:
'6x'

-- TEST: concat-precedence-27
-- SQL:
SELECT 'x' || 7 % 4;
-- EXPECTED:
'x3'

-- TEST: concat-precedence-28
-- SQL:
SELECT 7 % 4 || 'x';
-- EXPECTED:
'3x'

-- TEST: concat-precedence-29
-- SQL:
SELECT 'x' || 6 / 2;
-- EXPECTED:
'x3'

-- TEST: concat-precedence-30
-- SQL:
SELECT 6 / 2 || 'x';
-- EXPECTED:
'3x'

-- TEST: concat-precedence-31
-- SQL:
SELECT 'x' || 1 + 2 * 3;
-- EXPECTED:
'x7'

-- TEST: concat-precedence-32
-- SQL:
SELECT 2 * 3 + 1 || 'x';
-- EXPECTED:
'7x'

-- TEST: concat-precedence-33
-- SQL:
SELECT 'x' || 1 = 'x1';
-- EXPECTED:
true

-- TEST: concat-precedence-34
-- SQL:
SELECT 'x1' = 'x' || 1;
-- EXPECTED:
true

-- TEST: concat-precedence-35
-- SQL:
SELECT int_col || 'x' FROM arithmetic_space;
-- UNORDERED:
'1x', '2x', '3x'

-- TEST: concat-precedence-36
-- SQL:
SELECT double_col || 'x' FROM arithmetic_space;
-- UNORDERED:
'1.1x', '2.1x', '3.1x'

-- TEST: concat-precedence-37
-- SQL:
SELECT numeric_col || 'x' FROM arithmetic_space;
-- UNORDERED:
'2.2x', '4.4x', '6.6x'

-- TEST: concat-precedence-38
-- SQL:
SELECT bool_col || 'x' FROM arithmetic_space;
-- UNORDERED:
'TRUEx', 'FALSEx', 'TRUEx'

-- TEST: concat-precedence-39
-- SQL:
SELECT datetime_col || 'x' FROM arithmetic_space;
-- UNORDERED:
'2003-07-08T19:13:00+0300x', '2009-06-10T13:31:00+0300x', '2016-12-22T15:51:00+0300x'

-- TEST: concat-precedence-40
-- SQL:
SELECT 'x' || datetime_col FROM arithmetic_space;
-- UNORDERED:
'x2003-07-08T19:13:00+0300', 'x2009-06-10T13:31:00+0300', 'x2016-12-22T15:51:00+0300'

-- TEST: concat-precedence-41
-- SQL:
SELECT uuid_col || 'x' FROM arithmetic_space;
-- UNORDERED:
'11111111-1111-1111-1111-111111111111x', '22222222-2222-2222-2222-222222222222x', '33333333-3333-3333-3333-333333333333x'

-- TEST: concat-precedence-42
-- SQL:
SELECT bool_col || 'x' FROM arithmetic_space;
-- UNORDERED:
'TRUEx', 'TRUEx', 'FALSEx'

-- TEST: concat-precedence-43
-- SQL:
SELECT string_col || 'x' FROM arithmetic_space;
-- UNORDERED:
'123x', '123x', '123x'

-- TEST: concat-precedence-44
-- SQL:
SELECT double_col || 'x' FROM arithmetic_space;
-- UNORDERED:
'1.1x', '2.1x', '3.1x'
