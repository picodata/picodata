-- TEST: groupby
-- SQL:
DROP TABLE IF EXISTS testing_space;
DROP TABLE IF EXISTS arithmetic_space;
DROP TABLE IF EXISTS arithmetic_space2;
CREATE TABLE testing_space ("id" int primary key, "name" string, "product_units" int);
CREATE TABLE arithmetic_space (id int primary key, a int, b int, c int, d int, e int, f int, boolean_col bool, string_col string, number_col double);
CREATE TABLE arithmetic_space2 ("id" int primary key, a int, b int, c int, d int, e int, f int, boolean_col bool, string_col string, number_col double);
CREATE TABLE null_t ("na" int primary key, "nb" int, "nc" int);
INSERT INTO "testing_space" ("id", "name", "product_units") VALUES
            (1, '123', 1),
            (2, '1', 1),
            (3, '1', 1),
            (4, '2', 2),
            (5, '123', 2),
            (6, '2', 4);
INSERT INTO "arithmetic_space"
("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
VALUES (1, 1, 1, 1, 1, 2, 2, true, 'a', 3.14),
        (2, 1, 2, 1, 2, 2, 2, true, 'a', 2),
        (3, 2, 3, 1, 2, 2, 2, true, 'c', 3.14),
        (4, 2, 3, 1, 1, 2, 2, true, 'c', 2.14);
INSERT INTO "arithmetic_space2"
("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
VALUES 
    (1, 2, 1, 1, 1, 2, 2, true, 'a', 3.1415),
    (2, 2, 2, 1, 3, 2, 2, false, 'a', 3.1415),
    (3, 1, 1, 1, 1, 2, 2, false, 'b', 2.718),
    (4, 1, 1, 1, 1, 2, 2, true, 'b', 2.717);
INSERT INTO "null_t"
("na", "nb", "nc")
VALUES 
    (1, null, 1),
    (2, null, null),
    (3, null, 3),
    (4, 1, 2),
    (5, null, 1);

-- TEST: test_grouping-1
-- SQL:
SELECT "name"
    FROM "testing_space"
    GROUP BY "name"
-- EXPECTED:
'1', '123', '2'

-- TEST: test_grouping-2
-- SQL:
SELECT "name"
FROM "testing_space"
-- EXPECTED:
'123', '1', '1', '2', '123', '2'

-- TEST: expr_in_proj-1
-- SQL:
SELECT "name" || 'p' AS "name"
FROM "testing_space"
GROUP BY "name"
-- EXPECTED:
'1p', '123p', '2p'

-- TEST: expr_in_proj-2
-- SQL:
SELECT "a" + "b" AS e1, "a" / "b" AS e2
FROM "arithmetic_space"
GROUP BY "a", "b"
-- EXPECTED:
2, 1, 3, 0, 5, 0

-- TEST: different_column_types-1
-- SQL:
SELECT *
FROM (SELECT cast("number_col" AS decimal) AS col FROM "arithmetic_space")
GROUP BY col
-- EXPECTED:
Decimal('2'), Decimal('2.14'), Decimal('3.14')

-- TEST: different_column_types-2
-- SQL:
SELECT "f", "boolean_col", "string_col"
FROM "arithmetic_space"
GROUP BY "f", "boolean_col", "string_col"
-- EXPECTED:
2, True, 'a', 2, True, 'c'

-- TEST: different_column_types-3
-- SQL:
SELECT d, u
FROM (
    SELECT CAST("number_col" AS DOUBLE) AS d, CAST("number_col" AS INTEGER) AS u FROM "arithmetic_space2"
)
GROUP BY d, u
-- EXPECTED:
2.717, 2, 2.718, 2, 3.1415, 3

-- TEST: invalid-1
-- SQL:
SELECT "id" + "product_units" FROM "testing_space" GROUP BY "id"
-- ERROR:
invalid query: column "product_units" is not found in grouping expressions!

-- TEST: invalid-2
-- SQL:
SELECT * FROM "testing_space" GROUP BY "id" + 1
-- ERROR:
invalid query: column "id" is not found in grouping expressions!

-- TEST: invalid-3
-- SQL:
SELECT ("c"*"b"*"a")*count("c")/(("b"*"a"*"c")*count("c")) as u
from "arithmetic_space"
group by "a"*"b"*"c"
-- ERROR:
invalid query: column "c" is not found in grouping expressions!

-- TEST: invalid-4
-- SQL:
SELECT "id" + count("id") FROM "testing_space" GROUP BY "id" + count("id")
-- ERROR:
invalid query: aggregate functions are not allowed inside grouping expression. Got aggregate: count

-- TEST: invalid-5
-- SQL:
SELECT "name", "product_units" FROM "testing_space" GROUP BY "name"
-- ERROR:
invalid query: column "product_units" is not found in grouping expressions!

-- TEST: invalid-6
-- SQL:
SELECT "name", "product_units" FROM "testing_space" GROUP BY "name" "product_units"
-- ERROR:
rule parsing error

-- TEST: invalid-7
-- SQL:
SELECT "product_units" FROM "testing_space" GROUP BY "name"
-- ERROR:
invalid query: column "product_units" is not found in grouping expressions!

-- TEST: test_two_col-1
-- SQL:
SELECT "product_units", "name"
FROM "testing_space"
GROUP BY "product_units", "name"
-- EXPECTED:
1, '1', 1, '123', 2, '123', 2, '2', 4, '2'

-- TEST: test_two_col-2
-- SQL:
SELECT "product_units", "name"
FROM "testing_space"
GROUP BY "name", "product_units"
-- EXPECTED:
1, '1', 1, '123', 2, '123', 2, '2', 4, '2'

-- TEST: test_with_selection
-- SQL:
SELECT "product_units", "name"
FROM "testing_space"
WHERE "product_units" > 1
GROUP BY "product_units", "name"
-- EXPECTED:
2, '123', 2, '2', 4, '2'

-- TEST: test_with_join
-- SQL:
SELECT "id", "id2"
FROM "arithmetic_space"
INNER JOIN
    (SELECT "id" as "id2", "a" as "a2" from "arithmetic_space2") as t
ON "arithmetic_space"."id" = t."a2"
GROUP BY "id", "id2"
-- EXPECTED:
1, 3, 1, 4, 2, 1, 2, 2

-- TEST: test_with_join2-1
-- SQL:
SELECT "c", q.a1
FROM "arithmetic_space"
INNER JOIN
    (SELECT "b" AS b1, "a" AS a1 FROM "arithmetic_space2") AS q
ON "arithmetic_space"."c" = q.a1
GROUP BY "c", a1
-- EXPECTED:
1, 1

-- TEST: test_with_join2-2
-- SQL:
SELECT "c", q.a1
FROM "arithmetic_space"
INNER JOIN
    (SELECT "b" AS b1, "a" AS a1 FROM "arithmetic_space2") AS q
ON "arithmetic_space"."c" = q.a1
-- EXPECTED:
1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1

-- TEST: test_with_join3-1
-- SQL:
SELECT r."i", q."b"
FROM (SELECT "a" + "b" AS "i" FROM "arithmetic_space2" GROUP BY "a"+"b") AS r
INNER JOIN
    (SELECT "c", "b" FROM "arithmetic_space" GROUP BY "c", "b") AS q
ON r."i" = q."b"
GROUP BY r."i", q."b"
-- EXPECTED:
2, 2, 3, 3

-- TEST: test_with_join3-2
-- SQL:
SELECT r."i", q."b"
FROM (SELECT "a" AS "i" FROM "arithmetic_space2") AS r
INNER JOIN
    (SELECT "c", "b" FROM "arithmetic_space") AS q
ON r."i" = q."b"
-- EXPECTED:
2, 2, 2, 2, 1, 1, 1, 1

-- TEST: test_with_union-1
-- SQL:
SELECT "a" FROM "arithmetic_space" GROUP BY "a" UNION ALL SELECT "a" FROM "arithmetic_space2"
-- UNORDERED:
1, 2, 2, 2, 1, 1

-- TEST: test_with_union-2
-- SQL:
SELECT "a" FROM "arithmetic_space" GROUP BY "a" UNION ALL SELECT "a" FROM "arithmetic_space2" GROUP BY "a"
-- EXPECTED:
1, 2, 1, 2

-- TEST: test_with_union-3
-- SQL:
SELECT "a" FROM (
SELECT "a" FROM "arithmetic_space" GROUP BY "a" UNION ALL SELECT "a" FROM "arithmetic_space2" GROUP BY "a"
) GROUP BY "a"
-- EXPECTED:
1, 2

-- TEST: test_with_except-1
-- SQL:
SELECT "b" FROM "arithmetic_space" GROUP BY "b"
EXCEPT
SELECT "b" FROM "arithmetic_space2"
-- EXPECTED:
3

-- TEST: test_with_except-2
-- SQL:
SELECT * FROM (
    SELECT "a", "b" FROM "arithmetic_space" GROUP BY "a", "b"
    UNION ALL SELECT * FROM (
    SELECT "c", "d" FROM "arithmetic_space"
    EXCEPT
    SELECT "c", "d" FROM "arithmetic_space2" GROUP BY "c", "d")
) GROUP BY "a", "b"
-- EXPECTED:
1, 1, 1, 2, 2, 3

-- TEST: test_with_except-3
-- SQL:
SELECT "b" FROM "arithmetic_space"
EXCEPT
SELECT "b" FROM "arithmetic_space2"
GROUP BY "b"
-- EXPECTED:
3

-- TEST: test_with_subquery_1-1
-- SQL:
SELECT * FROM (
    SELECT "a", "b" FROM "arithmetic_space2"
    GROUP BY "a", "b"
)
-- EXPECTED:
1, 1, 2, 1, 2, 2

-- TEST: test_with_subquery_1-2
-- SQL:
SELECT * FROM (
    SELECT "a", "b" FROM "arithmetic_space2"
)
-- EXPECTED:
2, 1, 2, 2, 1, 1, 1, 1

-- TEST: test_with_subquery_2-1
-- SQL:
SELECT cast("number_col" AS integer) AS k FROM "arithmetic_space" GROUP BY "number_col"
-- EXPECTED:
2, 2, 3

-- TEST: test_with_subquery_2-2
-- SQL:
SELECT "f" FROM "arithmetic_space2"
WHERE "id" in (SELECT cast("number_col" AS integer) FROM "arithmetic_space" GROUP BY "number_col")
-- EXPECTED:
2, 2

-- TEST: test_with_subquery_2-3
-- SQL:
SELECT "f" FROM "arithmetic_space2"
WHERE "id" in (SELECT cast("number_col" AS integer) FROM "arithmetic_space" GROUP BY "number_col")
GROUP BY "f"
-- EXPECTED:
2

-- TEST: test_less_cols_in_proj
-- SQL:
SELECT "c" FROM "arithmetic_space"
GROUP BY "c", "d"
-- EXPECTED:
1, 1

-- TEST: test_less_cols_in_proj
-- SQL:
SELECT "c" FROM "arithmetic_space"
GROUP BY "c", "d"
-- EXPECTED:
1, 1

-- TEST: test_with_subquery_3
-- SQL:
SELECT "b", "string_col" FROM
(SELECT "b", "string_col" FROM "arithmetic_space2" GROUP BY "b", "string_col") AS t1
INNER JOIN
(SELECT "id" FROM "testing_space" WHERE "id" in (SELECT "a" FROM "arithmetic_space" GROUP BY "a")) AS t2
on t2."id" = t1."b"
WHERE "b" in (SELECT "c" FROM "arithmetic_space" GROUP BY "c")
GROUP BY "b", "string_col"
-- EXPECTED:
1, 'a', 1, 'b'

-- TEST: test_complex_1
-- SQL:
SELECT * FROM (
    SELECT "b" FROM "arithmetic_space"
    WHERE "a" in
        (SELECT "a" FROM "arithmetic_space2" WHERE "a" in
            (SELECT "b" FROM "arithmetic_space" GROUP BY "b")
        GROUP BY "a")
    GROUP BY "b"
    UNION ALL SELECT * FROM (
        SELECT "b" FROM "arithmetic_space2"
        EXCEPT
        SELECT "a" FROM "arithmetic_space"
    )
)
-- UNORDERED:
1, 2, 3

-- TEST: test_complex_2
-- SQL:
SELECT * FROM (
    SELECT "b" FROM "arithmetic_space"
    WHERE "c" in
        (SELECT "id" FROM "arithmetic_space2" WHERE "id" in
            (SELECT "b" FROM "arithmetic_space" GROUP BY "b")
        GROUP BY "id")
    GROUP BY "b"
    UNION ALL
    SELECT * FROM (
        SELECT "c" FROM "arithmetic_space2"
        WHERE "id" = 2 or "b" = 1
        GROUP BY "c"
        EXCEPT
        SELECT "a" FROM "arithmetic_space" GROUP BY "a")
) ORDER BY "b";
-- EXPECTED:
1, 2, 3

-- TEST: test_count_works-1
-- SQL:
SELECT "d", "e" from "arithmetic_space"
-- EXPECTED:
1, 2, 2, 2, 2, 2, 1, 2

-- TEST: test_count_works-2
-- SQL:
SELECT "d", count("e") from "arithmetic_space"
group by "d"
-- EXPECTED:
1, 2, 2, 2

-- TEST: test_count
-- SQL:
select cs, count("d") from (
    SELECT "d", count("e") + count("e" + "d") as cs from "arithmetic_space"
    group by "d"
) as t
where t."d" > 1
group by cs
-- EXPECTED:
4, 1

-- TEST: test_aggr_invalid
-- SQL:
SELECT "d", count(sum("e")) from "arithmetic_space" group by "d"
-- ERROR:
invalid query

-- TEST: test_groupby_arith_expression
-- SQL:
SELECT ("a"*"b"*"c")*count("c")/(("a"*"b"*"c")*count("c")) as u from "arithmetic_space"
        group by ("a"*"b"*"c")
-- EXPECTED:
1, 1, 1

-- TEST: test_grouping_by_concat
-- SQL:
SELECT "string_col2" || "string_col" as u from
(select "id" as "i", "string_col" as "string_col2" from "arithmetic_space") as "t1"
join "arithmetic_space2" on "t1"."i" = "arithmetic_space2"."id"
group by "string_col2" || "string_col"
-- EXPECTED:
'aa', 'cb'

-- TEST: test_groupby_bool_expr
-- SQL:
SELECT "b1" and "b1" as "c1", "boolean_col" or "b1" as "c2" from
(select "id" as "i", "boolean_col" as "b1" from "arithmetic_space") as "t1"
join "arithmetic_space2" on "t1"."i" = "arithmetic_space2"."id"
group by "b1" and "b1", "boolean_col" or "b1"
-- EXPECTED:
true, true

-- TEST: test_grouping_by_cast_expr
-- SQL:
SELECT cast("number_col" as double) from "arithmetic_space"
group by cast("number_col" as double)
-- EXPECTED:
2.0, 2.14, 3.14

-- TEST: test_aggr_valid-1
-- SQL:
SELECT sum("e" + "d") from "arithmetic_space"
-- EXPECTED:
14

-- TEST: test_aggr_valid-2
-- SQL:
SELECT "d", count("e" + "d") from "arithmetic_space" group by "d"
-- EXPECTED:
1, 2, 2, 2

-- TEST: test_aggr_valid-3
-- SQL:
SELECT "d", couNT ("e") from "arithmetic_space" group by "d"
-- EXPECTED:
1, 2, 2, 2

-- TEST: test_aggr_valid-4
-- SQL:
SELECT "d", count("e" * 10 + "a") from "arithmetic_space2" group by "d"
-- EXPECTED:
1, 3, 3, 1

-- TEST: test_aggr_valid-5
-- SQL:
SELECT "d", sum("e") = sum("b"), sum("e") != sum("a"), sum("e") > count("a"),
        (sum("e") > count("a")) or (sum("e") = sum("b")) from "arithmetic_space2" group by "d"
-- EXPECTED:
1, False, True, True, True, 3, True, False, True, True

-- TEST: test_aggr_valid-6
-- SQL:
SELECT "d", sum(("d" + "c")) from "arithmetic_space2" group by "d"
-- EXPECTED:
1, 6, 3, 4

-- TEST: test_aggr_valid-7
-- SQL:
SELECT "d", count(("d" < "id")) from "arithmetic_space" group by "d"
-- EXPECTED:
1, 2, 2, 2

-- TEST: test_aggr_valid-8
-- SQL:
SELECT "c", count(("b" in ("id"))) as ss from "arithmetic_space2" group by "c"
-- EXPECTED:
1, 4

-- TEST: test_union_single
-- SQL:
SELECT count("e") from (SELECT "e" from "arithmetic_space"
            GROUP BY "e"
            UNION ALL
            select * from (SELECT sum("e" + "d") from "arithmetic_space"
            UNION ALL
            SELECT sum("e") from "arithmetic_space"))
-- EXPECTED:
3

-- TEST: test_except_single-1
-- SQL:
SELECT "e" from "arithmetic_space"
GROUP BY "e"
EXCEPT
SELECT * from (
SELECT sum("e" + "d") from "arithmetic_space"
EXCEPT
SELECT sum("e") from "arithmetic_space")
-- EXPECTED:
2

-- TEST: test_except_single-2
-- SQL:
SELECT "e" from "arithmetic_space"
GROUP BY "e"
EXCEPT
SELECT sum("e" + "d") from "arithmetic_space"
-- EXPECTED:
2

-- TEST: test_except_single-3
-- SQL:
SELECT sum("e" + "d") from "arithmetic_space"
EXCEPT
SELECT "e" from "arithmetic_space"
WHERE "id" > 2
GROUP BY "e"
-- EXPECTED:
14

-- TEST: test_join_single6-1
-- SQL:
select o.a, o.b, i.c, i.d from (select sum("a") as a, count("b") as b from "arithmetic_space") as o
            inner join (select "c" + 3 as c, "d" + 4 as d from "arithmetic_space") as i
            on o.a = i.d or o.b = i.c
-- EXPECTED:
6, 4, 4, 5, 6, 4, 4, 6, 6, 4, 4, 6, 6, 4, 4, 5


-- TEST: test_join_single6-2
-- SQL:
select o.a, o.b, i.c, i.d from  (select "c" + 3 as c, "d" + 4 as d from "arithmetic_space") as i
            inner join (select sum("a") as a, count("b") as b from "arithmetic_space") as o
            on o.a = i.d or o.b = i.c and i.c in (select "id" from "arithmetic_space")
            where o.a > 5
-- EXPECTED:
6, 4, 4, 5, 6, 4, 4, 6, 6, 4, 4, 6, 6, 4, 4, 5

-- TEST: test_join_single7
-- SQL:
select i.a, o.d from  (select "c" + 3 as c, "d" + 4 as d from "arithmetic_space") as o
            inner join (select sum("a") as a, count("b") as b from "arithmetic_space") as i
            on i.a = cast(o.d as integer)
-- EXPECTED:
6, 6, 6, 6

-- TEST: test_join_single8
-- SQL:
select i.a, o.d from  (select "c" + 3 as c, "d" + 4 as d from "arithmetic_space") as o
            inner join (select sum("a") as a, count("b") as b from "arithmetic_space") as i
            on i.a < 10
-- EXPECTED:
Decimal('6'), 5, Decimal('6'), 6, Decimal('6'), 6, Decimal('6'), 5

-- TEST: test_join_single9 https://git.picodata.io/core/picodata/-/issues/1332
-- SQL:
-- select i.a, o.d from  (select "c" as c, "d" as d from "arithmetic_space" group by "c", "d") as o
--             inner join (select sum("a") as a, count("b") as b from "arithmetic_space") as i
--             on i.a < o.d + 5
-- EXPECTED:
-- 6, 2

-- TEST: test_join_single10
-- SQL:
select i.a, o.d from  (select "c" as c, "d" as d from "arithmetic_space") as o
            inner join (select sum("a") as a, count("b") as b from "arithmetic_space") as i
            on i.a = o.d + 4 and i.b = o.c + 3
-- EXPECTED:
6, 2, 6, 2

-- TEST: test_aggr_distinct
-- SQL:
SELECT "d", count(distinct "e"), count(distinct "e"+"a"), count(distinct "e"+"a") + sum(distinct "d"),
           sum("d") from "arithmetic_space" group by "d"
-- EXPECTED:
1, 1, 2, 3, 2, 2, 1, 2, 4, 4

-- TEST: test_aggr_distinct_without_groupby
-- SQL:
SELECT sum(distinct "d"), count("e"+"a"),
    count(distinct "e"+"a"), count(distinct "e"+"a") + sum(distinct "d"),
    sum("d") from "arithmetic_space"
-- EXPECTED:
3, 4, 2, 5, 6

-- TEST: test_select_distinct-1
-- SQL:
SELECT distinct "a"*2 from "arithmetic_space"
-- EXPECTED:
2, 4

-- TEST: test_select_distinct-2
-- SQL:
SELECT "a"*2 from "arithmetic_space" group by "a"*2
-- EXPECTED:
2, 4

-- TEST: test_select_distinct2
-- SQL:
SELECT distinct * from
        (select "e", "f" from "arithmetic_space")
-- EXPECTED:
2, 2

-- TEST: test_select_distinct3
-- SQL:
SELECT distinct sum("e") from
        (select "e" from "arithmetic_space")
-- EXPECTED:
8

-- TEST: test_select_distinct4-1
-- SQL:
SELECT distinct sum("e") from
        (select "e", "f" from "arithmetic_space")
        group by "f"
-- EXPECTED:
8

-- TEST: test_select_distinct4-2
-- SQL:
SELECT distinct sum("e") from
        (select "e", "f" from "arithmetic_space")
        group by "f"
-- EXPECTED:
8

-- TEST: test_count_asterisk-1
-- SQL:
SELECT count(*) from "arithmetic_space"
-- EXPECTED:
4

-- TEST: test_count_asterisk-2
-- SQL:
SELECT count(*) from "null_t"
-- EXPECTED:
5

-- TEST: test_count_asterisk_with_groupby
-- SQL:
SELECT count(*), "nb" from "null_t" group by "nb"
-- EXPECTED:
4, null, 1, 1

-- TEST: test_avg
-- SQL:
SELECT avg("c"), avg(distinct "c"), avg("b"), avg(distinct "b") from "arithmetic_space"
-- EXPECTED:
1, 1, 2.25, 2

-- TEST: test_avg_with_groupby
-- SQL:
SELECT "a", avg("b"), avg(distinct "b") FROM "arithmetic_space"
        GROUP BY "a"
-- EXPECTED:
1, 1.5, 1.5, 2, 3, 3

-- TEST: test_group_concat
-- SQL:
SELECT group_concat(cast("c" as string), ' '), group_concat(distinct cast("c" as string))
        from "arithmetic_space"
-- EXPECTED:
'1 1 1 1', '1'

-- TEST: test_group_concat_with_groupby
-- SQL:
SELECT "a", group_concat(cast("e" as string), '|'), group_concat(distinct cast("e" as string))
FROM "arithmetic_space"
GROUP BY "a"
-- EXPECTED:
1, '2|2', '2', 2, '2|2', '2'

-- TEST: test_min
-- SQL:
SELECT min("id"), min(distinct "d" / 2) from "arithmetic_space"
-- EXPECTED:
1, 0

-- TEST: test_min_with_groupby
-- SQL:
SELECT "a", min("b"), min(distinct "b") FROM "arithmetic_space" GROUP BY "a"
-- EXPECTED:
1, 1, 1, 2, 3, 3

-- TEST: test_max
-- SQL:
SELECT max("id"), max(distinct "d" / 2) from "arithmetic_space"
-- EXPECTED:
4, 1

-- TEST: test_max_with_groupby
-- SQL:
SELECT "a", max("b"), max(distinct "b") FROM "arithmetic_space" GROUP BY "a"
-- EXPECTED:
1, 2, 2, 2, 3, 3

-- TEST: test_total
-- SQL:
SELECT total("id"), total(distinct "d" / 2) from "arithmetic_space"
-- EXPECTED:
10, 1

-- TEST: test_distinct_asterisk_single_column
-- SQL:
WITH first  AS (SELECT "id" FROM "arithmetic_space" WHERE "id" = 1),
        second AS (SELECT "id" FROM "arithmetic_space2" WHERE "id" = 2)
SELECT DISTINCT * FROM first JOIN second ON TRUE
-- EXPECTED:
1, 2

-- TEST: test_distinct_asterisk_several_columns
-- SQL:
WITH first  AS (SELECT "id", "a" FROM "arithmetic_space"),
             second AS (SELECT "id", "a" FROM "arithmetic_space2")
        SELECT DISTINCT * FROM first JOIN second ON first."id" = second."id"
-- EXPECTED:
1, 1, 1, 2, 2, 1, 2, 2, 3, 2, 3, 1, 4, 2, 4, 1

-- TEST: test_total_no_rows
-- SQL:
SELECT total("id") from (
    select * from "arithmetic_space" inner join
    "null_t" on false
)
-- EXPECTED:
0

-- TEST: test_total_null_rows
-- SQL:
SELECT total("nb") from (
            select * from "arithmetic_space" left join
            "null_t" on false
        )
-- EXPECTED:
0

-- TEST: test_sum_no_rows
-- SQL:
SELECT sum("id") from (
            select * from "arithmetic_space" inner join
            "null_t" on false
        )
-- EXPECTED:
null

-- TEST: test_sum_null_rows
-- SQL:
SELECT sum("nb") from (
    select * from "arithmetic_space" left join
    "null_t" on false
)
-- EXPECTED:
null

-- TEST: test_total_with_groupby
-- SQL:
SELECT "a", total("b"), total(distinct "b") FROM "arithmetic_space"
        GROUP BY "a"
-- EXPECTED:
1, 3, 3, 2, 6, 3

-- TEST: test_having_with_sq-1
-- SQL:
SELECT "a", sum(distinct "b") as "sum", count(distinct "b") as "count" from "arithmetic_space"
        group by "a"
-- EXPECTED:
1, 3, 2, 2, 3, 1

-- TEST: test_having_with_sq-2
-- SQL:
SELECT "a", sum(distinct "b") as "sum", count(distinct "b") as "count" from "arithmetic_space"
group by "a" having count(distinct "b") in (select "a" from "arithmetic_space" where "a" = 2)
-- EXPECTED:
1, 3, 2

-- TEST: test_having1-1
-- SQL:
SELECT "a", sum("b") as "sum" from "arithmetic_space"
group by "a"
having sum("b") > 5
-- EXPECTED:
2, 6

-- TEST: test_having1-2
-- SQL:
SELECT "a", sum("b") as "sum" from "arithmetic_space"
group by "a"
-- EXPECTED:
1, Decimal('3'), 2, Decimal('6')

-- TEST: test_having2
-- SQL:
SELECT "a", sum("b") as "sum" from "arithmetic_space"
group by "a"
having "a" = 1
-- EXPECTED:
1, 3

-- TEST: test_having3
-- SQL:
SELECT "boolean_col", sum(distinct "f") as "sum" from "arithmetic_space"
        group by "boolean_col"
        having "boolean_col" = true
-- EXPECTED:
true, 2

-- TEST: test_having_no_groupby-1
-- SQL:
SELECT sum("a"), sum(distinct "a"), count("a"), count(distinct "a") from "arithmetic_space"
        having count(distinct "a") > 1
-- EXPECTED:
6, 3, 4, 2

-- TEST: test_having_no_groupby-2
-- SQL:
SELECT sum("a"), sum(distinct "a"), count("a"), count(distinct "a") from "arithmetic_space"
        having count(distinct "a") > 100
-- EXPECTED:

-- TEST: test_having_selection-1
-- SQL:
SELECT "string_col", count(distinct "string_col"), count("string_col")
from "arithmetic_space"
where "id" > 2 or "string_col" = 'a'
group by "string_col"
having sum(distinct "a") > 1
-- EXPECTED:
'c', 1, 2

-- TEST: test_having_selection-2
-- SQL:
SELECT "string_col", count(distinct "string_col"), count("string_col")
        from "arithmetic_space"
        where "id" > 2 or "string_col" = 'a'
        group by "string_col"
-- EXPECTED:
'a', 1, 2, 'c', 1, 2

-- TEST: test_having_join-1
-- SQL:
SELECT sum(distinct "a"), "b", s
from "arithmetic_space" as t1 inner join
(
    select cast(sum("a") / 6 as integer) as s
    from "arithmetic_space2"
    having count(distinct "a") > 1
) as t2
on t1."c" = t2.s
group by s, t1."b"
having sum(distinct "a") in (2, 3) and count(distinct t2.s) = 1
-- EXPECTED:
2, 3, 1

-- TEST: test_having_full_query-1
-- SQL:
SELECT t1."a", t2.b, t2.s, t1."d"
from "arithmetic_space" as t1 inner join
(select "b" as b, "string_col" as s from "arithmetic_space2") as t2
on t1."a" = t2.b
where t1."d" + t1."a" > 2
-- EXPECTED:
1, 1, 'a', 2, 1, 1, 'b', 2, 1, 1, 'b', 2, 2, 2, 'a', 2, 2, 2, 'a', 1

-- TEST: test_having_full_query-2
-- SQL:
SELECT t1."a", count(distinct s)
from "arithmetic_space" as t1 inner join
(select "b" as b, "string_col" as s from "arithmetic_space2") as t2
on t1."a" = t2.b
where t1."d" + t1."a" > 2
group by "a"
having count(distinct s) > 1
-- EXPECTED:
1, 2

-- TEST: test_having_inside_union https://git.picodata.io/core/picodata/-/issues/1394
-- SQL:
-- select sum(distinct "e") from "arithmetic_space"
-- having count(distinct "d") > 1
-- union all
-- select "b" from "arithmetic_space"
-- group by "b"
-- having count(distinct "d") > 1
-- EXPECTED:
-- 2, 3

-- TEST: test_groupby_having_inside_union
-- SQL:
select sum(distinct "e") from "arithmetic_space"
group by "e" having count(distinct "d") > 1
union all
select "b" from "arithmetic_space"
group by "b"
having count(distinct "d") > 1
-- EXPECTED:
2, 3

-- TEST: test_having_inside_union1
-- SQL:
select "e" from "arithmetic_space"
union all
select "b" from "arithmetic_space"
group by "b"
having count(distinct "d") > 1
-- EXPECTED:
2, 2, 2, 2, 3

-- TEST: test_having_inside_except
-- SQL:
select count("e") - 1 from "arithmetic_space"
having count("e") = 4
except
select "b" from "arithmetic_space"
group by "b"
having count(distinct "d") > 1
-- EXPECTED:

-- TEST: test_having_inside_except1
-- SQL:
select "d" from "arithmetic_space"
group by "d"
except
select sum(distinct "c") from "arithmetic_space"
having count(distinct "c") = 1
-- EXPECTED:
1, 2

-- TEST: test_alias_inside_groupby-1.0
-- SQL:
drop table if exists t;
create table t(a int primary key, b int);
insert into t values (1, 2), (2, 3), (3, 4);

-- TEST: test_alias_inside_groupby-1.1
-- SQL:
select a as a_1, sum(t.b) from t group by a_1, a;
-- EXPECTED:
1, 2,
2, 3,
3, 4

-- TEST: test_alias_inside_groupby-1.2
-- SQL:
select (t.a + 5) as a_1, sum(t.b) from t group by a_1, a;
-- EXPECTED:
6, 2,
7, 3,
8, 4

-- TEST: test_alias_inside_groupby-1.3
-- SQL:
select (t.a + 5) as a_1, sum(t.b) from t group by a_1 * 2, a;
-- EXPECTED:
6, 2,
7, 3,
8, 4

-- TEST: test_alias_inside_groupby-1.4
-- SQL:
SELECT
	CASE WHEN b < 50 THEN 'Low' WHEN b BETWEEN 50 AND 100 THEN 'Medium' ELSE 'High' END AS b_category,
	COUNT(*) AS row_count,
	MIN(a) AS min_a,
	MAX(a) AS max_a
FROM t
GROUP BY (CASE WHEN b < 50 THEN 'Low' WHEN b BETWEEN 50 AND 100 THEN 'Medium' ELSE 'High' END)
ORDER BY row_count DESC;
-- EXPECTED:
'Low', 3, 1, 3

-- TEST: test_alias_inside_groupby-1.5
-- SQL:
SELECT
	(a % 10) AS a_last_digit,
	(b % 5) AS b_remainder,
	COUNT(*) AS count_rows,
	SUM(a + b) AS total_sum
FROM t
GROUP BY a_last_digit, b_remainder
ORDER BY a_last_digit, b_remainder;
-- EXPECTED:
1, 2, 1, 3,
2, 3, 1, 5,
3, 4, 1, 7

-- TEST: test_alias_inside_groupby-1.6
-- SQL:
SELECT
	(a % 4) AS a_mod_group,
	COUNT(*) AS total_rows,
	SUM(CASE WHEN b > 50 THEN 1 ELSE 0 END) AS b_above_50_count,
	AVG(b) AS avg_positive_b,
	MAX(a) AS max_a_value
FROM t 
GROUP BY a_mod_group
ORDER BY a_mod_group;
-- EXPECTED:
1, 1, 0, 2, 1,
2, 1, 0, 3, 2,
3, 1, 0, 4, 3

-- TEST: test_alias_inside_groupby-1.7
-- SQL:
SELECT * FROM
	(SELECT a AS a_1 FROM t GROUP BY a_1) as t1
	JOIN (SELECT a AS a_2 FROM t GROUP BY a_2) as t2
	ON t1.a_1 = t2.a_2;
-- EXPECTED:
1, 1,
2, 2,
3, 3

-- TEST: test_alias_inside_groupby-1.8
-- SQL:
SELECT a_1 AS a FROM
	(SELECT a AS a_1 FROM t GROUP BY a_1
	UNION ALL
	SELECT (a + 1) AS a_1 FROM t GROUP BY a_1)
GROUP BY a
ORDER BY a;
-- EXPECTED:
1, 2, 3, 4

-- TEST: test_alias_inside_groupby-1.9
-- SQL:
SELECT a_1 AS a_2 FROM
	(SELECT a AS a_1 FROM t GROUP BY a_1)
GROUP BY a_2
ORDER BY a_2;
-- EXPECTED:
1, 2, 3

-- TEST: test_alias_inside_groupby-1.10
-- SQL:
select (select 1) as a from t group by a;
-- EXPECTED:
1, 1, 1
