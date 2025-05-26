-- TEST: test_left_outer_join
-- SQL:
DROP TABLE IF EXISTS arithmetic_space;
DROP TABLE IF EXISTS arithmetic_space2;
DROP TABLE IF EXISTS SPACE1;
DROP TABLE IF EXISTS SPACE2;
CREATE TABLE "SPACE1" (
    yearquarter int,
    a_to string,
    b_to string,
    a_from string,
    b_from string,
    c_by_ab decimal,
    d_by_ab decimal,
    d_c_diff decimal,
    field1 string,
    field2 string,
    primary key(yearquarter, a_to, b_to, a_from, b_from)
);
CREATE TABLE "SPACE2" (
    id int,
    yearquarter int,
    a string,
    b string,
    name string,
    field1 int,
    field2 decimal,
    field3 string,
    field4 int,
    field5 string,
    field6 decimal,
    field7 decimal,
    field8 decimal,
    field9 int,
    count_from int,
    count_to int,
    primary key(id, yearquarter)
);
CREATE TABLE arithmetic_space (id int primary key, a int, b int, c int, d int, e int, f int, boolean_col bool, string_col string, number_col double);
CREATE TABLE arithmetic_space2 ("id" int primary key, a int, b int, c int, d int, e int, f int, boolean_col bool, string_col string, number_col double);
CREATE TABLE null_t ("na" int primary key, "nb" int, "nc" int);
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
INSERT INTO "SPACE1" (
    "yearquarter","a_to","b_to","a_from","b_from",
    "c_by_ab","d_by_ab","d_c_diff","field1","field2"
)
VALUES (1, 'a', 'a', 'a', 'a', 1.0, 1.0, 1.0, 'a', 'a'),
(2, 'a', 'a', 'a', 'a', 2.0, 2.0, 2.0, 'a', 'a'),
(3, 'a', 'a', 'a', 'a', 3.0, 3.0, 3.0, 'a', 'a'),
(4, 'a', 'a', 'a', 'a', 4.0, 4.0, 4.0, 'a', 'a'),
(5, 'a', 'a', 'a', 'a', 5.0, 5.0, 5.0, 'a', 'a'),
(6, 'a', 'a', 'a', 'a', 6.0, 6.0, 6.0, 'a', 'a');
INSERT INTO "SPACE2" (
    "id","yearquarter","a","b","name","field1","field2",
    "field3","field4","field5","field6","field7","field8",
    "field9","count_from","count_to"
)
VALUES (4, 4, 'a', 'a', 'a', 4, 4.0, 'a', 4, 'a', 4.0, 4.0, 4.0, 4, 4, 4),
(5, 5, 'a', 'a', 'a', 5, 5.0, 'a', 5, 'a', 5.0, 5.0, 5.0, 5, 5, 5),
(6, 6, 'a', 'a', 'a', 6, 6.0, 'a', 6, 'a', 6.0, 6.0, 6.0, 6, 6, 6),
(7, 7, 'a', 'a', 'a', 7, 7.0, 'a', 7, 'a', 7.0, 7.0, 7.0, 7, 7, 7),
(8, 8, 'a', 'a', 'a', 8, 8.0, 'a', 8, 'a', 8.0, 8.0, 8.0, 8, 8, 8),
(9, 9, 'a', 'a', 'a', 9, 9.0, 'a', 9, 'a', 9.0, 9.0, 9.0, 9, 9, 9),
(10, 10, 'a', 'a', 'a', 10, 10.0, 'a', 10, 'a', 10.0, 10.0, 10.0, 10, 10, 10);

-- TEST: test_left_join_false_condition
-- SQL:
SELECT * from (select "a" as a from "arithmetic_space") as t1
        left join (select sum("f") as b from "arithmetic_space2") as t2
        on t1.a = t2.b
-- EXPECTED:
1, null, 1, null, 2, null, 2, null

-- TEST: test_left_join_local_execution-1
-- SQL:
select * from (select "id" as "A" from "arithmetic_space") as "T1"
left outer join (select "id" as "B" from "arithmetic_space2") as "T2"
on "T1"."A" = "T2"."B"
-- EXPECTED:
1, 1,
2, 2,
3, 3,
4, 4

-- TEST: test_left_join_local_execution-2
-- SQL:
explain select * from (select "id" as "A" from "arithmetic_space") as "T1"
left outer join (select "id" as "B" from "arithmetic_space2") as "T2"
on "T1"."A" = "T2"."B"
-- EXPECTED:
projection ("T1"."A"::int -> "A", "T2"."B"::int -> "B")
    left join on "T1"."A"::int = "T2"."B"::int
        scan "T1"
            projection ("arithmetic_space"."id"::int -> "A")
                scan "arithmetic_space"
        scan "T2"
            projection ("arithmetic_space2"."id"::int -> "B")
                scan "arithmetic_space2"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_inner_segment_motion-1
-- SQL:
select * from (select "id" as "A" from "arithmetic_space") as "T1"
left join (select "a" as "B" from "arithmetic_space2") as "T2"
on "T1"."A" = "T2"."B"
-- EXPECTED:
1, 1,
1, 1,
2, 2,
2, 2,
3, null,
4, null

-- TEST: test_inner_segment_motion-2
-- SQL:
explain select * from (select "id" as "A" from "arithmetic_space") as "T1"
left join (select "a" as "B" from "arithmetic_space2") as "T2"
on "T1"."A" = "T2"."B"
-- EXPECTED:
projection ("T1"."A"::int -> "A", "T2"."B"::int -> "B")
    left join on "T1"."A"::int = "T2"."B"::int
        scan "T1"
            projection ("arithmetic_space"."id"::int -> "A")
                scan "arithmetic_space"
        motion [policy: segment([ref("B")])]
            scan "T2"
                projection ("arithmetic_space2"."a"::int -> "B")
                    scan "arithmetic_space2"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = unknown

-- TEST: test_inner_full_motion-1
-- SQL:
select * from (select "id" as "A" from "arithmetic_space") as "T1"
left join (select "a" as "B" from "arithmetic_space2") as "T2"
on "T1"."A" < "T2"."B"
-- EXPECTED:
1, 2,
1, 2,
2, null,
3, null,
4, null

-- TEST: test_inner_full_motion-2
-- SQL:
explain select * from (select "id" as "A" from "arithmetic_space") as "T1"
left join (select "a" as "B" from "arithmetic_space2") as "T2"
on "T1"."A" < "T2"."B"
-- EXPECTED:
projection ("T1"."A"::int -> "A", "T2"."B"::int -> "B")
    left join on "T1"."A"::int < "T2"."B"::int
        scan "T1"
            projection ("arithmetic_space"."id"::int -> "A")
                scan "arithmetic_space"
        motion [policy: full]
            scan "T2"
                projection ("arithmetic_space2"."a"::int -> "B")
                    scan "arithmetic_space2"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_outer_segment_motion
-- SQL:
select * from (select sum("a") / 3 as a from "arithmetic_space") as t1
left join (select "id" as b from "arithmetic_space2") as t2
on t1.a = t2.b
-- EXPECTED:
2, 2

-- TEST: test_single_dist_outer
-- SQL:
select * from (select sum("a") / 3 as a from "arithmetic_space") as t1
left join (select "id" as b from "arithmetic_space2") as t2
on t1.a < t2.b
-- EXPECTED:
2, 3, 2, 4

-- TEST: test_single_dist_both
-- SQL:
select * from (select "id" as a from "arithmetic_space") as t1
left join (select "id" as b from "arithmetic_space2") as t2
on t1.a in (select "f" from "arithmetic_space2") or t1.a = 1 and t2.b = 4
-- EXPECTED:
1, 4, 2, 1, 2, 2, 2, 3, 2, 4, 3, None, 4, None

-- TEST: test_sq_with_full_motion-1
-- SQL:
select * from (select "a" as "A" from "arithmetic_space") as "T1"
left join (select "id" as "B" from "arithmetic_space2") as "T2"
on "T1"."A" in (select "a" + 1 from "arithmetic_space")
-- EXPECTED:
1, null,
1, null,
2, 1,
2, 2,
2, 3,
2, 4,
2, 1,
2, 2,
2, 3,
2, 4

-- TEST: test_sq_with_full_motion-2
-- SQL:
explain select * from (select "a" as "A" from "arithmetic_space") as "T1"
left join (select "id" as "B" from "arithmetic_space2") as "T2"
on "T1"."A" in (select "a" + 1 from "arithmetic_space")
-- EXPECTED:
projection ("T1"."A"::int -> "A", "T2"."B"::int -> "B")
    left join on "T1"."A"::int in ROW($0)
        scan "T1"
            projection ("arithmetic_space"."a"::int -> "A")
                scan "arithmetic_space"
        motion [policy: full]
            scan "T2"
                projection ("arithmetic_space2"."id"::int -> "B")
                    scan "arithmetic_space2"
subquery $0:
motion [policy: full]
            scan
                projection ("arithmetic_space"."a"::int + 1::int -> "col_1")
                    scan "arithmetic_space"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_sq_with_segment_motion-1
-- SQL:
select * from (select "id" as "A" from "arithmetic_space") as t1
left join (select "id" as "B" from "arithmetic_space2") as t2
on t1."A" in (select "c" from "arithmetic_space")
-- EXPECTED:
1, 1,
1, 2,
1, 3,
1, 4,
2, null,
3, null,
4, null

-- TEST: test_sq_with_segment_motion-2
-- SQL:
explain select * from (select "id" as "A" from "arithmetic_space") as t1
left join (select "id" as "B" from "arithmetic_space2") as t2
on t1."A" in (select "c" from "arithmetic_space")
-- EXPECTED:
projection ("t1"."A"::int -> "A", "t2"."B"::int -> "B")
    left join on "t1"."A"::int in ROW($0)
        scan "t1"
            projection ("arithmetic_space"."id"::int -> "A")
                scan "arithmetic_space"
        motion [policy: full]
            scan "t2"
                projection ("arithmetic_space2"."id"::int -> "B")
                    scan "arithmetic_space2"
subquery $0:
motion [policy: segment([ref("c")])]
            scan
                projection ("arithmetic_space"."c"::int -> "c")
                    scan "arithmetic_space"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = unknown

-- TEST: test_table_with_nulls1
-- SQL:
select * from (select "nb" as a from "null_t") as t1
left join (select "nc" as b from "null_t") as t2
on t1.a = t2.b
-- EXPECTED:
null, null,
null, null,
null, null,
1, 1,
1, 1,
null, null,

-- TEST: test_table_with_nulls2
-- SQL:
select * from (select "nb" as a from "null_t") as t1
left join (select "nc" as b from "null_t") as t2
on t1.a is not null
-- EXPECTED:
None, None,
None, None,
None, None,
1, None,
1, 1,
1, 1,
1, 2,
1, 3,
None, None,

-- TEST: test_empty_left_table
-- SQL:
select * from (select "nb" as a from "null_t" where false) as t1
left join (select "nc" as b from "null_t") as t2
on true
-- EXPECTED:


-- TEST: test_empty_right_table
-- SQL:
select * from (select "nb" as a from "null_t") as t1
left join (select "nc" as b from "null_t" where false) as t2
on true
-- EXPECTED:
None, None, None, None, None, None, 1, None, None, None

-- TEST: test_groupby_after_join
-- SQL:
select a, count(b) from (select "nb" as a from "null_t") as t1
left join (select "nc" as b from "null_t" where false) as t2
on true
group by a
-- EXPECTED:
null, 0, 1, 0

-- TEST: test_groupby_under_outer_child
-- SQL:
select * from (select "nb" as a from "null_t" group by "nb") as t1
left join (select "nc" as b from "null_t") as t2
on t1.a = t2.b
-- EXPECTED:
null, null,
1, 1,
1, 1

-- TEST: test_left_join_customer_query
-- SQL:
SELECT
  sp1."yearquarter",
  sp1."a_to" AS "a",
  sp1."b_to" AS "b",
  sp2."total",
  sp2."sp2_id",
  sp2."name"
FROM
  (select "yearquarter", "a_to", "b_to", "d_by_ab", "c_by_ab", "a_from", "b_from"
  from "SPACE1") AS sp1
  LEFT JOIN (
     SELECT
      sp2_1."id" AS "sp2_id",
      sp2_1."yearquarter" AS "sp2_yearquarter",
      sp2_1."a" AS "sp2_a",
      sp2_1."b" AS "sp2_b",
      sp2_1."field7",
      sp2_1."field6",
      0 AS "total",
      sp2_1."field5",
      sp2_1."name",
      sp2_1."field1",
      sp2_1."field2",
      sp2_1."field3",
      sp2_1."field4",
      sp2_1."field8",
      sp2_1."field9",
      sp2_1."count_from",
      sp2_1."count_to"
    FROM
      "SPACE2" AS sp2_1
  ) AS sp2 ON sp1."a_to" = sp2."sp2_a" AND sp1."b_to" = sp2."sp2_b" AND sp1."yearquarter" = sp2."sp2_yearquarter"
-- EXPECTED:
1, 'a', 'a', null, null, null,
2, 'a', 'a', null, null, null,
3, 'a', 'a', null, null, null,
4, 'a', 'a', 0, 4, 'a',
5, 'a', 'a', 0, 5, 'a',
6, 'a', 'a', 0, 6, 'a'

-- TEST: test_left_multi_join
-- SQL:
SELECT "SPACE1"."yearquarter", "SPACE2"."name" FROM "SPACE1"
LEFT JOIN "SPACE2"
ON "SPACE1"."a_to" = "SPACE2"."a" AND "SPACE1"."b_to" = "SPACE2"."b"
AND "SPACE1"."yearquarter" = "SPACE2"."yearquarter"
LEFT JOIN "SPACE2" as space3
ON "SPACE1"."a_to" = space3."a" AND "SPACE1"."b_to" = space3."b"
WHERE "SPACE2"."yearquarter" = 4
-- EXPECTED:
4, 'a',
4, 'a',
4, 'a',
4, 'a',
4, 'a',
4, 'a',
4, 'a'
