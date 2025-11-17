-- TEST: union1
-- SQL:
DROP TABLE IF EXISTS testing_space;
DROP TABLE IF EXISTS arithmetic_space;
DROP TABLE IF EXISTS arithmetic_space2;
DROP TABLE IF EXISTS "t";
CREATE TABLE testing_space ("id" int primary key, "name" string, "product_units" int);
CREATE TABLE arithmetic_space (id int primary key, a int, b int, c int, d int, e int, f int, boolean_col bool, string_col string, number_col double);
CREATE TABLE arithmetic_space2 ("id" int primary key, a int, b int, c int, d int, e int, f int, boolean_col bool, string_col string, number_col double);
CREATE TABLE null_t ("na" int primary key, "nb" int, "nc" int);
CREATE TABLE "t" ("a" int primary key, "b" int);
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

-- TEST: test_union_under_insert1-1
-- SQL:
insert into t
select id, a from arithmetic_space
union
select id, a from arithmetic_space
union
select id, a from arithmetic_space;

-- TEST: test_union_under_insert1-2
-- SQL:
SELECT * FROM t;
-- EXPECTED:
1, 1, 2, 1, 3, 2, 4, 2

-- TEST: test_union_under_insert1-3
-- SQL:
DELETE FROM t;

-- TEST: test_union_under_insert2-1
-- SQL:
insert into t
select * from (values (100, 200))
union
select * from (values (100, 200), (200, 100));

-- TEST: test_union_under_insert2-2
-- SQL:
SELECT * FROM t;
-- EXPECTED:
100, 200, 200, 100

-- TEST: test_union_under_insert2-3
-- SQL:
DELETE FROM t;

-- TEST: test_union_removes_duplicates-1
-- SQL:
select "name"
from "testing_space"
union all
select null from "testing_space" where false;
-- EXPECTED:
'123', '1', '1', '2', '123', '2'

-- TEST: test_union_removes_duplicates-2
-- SQL:
select "name"
from "testing_space"
union
select null from "testing_space" where false;
-- EXPECTED:
'1', '123', '2'

-- TEST: test_union_seg_vs_single
-- SQL:
select "a"
from "arithmetic_space"
union
select sum("a") / 3 from "arithmetic_space";
-- EXPECTED:
1, 2

-- TEST: test_union_seg_vs_any
-- SQL:
select "a", "b"
from "arithmetic_space"
union
select "a" + 1 - 1, "b" from "arithmetic_space";
-- EXPECTED:
1, 1, 1, 2, 2, 3

-- TEST: test_multi_union
-- SQL:
select * from (
    select "a"
    from "arithmetic_space"
    union
    select "a" from "arithmetic_space"
) union
select "product_units" from "testing_space";
-- EXPECTED:
1, 2, 4

-- TEST: test_union_diff_types
-- SQL:
select "a"
from "arithmetic_space"
union
select 'kek' || "name" from "testing_space";
-- ERROR:
invalid value

-- TEST: test_union_empty_children
-- SQL:
select "a"
from "arithmetic_space" where false
union
select "id" from "testing_space"
where false;
-- EXPECTED:

-- TEST: test_union_with_window_func
-- SQL:
select row_number() over () from t union select 1;
-- EXPECTED:
1

-- TEST: test_union_with_named_window
-- SQL:
select count(*) over win from t WINDOW win as () union select 1;
-- EXPECTED:
1

-- TEST: test_explain_union_with_window_func
-- SQL:
explain select row_number() over () from t union select 1;
-- EXPECTED:
motion [policy: full]
    union
        projection (row_number() over () -> "col_1")
            motion [policy: full]
                projection ("t"."a"::int -> "a", "t"."bucket_id"::int -> "bucket_id", "t"."b"::int -> "b")
                    scan "t"
        projection (1::int -> "col_1")
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_explain_union_with_named_window
-- SQL:
explain select count(*) over win from t WINDOW win as () union select 1;
-- EXPECTED:
motion [policy: full]
    union
        projection (count(*::int) over () -> "col_1")
            motion [policy: full]
                projection ("t"."a"::int -> "a", "t"."bucket_id"::int -> "bucket_id", "t"."b"::int -> "b")
                    scan "t"
        projection (1::int -> "col_1")
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_union_all_with_window_func
-- SQL:
select row_number() over () from t union all select 1;
-- EXPECTED:
1

-- TEST: test_explain_union_all_with_window_func
-- SQL:
explain select row_number() over () from t union all select 1;
-- EXPECTED:
union all
    projection (row_number() over () -> "col_1")
        motion [policy: full]
            projection ("t"."a"::int -> "a", "t"."bucket_id"::int -> "bucket_id", "t"."b"::int -> "b")
                scan "t"
    projection (1::int -> "col_1")
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]
