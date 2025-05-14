-- TEST: update
-- SQL:
DROP TABLE IF EXISTS testing_space;
DROP TABLE IF EXISTS arithmetic_space;
DROP TABLE IF EXISTS arithmetic_space2;
DROP TABLE IF EXISTS testing_space_bucket_in_the_middle;
CREATE TABLE testing_space ("id" int primary key, "name" string, "product_units" int);
CREATE TABLE testing_space_bucket_in_the_middle ("id" int primary key, "name" string, "product_units" int) DISTRIBUTED BY (id, name);
CREATE TABLE arithmetic_space (id int primary key, a int, b int, c int, d int, e int, f int, boolean_col bool, string_col string, number_col double);
CREATE TABLE arithmetic_space2 ("id" int primary key, a int, b int, c int, d int, e int, f int, boolean_col bool, string_col string, number_col double);
CREATE TABLE double_t ("id" int primary key, "r" double, "dec" decimal);
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
INSERT INTO "double_t"
("id", "r", "dec")
VALUES (1, 1e-1, 0.1), (2, 5e-1, 0.5);
INSERT INTO "testing_space_bucket_in_the_middle" ("id", "name", "product_units") VALUES
                (1, '1', 1),
                (2, '1', 1),
                (3, '1', 1)

-- TEST: test_basic-1
-- SQL:
update "testing_space" set "name" = 'It works!'

-- TEST: test_basic-2
-- SQL:
SELECT * FROM "testing_space"
-- EXPECTED:
1, 'It works!', 1,
2, 'It works!', 1,
3, 'It works!', 1,
4, 'It works!', 2,
5, 'It works!', 2,
6, 'It works!', 4

-- TEST: test_basic-3
-- SQL:
DELETE FROM "testing_space";
INSERT INTO "testing_space" ("id", "name", "product_units") VALUES
            (1, '123', 1),
            (2, '1', 1),
            (3, '1', 1),
            (4, '2', 2),
            (5, '123', 2),
            (6, '2', 4);

-- TEST: test_invalid-1
-- SQL:
update "testing_space" set "id" = 1 where "name" = 'Den'
-- ERROR:
invalid query: it is illegal to update primary key column: "id"

-- TEST: test_invalid-2
-- SQL:
update "testing_space" set "name" = 'a', "name" = 'b' where "id" = 1
-- ERROR:
The same column is specified twice in update list

-- TEST: test_invalid-3
-- SQL:
update "testing_space" set "name" = group_concat("name"), "name" = 'b' where "id" = 1
-- ERROR:
aggregate functions are not supported in update expression.

-- TEST: test_invalid-4
-- SQL:
update "testing_space" set "name" = 'a' || group_concat("name"), "name" = 'b' where "id" = 1
-- ERROR:
aggregate functions are not supported in update expression.

-- TEST: test_invalid-5
-- SQL:
update "testing_space" set "name" = 'a', "bucket_id" = 1 where "id" = 1
-- ERROR:
failed to update column: system column "bucket_id" cannot be updated

-- TEST: test_invalid-6
-- SQL:
update "testing_space" set "product_units" = select sum("id") from "t" where "id" = 1
-- ERROR:
rule parsing error

-- TEST: test_invalid-7
-- SQL:
 update "testing_space" set "product_units" = 'hello'::text
-- ERROR:
column "product_units" is of type int, but expression is of type text

-- TEST: test_invalid-8
-- SQL:
 update "testing_space" set "product_units" = 'hello'
-- ERROR:
failed to parse 'hello' as a value of type int, consider using explicit type casts

-- TEST: test_invalid-9
-- SQL:
update "testing_space" set "testing_space"."product_units" = 1
-- ERROR:
rule parsing error

-- TEST: test_where-1
-- SQL:
update "testing_space"
    set "name" = 'It works!'
    where "product_units" = 1

-- TEST: test_where-2
-- SQL:
SELECT * FROM "testing_space"
-- EXPECTED:
1, 'It works!', 1,
2, 'It works!', 1,
3, 'It works!', 1,
4, '2', 2,
5, '123', 2,
6, '2', 4
        
-- TEST: test_where-3
-- SQL:
DELETE FROM "testing_space";
INSERT INTO "testing_space" ("id", "name", "product_units") VALUES
            (1, '123', 1),
            (2, '1', 1),
            (3, '1', 1),
            (4, '2', 2),
            (5, '123', 2),
            (6, '2', 4);

-- TEST: test_join-1
-- SQL:
update "testing_space" set
"name" = u."string_col"
from (select "id" as "i", "string_col" from "arithmetic_space") as u
where "id" = "i"

-- TEST: test_join-2
-- SQL:
SELECT * FROM "testing_space"
-- EXPECTED:
1, 'a', 1,
2, 'a', 1,
3, 'c', 1,
4, 'c', 2,
5, '123', 2,
6, '2', 4
        
-- TEST: test_join-3
-- SQL:
DELETE FROM "testing_space";
INSERT INTO "testing_space" ("id", "name", "product_units") VALUES
            (1, '123', 1),
            (2, '1', 1),
            (3, '1', 1),
            (4, '2', 2),
            (5, '123', 2),
            (6, '2', 4);

-- TEST: test_join_with_multiple_pk_rows-1
-- SQL:
update "testing_space" set
"name" = "string_col"
from (select "id" as "i", "string_col" from "arithmetic_space") as u
where "id" = 1 and "string_col" = 'a'

-- TEST: test_join_with_multiple_pk_rows-2
-- SQL:
SELECT * FROM "testing_space"
-- EXPECTED:
1, 'a', 1,
2, '1', 1,
3, '1', 1,
4, '2', 2,
5, '123', 2,
6, '2', 4
        
-- TEST: test_join_with_multiple_pk_rows-3
-- SQL:
DELETE FROM "testing_space";
INSERT INTO "testing_space" ("id", "name", "product_units") VALUES
            (1, '123', 1),
            (2, '1', 1),
            (3, '1', 1),
            (4, '2', 2),
            (5, '123', 2),
            (6, '2', 4);

-- TEST: test_inverse_column_order-1
-- SQL:
update "testing_space" set
"product_units" = "i",
"name" = "string_col" || 'hello'
from (select "id" as "i", "string_col" from "arithmetic_space") as u
where "id" = "i" and "id" = 1

-- TEST: test_inverse_column_order-2
-- SQL:
SELECT * FROM "testing_space"
-- EXPECTED:
1, 'ahello', 1,
2, '1', 1,
3, '1', 1,
4, '2', 2,
5, '123', 2,
6, '2', 4

-- TEST: test_inverse_column_order-3
-- SQL:
DELETE FROM "testing_space";
INSERT INTO "testing_space" ("id", "name", "product_units") VALUES
            (1, '123', 1),
            (2, '1', 1),
            (3, '1', 1),
            (4, '2', 2),
            (5, '123', 2),
            (6, '2', 4);

-- TEST: test_shard_column_updated-1
-- SQL:
update "testing_space" set
"name" = 'some string',
"product_units" = 1000
where "id" = 1

-- TEST: test_shard_column_updated-2
-- SQL:
SELECT * FROM "testing_space"
-- EXPECTED:
1, 'some string', 1000,
2, '1', 1,
3, '1', 1,
4, '2', 2,
5, '123', 2,
6, '2', 4

-- TEST: test_shard_column_updated-3
-- SQL:
DELETE FROM "testing_space";
INSERT INTO "testing_space" ("id", "name", "product_units") VALUES
            (1, '123', 1),
            (2, '1', 1),
            (3, '1', 1),
            (4, '2', 2),
            (5, '123', 2),
            (6, '2', 4);

-- TEST: test_local_update-1
-- SQL:
update "arithmetic_space" set
"boolean_col" = false

-- TEST: test_local_update-2
-- SQL:
SELECT "boolean_col" FROM "arithmetic_space"
-- EXPECTED:
False, False, False, False

-- TEST: test_local_update-3
-- SQL:
DELETE FROM "arithmetic_space";
INSERT INTO "arithmetic_space"
("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
VALUES (1, 1, 1, 1, 1, 2, 2, true, 'a', 3.14),
        (2, 1, 2, 1, 2, 2, 2, true, 'a', 2),
        (3, 2, 3, 1, 2, 2, 2, true, 'c', 3.14),
        (4, 2, 3, 1, 1, 2, 2, true, 'c', 2.14);

-- TEST: test_local_update_ambiguous_join-1
-- SQL:
update "arithmetic_space" set
"boolean_col" = "boo"
from (select "boolean_col" as "boo" from "arithmetic_space2")

-- TEST: test_local_update_ambiguous_join-2
-- SQL:
SELECT "boolean_col" FROM "arithmetic_space"
-- EXPECTED:
true, true, true, true

-- TEST: test_local_update_ambiguous_join-3
-- SQL:
DELETE FROM "arithmetic_space";
INSERT INTO "arithmetic_space"
("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
VALUES (1, 1, 1, 1, 1, 2, 2, true, 'a', 3.14),
        (2, 1, 2, 1, 2, 2, 2, true, 'a', 2),
        (3, 2, 3, 1, 2, 2, 2, true, 'c', 3.14),
        (4, 2, 3, 1, 1, 2, 2, true, 'c', 2.14);

-- TEST: test_subquery_in_selection-1
-- SQL:
update "arithmetic_space" set
"c" = 1000
where "id" in (select avg("c") from "arithmetic_space2")

-- TEST: test_subquery_in_selection-2
-- SQL:
SELECT "c" FROM "arithmetic_space"
-- EXPECTED:
1000, 1, 1, 1

-- TEST: test_subquery_in_selection-3
-- SQL:
DELETE FROM "arithmetic_space";
INSERT INTO "arithmetic_space"
("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
VALUES (1, 1, 1, 1, 1, 2, 2, true, 'a', 3.14),
        (2, 1, 2, 1, 2, 2, 2, true, 'a', 2),
        (3, 2, 3, 1, 2, 2, 2, true, 'c', 3.14),
        (4, 2, 3, 1, 1, 2, 2, true, 'c', 2.14);

-- TEST: test_type_conversion-1
-- SQL:
update "double_t" set
"r" = 3.14,
"dec" = 27e-1

-- TEST: test_type_conversion-2
-- SQL:
SELECT * FROM "double_t"
-- EXPECTED:
1, 3.14, Decimal('2.7'), 2, 3.14, Decimal('2.7')

-- TEST: test_type_conversion-3
-- SQL:
DELETE FROM "double_t";
INSERT INTO "double_t"
("id", "r", "dec")
VALUES (1, 1e-1, 0.1), (2, 5e-1, 0.5);

-- TEST: test_bucket_id_in_the_middle-1
-- SQL:
update "testing_space_bucket_in_the_middle"
set "name" = '2'

-- TEST: test_bucket_id_in_the_middle-2
-- SQL:
SELECT "name" FROM "testing_space_bucket_in_the_middle"
-- EXPECTED:
'2', '2', '2'

-- TEST: test_bucket_id_in_the_middle-3
-- SQL:
update "testing_space_bucket_in_the_middle"
set "product_units" = 42

-- TEST: test_bucket_id_in_the_middle-4
-- SQL:
SELECT "product_units" FROM "testing_space_bucket_in_the_middle"
-- EXPECTED:
42, 42, 42

-- TEST: test_type_conversion-5
-- SQL:
DELETE FROM "testing_space_bucket_in_the_middle";
INSERT INTO "testing_space_bucket_in_the_middle" ("id", "name", "product_units") VALUES
                (1, '1', 1),
                (2, '1', 1),
                (3, '1', 1)
