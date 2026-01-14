-- TEST: operators
-- SQL:
DROP TABLE IF EXISTS testing_space;
DROP TABLE IF EXISTS testing_space_hist;
DROP TABLE IF EXISTS space_simple_shard_key;
DROP TABLE IF EXISTS space_simple_shard_key_hist;
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS many_columns;
CREATE TABLE testing_space ("id" int primary key, "name" string, "product_units" int);
CREATE TABLE testing_space_hist ("id" int primary key, "name" string, "product_units" int);
CREATE TABLE space_simple_shard_key ("id" int primary key, "name" string, "sysOp" int);
CREATE TABLE space_simple_shard_key_hist ("id" int primary key, "name" string, "sysOp" int);
CREATE TABLE t ("id" int primary key, "a" decimal);
CREATE TABLE many_columns ("id" int primary key, "db" double, "dm" decimal, "dt" datetime);
INSERT INTO "testing_space" ("id", "name", "product_units") VALUES
            (1, '123', 1);
INSERT INTO "testing_space_hist" ("id", "name", "product_units") VALUES
            (1, '123', 5);
INSERT INTO "space_simple_shard_key" ("id", "name", "sysOp") VALUES (1, 'ok', 1), (10, null, 0);
INSERT INTO "space_simple_shard_key_hist" ("id", "name", "sysOp") VALUES (1, 'ok_hist', 3), (2, 'ok_hist_2', 1);
INSERT INTO "t" ("id", "a") VALUES (1, 4.2), (2, 6.66);
INSERT INTO many_columns VALUES (1, 2, 3, '2026-01-17');

-- TEST: test_operator_1
-- SQL:
SELECT * FROM "testing_space" where "id" = 1 AND "id" = 2;
-- EXPECTED:

-- TEST: test_not_eq-1
-- SQL:
insert into "testing_space" ("id", "name", "product_units") values (2, '123', 2), (3, '123', 3);

-- TEST: test_not_eq-2
-- SQL:
SELECT * FROM "testing_space" where "id" <> 1;
-- EXPECTED:
2, '123', 2, 3, '123', 3

-- TEST: test_not_eq-3
-- SQL:
SELECT * FROM "testing_space" where "id" <> 1 and "product_units" <> 3;
-- EXPECTED:
2, '123', 2

-- TEST: test_not_eq-4
-- SQL:
DELETE FROM "testing_space";
INSERT INTO "testing_space" ("id", "name", "product_units") VALUES
            (1, '123', 1);

-- TEST: test_not_eq2-1
-- SQL:
insert into "t" ("id", "a") values (3, 0.0), (4, 0.0);

-- TEST: test_not_eq2-2
-- SQL:
SELECT "id", u FROM "t" join
                    (select "id" as u from "t") as q
                    on "t"."id" <> q.u;
-- EXPECTED:
1, 2, 1, 3, 1, 4, 2, 1, 2, 3, 2, 4, 3, 1, 3, 2, 3, 4, 4, 1, 4, 2, 4, 3

-- TEST: test_not_eq2-3
-- SQL:
DELETE FROM "t";
INSERT INTO "t" ("id", "a") VALUES (1, 4.2), (2, 6.66);

-- TEST: test_simple_shard_key_union_query
-- SQL:
SELECT * FROM (
            SELECT "id", "name" FROM "space_simple_shard_key" WHERE "sysOp" < 0
            UNION ALL
            SELECT "id", "name" FROM "space_simple_shard_key_hist" WHERE "sysOp" > 0
        ) as "t1"
        WHERE "id" = 1;
-- EXPECTED:
1, 'ok_hist'

-- TEST: test_complex_shard_key_union_query
-- SQL:
SELECT * FROM (
            SELECT "id", "name", "product_units" FROM "testing_space" WHERE "product_units" < 3
            UNION ALL
            SELECT "id", "name", "product_units" FROM "testing_space_hist" WHERE "product_units" > 3
        ) as "t1"
        WHERE "id" = 1 and "name" = '123';
-- EXPECTED:
1, '123', 1, 1, '123', 5

-- TEST: test_compare
-- SQL:
SELECT * FROM "t" where "id" < 2 and "a" > 5;
-- EXPECTED:


-- TEST: test_except-1
-- SQL:
insert into "t" ("id", "a") values (3, 777), (1000001, 6.66), (1000002, 6.66);

-- TEST: test_except-2
-- SQL:
SELECT "a" FROM "t" where "id" <= 3
        EXCEPT
        SELECT "a" FROM "t" where "id" > 3;
-- EXPECTED:
Decimal('4.2'), Decimal('777')

-- TEST: test_except-3
-- SQL:
DELETE FROM "t";
INSERT INTO "t" ("id", "a") VALUES (1, 4.2), (2, 6.66);

-- TEST: test_is_null
-- SQL:
SELECT "id" FROM "space_simple_shard_key" WHERE "name" IS NULL;
-- EXPECTED:
10

-- TEST: test_is_not_null_1
-- SQL:
SELECT "id" FROM "space_simple_shard_key" WHERE "name" IS NOT NULL and "id" = 10;
-- EXPECTED:

-- TEST: test_is_not_null_2
-- SQL:
SELECT "id" FROM "space_simple_shard_key" WHERE "name" IS NOT NULL;
-- EXPECTED:
1

-- TEST: test_in_subquery_select_from_table
-- SQL:
SELECT "id" FROM "space_simple_shard_key" WHERE "id" IN (SELECT "id" FROM "testing_space");
-- EXPECTED:
1

-- TEST: test_not_in_subquery_select_from_values
-- SQL:
SELECT "id" FROM "space_simple_shard_key"
WHERE "id" NOT IN (SELECT cast("COLUMN_2" as int) FROM (VALUES (1), (3)));
-- EXPECTED:
10

-- TEST: test_exists_subquery_select_from_values
-- SQL:
SELECT "id" FROM "t" WHERE EXISTS (SELECT 0 FROM (VALUES (1)));
-- EXPECTED:
1, 2

-- TEST: test_not_exists_subquery_select_from_values
-- SQL:
SELECT "id" FROM "t" WHERE NOT EXISTS (SELECT cast("COLUMN_1" as int) FROM (VALUES (1)));
-- EXPECTED:

-- TEST: test_exists_subquery_with_several_rows
-- SQL:
SELECT * FROM "testing_space" WHERE EXISTS (SELECT 0 FROM "t" WHERE "t"."id" = 1 or "t"."a" = (6.66));
-- EXPECTED:
1, '123', 1

-- TEST: test_not_exists_subquery_with_several_rows
-- SQL:
SELECT * FROM "testing_space"
        WHERE NOT EXISTS (SELECT 0 FROM "t" WHERE "t"."id" = 1 or "t"."a" = (6.66));
-- EXPECTED:

-- TEST: test_exists_nested
-- SQL:
SELECT * FROM "testing_space" WHERE EXISTS
        (SELECT 0 FROM (VALUES (1)) WHERE EXISTS (SELECT 0 FROM "t" WHERE "t"."id" = 1));
-- EXPECTED:
1, '123', 1

-- TEST: test_exists_partitioned_in_selection_condition-1
-- SQL:
SELECT * FROM "t";
-- EXPECTED:
1, Decimal('4.2'), 2, Decimal('6.66')

-- TEST: test_exists_partitioned_in_selection_condition-2
-- SQL:
SELECT * FROM "t" WHERE EXISTS (SELECT * FROM "testing_space");
-- EXPECTED:
1, Decimal('4.2'), 2, Decimal('6.66')

-- TEST: test_exists_partitioned_in_join_filter-1
-- SQL:
SELECT * FROM
            (SELECT "id" as "tid" FROM "t") as "t"
        INNER JOIN
            (SELECT "id" as "sid" FROM "space_simple_shard_key") as "s"
        ON true;
-- EXPECTED:
1, 1, 1, 10, 2, 1, 2, 10

-- TEST: test_exists_partitioned_in_join_filter-2
-- SQL:
SELECT * FROM
        (SELECT "id" as "tid" FROM "t") as "t"
INNER JOIN
        (SELECT "id" as "sid" FROM "space_simple_shard_key") as "s"
ON EXISTS (SELECT * FROM "testing_space");
-- EXPECTED:
1, 1, 1, 10, 2, 1, 2, 10

-- TEST: test_not_with_true_gives_false
-- SQL:
SELECT not true FROM (values (1));
-- EXPECTED:
false

-- TEST: test_double_not_with_true_gives_true
-- SQL:
SELECT not not true FROM (values (1));
-- EXPECTED:
true

-- TEST: test_not_with_parenthesis
-- SQL:
SELECT (not (not true)) FROM (values (1));
-- EXPECTED:
true

-- TEST: test_not_with_and
-- SQL:
SELECT not true and false FROM (values (1));
-- EXPECTED:
false

-- TEST: test_not_with_or
-- SQL:
SELECT not true or true FROM (values (1));
-- EXPECTED:
true

-- TEST: test_not_with_in
-- SQL:
select * from (values (1)) where 1 not in (values(1));
-- EXPECTED:


-- TEST: test_not_with_exists
-- SQL:
select "COLUMN_1" from (values (1)) where not exists (select 1 from (values (1)));
-- EXPECTED:

-- TEST: test_not_in_filter
-- SQL:
select "id" from "testing_space" where not "id" = 2;
-- EXPECTED:
1

-- TEST: test_not_in_condition
-- SQL:
select * from
        (select "id" as "tid" from "testing_space")
inner join
        "t"
on not "tid" != "id" and "a" = 4.2;
-- EXPECTED:
1, 1, Decimal('4.2')

-- TEST: test_not_between
-- SQL:
SELECT 1 not between 2 and 3 FROM (values (1));
-- EXPECTED:
true


-- TEST: test_between1
-- SQL:
SELECT "id" FROM "space_simple_shard_key" WHERE
        (SELECT "id" FROM "space_simple_shard_key_hist" WHERE "id" = 2) BETWEEN 1 AND 2;
-- EXPECTED:
1, 10

-- TEST: test_between2
-- SQL:
SELECT "id" FROM "space_simple_shard_key" WHERE
        "id" BETWEEN 1 AND 2;
-- EXPECTED:
1

-- TEST: test_between3
-- SQL:
select 1 between -2 and 5.7;
-- EXPECTED:
true

-- TEST: test_between4
-- SQL:
select '1' between '2' and '3';
-- EXPECTED:
false

-- TEST: test_between5
-- SQL:
select 6.5 between '2' and '3';
-- EXPECTED:
false

-- TEST: test_between6_subquery_type_unification
-- SQL:
select '100' between 100 and (select '100');
-- ERROR:
BETWEEN types text, int and text cannot be matched

-- TEST: test_between7_type_unification
-- SQL:
select 'false' between false and 'foo';
-- ERROR:
failed to parse 'foo' as a value of type bool, consider using explicit type casts

-- TEST: test_between8_type_unification
-- SQL:
select 'false' between 'bar' and true;
-- ERROR:
failed to parse 'bar' as a value of type bool, consider using explicit type casts

-- TEST: test_between9_type_defaulting
-- SQL:
select 1 from (select 1) where '' between '' and '';
-- EXPECTED:
1

-- TEST: test_between10_plan_tree_postorder
-- SQL:
select (1 between 1 and 20) between (2 between 1 and 30) and (3 between 1 and 40);
-- EXPECTED:
true

-- TEST: test_between11_plan_tree_postorder
-- SQL:
explain select (1 between 1 and 20) between (2 between 1 and 30) and (3 between 1 and 40);
-- EXPECTED:
projection ((((1::int >= 1::int) and (1::int <= 20::int)) >= ((2::int >= 1::int) and (2::int <= 30::int))) and (((1::int >= 1::int) and (1::int <= 20::int)) <= ((3::int >= 1::int) and (3::int <= 40::int))) -> "col_1")
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = any

-- TEST: test_between12_column_references
-- SQL:
select id from many_columns where db between id and dm;
-- EXPECTED:
1

-- TEST: test_between13_column_references
-- SQL:
select id from many_columns where id between db and dm;
-- EXPECTED:

-- TEST: test_between14_column_references_invalid
-- SQL:
select id from many_columns where dt between id and dm;
-- ERROR:
BETWEEN types datetime, int and numeric cannot be matched

-- TEST: test_join_inner_sq_no_alias
-- SQL:
select "i" from "testing_space" inner join
    (select "id" as "i", "a" as a from "t")
    on "id" = "i";
-- EXPECTED:
1
