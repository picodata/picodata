-- TEST: api
-- SQL:
DROP TABLE IF EXISTS testing_space;
DROP TABLE IF EXISTS testing_space_hist;
DROP TABLE IF EXISTS space_simple_shard_key;
DROP TABLE IF EXISTS space_simple_shard_key_hist;
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS "BROKEN";
DROP TABLE IF EXISTS space_t1;
CREATE TABLE testing_space ("id" int primary key, "name" string, "product_units" int);
CREATE TABLE testing_space_hist ("id" int primary key, "name" string, "product_units" int);
CREATE TABLE space_simple_shard_key ("id" int primary key, "name" string, "sysOp" int);
CREATE TABLE space_simple_shard_key_hist ("id" int primary key, "name" string, "sysOp" int);
CREATE TABLE datetime_t ("dt" datetime primary key, "a" int);
CREATE TABLE "BROKEN" ("id" INT PRIMARY KEY, "reqId" INT, "name" STRING, "department" STRING, manager STRING, salary INT, sysOp INT);
CREATE TABLE space_t1("a" INT PRIMARY KEY, "b" INT);
CREATE TABLE t("id" INT PRIMARY KEY, "a" DECIMAL);
INSERT INTO t VALUES(1, 4.2), (2, 6.66);
INSERT INTO "testing_space" ("id", "name", "product_units") VALUES
            (1, '123', 1);
INSERT INTO "testing_space_hist" ("id", "name", "product_units") VALUES
            (1, '123', 5);
INSERT INTO "space_simple_shard_key" ("id", "name", "sysOp") VALUES (1, 'ok', 1), (10, null, 0);
INSERT INTO "space_simple_shard_key_hist" ("id", "name", "sysOp") VALUES (1, 'ok_hist', 3), (2, 'ok_hist_2', 1);
INSERT INTO "datetime_t" ("dt", "a") VALUES ('2021-08-20 22:59:59 +180'::datetime, 1), ('2021-08-21 22:59:59 +180'::datetime, 1);

-- TEST: test_incorrect_query
-- SQL:
SELECT * FROM "testing_space" INNER JOIN "testing_space";
-- ERROR:
parsing error

-- TEST: test_query_errored-1
-- SQL:
SELECT * FROM "NotFoundSpace";
-- ERROR:
table with name "NotFoundSpace" not found

-- TEST: test_query_errored-2
-- SQL:
SELECT "NotFoundColumn" FROM "testing_space";
-- ERROR:
column with name "NotFoundColumn" not found

-- TEST: test_query_errored-3
-- SQL:
SELECT * FROM "testing_space" where "id" = ?;
-- ERROR:
(invalid query|protocol violation: bind message supplies 0 parameters, but prepared statement "" requires 1)

-- TEST: test_simple_shard_key_query-1
-- SQL:
SELECT * FROM "testing_space" where "id" = 5;
-- EXPECTED:


-- TEST: test_simple_shard_key_query-2
-- SQL:
SELECT *, "bucket_id" FROM "space_simple_shard_key" where "id" = 1.000;
-- EXPECTED:
1, 'ok', 1, 1934

-- TEST: test_complex_shard_key_query-1
-- SQL:
SELECT *, "bucket_id" FROM "testing_space" where "id" = 1 and "name" = '457';
-- EXPECTED:


-- TEST: test_complex_shard_key_query-2
-- SQL:
SELECT *, "bucket_id" FROM "testing_space" where "id" = 1 and "name" = '123';
-- EXPECTED:
1, '123', 1, 1934

-- TEST: test_null_col_result
-- SQL:
SELECT "id", "name" FROM "space_simple_shard_key" WHERE "id" = 10;
-- EXPECTED:
10, null

-- TEST: test_anonymous_cols_naming
-- SQL:
SELECT * FROM "testing_space"
    WHERE "id" in (SELECT "id" FROM "space_simple_shard_key_hist" WHERE "sysOp" > 0)
        OR "id" in (SELECT "id" FROM "space_simple_shard_key_hist" WHERE "sysOp" > 0);
-- EXPECTED:
1, '123', 1

-- TEST: test_decimal_double
-- SQL:
SELECT *, "bucket_id" FROM "t";
-- EXPECTED:
1, Decimal('4.2'), 1934, 2, Decimal('6.66'), 1410

-- TEST: test_bucket_id_in_join
-- SQL:
SELECT * FROM "space_simple_shard_key" as "t1" JOIN (SELECT "a" FROM "t") as "t2"
        ON "t1"."id" = "t2"."a";
-- EXPECTED:


-- TEST: test_lowercase1
-- SQL:
SELECT id FROM "BROKEN";
-- EXPECTED:


-- TEST: test_lowercase2
-- SQL:
SELECT "a" FROM space_t1;
-- EXPECTED:


-- TEST: test_union_operator_works
-- SQL:
select "id" from "t"
union
select "id" from "t"
union
select "id" from "t"
union
select "id" from "t"
union
select "id" from "t";
-- EXPECTED:
1, 2

-- TEST: test_lower_upper-1
-- SQL:
select lower("COLUMN_1") as a, upper("COLUMN_1") as b from (values ('Aba'));
-- EXPECTED:
'aba', 'ABA'

-- TEST: test_lower_upper-2
-- SQL:
select upper(lower("COLUMN_1")) as a, lower(upper("COLUMN_1")) as b from (values ('Aba'));
-- EXPECTED:
'ABA', 'aba'

-- TEST: test_like_works
-- SQL:
select id from testing_space
where name like '123'
and name like '1__'
and name like '%2_'
and '%_%' like '\%\_\%' escape '\'
and 'A' || 'a' like '_' || '%'
and 'A' || '_' like '_' || '\_' escape '' || '\'
and (values ('a')) like (values ('_'))
and (values ('_')) like (values ('\_')) escape (values ('\'))
and (select name from testing_space) like (select name from testing_space)
and '_' like '\_'
and '%' like '\%'
and 'aBa' ilike 'aba'
and 'B' || 'b' ilike 'b' || 'B'
and '_' ilike '\_' escape '\';
-- EXPECTED:
1

-- TEST: test_select_without_scan-1
-- SQL:
select 1 as foo, 2 + 3 as bar;
-- EXPECTED:
1, 5

-- TEST: test_select_without_scan-2
-- SQL:
select (select id from t where id = 1), (select count(*) from t) as bar, (values (30));
-- EXPECTED:
1, 2, 30

-- TEST: test_select_without_scan-3
-- SQL:
select id, (select 3 as b) from testing_space
where id in (select 1) and name in (select '123');
-- EXPECTED:
1, 3

-- TEST: test_select_without_scan-4
-- SQL:
select 1, 2;
-- EXPECTED:
1, 2

-- TEST: test_select_without_scan-5
-- SQL:
select (values (100, 500)) as bar;
-- ERROR:
subquery must return only one column
