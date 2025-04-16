-- TEST: insert
-- SQL:
DROP TABLE IF EXISTS space_simple_shard_key;
DROP TABLE IF EXISTS space_simple_shard_key_hist;
DROP TABLE IF EXISTS arithmetic_space;
DROP TABLE IF EXISTS double_t;
CREATE TABLE arithmetic_space (id int primary key, a int, b int, c int, d int, e int, f int, boolean_col bool, string_col string, number_col double);
CREATE TABLE t ("id" int primary key, "a" decimal);
CREATE TABLE double_t ("id" int primary key, "r" double, "dec" decimal);
CREATE TABLE unique_secondary_index ("ua" int primary key, "ub" int, "uc" int);
CREATE TABLE space_simple_shard_key ("id" int primary key, "name" string, "sysOp" int);
CREATE TABLE space_simple_shard_key_hist ("id" int primary key, "name" string, "sysOp" int);
INSERT INTO "space_simple_shard_key" ("id", "name", "sysOp") VALUES (1, 'ok', 1), (10, null, 0);
INSERT INTO "space_simple_shard_key_hist" ("id", "name", "sysOp") VALUES (1, 'ok_hist', 3), (2, 'ok_hist_2', 1);
INSERT INTO "t" ("id", "a") VALUES (1, 4.2), (2, 6.66);
INSERT INTO "unique_secondary_index" VALUES (1, 1, 1), (2, 1, 2);

-- TEST: test_insert_1-1
-- SQL:
INSERT INTO "space_simple_shard_key" SELECT * FROM "space_simple_shard_key_hist" WHERE "id" > 1

-- TEST: test_insert_1-2
-- SQL:
SELECT *, "bucket_id" FROM "space_simple_shard_key"
-- EXPECTED:
1, 'ok', 1, 1934, 2, 'ok_hist_2', 1, 1410, 10, None, 0, 626

-- TEST: test_insert_1-3
-- SQL:
DELETE FROM "space_simple_shard_key";

-- TEST: test_insert_2-1
-- SQL:
INSERT INTO "space_simple_shard_key" ("id", "name", "sysOp") VALUES (1, 'ok', 1), (10, null, 0);
INSERT INTO "space_simple_shard_key" ("name", "sysOp", "id") SELECT 'four', 5, 3 FROM "space_simple_shard_key_hist" WHERE "id" IN (SELECT 1 FROM "space_simple_shard_key");

-- TEST: test_insert_2-2
-- SQL:
SELECT *, "bucket_id" FROM "space_simple_shard_key"
-- EXPECTED:
1, 'ok', 1, 1934, 3, 'four', 5, 1958, 10, null, 0, 626

-- TEST: test_insert_2-3
-- SQL:
DELETE FROM "space_simple_shard_key";

-- TEST: test_insert_3-1
-- SQL:
INSERT INTO "space_simple_shard_key" ("sysOp", "id") VALUES (5, 4), (6, 5)

-- TEST: test_insert_3-2
-- SQL:
SELECT *, "bucket_id" FROM "space_simple_shard_key"
-- EXPECTED:
4, null, 5, 2752, 5, null, 6, 219

-- TEST: test_insert_3-3
-- SQL:
DELETE FROM "space_simple_shard_key";

-- TEST: test_insert_4-1
-- SQL:
INSERT INTO "space_simple_shard_key" ("sysOp", "id", "name") VALUES (5, 4, 'кириллица'), (6, 5, 'КИРИЛЛИЦА')

-- TEST: test_insert_4-2
-- SQL:
SELECT *, "bucket_id" FROM "space_simple_shard_key"
-- EXPECTED:
4, 'кириллица', 5, 2752, 5, 'КИРИЛЛИЦА', 6, 219

-- TEST: test_insert_4-3
-- SQL:
DELETE FROM "space_simple_shard_key";

-- TEST: test_insert_6-1
-- SQL:
INSERT INTO "space_simple_shard_key" ("sysOp", "id", "name") VALUES (7, -9223372036854775808, 'bigint')

-- TEST: test_insert_6-2
-- SQL:
SELECT *, "bucket_id" FROM "space_simple_shard_key"
-- EXPECTED:
-9223372036854775808, 'bigint', 7, 413

-- TEST: test_insert_6-3
-- SQL:
DELETE FROM "space_simple_shard_key";

-- TEST: test_insert_7-1
-- SQL:
VALUES (8, 8, null), (9, 9, 'hello')
-- EXPECTED:
8, 8, null, 9, 9, 'hello'

-- TEST: test_insert_7-2
-- SQL:
VALUES (9, 9, 'hello'), (8, 8, null)
-- EXPECTED:
9, 9, 'hello', 8, 8, null

-- TEST: test_insert_7-3-1
-- SQL:
INSERT INTO "space_simple_shard_key" ("id", "name", "sysOp") VALUES (1, 'ok', 1), (10, null, 0);
INSERT INTO "space_simple_shard_key" ("sysOp", "id", "name") VALUES (8, 8, '')

-- TEST: test_insert_7-3-2
-- SQL:
SELECT *, "bucket_id" FROM "space_simple_shard_key"
-- EXPECTED:
1, 'ok', 1, 1934, 8, '', 8, 2564, 10, null, 0, 626

-- TEST: test_insert_7-3-3
-- SQL:
DELETE FROM "space_simple_shard_key"

-- TEST: test_insert_8-1-1
-- SQL:
INSERT INTO "space_simple_shard_key" ("sysOp", "id", "name") VALUES (20, 20, null), (21, 21, 'hello');
INSERT INTO "space_simple_shard_key" ("sysOp", "id", "name") VALUES (8, 8, null), (9, 9, 'hello');
INSERT INTO "space_simple_shard_key" ("sysOp", "id", "name") VALUES (22, 22, null);
INSERT INTO "space_simple_shard_key" ("sysOp", "id", "name") VALUES (23, 23, null);

-- TEST: test_insert_8-1-2
-- SQL:
SELECT * FROM "space_simple_shard_key"
-- EXPECTED:
8, null, 8, 9, 'hello', 9, 20, null, 20, 21, 'hello', 21, 22, null, 22, 23, null, 23


-- TEST: test_insert_8-1-3
-- SQL:
DELETE FROM "space_simple_shard_key"

-- TEST: test_insert_8-2-1
-- SQL:
INSERT INTO "arithmetic_space"
("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
VALUES  (1, 1, 1, 1, 1, 2, 2, true, 'a', 3.14),
        (2, 1, 2, 1, 2, 2, 2, true, 'a', 3),
        (3, 2, 3, 1, 2, 2, 2, true, 'c', 3.14),
        (4, 2, 3, 1, 1, 2, 2, true, 'c', 3.1475)

-- TEST: test_insert_8-2-2
-- SQL:
SELECT * FROM "arithmetic_space"
-- EXPECTED:
1, 1, 1, 1, 1, 2, 2, True, 'a', 3.14, 2, 1, 2, 1, 2, 2, 2, True, 'a', 3.0, 3, 2, 3, 1, 2, 2, 2, True, 'c', 3.14, 4, 2, 3, 1, 1, 2, 2, True, 'c', 3.1475

-- TEST: test_insert_8-2-3
-- SQL:
DELETE FROM "arithmetic_space"

-- TEST: test_insert_9-1
-- SQL:
INSERT INTO "space_simple_shard_key" ("id", "name", "sysOp") VALUES (1, 'ok', 1), (10, null, 0);

-- TEST: test_insert_9-2
-- SQL:
SELECT * FROM "space_simple_shard_key"
-- EXPECTED:
1, 'ok', 1, 10, None, 0

-- TEST: test_insert_9-3
-- SQL:
INSERT INTO "space_simple_shard_key" SELECT * FROM "space_simple_shard_key_hist" WHERE "id" = 1 AND "id" = 2

-- TEST: test_insert_9-4
-- SQL:
SELECT * FROM "space_simple_shard_key"
-- EXPECTED:
1, 'ok', 1, 10, None, 0

-- TEST: test_insert_9-5
-- SQL:
DELETE FROM "space_simple_shard_key"

-- TEST: test_insert_on_conflict_do_nothing-1
-- SQL:
INSERT INTO "space_simple_shard_key" ("id", "name", "sysOp") VALUES (1, 'ok', 1), (10, null, 0);

-- TEST: test_insert_on_conflict_do_nothing-2
-- SQL:
SELECT * FROM "space_simple_shard_key"
-- EXPECTED:
1, 'ok', 1, 10, None, 0

-- TEST: test_insert_on_conflict_do_nothing-3
-- SQL:
INSERT INTO "space_simple_shard_key" VALUES (1, '1', 1) ON CONFLICT DO NOTHING

-- TEST: test_insert_on_conflict_do_nothing-4
-- SQL:
SELECT * FROM "space_simple_shard_key"
-- EXPECTED:
1, 'ok', 1, 10, None, 0

-- TEST: test_insert_on_conflict_do_nothing-5
-- SQL:
DELETE FROM "space_simple_shard_key"

-- TEST: test_insert_on_conflict_do_replace-1
-- SQL:
INSERT INTO "space_simple_shard_key" ("id", "name", "sysOp") VALUES (1, 'ok', 1), (10, null, 0);

-- TEST: test_insert_on_conflict_do_replace-2
-- SQL:
SELECT * FROM "space_simple_shard_key"
-- EXPECTED:
1, 'ok', 1, 10, None, 0

-- TEST: test_insert_on_conflict_do_replace-3
-- SQL:
INSERT INTO "space_simple_shard_key" ("id", "name", "sysOp") VALUES (1, '1', 1) ON CONFLICT DO REPLACE

-- TEST: test_insert_on_conflict_do_replace-4
-- SQL:
SELECT * FROM "space_simple_shard_key"
-- EXPECTED:
1, '1', 1, 10, null, 0

-- TEST: test_insert_on_conflict_do_replace-5
-- SQL:
DELETE FROM "space_simple_shard_key"

-- TEST: test_insert_select_on_conflict_do_replace-1
-- SQL:
INSERT INTO "space_simple_shard_key" ("id", "name", "sysOp") VALUES (1, 'ok', 1), (10, null, 0);

-- TEST: test_insert_select_on_conflict_do_replace-2
-- SQL:
SELECT * FROM "space_simple_shard_key"
-- EXPECTED:
1, 'ok', 1, 10, None, 0

-- TEST: test_insert_select_on_conflict_do_replace-3
-- SQL:
INSERT INTO "space_simple_shard_key" ("id", "name", "sysOp")
SELECT "id", "name" || '1', "sysOp" + 4
FROM "space_simple_shard_key"
ON CONFLICT DO REPLACE

-- TEST: test_insert_select_on_conflict_do_replace-4
-- SQL:
SELECT * FROM "space_simple_shard_key"
-- EXPECTED:
1, 'ok1', 5, 10, null, 4

-- TEST: test_insert_select_on_conflict_do_replace-5
-- SQL:
DELETE FROM "space_simple_shard_key"

-- TEST: test_double_conversion-1
-- SQL:
INSERT INTO "double_t" values (1, 2.5, 2.5e-1), (1, 2.5e-1, 2.5)
ON CONFLICT DO REPLACE

-- TEST: test_double_conversion-2
-- SQL:
SELECT * FROM "double_t"
-- EXPECTED:
1, 0.25, Decimal('2.5')

-- TEST: test_double_conversion-3
-- SQL:
DELETE FROM "double_t"
