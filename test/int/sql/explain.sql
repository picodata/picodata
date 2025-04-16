-- TEST: explain
-- SQL:
DROP TABLE IF EXISTS testing_space;
DROP TABLE IF EXISTS testing_space_hist;
DROP TABLE IF EXISTS space_simple_shard_key;
DROP TABLE IF EXISTS space_simple_shard_key_hist;
CREATE TABLE testing_space ("id" int primary key, "name" string, "product_units" int);
CREATE TABLE testing_space_hist ("id" int primary key, "name" string, "product_units" int);
CREATE TABLE space_simple_shard_key ("id" int primary key, "name" string, "sysOp" int);
CREATE TABLE space_simple_shard_key_hist ("id" int primary key, "name" string, "sysOp" int);
CREATE TABLE arithmetic_space (id int primary key, a int, b int, c int, d int, e int, f int, boolean_col bool, string_col string, number_col double);
CREATE TABLE arithmetic_space2 ("id" int primary key, a int, b int, c int, d int, e int, f int, boolean_col bool, string_col string, number_col double);
INSERT INTO "arithmetic_space"
("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
VALUES (1, 1, 1, 1, 1, 1, 1, true, '123', 1);
INSERT INTO "arithmetic_space2"
("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
VALUES (1, 1, 1, 1, 1, 1, 1, false, '123', 1);
INSERT INTO "testing_space" ("id", "name", "product_units") VALUES
            (1, '123', 1);
INSERT INTO "testing_space_hist" ("id", "name", "product_units") VALUES
            (1, '123', 5);
INSERT INTO "space_simple_shard_key" ("id", "name", "sysOp") VALUES (1, 'ok', 1), (10, null, 0);
INSERT INTO "space_simple_shard_key_hist" ("id", "name", "sysOp") VALUES (1, 'ok_hist', 3), (2, 'ok_hist_2', 1);

-- TEST: test_motion_explain
-- SQL:
EXPLAIN SELECT "id", "name" FROM "testing_space"
    WHERE "id" in (SELECT "id" FROM "space_simple_shard_key_hist" WHERE "sysOp" < 0)
-- EXPECTED:
projection ("testing_space"."id"::integer -> "id", "testing_space"."name"::string -> "name")
    selection ROW("testing_space"."id"::integer) in ROW($0)
        scan "testing_space"
subquery $0:
scan
            projection ("space_simple_shard_key_hist"."id"::integer -> "id")
                selection ROW("space_simple_shard_key_hist"."sysOp"::integer) < ROW(0::unsigned)
                    scan "space_simple_shard_key_hist"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_join_explain
-- SQL:
EXPLAIN SELECT *
FROM
    (SELECT "id", "name" FROM "space_simple_shard_key" WHERE "sysOp" < 1
     UNION ALL
     SELECT "id", "name" FROM "space_simple_shard_key_hist" WHERE "sysOp" > 0) AS "t3"
INNER JOIN
    (SELECT "id" as "tid"  FROM "testing_space" where "id" <> 1) AS "t8"
    ON "t3"."id" = "t8"."tid"
WHERE "t3"."name" = '123'
-- EXPECTED:
projection ("t3"."id"::integer -> "id", "t3"."name"::string -> "name", "t8"."tid"::integer -> "tid")
    selection ROW("t3"."name"::string) = ROW('123'::string)
        join on ROW("t3"."id"::integer) = ROW("t8"."tid"::integer)
            scan "t3"
                union all
                    projection ("space_simple_shard_key"."id"::integer -> "id", "space_simple_shard_key"."name"::string -> "name")
                        selection ROW("space_simple_shard_key"."sysOp"::integer) < ROW(1::unsigned)
                            scan "space_simple_shard_key"
                    projection ("space_simple_shard_key_hist"."id"::integer -> "id", "space_simple_shard_key_hist"."name"::string -> "name")
                        selection ROW("space_simple_shard_key_hist"."sysOp"::integer) > ROW(0::unsigned)
                            scan "space_simple_shard_key_hist"
            scan "t8"
                projection ("testing_space"."id"::integer -> "tid")
                    selection ROW("testing_space"."id"::integer) <> ROW(1::unsigned)
                        scan "testing_space"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_valid_explain
-- SQL:
EXPLAIN SELECT * FROM (
            SELECT "id", "name" FROM "space_simple_shard_key" WHERE "sysOp" < 0
            UNION ALL
            SELECT "id", "name" FROM
            "space_simple_shard_key_hist" WHERE "sysOp" > 0
        ) as "t1"
        WHERE "id" = 1
-- EXPECTED:
projection ("t1"."id"::integer -> "id", "t1"."name"::string -> "name")
    selection ROW("t1"."id"::integer) = ROW(1::unsigned)
        scan "t1"
            union all
                projection ("space_simple_shard_key"."id"::integer -> "id", "space_simple_shard_key"."name"::string -> "name")
                    selection ROW("space_simple_shard_key"."sysOp"::integer) < ROW(0::unsigned)
                        scan "space_simple_shard_key"
                projection ("space_simple_shard_key_hist"."id"::integer -> "id", "space_simple_shard_key_hist"."name"::string -> "name")
                    selection ROW("space_simple_shard_key_hist"."sysOp"::integer) > ROW(0::unsigned)
                        scan "space_simple_shard_key_hist"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1934]

-- TEST: test_explain_arithmetic_selection-1
-- SQL:
EXPLAIN select "id" from "arithmetic_space" where "a" + "b" = "b" + "a"
-- EXPECTED:
projection ("arithmetic_space"."id"::integer -> "id")
    selection (ROW("arithmetic_space"."a"::integer) + ROW("arithmetic_space"."b"::integer)) = (ROW("arithmetic_space"."b"::integer) + ROW("arithmetic_space"."a"::integer))
        scan "arithmetic_space"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_explain_arithmetic_selection-2
-- SQL:
EXPLAIN select "id" from "arithmetic_space" where "a" + "b" > 0 and "b" * "a" = 5
-- EXPECTED:
projection ("arithmetic_space"."id"::integer -> "id")
    selection ((ROW("arithmetic_space"."a"::integer) + ROW("arithmetic_space"."b"::integer)) > ROW(0::unsigned)) and ((ROW("arithmetic_space"."b"::integer) * ROW("arithmetic_space"."a"::integer)) = ROW(5::unsigned))
        scan "arithmetic_space"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_explain_arithmetic_selection-3
-- SQL:
EXPLAIN SELECT *
FROM
    (SELECT "id", "a" FROM "arithmetic_space" WHERE "c" < 0
    UNION ALL
    SELECT "id", "a" FROM "arithmetic_space" WHERE "c" > 0) AS "t3"
INNER JOIN
    (SELECT "id" as "id1" FROM "arithmetic_space2" WHERE "c" < 0) AS "t8"
ON "t3"."id" + "t3"."a" * 2 = "t8"."id1" + 4
WHERE "t3"."id" = 2
-- EXPECTED:
projection ("t3"."id"::integer -> "id", "t3"."a"::integer -> "a", "t8"."id1"::integer -> "id1")
    selection ROW("t3"."id"::integer) = ROW(2::unsigned)
        join on (ROW("t3"."id"::integer) + (ROW("t3"."a"::integer) * ROW(2::unsigned))) = (ROW("t8"."id1"::integer) + ROW(4::unsigned))
            scan "t3"
                union all
                    projection ("arithmetic_space"."id"::integer -> "id", "arithmetic_space"."a"::integer -> "a")
                        selection ROW("arithmetic_space"."c"::integer) < ROW(0::unsigned)
                            scan "arithmetic_space"
                    projection ("arithmetic_space"."id"::integer -> "id", "arithmetic_space"."a"::integer -> "a")
                        selection ROW("arithmetic_space"."c"::integer) > ROW(0::unsigned)
                            scan "arithmetic_space"
            motion [policy: full]
                scan "t8"
                    projection ("arithmetic_space2"."id"::integer -> "id1")
                        selection ROW("arithmetic_space2"."c"::integer) < ROW(0::unsigned)
                            scan "arithmetic_space2"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_explain_arithmetic_selection-4
-- SQL:
EXPLAIN SELECT *
FROM
    (SELECT "id", "a" FROM "arithmetic_space" WHERE "c" + "a" < 0
    UNION ALL
    SELECT "id", "a" FROM "arithmetic_space" WHERE "c" > 0) AS "t3"
INNER JOIN
    (SELECT "id" as "id1" FROM "arithmetic_space2" WHERE "c" < 0) AS "t8"
ON "t3"."id" + "t3"."a" * 2 = "t8"."id1" + 4
WHERE "t3"."id" = 2
-- EXPECTED:
projection ("t3"."id"::integer -> "id", "t3"."a"::integer -> "a", "t8"."id1"::integer -> "id1")
    selection ROW("t3"."id"::integer) = ROW(2::unsigned)
        join on (ROW("t3"."id"::integer) + (ROW("t3"."a"::integer) * ROW(2::unsigned))) = (ROW("t8"."id1"::integer) + ROW(4::unsigned))
            scan "t3"
                union all
                    projection ("arithmetic_space"."id"::integer -> "id", "arithmetic_space"."a"::integer -> "a")
                        selection (ROW("arithmetic_space"."c"::integer) + ROW("arithmetic_space"."a"::integer)) < ROW(0::unsigned)
                            scan "arithmetic_space"
                    projection ("arithmetic_space"."id"::integer -> "id", "arithmetic_space"."a"::integer -> "a")
                        selection ROW("arithmetic_space"."c"::integer) > ROW(0::unsigned)
                            scan "arithmetic_space"
            motion [policy: full]
                scan "t8"
                    projection ("arithmetic_space2"."id"::integer -> "id1")
                        selection ROW("arithmetic_space2"."c"::integer) < ROW(0::unsigned)
                            scan "arithmetic_space2"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_explain_arithmetic_projection-1
-- SQL:
EXPLAIN select "id" + 2 from "arithmetic_space"
-- EXPECTED:
projection (ROW("arithmetic_space"."id"::integer) + ROW(2::unsigned) -> "col_1")
    scan "arithmetic_space"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_explain_arithmetic_projection-2
-- SQL:
EXPLAIN select "a" + "b" * "c" from "arithmetic_space"
-- EXPECTED:
projection (ROW("arithmetic_space"."a"::integer) + (ROW("arithmetic_space"."b"::integer) * ROW("arithmetic_space"."c"::integer)) -> "col_1")
    scan "arithmetic_space"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_explain_arithmetic_projection-3
-- SQL:
EXPLAIN select ("a" + "b") * "c" from "arithmetic_space"
-- EXPECTED:
projection ((ROW("arithmetic_space"."a"::integer) + ROW("arithmetic_space"."b"::integer)) * ROW("arithmetic_space"."c"::integer) -> "col_1")
    scan "arithmetic_space"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_explain_arithmetic_projection-4
-- SQL:
EXPLAIN select "a" > "b" from "arithmetic_space"
-- EXPECTED:
projection (ROW("arithmetic_space"."a"::integer) > ROW("arithmetic_space"."b"::integer) -> "col_1")
    scan "arithmetic_space"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_explain_arithmetic_projection-5
-- SQL:
EXPLAIN select "a" is null from "arithmetic_space"
-- EXPECTED:
projection (ROW("arithmetic_space"."a"::integer) is null -> "col_1")
    scan "arithmetic_space"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]
