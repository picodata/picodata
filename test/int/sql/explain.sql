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
projection ("testing_space"."id"::int -> "id", "testing_space"."name"::string -> "name")
    selection "testing_space"."id"::int in ROW($0)
        scan "testing_space"
subquery $0:
scan
            projection ("space_simple_shard_key_hist"."id"::int -> "id")
                selection "space_simple_shard_key_hist"."sysOp"::int < 0::int
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
projection ("t3"."id"::int -> "id", "t3"."name"::string -> "name", "t8"."tid"::int -> "tid")
    selection "t3"."name"::string = '123'::string
        join on "t3"."id"::int = "t8"."tid"::int
            scan "t3"
                union all
                    projection ("space_simple_shard_key"."id"::int -> "id", "space_simple_shard_key"."name"::string -> "name")
                        selection "space_simple_shard_key"."sysOp"::int < 1::int
                            scan "space_simple_shard_key"
                    projection ("space_simple_shard_key_hist"."id"::int -> "id", "space_simple_shard_key_hist"."name"::string -> "name")
                        selection "space_simple_shard_key_hist"."sysOp"::int > 0::int
                            scan "space_simple_shard_key_hist"
            scan "t8"
                projection ("testing_space"."id"::int -> "tid")
                    selection "testing_space"."id"::int <> 1::int
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
projection ("t1"."id"::int -> "id", "t1"."name"::string -> "name")
    selection "t1"."id"::int = 1::int
        scan "t1"
            union all
                projection ("space_simple_shard_key"."id"::int -> "id", "space_simple_shard_key"."name"::string -> "name")
                    selection "space_simple_shard_key"."sysOp"::int < 0::int
                        scan "space_simple_shard_key"
                projection ("space_simple_shard_key_hist"."id"::int -> "id", "space_simple_shard_key_hist"."name"::string -> "name")
                    selection "space_simple_shard_key_hist"."sysOp"::int > 0::int
                        scan "space_simple_shard_key_hist"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1934]

-- TEST: test_explain_arithmetic_selection-1
-- SQL:
EXPLAIN select "id" from "arithmetic_space" where "a" + "b" = "b" + "a"
-- EXPECTED:
projection ("arithmetic_space"."id"::int -> "id")
    selection ("arithmetic_space"."a"::int + "arithmetic_space"."b"::int) = ("arithmetic_space"."b"::int + "arithmetic_space"."a"::int)
        scan "arithmetic_space"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_explain_arithmetic_selection-2
-- SQL:
EXPLAIN select "id" from "arithmetic_space" where "a" + "b" > 0 and "b" * "a" = 5
-- EXPECTED:
projection ("arithmetic_space"."id"::int -> "id")
    selection (("arithmetic_space"."a"::int + "arithmetic_space"."b"::int) > 0::int) and (("arithmetic_space"."b"::int * "arithmetic_space"."a"::int) = 5::int)
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
projection ("t3"."id"::int -> "id", "t3"."a"::int -> "a", "t8"."id1"::int -> "id1")
    selection "t3"."id"::int = 2::int
        join on ("t3"."id"::int + ("t3"."a"::int * 2::int)) = ("t8"."id1"::int + 4::int)
            scan "t3"
                union all
                    projection ("arithmetic_space"."id"::int -> "id", "arithmetic_space"."a"::int -> "a")
                        selection "arithmetic_space"."c"::int < 0::int
                            scan "arithmetic_space"
                    projection ("arithmetic_space"."id"::int -> "id", "arithmetic_space"."a"::int -> "a")
                        selection "arithmetic_space"."c"::int > 0::int
                            scan "arithmetic_space"
            motion [policy: full]
                scan "t8"
                    projection ("arithmetic_space2"."id"::int -> "id1")
                        selection "arithmetic_space2"."c"::int < 0::int
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
projection ("t3"."id"::int -> "id", "t3"."a"::int -> "a", "t8"."id1"::int -> "id1")
    selection "t3"."id"::int = 2::int
        join on ("t3"."id"::int + ("t3"."a"::int * 2::int)) = ("t8"."id1"::int + 4::int)
            scan "t3"
                union all
                    projection ("arithmetic_space"."id"::int -> "id", "arithmetic_space"."a"::int -> "a")
                        selection ("arithmetic_space"."c"::int + "arithmetic_space"."a"::int) < 0::int
                            scan "arithmetic_space"
                    projection ("arithmetic_space"."id"::int -> "id", "arithmetic_space"."a"::int -> "a")
                        selection "arithmetic_space"."c"::int > 0::int
                            scan "arithmetic_space"
            motion [policy: full]
                scan "t8"
                    projection ("arithmetic_space2"."id"::int -> "id1")
                        selection "arithmetic_space2"."c"::int < 0::int
                            scan "arithmetic_space2"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_explain_arithmetic_projection-1
-- SQL:
EXPLAIN select "id" + 2 from "arithmetic_space"
-- EXPECTED:
projection ("arithmetic_space"."id"::int + 2::int -> "col_1")
    scan "arithmetic_space"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_explain_arithmetic_projection-2
-- SQL:
EXPLAIN select "a" + "b" * "c" from "arithmetic_space"
-- EXPECTED:
projection ("arithmetic_space"."a"::int + ("arithmetic_space"."b"::int * "arithmetic_space"."c"::int) -> "col_1")
    scan "arithmetic_space"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_explain_arithmetic_projection-3
-- SQL:
EXPLAIN select ("a" + "b") * "c" from "arithmetic_space"
-- EXPECTED:
projection (("arithmetic_space"."a"::int + "arithmetic_space"."b"::int) * "arithmetic_space"."c"::int -> "col_1")
    scan "arithmetic_space"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_explain_arithmetic_projection-4
-- SQL:
EXPLAIN select "a" > "b" from "arithmetic_space"
-- EXPECTED:
projection ("arithmetic_space"."a"::int > "arithmetic_space"."b"::int -> "col_1")
    scan "arithmetic_space"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_explain_arithmetic_projection-5
-- SQL:
EXPLAIN select "a" is null from "arithmetic_space"
-- EXPECTED:
projection ("arithmetic_space"."a"::int is null -> "col_1")
    scan "arithmetic_space"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test_explain_trim-1
-- SQL:
EXPLAIN WITH t(a) AS (SELECT '1') SELECT * FROM t t1 WHERE t1.a = trim('');
-- EXPECTED:
projection ("t1"."a"::string -> "a")
    selection "t1"."a"::string = TRIM(''::string)
        scan cte t1($0)
subquery $0:
projection ("t"."col_1"::string -> "a")
                scan "t"
                    projection ('1'::string -> "col_1")
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = any

-- TEST: test_explain_trim-2
-- SQL:
EXPLAIN SELECT CASE WHEN TRUE THEN '1' ELSE TRIM('2') END;
-- EXPECTED:
projection (case when true::bool then '1'::string else TRIM('2'::string) end -> "col_1")
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = any
