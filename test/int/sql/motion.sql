-- TEST: motion
-- SQL:
DROP TABLE IF EXISTS testing_space;
DROP TABLE IF EXISTS testing_space_hist;
DROP TABLE IF EXISTS space_simple_shard_key;
DROP TABLE IF EXISTS space_simple_shard_key_hist;
CREATE TABLE testing_space ("id" int primary key, "name" string, "product_units" int);
CREATE TABLE testing_space_hist ("id" int primary key, "name" string, "product_units" int);
CREATE TABLE space_simple_shard_key ("id" int primary key, "name" string, "sysOp" int);
CREATE TABLE space_simple_shard_key_hist ("id" int primary key, "name" string, "sysOp" int);
INSERT INTO "testing_space" ("id", "name", "product_units") VALUES
            (1, '123', 1);
INSERT INTO "testing_space_hist" ("id", "name", "product_units") VALUES
            (1, '123', 5);
INSERT INTO "space_simple_shard_key" ("id", "name", "sysOp") VALUES (1, 'ok', 1), (10, null, 0);
INSERT INTO "space_simple_shard_key_hist" ("id", "name", "sysOp") VALUES (1, 'ok_hist', 3), (2, 'ok_hist_2', 1);

-- TEST: test_simple_motion_query
-- SQL:
SELECT "id", "name" FROM "space_simple_shard_key"
        WHERE "id" in (SELECT "id" FROM "testing_space_hist" WHERE "product_units" > 3)
-- EXPECTED:
1, 'ok'

-- TEST: test_motion_query
-- SQL:
SELECT * FROM (
            SELECT "id", "name" FROM "space_simple_shard_key" WHERE "sysOp" > 0
            UNION ALL
            SELECT "id", "name" FROM "space_simple_shard_key_hist" WHERE "sysOp" > 0
        ) as "t1"
        WHERE "id" in (SELECT "id" FROM (
            SELECT "id", "name" FROM "testing_space" WHERE "product_units" < 3
            UNION ALL
            SELECT "id", "name" FROM "testing_space_hist" WHERE "product_units" > 3
        ) as "t2"
        WHERE "id" = 1.00 and "name" = '123')
-- EXPECTED:
1, 'ok', 1, 'ok_hist'

-- TEST: test_join_motion_query
-- SQL:
SELECT "t3"."id", "t3"."name", "t8"."product_units"
    FROM
        (SELECT "id", "name"
            FROM "space_simple_shard_key"
            WHERE "sysOp" > 0
        UNION ALL
            SELECT "id", "name"
            FROM "space_simple_shard_key_hist"
            WHERE "sysOp" > 0) AS "t3"
    INNER JOIN
        (SELECT "id" as "id1", "product_units"
        FROM "testing_space"
        WHERE "product_units" < 0
        UNION ALL
        SELECT "id" as "id1", "product_units"
        FROM "testing_space_hist"
        WHERE "product_units" > 0) AS "t8"
        ON "t3"."id" = "t8"."id1"
    WHERE "t3"."id" = 1
-- EXPECTED:
1, 'ok', 5, 1, 'ok_hist', 5

-- TEST: test_empty_motion_result-1
-- SQL:
SELECT "id", "name" FROM "testing_space"
    WHERE "id" in (SELECT "id" FROM "space_simple_shard_key_hist" WHERE "sysOp" < 0)
-- EXPECTED:


-- TEST: test_empty_motion_result-2
-- SQL:
SELECT "id", "name" FROM "testing_space"
    WHERE ("id", "name") in (SELECT "id", "name" FROM "space_simple_shard_key_hist" WHERE "sysOp" < 0)
-- EXPECTED:


-- TEST: test_empty_motion_result-3
-- SQL:
SELECT *, "bucket_id" FROM "testing_space"
    WHERE "id" in (SELECT "id" FROM "space_simple_shard_key_hist" WHERE "sysOp" > 0)
        OR "id" in (SELECT "id" FROM "space_simple_shard_key_hist" WHERE "sysOp" < 0)
-- EXPECTED:
1, '123', 1, 1934

-- TEST: test_motion_dotted_name
-- SQL:
SELECT "sysOp", "product_units" FROM "testing_space"
    INNER JOIN (SELECT "sysOp" FROM (SELECT "product_units" from "testing_space_hist") as r
    INNER JOIN "space_simple_shard_key"
    on r."product_units" = "space_simple_shard_key"."sysOp") as q
    on q."sysOp" = "testing_space"."product_units"
-- EXPECTED:

-- TEST: test_subquery_under_motion_without_alias
-- SQL:
SELECT * FROM
        (SELECT "id" as "tid" FROM "testing_space")
INNER JOIN
        (SELECT "id" as "sid" FROM "space_simple_shard_key")
ON true
-- EXPECTED:
1, 1, 1, 10

-- TEST: test_subquery_under_motion_with_alias
-- SQL:
SELECT * FROM
        (SELECT "id" as "tid" FROM "testing_space")
INNER JOIN
        (SELECT "id" as "sid" FROM "space_simple_shard_key") as "smth"
ON true
-- EXPECTED:
1, 1, 1, 10

-- TEST: test_nested_joins_with_motions
-- SQL:
SELECT t1."id" FROM "testing_space" as t1
JOIN "space_simple_shard_key" as t2
ON t1."id" = t2."id"
JOIN "space_simple_shard_key_hist" as t3
ON t2."id" = t3."id"
WHERE t1."id" = 1
-- EXPECTED:
1

-- TEST: test_join_segment_motion-1.1
-- SQL:
insert into "space_simple_shard_key" ("id", "name", "sysOp") values (2, '222', 2), (3, '333', 3)

-- TEST: test_join_segment_motion-1.2
-- SQL:
SELECT "t1"."id" FROM (
    SELECT "id" FROM "space_simple_shard_key"
) as "t1"
JOIN (
    SELECT "sysOp" FROM "space_simple_shard_key"
) as "t2"
ON "t1"."id" = "t2"."sysOp"
-- UNORDERED:
1, 2, 3
