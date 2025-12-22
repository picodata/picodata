-- TEST: sq_as_expr
-- SQL:
DROP TABLE IF EXISTS testing_space;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS null_t;
CREATE TABLE testing_space ("id" int primary key, "name" string, "product_units" int);
CREATE TABLE t1 (a int primary key, b int, c int);
CREATE TABLE null_t ("na" int primary key, "nb" int, "nc" int);
INSERT INTO t1 ("a", "b", "c") VALUES
            (1, 1, 1);
INSERT INTO "testing_space" ("id", "name", "product_units") VALUES
            (1, '123', 1),
            (2, '1', 1),
            (3, '1', 1),
            (4, '2', 2),
            (5, '123', 2),
            (6, '2', 4);
INSERT INTO "null_t"
("na", "nb", "nc")
VALUES 
    (1, null, 1),
    (2, null, null),
    (3, null, 3),
    (4, 1, 2),
    (5, null, 1);

-- TEST: test_under_projection-1
-- SQL:
SELECT (VALUES (1)) FROM "testing_space" WHERE "id" = 1;
-- EXPECTED:
1

-- TEST: test_under_projection-2
-- SQL:
SELECT (VALUES (1)), (VALUES (2)) FROM "testing_space" WHERE "id" = 1;
-- EXPECTED:
1, 2

-- TEST: test_under_projection-3
-- SQL:
SELECT (VALUES ((VALUES (3)))) FROM "testing_space" WHERE "id" = 1;
-- EXPECTED:
3

-- TEST: test_under_projection-4
-- SQL:
SELECT (SELECT "id" FROM "testing_space" WHERE "id" = 1) + "id" FROM "testing_space" WHERE "id" in (1, 2, 3);
-- EXPECTED:
2, 3, 4


-- TEST: test_under_selection-1
-- SQL:
SELECT "id" FROM "testing_space" WHERE "id" = (VALUES (1));
-- EXPECTED:
1

-- TEST: test_under_selection-2
-- SQL:
SELECT "id" FROM "testing_space" WHERE "id" = (VALUES (1)) + (VALUES (3)) / (VALUES (2));
-- EXPECTED:
2

-- TEST: test_under_group_by-1
-- SQL:
SELECT count(*) FROM "testing_space" GROUP BY "product_units" + (VALUES (1));
-- EXPECTED:
3, 2, 1

-- TEST: test_under_group_by-2
-- SQL:
SELECT sum("id") + 1, count(*)
FROM "testing_space"
GROUP BY "product_units" + (VALUES (1))
HAVING sum("id") + (VALUES (1)) > 7;
-- EXPECTED:
10, 2

-- TEST: test_under_order_by-1
-- SQL:
SELECT "name", "id" FROM "testing_space" ORDER BY "name" || (VALUES ('a'));
-- EXPECTED:
'123', 1,
'123', 5, 
'1', 2,
'1', 3,
'2', 4,
'2', 6 

-- TEST: test_under_cte-1
-- SQL:
WITH "my_cte" ("first") AS (VALUES (cast(1 as string)), ((SELECT "name" FROM "testing_space" WHERE "id" = 1)))
SELECT "first" FROM "my_cte";
-- EXPECTED:
'1', '123'

-- TEST: test_under_join-1
-- SQL:
SELECT "id" FROM "testing_space" JOIN "null_t" ON
(SELECT true FROM "null_t" WHERE "na" = 1) AND "product_units" = "na" AND "name" != (VALUES ('123'));
-- EXPECTED:
2, 3, 4, 6

-- TEST: test_under_insert-1.1
-- SQL:
INSERT INTO "testing_space"
          VALUES
          (
            (VALUES (11)),
            (VALUES ('111')) || (VALUES ('222')),
            (SELECT 42 FROM "testing_space" WHERE "id" = 1)
          ),
          (
            (SELECT 42 FROM "testing_space" WHERE "id" = 1),
            'aba',
            33);

-- TEST: test_under_insert-1.2
-- SQL:
SELECT * FROM "testing_space";
-- EXPECTED:
1, '123', 1,
2, '1', 1,
3, '1', 1,
4, '2', 2,
5, '123', 2,
6, '2', 4,
11, '111222', 42,
42, 'aba', 33

-- TEST: test_under_insert-1.3
-- SQL:
DELETE FROM "testing_space";
INSERT INTO "testing_space" ("id", "name", "product_units") VALUES
            (1, '123', 1),
            (2, '1', 1),
            (3, '1', 1),
            (4, '2', 2),
            (5, '123', 2),
            (6, '2', 4);

-- TEST: test_under_update-1.1
-- SQL:
update "testing_space"
set "name" = (SELECT "name" FROM "testing_space" WHERE "product_units" = 4), "product_units" = (VALUES (42));

-- TEST: test_under_update-1.2
-- SQL:
SELECT * FROM "testing_space";
-- EXPECTED:
1, '2', 42,
2, '2', 42,
3, '2', 42,
4, '2', 42,
5, '2', 42,
6, '2', 42

-- TEST: test_under_update-1.3
-- SQL:
DELETE FROM "testing_space";
INSERT INTO "testing_space" ("id", "name", "product_units") VALUES
            (1, '123', 1),
            (2, '1', 1),
            (3, '1', 1),
            (4, '2', 2),
            (5, '123', 2),
            (6, '2', 4);

-- TEST: test-scalar-subquery-with-group-by
-- SQL:
SELECT (VALUES (1)) FROM testing_space GROUP BY product_units;
-- EXPECTED:
1,
1,
1

-- TEST: test-scalar-subquery-with-aggregates
-- SQL:
SELECT (VALUES (1)), SUM(id) FROM testing_space;
-- EXPECTED:
1, 21

-- TEST: test-subquery-under-aggregate
-- SQL:
SELECT SUM((SELECT id FROM testing_space WHERE id = 1)) FROM testing_space;
-- EXPECTED:
6

-- TEST: test-subquery-the-only-output-with-group-by
-- SQL:
SELECT (SELECT 1) FROM testing_space GROUP BY product_units;
-- EXPECTED:
1,
1,
1

-- TEST: test-subquery-second-output-with-group-by
-- SQL:
SELECT product_units, (SELECT 1) FROM testing_space GROUP BY product_units ORDER BY product_units;
-- EXPECTED:
1, 1,
2, 1,
4, 1

-- TEST: test-subquery-under-group-by
-- SQL:
SELECT (SELECT 1) FROM testing_space GROUP BY (SELECT 1);
-- EXPECTED:
1

-- TEST: test-subquery-under-group-by-with-column
-- SQL:
SELECT (SELECT 1) FROM testing_space GROUP BY product_units, (SELECT 1);
-- EXPECTED:
1,
1,
1

-- TEST: test-subquery-under-group-by-with-column-under-output
-- SQL:
SELECT product_units, (SELECT 1) FROM testing_space GROUP BY product_units, (SELECT 1) ORDER BY product_units;
-- EXPECTED:
1, 1,
2, 1,
4, 1

-- TEST: test-subquery-under-group-by-column-not-found
-- SQL:
SELECT id, (SELECT 1) FROM testing_space GROUP BY (SELECT 1);
-- ERROR:
invalid query: column "id" is not found in grouping expressions!

-- TEST: test-subquery-with-distinct
-- SQL:
SELECT DISTINCT (SELECT 1) FROM testing_space;
-- EXPECTED:
1

-- TEST: test-subquery-with-distinct-and-column
-- SQL:
SELECT DISTINCT (SELECT 1), product_units FROM testing_space ORDER BY product_units;
-- EXPECTED:
1, 1,
1, 2,
1, 4,

-- TEST: test-subquery-under-having
-- SQL:
SELECT DISTINCT product_units, (SELECT 1) FROM testing_space HAVING (SELECT true) ORDER BY product_units;
-- EXPECTED:
1, 1,
2, 1,
4, 1

-- TEST: test-several-subqueries-in-output
-- SQL:
SELECT DISTINCT (SELECT 1), SUM((SELECT SUM(product_units) FROM testing_space)) FROM testing_space;
-- EXPECTED:
1, 66

-- TEST: test-several-subqueries-under-subtree
-- SQL:
SELECT DISTINCT (SELECT 1) + (SELECT 1) FROM testing_space;
-- EXPECTED:
2

-- TEST: test-subquery-under-distinct-aggregate
-- SQL:
SELECT SUM(DISTINCT (SELECT 1)) FROM testing_space;
-- EXPECTED:
1

-- TEST: test-subquery-under-distinct-aggregate-subtree
-- SQL:
SELECT SUM(DISTINCT (SELECT 1) + (SELECT 2)) FROM testing_space;
-- EXPECTED:
3

-- TEST: test-subquery-under-distinct-aggregate-and-outside
-- SQL:
SELECT (SELECT 1), SUM(DISTINCT (SELECT 2)), (SELECT 3) FROM testing_space;
-- EXPECTED:
1, 2, 3

-- TEST: test-subquery-under-several-aggregates
-- SQL:
SELECT product_units, AVG((SELECT SUM(id) FROM testing_space)), product_units + 1, SUM((SELECT SUM(id) FROM testing_space)) FROM testing_space GROUP BY product_units;
-- EXPECTED:
1, 21, 2, 63,
2, 21, 3, 42,
4, 21, 5, 21

-- TEST: test-subquery-under-having-aggregate
-- SQL:
SELECT sum((select 1)) FROM testing_space HAVING sum((select id from testing_space limit 1)) > (select 1);
-- EXPECTED:
6

-- TEST: test-subquery-under-having-aggregate-and-projection
-- SQL:
SELECT sum((select id from testing_space where id = 3)) FROM testing_space HAVING sum((select id from testing_space limit 1)) > (select 1);
-- EXPECTED:
18

-- TEST: test-single-sum-with-subquery
-- SQL:
SELECT SUM((SELECT * FROM (VALUES(1)))) FROM testing_space;
-- EXPECTED:
6

-- TEST: test-distinct-aggr-and-group-by-with-subquery
-- SQL:
SELECT SUM(DISTINCT (SELECT MIN(id) FROM testing_space)) FROM testing_space GROUP BY (select 1);
-- EXPECTED:
1

-- TEST: test-subquery-equality-distribution-issue-2006
-- SQL:
SELECT t1.a FROM t1 d JOIN t1 ON t1.b = d.a WHERE t1.b = d.c AND (SELECT e.a FROM t1 d JOIN t1 ON d.a = t1.a JOIN t1 e ON c = b) = (SELECT a FROM t1);
-- EXPECTED:
1

-- TEST: test-explain-plan-subquery-as-expression-under-projection
-- SQL:
EXPLAIN SELECT (values (1)) from testing_space;
-- EXPECTED:
projection (ROW($0) -> "col_1")
    scan "testing_space"
subquery $0:
scan
        motion [policy: full, program: ReshardIfNeeded]
            values
                value row (data=ROW(1::int))
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test-explain-plan-subquery-as-expression-under-order-by
-- SQL:
EXPLAIN SELECT "id" FROM "testing_space" ORDER BY "id" + (VALUES (1));
-- EXPECTED:
projection ("id"::int -> "id")
    order by ("id"::int + ROW($0))
        motion [policy: full, program: ReshardIfNeeded]
            scan
                projection ("testing_space"."id"::int -> "id")
                    scan "testing_space"
subquery $0:
scan
            motion [policy: full, program: ReshardIfNeeded]
                values
                    value row (data=ROW(1::int))
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test-explain-plan-subquery-as-expression-under-projection-nested
-- SQL:
EXPLAIN SELECT (values ((values (1)))) from testing_space;
-- EXPECTED:
projection (ROW($1) -> "col_1")
    scan "testing_space"
subquery $0:
scan
                        motion [policy: full, program: ReshardIfNeeded]
                            values
                                value row (data=ROW(1::int))
subquery $1:
scan
        motion [policy: full, program: ReshardIfNeeded]
            values
                value row (data=ROW(ROW($0)))
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test-explain-plan-subquery-as-expression-under-group-by
-- SQL:
EXPLAIN SELECT (values ((values (1)))) from testing_space;
-- EXPECTED:
projection (ROW($1) -> "col_1")
    scan "testing_space"
subquery $0:
scan
                        motion [policy: full, program: ReshardIfNeeded]
                            values
                                value row (data=ROW(1::int))
subquery $1:
scan
        motion [policy: full, program: ReshardIfNeeded]
            values
                value row (data=ROW(ROW($0)))
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test-explain-plan-subquery-as-expression-under-selection
-- SQL:
EXPLAIN SELECT "id" FROM "testing_space" WHERE (VALUES (true));
-- EXPECTED:
projection ("testing_space"."id"::int -> "id")
    selection ROW($0)
        scan "testing_space"
subquery $0:
scan
            motion [policy: full, program: ReshardIfNeeded]
                values
                    value row (data=ROW(true::bool))
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: test-explain-plan-subquery-as-expression-under-projection-several
-- SQL:
EXPLAIN SELECT (values (1)), (values (2)) from testing_space;
-- EXPECTED:
projection (ROW($1) -> "col_1", ROW($0) -> "col_2")
    scan "testing_space"
subquery $0:
scan
        motion [policy: full, program: ReshardIfNeeded]
            values
                value row (data=ROW(2::int))
subquery $1:
scan
        motion [policy: full, program: ReshardIfNeeded]
            values
                value row (data=ROW(1::int))
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]