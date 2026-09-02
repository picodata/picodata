-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

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
-- UNORDERED:
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
-- UNORDERED:
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
SELECT "name", "id" FROM "testing_space" ORDER BY "name" || (VALUES ('a')), "id";
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
-- UNORDERED:
'1', '123'

-- TEST: test_under_join-1
-- SQL:
SELECT "id" FROM "testing_space" JOIN "null_t" ON
(SELECT true FROM "null_t" WHERE "na" = 1) AND "product_units" = "na" AND "name" != (VALUES ('123'));
-- UNORDERED:
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
-- UNORDERED:
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
-- UNORDERED:
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
-- UNORDERED:
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
-- UNORDERED:
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
-- UNORDERED:
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
-- UNORDERED:
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (ROW($0) -> col_1)
  scan testing_space
subquery $0:
  scan
    motion [policy: full, program: ReshardIfNeeded]
      values
        value ROW(1::int)
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: test-explain-plan-subquery-as-expression-under-order-by
-- SQL:
EXPLAIN SELECT "id" FROM "testing_space" ORDER BY "id" + (VALUES (1));
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (id::int)
  order by (id::int + ROW($0))
    motion [policy: full, program: ReshardIfNeeded]
      scan
        projection (testing_space.id::int -> id)
          scan testing_space
subquery $0:
  scan
    motion [policy: full, program: ReshardIfNeeded]
      values
        value ROW(1::int)
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: test-explain-plan-subquery-as-expression-under-projection-nested
-- SQL:
EXPLAIN SELECT (values ((values (1)))) from testing_space;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (ROW($1) -> col_1)
  scan testing_space
subquery $0:
  scan
    motion [policy: full, program: ReshardIfNeeded]
      values
        value ROW(1::int)
subquery $1:
  scan
    motion [policy: full, program: ReshardIfNeeded]
      values
        value ROW(ROW($0))
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: test-explain-plan-subquery-as-expression-under-group-by
-- SQL:
EXPLAIN SELECT (values ((values (1)))) from testing_space;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (ROW($1) -> col_1)
  scan testing_space
subquery $0:
  scan
    motion [policy: full, program: ReshardIfNeeded]
      values
        value ROW(1::int)
subquery $1:
  scan
    motion [policy: full, program: ReshardIfNeeded]
      values
        value ROW(ROW($0))
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: test-explain-plan-subquery-as-expression-under-selection
-- SQL:
EXPLAIN SELECT "id" FROM "testing_space" WHERE (VALUES (true));
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (testing_space.id::int -> id)
  selection (ROW($0))
    scan testing_space
subquery $0:
  scan
    motion [policy: full, program: ReshardIfNeeded]
      values
        value ROW(true::bool)
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: test-explain-plan-subquery-as-expression-under-projection-several
-- SQL:
EXPLAIN SELECT (values (1)), (values (2)) from testing_space;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (ROW($1) -> col_1, ROW($0) -> col_2)
  scan testing_space
subquery $0:
  scan
    motion [policy: full, program: ReshardIfNeeded]
      values
        value ROW(2::int)
subquery $1:
  scan
    motion [policy: full, program: ReshardIfNeeded]
      values
        value ROW(1::int)
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: test-distinct-with-subquery-single-column
-- SQL:
SELECT DISTINCT (VALUES (1)) FROM testing_space;
-- EXPECTED:
1

-- TEST: test-distinct-with-subquery-from-table
-- SQL:
SELECT DISTINCT (SELECT a FROM t1) FROM testing_space;
-- EXPECTED:
1

-- TEST: test-distinct-with-subquery-several
-- SQL:
SELECT DISTINCT (VALUES (1)), (VALUES (2)) FROM testing_space;
-- EXPECTED:
1, 2

-- TEST: test-distinct-with-subquery-and-column
-- SQL:
SELECT DISTINCT "product_units", (SELECT a FROM t1) FROM testing_space ORDER BY 1;
-- EXPECTED:
1, 1, 2, 1, 4, 1

-- TEST: test-distinct-with-subquery-and-string-column
-- SQL:
SELECT DISTINCT "name", (SELECT c FROM t1) FROM testing_space ORDER BY 1;
-- EXPECTED:
'1', 1, '123', 1, '2', 1

-- TEST: test-distinct-with-subquery-inside-arithmetic
-- SQL:
SELECT DISTINCT "product_units" + (SELECT a FROM t1) FROM testing_space ORDER BY 1;
-- EXPECTED:
2, 3, 5

-- TEST: test-distinct-with-subquery-inside-multiplication
-- SQL:
SELECT DISTINCT (SELECT b FROM t1) * "product_units" FROM testing_space ORDER BY 1;
-- EXPECTED:
1, 2, 4

-- TEST: test-distinct-with-nested-subquery
-- SQL:
SELECT DISTINCT (SELECT (VALUES (7)) FROM t1) FROM testing_space;
-- EXPECTED:
7

-- TEST: test-distinct-with-subquery-and-order-by
-- SQL:
SELECT DISTINCT (SELECT a FROM t1), "product_units" FROM testing_space ORDER BY 2 DESC;
-- EXPECTED:
1, 4, 1, 2, 1, 1

-- TEST: test-distinct-with-subquery-over-single-row-table
-- SQL:
SELECT DISTINCT (SELECT a FROM t1) FROM t1;
-- EXPECTED:
1

-- TEST: test-distinct-with-subquery-in-where
-- SQL:
SELECT DISTINCT "product_units" FROM testing_space WHERE "id" > (SELECT a FROM t1) ORDER BY 1;
-- EXPECTED:
1, 2, 4

-- TEST: test-distinct-aggregate-over-subquery
-- SQL:
SELECT count(DISTINCT (SELECT a FROM t1)) FROM testing_space;
-- EXPECTED:
1

-- TEST: test-distinct-with-subquery-and-group-by
-- SQL:
SELECT DISTINCT sum("product_units") + (SELECT a FROM t1) FROM testing_space GROUP BY "name" ORDER BY 1;
-- EXPECTED:
3, 4, 7

-- TEST: test-explain-plan-subquery-as-expression-under-distinct
-- SQL:
EXPLAIN SELECT DISTINCT (values (1)) from testing_space;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (gr_expr_1::int -> col_1)
  group by (gr_expr_1::int) output (gr_expr_1::int)
    motion [policy: full, program: ReshardIfNeeded]
      projection (ROW($0) -> gr_expr_1)
        group by (ROW($0)) output (testing_space.id::int -> id, testing_space.bucket_id::int -> bucket_id, testing_space.name::string -> name, testing_space.product_units::int -> product_units)
          scan testing_space
subquery $0:
  scan
    motion [policy: full, program: ReshardIfNeeded]
      values
        value ROW(1::int)
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: test-explain-plan-subquery-as-expression-under-distinct-with-column
-- SQL:
EXPLAIN SELECT DISTINCT "product_units", (values (1)) from testing_space;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (gr_expr_1::int -> product_units, gr_expr_2::int -> col_1)
  group by (gr_expr_1::int, gr_expr_2::int) output (gr_expr_1::int, gr_expr_2::int)
    motion [policy: full, program: ReshardIfNeeded]
      projection (testing_space.product_units::int -> gr_expr_1, ROW($0) -> gr_expr_2)
        group by (testing_space.product_units::int, ROW($0)) output (testing_space.id::int -> id, testing_space.bucket_id::int -> bucket_id, testing_space.name::string -> name, testing_space.product_units::int -> product_units)
          scan testing_space
subquery $0:
  scan
    motion [policy: full, program: ReshardIfNeeded]
      values
        value ROW(1::int)
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: test-explain-plan-subquery-as-expression-under-distinct-several
-- SQL:
EXPLAIN SELECT DISTINCT (values (1)), (values (2)) from testing_space;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (gr_expr_1::int -> col_1, gr_expr_2::int -> col_2)
  group by (gr_expr_1::int, gr_expr_2::int) output (gr_expr_1::int, gr_expr_2::int)
    motion [policy: full, program: ReshardIfNeeded]
      projection (ROW($1) -> gr_expr_1, ROW($0) -> gr_expr_2)
        group by (ROW($1), ROW($0)) output (testing_space.id::int -> id, testing_space.bucket_id::int -> bucket_id, testing_space.name::string -> name, testing_space.product_units::int -> product_units)
          scan testing_space
subquery $0:
  scan
    motion [policy: full, program: ReshardIfNeeded]
      values
        value ROW(2::int)
subquery $1:
  scan
    motion [policy: full, program: ReshardIfNeeded]
      values
        value ROW(1::int)
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: test-explain-plan-subquery-as-expression-under-distinct-from-table
-- SQL:
EXPLAIN SELECT DISTINCT (SELECT a FROM t1) from testing_space;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (gr_expr_1::int -> col_1)
  group by (gr_expr_1::int) output (gr_expr_1::int)
    motion [policy: full, program: ReshardIfNeeded]
      projection (ROW($0) -> gr_expr_1)
        group by (ROW($0)) output (testing_space.id::int -> id, testing_space.bucket_id::int -> bucket_id, testing_space.name::string -> name, testing_space.product_units::int -> product_units)
          scan testing_space
subquery $0:
  motion [policy: full, program: ReshardIfNeeded]
    scan
      projection (t1.a::int -> a)
        scan t1
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: test-explain-plan-subquery-as-expression-under-distinct-in-insert
-- SQL:
EXPLAIN INSERT INTO t1 VALUES ((SELECT DISTINCT (SELECT a FROM t1) FROM testing_space), 1, 1);
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
insert into t1 on conflict: fail
  motion [policy: segment([ref("COLUMN_1")]), program: ReshardIfNeeded]
    values
      value ROW(ROW($1), 1::int, 1::int)
subquery $0:
  motion [policy: full, program: ReshardIfNeeded]
    scan
      projection (t1.a::int -> a)
        scan t1
subquery $1:
  motion [policy: full, program: ReshardIfNeeded]
    scan
      projection (gr_expr_1::int -> col_1)
        group by (gr_expr_1::int) output (gr_expr_1::int)
          motion [policy: full, program: ReshardIfNeeded]
            projection (ROW($0) -> gr_expr_1)
              group by (ROW($0)) output (testing_space.id::int -> id, testing_space.bucket_id::int -> bucket_id, testing_space.name::string -> name, testing_space.product_units::int -> product_units)
                scan testing_space
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: test-scalar-subquery-under-group-by
-- SQL:
SELECT "id" + (SELECT 1) FROM testing_space GROUP BY "id" + (SELECT 1);
-- UNORDERED:
2,
3,
4,
5,
6,
7

-- TEST: test-scalar-subquery-under-group-by-different-subquery
-- SQL:
SELECT "id" + (SELECT 1) FROM testing_space GROUP BY "id" + (SELECT 2);
-- ERROR:
invalid query: column "id" is not found in grouping expressions!

-- TEST: test-scalar-subquery-under-group-by-commuted
-- SQL:
SELECT "id" + (SELECT 1) FROM testing_space GROUP BY (SELECT 1) + "id";
-- ERROR:
invalid query: column "id" is not found in grouping expressions!

-- TEST: test-scalar-subquery-under-group-by-as-subexpression
-- SQL:
SELECT "id" + (SELECT 2) + (SELECT 1) FROM testing_space GROUP BY "id" + (SELECT 2);
-- UNORDERED:
4,
5,
6,
7,
8,
9

-- TEST: test-scalar-subquery-under-group-by-wrong-operand-order
-- SQL:
SELECT "id" + (SELECT 1) + (SELECT 2) FROM testing_space GROUP BY "id" + (SELECT 2);
-- ERROR:
invalid query: column "id" is not found in grouping expressions!

-- TEST: test-scalar-subquery-under-group-by-duplicated
-- SQL:
SELECT "id" + (SELECT 1) FROM testing_space GROUP BY "id" + (SELECT 1), "id" + (SELECT 1);
-- UNORDERED:
2,
3,
4,
5,
6,
7

-- TEST: test-scalar-subquery-under-group-by-from-table
-- SQL:
SELECT "product_units" + (SELECT a FROM t1) FROM testing_space GROUP BY "product_units" + (SELECT a FROM t1);
-- UNORDERED:
2,
3,
5

-- TEST: test-scalar-subquery-under-group-by-with-having
-- SQL:
SELECT (SELECT 1) FROM testing_space GROUP BY (SELECT 1) HAVING (SELECT 1) > 0;
-- EXPECTED:
1

-- TEST: test-scalar-subquery-under-group-by-boolean
-- SQL:
SELECT sum("id")::int FROM testing_space GROUP BY (SELECT true) HAVING (SELECT true);
-- EXPECTED:
21

-- TEST: test-explain-plan-scalar-subquery-under-group-by
-- SQL:
EXPLAIN SELECT "id" + (SELECT 1) FROM testing_space GROUP BY "id" + (SELECT 1);
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (gr_expr_1::int -> col_1)
  group by (gr_expr_1::int) output (gr_expr_1::int)
    motion [policy: full, program: ReshardIfNeeded]
      projection (testing_space.id::int + ROW($0) -> gr_expr_1)
        group by (testing_space.id::int + ROW($0)) output (testing_space.id::int -> id, testing_space.bucket_id::int -> bucket_id, testing_space.name::string -> name, testing_space.product_units::int -> product_units)
          scan testing_space
subquery $0:
  scan
    projection (1::int -> col_1)
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: test-scalar-subquery-under-group-by-join-setup
-- SQL:
DROP TABLE IF EXISTS gt;
CREATE TABLE gt (a int primary key, b int);
INSERT INTO gt VALUES (1, 1), (2, 1), (3, 5);

-- TEST: test-scalar-subquery-under-group-by-with-join
-- SQL:
SELECT "id" + (SELECT sum(x.a)::int FROM gt AS x JOIN gt AS y ON x.a = y.b) FROM testing_space GROUP BY "id" + (SELECT sum(x.a)::int FROM gt AS x JOIN gt AS y ON x.a = y.b);
-- UNORDERED:
3,
4,
5,
6,
7,
8

-- TEST: test-scalar-subquery-under-group-by-with-join-swapped-sides
-- SQL:
SELECT "id" + (SELECT sum(x.a)::int FROM gt AS x JOIN gt AS y ON x.a = y.b) FROM testing_space GROUP BY "id" + (SELECT sum(x.a)::int FROM gt AS x JOIN gt AS y ON y.a = x.b);
-- ERROR:
invalid query: column "id" is not found in grouping expressions!

-- TEST: test-scalar-subquery-under-group-by-nested
-- SQL:
SELECT "id" + (SELECT (SELECT 1)) FROM testing_space GROUP BY "id" + (SELECT (SELECT 1));
-- UNORDERED:
2,
3,
4,
5,
6,
7

-- TEST: test-scalar-subquery-under-group-by-nested-different
-- SQL:
SELECT "id" + (SELECT (SELECT 1)) FROM testing_space GROUP BY "id" + (SELECT (SELECT 2));
-- ERROR:
invalid query: column "id" is not found in grouping expressions!

-- TEST: test-scalar-subquery-under-group-by-two-distinct
-- SQL:
SELECT "id" + (SELECT 1), (SELECT 2) FROM testing_space GROUP BY "id" + (SELECT 1), (SELECT 2);
-- UNORDERED:
2, 2,
3, 2,
4, 2,
5, 2,
6, 2,
7, 2

-- TEST: test-scalar-subquery-inside-aggregate-under-group-by
-- SQL:
SELECT "name", sum("id" + (SELECT 1))::int FROM testing_space GROUP BY "name";
-- UNORDERED:
'1', 7,
'123', 8,
'2', 12

-- TEST: test-scalar-subquery-under-group-by-single-bucket
-- SQL:
SELECT "id" + (SELECT 1) FROM testing_space WHERE "id" = 1 GROUP BY "id" + (SELECT 1);
-- EXPECTED:
2

-- TEST: test-explain-raw-scalar-subquery-under-group-by
-- SQL:
explain (raw, buckets) SELECT "id" + (SELECT 1) FROM testing_space GROUP BY "id" + (SELECT 1);
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "testing_space"."id" + ( SELECT CAST(1 AS int) as "col_1") as "gr_expr_1" FROM "testing_space" GROUP BY "testing_space"."id" + ( SELECT CAST(1 AS int) as "col_1")
''
plan:
    [0] SCAN TABLE testing_space (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
    [0] EXECUTE SCALAR SUBQUERY 1
    [0] EXECUTE SCALAR SUBQUERY 2
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT "COL_0" as "col_1" FROM ( SELECT "COL_0" FROM "_tmp_11989410818975029079_0136" ) GROUP BY "COL_0"
''
plan:
    [0] SCAN TABLE _tmp_11989410818975029079_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: test-explain-raw-scalar-subquery-under-group-by-with-having
-- SQL:
explain (raw, buckets) SELECT "id" + (SELECT 1) FROM testing_space GROUP BY "id" + (SELECT 1) HAVING "id" + (SELECT 1) > 3;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "testing_space"."id" + ( SELECT CAST(1 AS int) as "col_1") as "gr_expr_1" FROM "testing_space" GROUP BY "testing_space"."id" + ( SELECT CAST(1 AS int) as "col_1")
''
plan:
    [0] SCAN TABLE testing_space (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
    [0] EXECUTE SCALAR SUBQUERY 1
    [0] EXECUTE SCALAR SUBQUERY 2
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT "COL_0" as "col_1" FROM ( SELECT "COL_0" FROM "_tmp_4864343951701269942_0136" ) GROUP BY "COL_0" HAVING "COL_0" > CAST(3 AS int)
''
plan:
    [0] SCAN TABLE _tmp_4864343951701269942_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: test-explain-raw-scalar-subquery-under-group-by-from-table
-- SQL:
explain (raw, buckets) SELECT "product_units" + (SELECT a FROM t1) FROM testing_space GROUP BY "product_units" + (SELECT a FROM t1);
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "t1"."a" FROM "t1"
''
plan:
    [0] SCAN TABLE t1 (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 2. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "testing_space"."product_units" + ( SELECT "COL_0" FROM "_tmp_12652626258037743691_0136" ) as "gr_expr_1" FROM "testing_space" GROUP BY "testing_space"."product_units" + ( SELECT "COL_0" FROM "_tmp_12652626258037743691_0136" )
''
plan:
    [0] SCAN TABLE testing_space (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
    [0] EXECUTE SCALAR SUBQUERY 1
    [1] SCAN TABLE _tmp_12652626258037743691_0136 (~1048576 rows)
    [0] EXECUTE SCALAR SUBQUERY 2
    [2] SCAN TABLE _tmp_12652626258037743691_0136 (~1048576 rows)
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 3. Query (ROUTER) │
╰───────────────────╯
''
SELECT "COL_0" as "col_1" FROM ( SELECT "COL_0" FROM "_tmp_16926401014272153723_2136" ) GROUP BY "COL_0"
''
plan:
    [0] SCAN TABLE _tmp_16926401014272153723_2136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: test-explain-raw-scalar-subquery-under-group-by-with-join
-- SQL:
explain (raw, buckets) SELECT "id" + (SELECT sum(x.a)::int FROM gt AS x JOIN gt AS y ON x.a = y.b) FROM testing_space GROUP BY "id" + (SELECT sum(x.a)::int FROM gt AS x JOIN gt AS y ON x.a = y.b);
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "y"."a", "y"."bucket_id", "y"."b" FROM "gt" as "y"
''
plan:
    [0] SCAN TABLE gt AS y (~1048576 rows)
''
buckets <= [1-3000]
''
╭─────────────────────────────────╮
│ 2. Query (DYN-FILTERED STORAGE) │
╰─────────────────────────────────╯
''
SELECT sum (CAST ("x"."a" as int)) as "sum_1" FROM "gt" as "x" INNER JOIN ( SELECT "COL_0", "COL_1", "COL_2" FROM "_tmp_17642031497329535758_0136" ) as "y" ON "x"."a" = "y"."COL_2"
''
plan:
    [0] SCAN TABLE _tmp_17642031497329535758_0136 (~1048576 rows)
        [0] SEARCH TABLE gt AS x USING PRIMARY KEY (a=?) (~1 row)
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 3. Query (ROUTER) │
╰───────────────────╯
''
SELECT CAST (sum ("COL_0") as int) as "col_1" FROM ( SELECT "COL_0" FROM "_tmp_6980489893833825880_1136" )
''
plan:
    [0] SCAN TABLE _tmp_6980489893833825880_1136 (~1048576 rows)
''
buckets = any
''
╭──────────────────────────╮
│ 4. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "testing_space"."id" + ( SELECT "COL_0" FROM "_tmp_4263012133951791919_2136" ) as "gr_expr_1" FROM "testing_space" GROUP BY "testing_space"."id" + ( SELECT "COL_0" FROM "_tmp_4263012133951791919_2136" )
''
plan:
    [0] SCAN TABLE testing_space (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
    [0] EXECUTE SCALAR SUBQUERY 1
    [1] SCAN TABLE _tmp_4263012133951791919_2136 (~1048576 rows)
    [0] EXECUTE SCALAR SUBQUERY 2
    [2] SCAN TABLE _tmp_4263012133951791919_2136 (~1048576 rows)
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 5. Query (ROUTER) │
╰───────────────────╯
''
SELECT "COL_0" as "col_1" FROM ( SELECT "COL_0" FROM "_tmp_2816073480909527358_6136" ) GROUP BY "COL_0"
''
plan:
    [0] SCAN TABLE _tmp_2816073480909527358_6136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: test-explain-raw-scalar-subquery-under-group-by-two-distinct
-- SQL:
explain (raw, buckets) SELECT "id" + (SELECT 1), (SELECT 2) FROM testing_space GROUP BY "id" + (SELECT 1), (SELECT 2);
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "testing_space"."id" + ( SELECT CAST(1 AS int) as "col_1") as "gr_expr_1", ( SELECT CAST(2 AS int) as "col_1") as "gr_expr_2" FROM "testing_space" GROUP BY "testing_space"."id" + ( SELECT CAST(1 AS int) as "col_1"), ( SELECT CAST(2 AS int) as "col_1")
''
plan:
    [0] SCAN TABLE testing_space (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
    [0] EXECUTE SCALAR SUBQUERY 1
    [0] EXECUTE SCALAR SUBQUERY 2
    [0] EXECUTE SCALAR SUBQUERY 3
    [0] EXECUTE SCALAR SUBQUERY 4
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT "COL_0" as "col_1", "COL_1" as "col_2" FROM ( SELECT "COL_0", "COL_1" FROM "_tmp_18169589035776508402_0136" ) GROUP BY "COL_0", "COL_1"
''
plan:
    [0] SCAN TABLE _tmp_18169589035776508402_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: test-explain-raw-scalar-subquery-under-group-by-nested
-- SQL:
explain (raw, buckets) SELECT "id" + (SELECT (SELECT 1)) FROM testing_space GROUP BY "id" + (SELECT (SELECT 1));
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "testing_space"."id" + ( SELECT ( SELECT CAST(1 AS int) as "col_1") as "col_1" ) as "gr_expr_1" FROM "testing_space" GROUP BY "testing_space"."id" + ( SELECT ( SELECT CAST(1 AS int) as "col_1") as "col_1" )
''
plan:
    [0] SCAN TABLE testing_space (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
    [0] EXECUTE SCALAR SUBQUERY 1
    [1] EXECUTE SCALAR SUBQUERY 2
    [0] EXECUTE SCALAR SUBQUERY 3
    [3] EXECUTE SCALAR SUBQUERY 4
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT "COL_0" as "col_1" FROM ( SELECT "COL_0" FROM "_tmp_11989410818975029079_0136" ) GROUP BY "COL_0"
''
plan:
    [0] SCAN TABLE _tmp_11989410818975029079_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: test-explain-raw-scalar-subquery-under-group-by-single-bucket
-- SKIP_FOR: 2rsX1
-- SQL:
explain (raw, buckets) SELECT "id" + (SELECT 1) FROM testing_space WHERE "id" = 1 GROUP BY "id" + (SELECT 1);
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT "testing_space"."id" + ( SELECT CAST(1 AS int) as "col_1") as "col_1" FROM "testing_space" WHERE "testing_space"."id" = CAST(1 AS int) GROUP BY "testing_space"."id" + ( SELECT CAST(1 AS int) as "col_1")
''
plan:
    [0] SEARCH TABLE testing_space USING PRIMARY KEY (id=?) (~1 row)
    [0] EXECUTE SCALAR SUBQUERY 1
    [0] EXECUTE SCALAR SUBQUERY 2
''
buckets = [1934]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: test-explain-raw-scalar-subquery-inside-aggregate
-- SQL:
explain (raw, buckets) SELECT "name", sum("id" + (SELECT 1))::int FROM testing_space GROUP BY "name";
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "testing_space"."name" as "gr_expr_1", sum ( CAST ( ( "testing_space"."id" + ( SELECT CAST(1 AS int) as "col_1") ) as int ) ) as "sum_1" FROM "testing_space" GROUP BY "testing_space"."name"
''
plan:
    [0] SCAN TABLE testing_space (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
    [0] EXECUTE SCALAR SUBQUERY 1
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT "COL_0" as "name", CAST (sum ("COL_1") as int) as "col_1" FROM ( SELECT "COL_0", "COL_1" FROM "_tmp_5039169186347684519_0136" ) GROUP BY "COL_0"
''
plan:
    [0] SCAN TABLE _tmp_5039169186347684519_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: test-groupby-sq-matching-setup
-- SQL:
DROP TABLE IF EXISTS qt;
DROP TABLE IF EXISTS qg;
CREATE TABLE qt (a int, b int, c int, d int, PRIMARY KEY (d)) DISTRIBUTED BY (a, b);
CREATE TABLE qg (g int PRIMARY KEY) DISTRIBUTED GLOBALLY;
INSERT INTO qt VALUES (1, 2, 2, 1), (2, 2, 2, 2), (3, 2, 2, 3);
INSERT INTO qg VALUES (1), (10);

-- TEST: test-groupby-sq-differs-in-distinct
-- SQL:
SELECT a + (SELECT b FROM qt) FROM qt GROUP BY a + (SELECT DISTINCT b FROM qt);
-- ERROR:
invalid query: column "a" is not found in grouping expressions!

-- TEST: test-groupby-sq-differs-in-limit
-- SQL:
SELECT a + (SELECT b FROM qt LIMIT 2) FROM qt GROUP BY a + (SELECT b FROM qt LIMIT 1);
-- ERROR:
invalid query: column "a" is not found in grouping expressions!

-- TEST: test-groupby-sq-differs-in-order-by-entity
-- SQL:
SELECT a + (SELECT b FROM qt ORDER BY 1 LIMIT 1) FROM qt GROUP BY a + (SELECT b FROM qt ORDER BY b LIMIT 1);
-- ERROR:
invalid query: column "a" is not found in grouping expressions!

-- TEST: test-groupby-sq-differs-in-order-by-direction
-- SQL:
SELECT a + (SELECT b FROM qt ORDER BY b DESC LIMIT 1) FROM qt GROUP BY a + (SELECT b FROM qt ORDER BY b ASC LIMIT 1);
-- ERROR:
invalid query: column "a" is not found in grouping expressions!

-- TEST: test-groupby-sq-differs-in-set-operation
-- SQL:
SELECT a + (SELECT b FROM qt UNION SELECT c FROM qt LIMIT 1) FROM qt GROUP BY a + (SELECT b FROM qt UNION ALL SELECT c FROM qt LIMIT 1);
-- ERROR:
invalid query: column "a" is not found in grouping expressions!

-- TEST: test-groupby-sq-differs-in-except-operand
-- SQL:
SELECT a + (SELECT b FROM qt EXCEPT SELECT d FROM qt LIMIT 1) FROM qt GROUP BY a + (SELECT b FROM qt EXCEPT SELECT c FROM qt LIMIT 1);
-- ERROR:
invalid query: column "a" is not found in grouping expressions!

-- TEST: test-groupby-sq-differs-in-filter
-- SQL:
SELECT a + (SELECT b FROM qt WHERE a = 1 AND c = 1 LIMIT 1) FROM qt GROUP BY a + (SELECT b FROM qt WHERE a = 1 LIMIT 1);
-- ERROR:
invalid query: column "a" is not found in grouping expressions!

-- TEST: test-groupby-sq-differs-in-having
-- SQL:
SELECT a + (SELECT count(b) FROM qt GROUP BY b HAVING count(c) > 2 LIMIT 1) FROM qt GROUP BY a + (SELECT count(b) FROM qt GROUP BY b HAVING count(c) > 1 LIMIT 1);
-- ERROR:
invalid query: column "a" is not found in grouping expressions!

-- TEST: test-groupby-sq-differs-in-cast
-- SQL:
SELECT a + (SELECT b::double FROM qt LIMIT 1) FROM qt GROUP BY a + (SELECT b::int FROM qt LIMIT 1);
-- ERROR:
invalid query: column "a" is not found in grouping expressions!

-- TEST: test-groupby-sq-differs-in-values
-- SQL:
SELECT a + (VALUES (2)) FROM qt GROUP BY a + (VALUES (1));
-- ERROR:
invalid query: column "a" is not found in grouping expressions!

-- TEST: test-groupby-sq-differs-in-nested-subquery
-- SQL:
SELECT a + (SELECT count(*) FROM qt WHERE a IN (SELECT c FROM qt)) FROM qt GROUP BY a + (SELECT count(*) FROM qt WHERE a IN (SELECT b FROM qt));
-- ERROR:
invalid query: column "a" is not found in grouping expressions!

-- TEST: test-groupby-sq-differs-in-join-kind
-- SQL:
SELECT a + (SELECT count(x.c) FROM qt AS x JOIN qt AS y ON x.a = y.b) FROM qt GROUP BY a + (SELECT count(x.c) FROM qt AS x LEFT JOIN qt AS y ON x.a = y.b);
-- ERROR:
invalid query: column "a" is not found in grouping expressions!

-- TEST: test-groupby-sq-differs-in-join-condition-sides
-- SQL:
SELECT a + (SELECT count(x.c) FROM qt AS x JOIN qt AS y ON y.a = x.b) FROM qt GROUP BY a + (SELECT count(x.c) FROM qt AS x JOIN qt AS y ON x.a = y.b);
-- ERROR:
invalid query: column "a" is not found in grouping expressions!

-- TEST: test-groupby-sq-differs-in-join-output-sides
-- SQL:
SELECT a + (SELECT x.c FROM qt AS x JOIN qt AS y ON x.a = y.b LIMIT 1) FROM qt GROUP BY a + (SELECT y.c FROM qt AS x JOIN qt AS y ON x.a = y.b LIMIT 1);
-- ERROR:
invalid query: column "a" is not found in grouping expressions!

-- TEST: test-groupby-sq-differs-in-selection-sides
-- SQL:
SELECT a + (SELECT count(x.c) FROM qt AS x JOIN qt AS y ON true WHERE y.a = x.b) FROM qt GROUP BY a + (SELECT count(x.c) FROM qt AS x JOIN qt AS y ON true WHERE x.a = y.b);
-- ERROR:
invalid query: column "a" is not found in grouping expressions!

-- TEST: test-groupby-sq-differs-in-inner-join-of-3-way
-- SQL:
SELECT a + (SELECT count(z.c) FROM qt AS x JOIN qt AS y ON y.a = x.b JOIN qt AS z ON y.a = z.b) FROM qt GROUP BY a + (SELECT count(z.c) FROM qt AS x JOIN qt AS y ON x.a = y.b JOIN qt AS z ON y.a = z.b);
-- ERROR:
invalid query: column "a" is not found in grouping expressions!

-- TEST: test-groupby-sq-differs-in-outer-join-of-3-way
-- SQL:
SELECT a + (SELECT count(z.c) FROM qt AS x JOIN qt AS y ON x.a = y.b JOIN qt AS z ON z.a = y.b) FROM qt GROUP BY a + (SELECT count(z.c) FROM qt AS x JOIN qt AS y ON x.a = y.b JOIN qt AS z ON y.a = z.b);
-- ERROR:
invalid query: column "a" is not found in grouping expressions!

-- TEST: test-groupby-sq-differs-in-join-condition-subquery
-- SQL:
SELECT a + (SELECT count(x.c) FROM qt AS x JOIN qt AS y ON x.a IN (SELECT c FROM qt)) FROM qt GROUP BY a + (SELECT count(x.c) FROM qt AS x JOIN qt AS y ON x.a IN (SELECT b FROM qt));
-- ERROR:
invalid query: column "a" is not found in grouping expressions!

-- TEST: test-groupby-sq-equal-distinct
-- SQL:
SELECT a + (SELECT DISTINCT b FROM qt) FROM qt GROUP BY a + (SELECT DISTINCT b FROM qt);
-- UNORDERED:
3,
4,
5

-- TEST: test-groupby-sq-equal-order-by
-- SQL:
SELECT a + (SELECT b FROM qt ORDER BY b LIMIT 1) FROM qt GROUP BY a + (SELECT b FROM qt ORDER BY b LIMIT 1);
-- EXPECTED:
3,
4,
5

-- TEST: test-groupby-sq-equal-union-all
-- SQL:
SELECT a + (SELECT b FROM qt UNION ALL SELECT c FROM qt LIMIT 1) FROM qt GROUP BY a + (SELECT b FROM qt UNION ALL SELECT c FROM qt LIMIT 1);
-- UNORDERED:
3,
4,
5

-- TEST: test-groupby-sq-equal-except
-- SQL:
SELECT a + (SELECT b FROM qt EXCEPT SELECT c FROM qt LIMIT 1) FROM qt GROUP BY a + (SELECT b FROM qt EXCEPT SELECT c FROM qt LIMIT 1);
-- EXPECTED:
null

-- TEST: test-groupby-sq-equal-having
-- SQL:
SELECT a + (SELECT count(b) FROM qt GROUP BY b HAVING count(c) > 1 LIMIT 1) FROM qt GROUP BY a + (SELECT count(b) FROM qt GROUP BY b HAVING count(c) > 1 LIMIT 1);
-- UNORDERED:
4,
5,
6

-- TEST: test-groupby-sq-equal-cast
-- SQL:
SELECT a + (SELECT b::int FROM qt LIMIT 1) FROM qt GROUP BY a + (SELECT b::int FROM qt LIMIT 1);
-- UNORDERED:
3,
4,
5

-- TEST: test-groupby-sq-equal-values
-- SQL:
SELECT a + (VALUES (1)) FROM qt GROUP BY a + (VALUES (1));
-- UNORDERED:
2,
3,
4

-- TEST: test-groupby-sq-equal-nested-subquery
-- SQL:
SELECT a + (SELECT count(*) FROM qt WHERE a IN (SELECT b FROM qt)) FROM qt GROUP BY a + (SELECT count(*) FROM qt WHERE a IN (SELECT b FROM qt));
-- UNORDERED:
2,
3,
4

-- TEST: test-groupby-sq-equal-join
-- SQL:
SELECT a + (SELECT count(x.c) FROM qt AS x JOIN qt AS y ON x.a = y.b) FROM qt GROUP BY a + (SELECT count(x.c) FROM qt AS x JOIN qt AS y ON x.a = y.b);
-- UNORDERED:
4,
5,
6

-- TEST: test-groupby-sq-equal-3-way-join
-- SQL:
SELECT a + (SELECT count(z.c) FROM qt AS x JOIN qt AS y ON x.a = y.b JOIN qt AS z ON y.a = z.b) FROM qt GROUP BY a + (SELECT count(z.c) FROM qt AS x JOIN qt AS y ON x.a = y.b JOIN qt AS z ON y.a = z.b);
-- UNORDERED:
4,
5,
6

-- TEST: test-groupby-sq-equal-join-condition-subquery
-- SQL:
SELECT a + (SELECT count(x.c) FROM qt AS x JOIN qt AS y ON x.a IN (SELECT b FROM qt)) FROM qt GROUP BY a + (SELECT count(x.c) FROM qt AS x JOIN qt AS y ON x.a IN (SELECT b FROM qt));
-- UNORDERED:
4,
5,
6

-- TEST: test-groupby-sq-equal-selection-under-join
-- SQL:
SELECT a + (SELECT count(x.c) FROM qt AS x JOIN qt AS y ON true WHERE x.a = y.b) FROM qt GROUP BY a + (SELECT count(x.c) FROM qt AS x JOIN qt AS y ON true WHERE x.a = y.b);
-- UNORDERED:
4,
5,
6

-- TEST: test-groupby-sq-differs-in-window-partition
-- SQL:
SELECT a + (SELECT count(*) OVER (PARTITION BY b) FROM qt LIMIT 1) FROM qt GROUP BY a + (SELECT count(*) OVER (PARTITION BY c) FROM qt LIMIT 1);
-- ERROR:
invalid query: column "a" is not found in grouping expressions!

-- TEST: test-groupby-sq-differs-in-window-function
-- SQL:
SELECT a + (SELECT count(*) OVER () FROM qt LIMIT 1) FROM qt GROUP BY a + (SELECT sum(b) OVER () FROM qt LIMIT 1);
-- ERROR:
invalid query: column "a" is not found in grouping expressions!

-- TEST: test-groupby-differs-in-function-arity
-- SQL:
SELECT substr(cast(d as text), 1) FROM qt GROUP BY substr(cast(d as text), 1, 2);
-- ERROR:
invalid query: column "d" is not found in grouping expressions!

-- TEST: test-groupby-differs-in-case-arm-count
-- SQL:
SELECT case when d = 1 then 10 end FROM qt GROUP BY case when d = 1 then 10 when d = 2 then 20 end;
-- ERROR:
invalid query: column "d" is not found in grouping expressions!

-- TEST: test-groupby-sq-equal-window
-- SQL:
SELECT a + (SELECT count(*) OVER () FROM qt LIMIT 1) FROM qt GROUP BY a + (SELECT count(*) OVER () FROM qt LIMIT 1);
-- UNORDERED:
4,
5,
6

-- TEST: test-groupby-sq-equal-window-partition
-- SQL:
SELECT a + (SELECT count(*) OVER (PARTITION BY b) FROM qt LIMIT 1) FROM qt GROUP BY a + (SELECT count(*) OVER (PARTITION BY b) FROM qt LIMIT 1);
-- UNORDERED:
4,
5,
6

-- TEST: test-groupby-equal-function-arity
-- SQL:
SELECT substr(cast(d as text), 1, 2) FROM qt GROUP BY substr(cast(d as text), 1, 2);
-- UNORDERED:
'1',
'2',
'3'

-- TEST: test-groupby-equal-case-arm-count
-- SQL:
SELECT case when d = 1 then 10 when d = 2 then 20 end FROM qt GROUP BY case when d = 1 then 10 when d = 2 then 20 end;
-- UNORDERED:
null,
10,
20,

-- TEST: test-groupby-sq-equal-unnamed-derived-table
-- SQL:
SELECT a + (SELECT b FROM (SELECT b FROM qt LIMIT 1)) FROM qt GROUP BY a + (SELECT b FROM (SELECT b FROM qt LIMIT 1));
-- UNORDERED:
3,
4,
5

-- TEST: test-groupby-sq-equal-differently-aliased-derived-table
-- SQL:
SELECT a + (SELECT b FROM (SELECT b FROM qt LIMIT 1) AS q1) FROM qt GROUP BY a + (SELECT b FROM (SELECT b FROM qt LIMIT 1) AS q2);
-- UNORDERED:
3,
4,
5

-- TEST: test-groupby-sq-equal-differently-aliased-scan
-- SQL:
SELECT a + (SELECT count(x.c) FROM qt AS x) FROM qt GROUP BY a + (SELECT count(y.c) FROM qt AS y);
-- UNORDERED:
4,
5,
6

-- TEST: test-groupby-sq-equal-join-over-unnamed-derived-table
-- SQL:
SELECT a + (SELECT count(x.c) FROM qt AS x JOIN (SELECT b AS bb FROM qt) ON x.a = bb) FROM qt GROUP BY a + (SELECT count(x.c) FROM qt AS x JOIN (SELECT b AS bb FROM qt) ON x.a = bb);
-- UNORDERED:
4,
5,
6

-- TEST: test-groupby-sq-differs-in-derived-table-limit
-- SQL:
SELECT a + (SELECT b FROM (SELECT b FROM qt LIMIT 1)) FROM qt GROUP BY a + (SELECT b FROM (SELECT b FROM qt LIMIT 2));
-- ERROR:
invalid query: column "a" is not found in grouping expressions!

-- TEST: test-groupby-sq-equal-left-join-over-global-table
-- SQL:
SELECT a + (SELECT sum(c) FROM qg LEFT JOIN qt ON g = a) FROM qt GROUP BY a + (SELECT sum(c) FROM qg LEFT JOIN qt ON g = a);
-- UNORDERED:
3,
4,
5

-- TEST: test-groupby-sq-differs-in-left-join-condition
-- SQL:
SELECT a + (SELECT sum(c) FROM qg LEFT JOIN qt ON g = a) FROM qt GROUP BY a + (SELECT sum(c) FROM qg LEFT JOIN qt ON g = b);
-- ERROR:
invalid query: column "a" is not found in grouping expressions!

-- TEST: test-not-over-subquery-without-tables
-- SQL:
SELECT * FROM (VALUES (1)) WHERE NOT (SELECT false);
-- EXPECTED:
1

-- TEST: test-not-over-subquery
-- SQL:
SELECT a FROM t1 WHERE NOT (SELECT b > 100 FROM t1);
-- EXPECTED:
1

-- TEST: test-not-over-subquery-false
-- SQL:
SELECT a FROM t1 WHERE NOT (SELECT b = 1 FROM t1);
-- EXPECTED:

-- TEST: test-not-over-subquery-double
-- SQL:
SELECT a FROM t1 WHERE NOT NOT (SELECT b = 1 FROM t1);
-- EXPECTED:
1

-- TEST: test-not-over-subquery-in-join-condition
-- SQL:
SELECT t1.a FROM t1 INNER JOIN null_t ON NOT (SELECT b > 100 FROM t1) WHERE "na" = 1;
-- EXPECTED:
1
