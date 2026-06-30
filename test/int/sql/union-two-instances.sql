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
CREATE TABLE "g" ("a" int primary key, "b" int) DISTRIBUTED GLOBALLY;
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
SELECT * FROM t ORDER BY 1;
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
SELECT * FROM t ORDER BY 1;
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
select null from "testing_space" where false order by 1;
-- EXPECTED:
'1', '1', '123', '123', '2', '2'

-- TEST: test_union_removes_duplicates-2
-- SQL:
select "name"
from "testing_space"
union
select null from "testing_space" where false order by 1;
-- EXPECTED:
'1', '123', '2'

-- TEST: test_union_seg_vs_single
-- SQL:
select "a"
from "arithmetic_space"
union
select sum("a") / 3 from "arithmetic_space" order by 1;
-- EXPECTED:
1, 2

-- TEST: test_union_seg_vs_any
-- SQL:
select "a", "b"
from "arithmetic_space"
union
select "a" + 1 - 1, "b" from "arithmetic_space" order by 1;
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
select "product_units" from "testing_space" order by 1;
-- EXPECTED:
1, 2, 4

-- TEST: test_union_diff_types
-- SQL:
select "a"
from "arithmetic_space"
union
select 'kek' || "name" from "testing_space";
-- ERROR:
Unable to unify inconsistent types: Integer and String

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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
motion [policy: full, program: RemoveDuplicates]
  union
    projection (row_number() over () -> col_1)
      motion [policy: full, program: ReshardIfNeeded]
        projection (t.a::int -> a)
          scan t
    projection (1::int -> col_1)
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1-3000]

-- TEST: test_explain_union_with_named_window
-- SQL:
explain select count(*) over win from t WINDOW win as () union select 1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
motion [policy: full, program: RemoveDuplicates]
  union
    projection (count(*) over () -> col_1)
      motion [policy: full, program: ReshardIfNeeded]
        projection (t.a::int -> a)
          scan t
    projection (1::int -> col_1)
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
union all
  projection (row_number() over () -> col_1)
    motion [policy: full, program: ReshardIfNeeded]
      projection (t.a::int -> a)
        scan t
  projection (1::int -> col_1)
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1-3000]

-- TEST: explain-union-global-sharded-1
-- SQL:
EXPLAIN (RAW, BUCKETS)
SELECT * FROM t UNION ALL SELECT * FROM g;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────────────────────╮
│ 1.1. Query (CONST-FILTERED STORAGE, 1/2) │
╰──────────────────────────────────────────╯
''
SELECT "t"."a", "t"."b" FROM "t" UNION ALL SELECT * FROM "g"
''
plan:
    [1] SCAN TABLE t (~1048576 rows)
    [2] SCAN TABLE g (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets <= [1-3000]
''
╭──────────────────────────────────────────╮
│ 1.2. Query (CONST-FILTERED STORAGE, 1/2) │
╰──────────────────────────────────────────╯
''
SELECT "t"."a", "t"."b" FROM "t" UNION ALL select cast(null as int) as "a", cast(null as int) as "b" where false
''
plan:
    [1] SCAN TABLE t (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets <= [1-3000]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1-3000]

-- TEST: explain-union-global-sharded-2
-- SQL:
EXPLAIN (RAW, BUCKETS)
SELECT * FROM t WHERE a = 1 UNION ALL SELECT * FROM g;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/2) │
╰────────────────────────────────────────╯
''
SELECT "t"."a", "t"."b" FROM "t" WHERE "t"."a" = CAST(1 AS int) UNION ALL SELECT * FROM "g"
''
plan:
    [1] SEARCH TABLE t USING PRIMARY KEY (a=?) (~1 row)
    [2] SCAN TABLE g (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets = [1934]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: explain-union-global-sharded-3
-- SQL:
EXPLAIN (RAW, BUCKETS)
SELECT * FROM t WHERE a = 1 and a = 2 UNION ALL SELECT * FROM g;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭───────────────────╮
│ 1. Query (ROUTER) │
╰───────────────────╯
''
SELECT "t"."a", "t"."b" FROM "t" WHERE "t"."a" = CAST(1 AS int) and "t"."a" = CAST(2 AS int) UNION ALL SELECT * FROM "g"
''
plan:
    [1] SEARCH TABLE t USING PRIMARY KEY (a=?) (~1 row)
    [2] SCAN TABLE g (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets = []
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = []

-- TEST: explain-union-global-sharded-4
-- SQL:
EXPLAIN (RAW, BUCKETS)
SELECT * FROM t WHERE a in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10) UNION ALL SELECT * FROM g;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────────────────────╮
│ 1.1. Query (CONST-FILTERED STORAGE, 1/2) │
╰──────────────────────────────────────────╯
''
SELECT "t"."a", "t"."b" FROM "t" WHERE "t"."a" in ( CAST(1 AS int), CAST(2 AS int), CAST(3 AS int), CAST(4 AS int), CAST(5 AS int), CAST(6 AS int), CAST(7 AS int), CAST(8 AS int), CAST(9 AS int), CAST(10 AS int) ) UNION ALL SELECT * FROM "g"
''
plan:
    [1] SEARCH TABLE t USING PRIMARY KEY (a=?) (~10 rows)
    [1] EXECUTE LIST SUBQUERY 2
    [2] SCAN TABLE g (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets <= [1-3000]
''
╭──────────────────────────────────────────╮
│ 1.2. Query (CONST-FILTERED STORAGE, 1/2) │
╰──────────────────────────────────────────╯
''
SELECT "t"."a", "t"."b" FROM "t" WHERE "t"."a" in ( CAST(1 AS int), CAST(2 AS int), CAST(3 AS int), CAST(4 AS int), CAST(5 AS int), CAST(6 AS int), CAST(7 AS int), CAST(8 AS int), CAST(9 AS int), CAST(10 AS int) ) UNION ALL select cast(null as int) as "a", cast(null as int) as "b" where false
''
plan:
    [1] SEARCH TABLE t USING PRIMARY KEY (a=?) (~10 rows)
    [1] EXECUTE LIST SUBQUERY 2
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets <= [1-3000]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [219,626,799,1410,1860,1934,1958,2564,2752,2852]

-- TEST: explain-union-global-sharded-5
-- SQL:
EXPLAIN (RAW, BUCKETS)
SELECT * FROM t WHERE a IN (SELECT MIN(a) FROM t) UNION ALL SELECT * FROM g;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT min (CAST ("t"."a" as int)) as "min_1" FROM "t"
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
''
buckets = [1-3000]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT min ("COL_0") as "col_1" FROM ( SELECT "COL_0" FROM "_tmp_6332101395320054053_0136" )
''
plan:
    [0] SEARCH TABLE _tmp_6332101395320054053_0136 USING PRIMARY KEY (~1048576 rows)
''
buckets = any
''
╭───────────────────────────────────────────╮
│ 3.1. Query (DYN-FILTERED STORAGE, <= 1/2) │
╰───────────────────────────────────────────╯
''
SELECT "t"."a", "t"."b" FROM "t" WHERE "t"."a" in ( SELECT "COL_0" FROM "_tmp_14386139170834652946_1136" ) UNION ALL SELECT * FROM "g"
''
plan:
    [1] SEARCH TABLE t USING PRIMARY KEY (a=?) (~24 rows)
    [1] EXECUTE LIST SUBQUERY 2
    [2] SCAN TABLE _tmp_14386139170834652946_1136 (~1048576 rows)
    [3] SCAN TABLE g (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 3 (UNION ALL)
''
buckets <= [1-3000]
''
╭──────────────────────────────────────────╮
│ 3.2. Query (CONST-FILTERED STORAGE, 1/2) │
╰──────────────────────────────────────────╯
''
SELECT "t"."a", "t"."b" FROM "t" WHERE "t"."a" in ( SELECT "COL_0" FROM "_tmp_14386139170834652946_1136" ) UNION ALL select cast(null as int) as "a", cast(null as int) as "b" where false
''
plan:
    [1] SEARCH TABLE t USING PRIMARY KEY (a=?) (~24 rows)
    [1] EXECUTE LIST SUBQUERY 2
    [2] SCAN TABLE _tmp_14386139170834652946_1136 (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 3 (UNION ALL)
''
buckets <= [1-3000]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = unknown

-- TEST: explain-union-global-sharded-6
-- SQL:
EXPLAIN (RAW, BUCKETS)
SELECT * FROM t WHERE a = 5 and a IN (SELECT MIN(a) FROM t) UNION ALL SELECT * FROM g;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT min (CAST ("t"."a" as int)) as "min_1" FROM "t"
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
''
buckets = [1-3000]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT min ("COL_0") as "col_1" FROM ( SELECT "COL_0" FROM "_tmp_6332101395320054053_0136" )
''
plan:
    [0] SEARCH TABLE _tmp_6332101395320054053_0136 USING PRIMARY KEY (~1048576 rows)
''
buckets = any
''
╭─────────────────────────────────────────╮
│ 3. Query (DYN-FILTERED STORAGE, <= 1/2) │
╰─────────────────────────────────────────╯
''
SELECT "t"."a", "t"."b" FROM "t" WHERE "t"."a" = CAST(5 AS int) and "t"."a" in ( SELECT "COL_0" FROM "_tmp_3894500812078124028_1136" ) UNION ALL SELECT * FROM "g"
''
plan:
    [1] SEARCH TABLE t USING PRIMARY KEY (a=?) (~1 row)
    [1] EXECUTE LIST SUBQUERY 2
    [2] SCAN TABLE _tmp_3894500812078124028_1136 (~1048576 rows)
    [3] SCAN TABLE g (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 3 (UNION ALL)
''
buckets <= [219]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = unknown
