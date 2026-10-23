-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- TEST: explain-setup
-- SQL:
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS tt;
DROP TABLE IF EXISTS g;
DROP TABLE IF EXISTS b;
DROP TABLE IF EXISTS c;
DROP TABLE IF EXISTS testing_space;
CREATE TABLE testing_space ("id" int primary key, "name" string, "product_units" int);
CREATE TABLE t (a INT, b DOUBLE, c TEXT, PRIMARY KEY (c, a));
CREATE TABLE tt (d INT PRIMARY KEY);
CREATE TABLE b (id INT PRIMARY KEY, val INT);
CREATE TABLE c (id INT PRIMARY KEY, val INT);
CREATE TABLE g (a INT PRIMARY KEY, b DOUBLE, c TEXT) DISTRIBUTED GLOBALLY;

-- TEST: raw-buckets-select
-- SQL:
explain (raw, buckets) select * from _pico_table;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭───────────────────╮
│ 1. Query (ROUTER) │
╰───────────────────╯
''
SELECT * FROM "_pico_table"
''
plan:
    [0] SCAN TABLE _pico_table (~1048576 rows)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = any

-- TEST: raw-buckets-join-many
-- SQL:
explain (raw, buckets) select * from t join t as t2 on true group by 1, 2, 3, 4, 5, 6 order by 4 limit 5;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "t2"."a", "t2"."b", "t2"."c", "t2"."bucket_id" FROM "t" as "t2"
''
plan:
    [0] SCAN TABLE t AS t2 (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 2. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "gr_expr_1", "gr_expr_2", "gr_expr_3", "gr_expr_4", "gr_expr_5", "gr_expr_6" FROM ( SELECT "t"."a" as "gr_expr_1", "t"."b" as "gr_expr_2", "t"."c" as "gr_expr_3", "t2"."COL_0" as "gr_expr_4", "t2"."COL_1" as "gr_expr_5", "t2"."COL_2" as "gr_expr_6" FROM "t" INNER JOIN "_tmp_11008086563426014529_0136" as "t2" ON true::bool GROUP BY "t"."a", "t"."b", "t"."c", "t2"."COL_0", "t2"."COL_1", "t2"."COL_2" ) ORDER BY 4 LIMIT 5
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
        [0] SCAN TABLE _tmp_11008086563426014529_0136 AS t2 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 3. Query (ROUTER) │
╰───────────────────╯
''
SELECT "a", "b", "c", "a", "b", "c" FROM ( SELECT "COL_0" as "a", "COL_1" as "b", "COL_2" as "c", "COL_3" as "a", "COL_4" as "b", "COL_5" as "c" FROM "_tmp_2863660651043114778_1136" GROUP BY "COL_0", "COL_1", "COL_2", "COL_3", "COL_4", "COL_5" ) ORDER BY 4 LIMIT 5
''
plan:
    [0] SCAN TABLE _tmp_2863660651043114778_1136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: raw-buckets-union-many
-- SQL:
explain (raw, buckets, fmt) select * from t union select * from t group by 1, 2, 3 order by 3 limit 5;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "t"."a" as "gr_expr_1",
  "t"."b" as "gr_expr_2",
  "t"."c" as "gr_expr_3"
FROM
  "t"
GROUP BY
  "t"."a",
  "t"."b",
  "t"."c"
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "COL_0" as "a",
  "COL_1" as "b",
  "COL_2" as "c"
FROM
  "_tmp_5487442298234827123_0136"
GROUP BY
  "COL_0",
  "COL_1",
  "COL_2"
''
plan:
    [0] SCAN TABLE _tmp_5487442298234827123_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = any
''
╭─────────────────────────────────╮
│ 3. Query (DYN-FILTERED STORAGE) │
╰─────────────────────────────────╯
''
SELECT
  "a",
  "b",
  "c"
FROM
  (
    SELECT
      "t"."a",
      "t"."b",
      "t"."c"
    FROM
      "t"
    UNION
    SELECT
      "COL_0",
      "COL_1",
      "COL_2"
    FROM
      "_tmp_7081456438427139582_1136"
  )
ORDER BY
  3
LIMIT
  5
''
plan:
    [2] SCAN TABLE t (~1048576 rows)
    [3] SCAN TABLE _tmp_7081456438427139582_1136 (~1048576 rows)
    [1] COMPOUND SUBQUERIES 2 AND 3 USING TEMP B-TREE (UNION)
    [0] SCAN SUBQUERY 1 (~1 row)
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 4. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "COL_0" as "a",
  "COL_1" as "b",
  "COL_2" as "c"
FROM
  (
    (
      SELECT
        "COL_0",
        "COL_1",
        "COL_2"
      FROM
        "_tmp_16149730352562207223_2136"
    )
  )
ORDER BY
  3
LIMIT
  5
''
plan:
    [0] SCAN TABLE _tmp_16149730352562207223_2136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: buckets-raw-fmt-delete
-- SKIP_FOR: 2rsX1
-- SQL:
explain (buckets, raw, fmt) delete from t where a = 5 and c = 'lol';
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t"."c" as "pk_col_0",
  "t"."a" as "pk_col_1"
FROM
  "t"
WHERE
  "t"."a" = 5::int
  and "t"."c" = 'lol'::text
''
plan:
    [0] SEARCH TABLE t USING PRIMARY KEY (c=? AND a=?) (~1 row)
''
buckets = [442]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [442]

-- TEST: buckets-raw-fmt-update
-- SKIP_FOR: 2rsX1
-- SQL:
explain (buckets, raw, fmt) update t set b = b + 1 where a = 42 and c = 'kek';
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t"."b" + 1::int as "col_0",
  "t"."c" as "col_1",
  "t"."a" as "col_2"
FROM
  "t"
WHERE
  "t"."a" = 42::int
  and "t"."c" = 'kek'::text
''
plan:
    [0] SEARCH TABLE t USING PRIMARY KEY (c=? AND a=?) (~1 row)
''
buckets = [2873]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [2873]

-- TEST: logical-buckets-raw-fmt-update
-- SKIP_FOR: 2rsX1
-- SQL:
explain (buckets, logical, raw, fmt) update t set b = b + 1 where a = 42 and c = 'kek';
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
update t (b = col_0)
  motion [policy: local, program: ReshardIfNeeded]
    projection (
      t.b::double + 1::int -> col_0,
      t.c::text -> col_1,
      t.a::int -> col_2
    )
      selection (
        (t.a::int = 42::int and t.c::text = 'kek'::text)
      )
        scan t
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t"."b" + 1::int as "col_0",
  "t"."c" as "col_1",
  "t"."a" as "col_2"
FROM
  "t"
WHERE
  "t"."a" = 42::int
  and "t"."c" = 'kek'::text
''
plan:
    [0] SEARCH TABLE t USING PRIMARY KEY (c=? AND a=?) (~1 row)
''
buckets = [2873]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [2873]

-- TEST: logical-buckets-raw-fmt-delete
-- SKIP_FOR: 2rsX1
-- SQL:
explain (buckets, logical, raw, fmt) delete from t where a = 5 and c = 'lol';
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
delete from t
  motion [policy: local, program: [PrimaryKey(0, 1), ReshardIfNeeded]]
    projection (t.c::text -> pk_col_0, t.a::int -> pk_col_1)
      selection (
        (t.a::int = 5::int and t.c::text = 'lol'::text)
      )
        scan t
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t"."c" as "pk_col_0",
  "t"."a" as "pk_col_1"
FROM
  "t"
WHERE
  "t"."a" = 5::int
  and "t"."c" = 'lol'::text
''
plan:
    [0] SEARCH TABLE t USING PRIMARY KEY (c=? AND a=?) (~1 row)
''
buckets = [442]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [442]

-- TEST: logical-buckets-raw-fmt-insert
-- SQL:
explain (buckets, logical, raw, fmt) insert into t values (2, 2, '2');
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
insert into t on conflict: fail
  motion [policy: segment([ref("COLUMN_3"), ref("COLUMN_1")]), program: ReshardIfNeeded]
    values
      value ROW(2::int, 2::int, '2'::text)
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭───────────────────╮
│ 1. Query (ROUTER) │
╰───────────────────╯
''
VALUES (2::int, 2::int, '2'::text)
''
plan:
    [0] TRIVIAL
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [2356]

-- TEST: logical-buckets-raw-select
-- SQL:
explain (buckets, logical, raw) select a from t group by a order by a limit 1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 1
  projection (a::int)
    order by (a::int)
      scan
        projection (gr_expr_1::int -> a)
          group by (gr_expr_1::int)
            motion [policy: full, program: ReshardIfNeeded]
              limit 1
                projection (gr_expr_1::int)
                  order by (gr_expr_1::int)
                    scan
                      projection (t.a::int -> gr_expr_1)
                        group by (t.a::int)
                          scan t
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "gr_expr_1" FROM ( SELECT "t"."a" as "gr_expr_1" FROM "t" GROUP BY "t"."a" ) ORDER BY "gr_expr_1" LIMIT 1
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT "a" FROM ( SELECT "COL_0" as "a" FROM "_tmp_6374732083922181742_0136" GROUP BY "COL_0" ) ORDER BY "a" LIMIT 1
''
plan:
    [0] SCAN TABLE _tmp_6374732083922181742_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: logical-buckets-select
-- SQL:
explain (logical, buckets) select a from t group by a order by a limit 1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 1
  projection (a::int)
    order by (a::int)
      scan
        projection (gr_expr_1::int -> a)
          group by (gr_expr_1::int)
            motion [policy: full, program: ReshardIfNeeded]
              limit 1
                projection (gr_expr_1::int)
                  order by (gr_expr_1::int)
                    scan
                      projection (t.a::int -> gr_expr_1)
                        group by (t.a::int)
                          scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: logical-raw-insert
-- SQL:
explain (logical, raw) insert into t select d, d, d from tt order by 1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
insert into t on conflict: fail
  motion [policy: segment([ref(d), ref(d)]), program: ReshardIfNeeded]
    projection (d::int, d::int, d::int)
      order by (1)
        motion [policy: full, program: ReshardIfNeeded]
          scan
            projection (tt.d::int -> d, tt.d::int -> d, tt.d::int -> d)
              scan tt
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "tt"."d", "tt"."d", "tt"."d" FROM "tt"
''
plan:
    [0] SCAN TABLE tt (~1048576 rows)
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT "COL_0" as "d", "COL_1" as "d", "COL_2" as "d" FROM "_tmp_8151812527726332701_0136" ORDER BY 1
''
plan:
    [0] SCAN TABLE _tmp_8151812527726332701_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR ORDER BY

-- TEST: default-select
-- SQL:
explain select a from t group by a order by a limit 1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 1
  projection (a::int)
    order by (a::int)
      scan
        projection (gr_expr_1::int -> a)
          group by (gr_expr_1::int)
            motion [policy: full, program: ReshardIfNeeded]
              limit 1
                projection (gr_expr_1::int)
                  order by (gr_expr_1::int)
                    scan
                      projection (t.a::int -> gr_expr_1)
                        group by (t.a::int)
                          scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: raw-buckets-forward-logical-select
-- SQL:
explain (raw, buckets, logical, forward) select a from t union all select b from t group by 1 order by 1 limit 1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 1
  projection (a::double)
    order by (1)
      motion [policy: full, program: ReshardIfNeeded]
        limit 1
          projection (a::double)
            order by (1)
              scan
                union all
                  projection (t.a::int -> a)
                    scan t
                  motion [policy: segment([ref(b)]), program: ReshardIfNeeded]
                    projection (gr_expr_1::double -> b)
                      group by (gr_expr_1::double)
                        motion [policy: full, program: ReshardIfNeeded]
                          projection (t.b::double -> gr_expr_1)
                            group by (t.b::double)
                              scan t
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "t"."b" as "gr_expr_1" FROM "t" GROUP BY "t"."b"
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT "COL_0" as "b" FROM "_tmp_1820305626910533369_0136" GROUP BY "COL_0"
''
plan:
    [0] SCAN TABLE _tmp_1820305626910533369_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = any
''
╭─────────────────────────────────╮
│ 3. Query (DYN-FILTERED STORAGE) │
╰─────────────────────────────────╯
''
SELECT "a" FROM ( SELECT "t"."a" FROM "t" UNION ALL SELECT "COL_0" FROM "_tmp_7086731220766296322_1136" ) ORDER BY 1 LIMIT 1
''
plan:
    [1] SCAN TABLE t (~1048576 rows)
    [1] USE TEMP B-TREE FOR ORDER BY
    [2] SCAN TABLE _tmp_7086731220766296322_1136 (~1048576 rows)
    [2] USE TEMP B-TREE FOR ORDER BY
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 4. Query (ROUTER) │
╰───────────────────╯
''
SELECT "COL_0" as "a" FROM "_tmp_3281720111401187408_2136" ORDER BY 1 LIMIT 1
''
plan:
    [0] SCAN TABLE _tmp_3281720111401187408_2136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Forward                                                            
──────────────────────────────────────────────────────────────────────
''
forward analysis (on > ro_to_rw > off):
  forward = on
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: raw-buckets-forward-logical-fmt-select-join
-- SQL:
explain (raw, buckets, logical, forward, fmt) select a from t join (select b from t) tt on tt.b = t.a group by 1 order by 1 limit 1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 1
  projection (a::int)
    order by (1)
      scan
        projection (gr_expr_1::int -> a)
          group by (gr_expr_1::int)
            motion [policy: full, program: ReshardIfNeeded]
              limit 1
                projection (gr_expr_1::int)
                  order by (1)
                    scan
                      projection (t.a::int -> gr_expr_1)
                        group by (t.a::int)
                          join on (tt.b::double = t.a::int)
                            scan t
                            motion [policy: full, program: ReshardIfNeeded]
                              scan tt
                                projection (t.b::double -> b)
                                  scan t
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "t"."b" FROM "t"
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 2. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "gr_expr_1"
FROM
  (
    SELECT
      "t"."a" as "gr_expr_1"
    FROM
      "t"
      INNER JOIN "_tmp_2006172319729912025_0136" as "tt" ON "tt"."COL_0" = "t"."a"
    GROUP BY
      "t"."a"
  )
ORDER BY
  1
LIMIT
  1
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
        [0] SCAN TABLE _tmp_2006172319729912025_0136 AS tt (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 3. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "a"
FROM
  (
    SELECT
      "COL_0" as "a"
    FROM
      "_tmp_3318674698189894884_1136"
    GROUP BY
      "COL_0"
  )
ORDER BY
  1
LIMIT
  1
''
plan:
    [0] SCAN TABLE _tmp_3318674698189894884_1136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Forward                                                            
──────────────────────────────────────────────────────────────────────
''
forward analysis (on > ro_to_rw > off):
  forward = on
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: raw-forward-delete
-- SQL:
explain (raw, forward) delete from t where a = 1 and c = '2' or a in (1, 2, 3);
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "t"."c" as "pk_col_0", "t"."a" as "pk_col_1" FROM "t" WHERE "t"."a" = 1::int and "t"."c" = '2'::text or "t"."a" in (1::int, 2::int, 3::int)
''
plan:
    [0] SCAN TABLE t (~983040 rows)
    [0] EXECUTE LIST SUBQUERY 1
''
──────────────────────────────────────────────────────────────────────
 # Forward                                                            
──────────────────────────────────────────────────────────────────────
''
forward analysis (on > ro_to_rw > off):
  forward = on

-- TEST: raw-forward-select
-- SKIP_FOR: 2rsX1
-- SQL:
explain (raw, forward) select a from t where a = 1 and c = '2' union select id::int from _pico_table;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT "t"."a" FROM "t" WHERE "t"."a" = 1::int and "t"."c" = '2'::text UNION SELECT "_pico_table"."id"::int as "col_1" FROM "_pico_table"
''
plan:
    [1] SEARCH TABLE t USING PRIMARY KEY (c=? AND a=?) (~1 row)
    [2] SCAN TABLE _pico_table (~1048576 rows)
    [0] COMPOUND SUBQUERIES 1 AND 2 USING TEMP B-TREE (UNION)
''
──────────────────────────────────────────────────────────────────────
 # Forward                                                            
──────────────────────────────────────────────────────────────────────
''
forward analysis (on > ro_to_rw > off):
  forward = off

-- TEST: raw-forward-context-select
-- SQL:
explain (raw, forward, context) select id from _pico_table where id = 1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭───────────────────╮
│ 1. Query (ROUTER) │
╰───────────────────╯
''
SELECT "_pico_table"."id" FROM "_pico_table" WHERE "_pico_table"."id" = 1::int
''
plan:
    [0] SEARCH TABLE _pico_table USING PRIMARY KEY (id=?) (~1 row)
''
──────────────────────────────────────────────────────────────────────
 # Forward                                                            
──────────────────────────────────────────────────────────────────────
''
forward analysis (on > ro_to_rw > off):
  forward = off
''
──────────────────────────────────────────────────────────────────────
 # Context                                                            
──────────────────────────────────────────────────────────────────────
''
sql_vdbe_opcode_max = 45000
sql_motion_row_max = 5000

-- TEST: logical-buckets-context-select
-- SQL:
explain (logical, buckets, context) select id from _pico_table where id = 1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (_pico_table.id::int -> id)
  selection (_pico_table.id::int = 1::int)
    scan _pico_table
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Context                                                            
──────────────────────────────────────────────────────────────────────
''
sql_vdbe_opcode_max = 45000
sql_motion_row_max = 5000

-- TEST: buckets-context-delete
-- SQL:
explain (buckets, context) delete from t where a = 42;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]
''
──────────────────────────────────────────────────────────────────────
 # Context                                                            
──────────────────────────────────────────────────────────────────────
''
sql_vdbe_opcode_max = 45000
sql_motion_row_max = 5000

-- TEST: buckets-raw-block-delete-select
-- SKIP_FOR: 2rsX1
-- SQL:
EXPLAIN (buckets, raw)
DO $$ BEGIN
    RETURN QUERY SELECT * FROM tt WHERE d = 42;
    DELETE FROM tt WHERE d = 42;
END $$;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭───────────────────────────────────────────────╮
│ 1. Return query (CONST-FILTERED STORAGE, 1/1) │
╰───────────────────────────────────────────────╯
''
SELECT "tt"."d" FROM "tt" WHERE "tt"."d" = 42::int
''
plan:
    [0] SEARCH TABLE tt USING PRIMARY KEY (d=?) (~1 row)
''
╭────────────────────────────────────────╮
│ 2. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
DELETE FROM "tt" WHERE "tt"."d" = 42::int
''
plan:
    [0] SEARCH TABLE tt USING PRIMARY KEY (d=?) (~1 row)
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [2426]

-- TEST: logical-forward-block-let-if-insert-select
-- SKIP_FOR: 2rsX1
-- SQL:
EXPLAIN (logical, forward)
DO $$ BEGIN
    LET var_1 = (SELECT c::INT FROM g WHERE true);

    IF var_1 = 300 THEN
        INSERT INTO tt VALUES (42);
    END IF;
    
    INSERT INTO tt VALUES (42);
END $$;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────────────────────────╮
│ 1. Let "var_1" (CONST-FILTERED STORAGE, 1/1) │
╰──────────────────────────────────────────────╯
''
SELECT "g"."c"::int as "col_1" FROM "g" WHERE true::bool
''
projection (g.c::text::int -> col_1)
  selection (true::bool)
    scan g
''
╭────────────────────────────────────────────╮
│ 2.1. If cond (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────────╯
''
SELECT :var_1::int = 300::int as "cond"
''
projection (:var_1::int = 300::int -> cond)
''
╭───────────────────────────────────────────────────╮
│ 2.2. If body: Query (CONST-FILTERED STORAGE, 1/1) │
╰───────────────────────────────────────────────────╯
''
INSERT INTO "tt" ("d", "bucket_id") VALUES (42::int, 2426::int)
''
insert into tt on conflict: fail
  values
    value ROW(42::int, 2426::int)
''
╭────────────────────────────────────────╮
│ 3. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
INSERT INTO "tt" ("d", "bucket_id") VALUES (42::int, 2426::int)
''
insert into tt on conflict: fail
  values
    value ROW(42::int, 2426::int)
''
──────────────────────────────────────────────────────────────────────
 # Forward                                                            
──────────────────────────────────────────────────────────────────────
''
forward analysis (on > ro_to_rw > off):
  forward = off

-- TEST: raw-logical-forward-buckets-context-block-selects
-- SKIP_FOR: 2rsX1
-- SQL:
EXPLAIN (raw, logical, forward, buckets, context)
DO $$ BEGIN
    LET a1 = (SELECT a FROM g WHERE b = 2.5);
    LET a2 = (SELECT b FROM t WHERE a = 42 and c = 'kek' ORDER BY 1 DESC LIMIT 1);
    RETURN QUERY SELECT a FROM g;

    IF a2 = a1 THEN
        UPDATE t SET b = 5.5 WHERE a = 42 AND c = 'kek';
    END IF;
    
    IF a1 = -1 THEN
        INSERT INTO t (a, c) VALUES (42, 'kek');
    END IF;

END $$;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
╭───────────────────────────────────────────╮
│ 1. Let "a1" (CONST-FILTERED STORAGE, 1/1) │
╰───────────────────────────────────────────╯
''
SELECT "g"."a" FROM "g" WHERE "g"."b" = 2.5::decimal
''
projection (g.a::int -> a)
  selection (g.b::double = 2.5::decimal)
    scan g
''
╭───────────────────────────────────────────╮
│ 2. Let "a2" (CONST-FILTERED STORAGE, 1/1) │
╰───────────────────────────────────────────╯
''
SELECT "b" FROM ( SELECT "t"."b" FROM "t" WHERE "t"."a" = 42::int and "t"."c" = 'kek'::text ) ORDER BY 1 DESC LIMIT 1
''
limit 1
  projection (b::double)
    order by (1 desc)
      scan
        projection (t.b::double -> b)
          selection ((t.a::int = 42::int and t.c::text = 'kek'::text))
            scan t
''
╭───────────────────────────────────────────────╮
│ 3. Return query (CONST-FILTERED STORAGE, 1/1) │
╰───────────────────────────────────────────────╯
''
SELECT "g"."a" FROM "g"
''
projection (g.a::int -> a)
  scan g
''
╭────────────────────────────────────────────╮
│ 4.1. If cond (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────────╯
''
SELECT :a2::double = :a1::int as "cond"
''
projection (:a2::double = :a1::int -> cond)
''
╭───────────────────────────────────────────────────╮
│ 4.2. If body: Query (CONST-FILTERED STORAGE, 1/1) │
╰───────────────────────────────────────────────────╯
''
UPDATE "t" SET "b" = 5.5::decimal WHERE "t"."a" = 42::int and "t"."c" = 'kek'::text
''
update t (b = col_0)
  projection (5.5::decimal -> col_0, t.c::text -> col_1, t.a::int -> col_2)
    selection ((t.a::int = 42::int and t.c::text = 'kek'::text))
      scan t
''
╭────────────────────────────────────────────╮
│ 5.1. If cond (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────────╯
''
SELECT :a1::int = (-1)::int as "cond"
''
projection (:a1::int = -1::int -> cond)
''
╭───────────────────────────────────────────────────╮
│ 5.2. If body: Query (CONST-FILTERED STORAGE, 1/1) │
╰───────────────────────────────────────────────────╯
''
INSERT INTO "t" ("a", "c", "bucket_id") VALUES (42::int, 'kek'::text, 2873::int)
''
insert into t on conflict: fail
  values
    value ROW(42::int, 'kek'::text, 2873::int)
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭───────────────────────────────────────────╮
│ 1. Let "a1" (CONST-FILTERED STORAGE, 1/1) │
╰───────────────────────────────────────────╯
''
SELECT "g"."a" FROM "g" WHERE "g"."b" = 2.5::decimal
''
plan:
    [0] SCAN TABLE g (~262144 rows)
''
╭───────────────────────────────────────────╮
│ 2. Let "a2" (CONST-FILTERED STORAGE, 1/1) │
╰───────────────────────────────────────────╯
''
SELECT "b" FROM ( SELECT "t"."b" FROM "t" WHERE "t"."a" = 42::int and "t"."c" = 'kek'::text ) ORDER BY 1 DESC LIMIT 1
''
plan:
    [0] SEARCH TABLE t USING PRIMARY KEY (c=? AND a=?) (~1 row)
''
╭───────────────────────────────────────────────╮
│ 3. Return query (CONST-FILTERED STORAGE, 1/1) │
╰───────────────────────────────────────────────╯
''
SELECT "g"."a" FROM "g"
''
plan:
    [0] SCAN TABLE g (~1048576 rows)
''
╭────────────────────────────────────────────╮
│ 4.1. If cond (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────────╯
''
SELECT :a2::double = :a1::int as "cond"
''
plan:
    [0] TRIVIAL
''
╭───────────────────────────────────────────────────╮
│ 4.2. If body: Query (CONST-FILTERED STORAGE, 1/1) │
╰───────────────────────────────────────────────────╯
''
UPDATE "t" SET "b" = 5.5::decimal WHERE "t"."a" = 42::int and "t"."c" = 'kek'::text
''
plan:
    [0] SEARCH TABLE t USING PRIMARY KEY (c=? AND a=?) (~1 row)
''
╭────────────────────────────────────────────╮
│ 5.1. If cond (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────────╯
''
SELECT :a1::int = (-1)::int as "cond"
''
plan:
    [0] TRIVIAL
''
╭───────────────────────────────────────────────────╮
│ 5.2. If body: Query (CONST-FILTERED STORAGE, 1/1) │
╰───────────────────────────────────────────────────╯
''
INSERT INTO "t" ("a", "c", "bucket_id") VALUES (42::int, 'kek'::text, 2873::int)
''
plan:
    [0] TRIVIAL
''
──────────────────────────────────────────────────────────────────────
 # Forward                                                            
──────────────────────────────────────────────────────────────────────
''
forward analysis (on > ro_to_rw > off):
  forward = off
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [2873]
''
──────────────────────────────────────────────────────────────────────
 # Context                                                            
──────────────────────────────────────────────────────────────────────
''
sql_vdbe_opcode_max = 45000
sql_motion_row_max = 5000

-- TEST: raw-logical-forward-buckets-context-block-fmt-dml
-- SKIP_FOR: 2rsX1
-- SQL:
EXPLAIN (raw, logical, forward, buckets, context, fmt)
DO $$ BEGIN
    UPDATE t SET b = 2.0 WHERE a = 42 and c = 'lol';
    DELETE FROM t WHERE a = 42 and c = 'lol';
    INSERT INTO t VALUES (42, 2.5, 'lol');
END $$;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
UPDATE
  "t"
SET
  "b" = 2.0::decimal
WHERE
  "t"."a" = 42::int
  and "t"."c" = 'lol'::text
''
update t (b = col_0)
  projection (
    2.0::decimal -> col_0,
    t.c::text -> col_1,
    t.a::int -> col_2
  )
    selection (
      (t.a::int = 42::int and t.c::text = 'lol'::text)
    )
      scan t
''
╭────────────────────────────────────────╮
│ 2. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
DELETE FROM "t" WHERE "t"."a" = 42::int and "t"."c" = 'lol'::text
''
delete from t
  projection (t.c::text -> pk_col_0, t.a::int -> pk_col_1)
    selection (
      (t.a::int = 42::int and t.c::text = 'lol'::text)
    )
      scan t
''
╭────────────────────────────────────────╮
│ 3. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
INSERT INTO
  "t" ("a", "b", "c", "bucket_id")
VALUES
  (42::int, 2.5::decimal, 'lol'::text, 739::int)
''
insert into t on conflict: fail
  values
    value ROW(42::int, 2.5::decimal, 'lol'::text, 739::int)
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
UPDATE
  "t"
SET
  "b" = 2.0::decimal
WHERE
  "t"."a" = 42::int
  and "t"."c" = 'lol'::text
''
plan:
    [0] SEARCH TABLE t USING PRIMARY KEY (c=? AND a=?) (~1 row)
''
╭────────────────────────────────────────╮
│ 2. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
DELETE FROM "t" WHERE "t"."a" = 42::int and "t"."c" = 'lol'::text
''
plan:
    [0] SEARCH TABLE t USING PRIMARY KEY (c=? AND a=?) (~1 row)
''
╭────────────────────────────────────────╮
│ 3. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
INSERT INTO
  "t" ("a", "b", "c", "bucket_id")
VALUES
  (42::int, 2.5::decimal, 'lol'::text, 739::int)
''
plan:
    [0] TRIVIAL
''
──────────────────────────────────────────────────────────────────────
 # Forward                                                            
──────────────────────────────────────────────────────────────────────
''
forward analysis (on > ro_to_rw > off):
  forward = off
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [739]
''
──────────────────────────────────────────────────────────────────────
 # Context                                                            
──────────────────────────────────────────────────────────────────────
''
sql_vdbe_opcode_max = 45000
sql_motion_row_max = 5000

-- TEST: raw-logical--block-unused-let
-- SKIP_FOR: 2rsX1
-- SQL:
EXPLAIN (raw, logical)
DO $$ BEGIN
    LET var = (SELECT 1);
    LET var = (SELECT a FROM t WHERE a = 42 and c = 'lol');
    RETURN QUERY SELECT var;

    UPDATE t SET b = 2.0 WHERE a = 42 and c = 'lol';
    DELETE FROM t WHERE a = 42 and c = 'lol';
    INSERT INTO t VALUES (42, 2.5, 'lol');
END $$;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
╭───────────────────────────────────────────────────────╮
│ 1. **Unused** let "var" (CONST-FILTERED STORAGE, 1/1) │
╰───────────────────────────────────────────────────────╯
''
SELECT 1::int as "col_1"
''
projection (1::int -> col_1)
''
╭────────────────────────────────────────────╮
│ 2. Let "var" (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────────╯
''
SELECT "t"."a" FROM "t" WHERE "t"."a" = 42::int and "t"."c" = 'lol'::text
''
projection (t.a::int -> a)
  selection ((t.a::int = 42::int and t.c::text = 'lol'::text))
    scan t
''
╭───────────────────────────────────────────────╮
│ 3. Return query (CONST-FILTERED STORAGE, 1/1) │
╰───────────────────────────────────────────────╯
''
SELECT :var::int as "col_1"
''
projection (:var::int -> col_1)
''
╭────────────────────────────────────────╮
│ 4. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
UPDATE "t" SET "b" = 2.0::decimal WHERE "t"."a" = 42::int and "t"."c" = 'lol'::text
''
update t (b = col_0)
  projection (2.0::decimal -> col_0, t.c::text -> col_1, t.a::int -> col_2)
    selection ((t.a::int = 42::int and t.c::text = 'lol'::text))
      scan t
''
╭────────────────────────────────────────╮
│ 5. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
DELETE FROM "t" WHERE "t"."a" = 42::int and "t"."c" = 'lol'::text
''
delete from t
  projection (t.c::text -> pk_col_0, t.a::int -> pk_col_1)
    selection ((t.a::int = 42::int and t.c::text = 'lol'::text))
      scan t
''
╭────────────────────────────────────────╮
│ 6. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
INSERT INTO "t" ("a", "b", "c", "bucket_id") VALUES (42::int, 2.5::decimal, 'lol'::text, 739::int)
''
insert into t on conflict: fail
  values
    value ROW(42::int, 2.5::decimal, 'lol'::text, 739::int)
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭───────────────────────────────────────────────────────╮
│ 1. **Unused** let "var" (CONST-FILTERED STORAGE, 1/1) │
╰───────────────────────────────────────────────────────╯
''
SELECT 1::int as "col_1"
''
plan:
    [0] TRIVIAL
''
╭────────────────────────────────────────────╮
│ 2. Let "var" (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────────╯
''
SELECT "t"."a" FROM "t" WHERE "t"."a" = 42::int and "t"."c" = 'lol'::text
''
plan:
    [0] SEARCH TABLE t USING PRIMARY KEY (c=? AND a=?) (~1 row)
''
╭───────────────────────────────────────────────╮
│ 3. Return query (CONST-FILTERED STORAGE, 1/1) │
╰───────────────────────────────────────────────╯
''
SELECT :var::int as "col_1"
''
plan:
    [0] TRIVIAL
''
╭────────────────────────────────────────╮
│ 4. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
UPDATE "t" SET "b" = 2.0::decimal WHERE "t"."a" = 42::int and "t"."c" = 'lol'::text
''
plan:
    [0] SEARCH TABLE t USING PRIMARY KEY (c=? AND a=?) (~1 row)
''
╭────────────────────────────────────────╮
│ 5. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
DELETE FROM "t" WHERE "t"."a" = 42::int and "t"."c" = 'lol'::text
''
plan:
    [0] SEARCH TABLE t USING PRIMARY KEY (c=? AND a=?) (~1 row)
''
╭────────────────────────────────────────╮
│ 6. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
INSERT INTO "t" ("a", "b", "c", "bucket_id") VALUES (42::int, 2.5::decimal, 'lol'::text, 739::int)
''
plan:
    [0] TRIVIAL

-- TEST: raw-buckets-select
-- SQL:
explain (raw, buckets) select * from t order by 1 limit 1000;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "a", "b", "c" FROM ( SELECT "t"."a", "t"."b", "t"."c" FROM "t" ) ORDER BY 1 LIMIT 1000
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT "COL_0" as "a", "COL_1" as "b", "COL_2" as "c" FROM "_tmp_11686617587472461054_0136" ORDER BY 1 LIMIT 1000
''
plan:
    [0] SCAN TABLE _tmp_11686617587472461054_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: raw-buckets-select-join
-- SQL:
explain (raw, buckets) select * from t join g on t.a = g.a and g.c = 'lol';
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "t"."a", "t"."b", "t"."c", "g".* FROM "t" INNER JOIN "g" ON "t"."a" = "g"."a" and "g"."c" = 'lol'::text
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
        [0] SEARCH TABLE g USING PRIMARY KEY (a=?) (~1 row)
''
buckets <= [1-3000]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: raw-buckets-fmt-select-join
-- SQL:
explain (raw, buckets, fmt) select * from t join g on t.a = g.a and g.c = 'lol' group by 1, 2, 3, 4, 5, 6 order by 4 limit 10;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "gr_expr_1",
  "gr_expr_2",
  "gr_expr_3",
  "gr_expr_4",
  "gr_expr_5",
  "gr_expr_6"
FROM
  (
    SELECT
      "t"."a" as "gr_expr_1",
      "t"."b" as "gr_expr_2",
      "t"."c" as "gr_expr_3",
      "g"."a" as "gr_expr_4",
      "g"."b" as "gr_expr_5",
      "g"."c" as "gr_expr_6"
    FROM
      "t"
      INNER JOIN "g" ON "t"."a" = "g"."a"
      and "g"."c" = 'lol'::text
    GROUP BY
      "t"."a",
      "t"."b",
      "t"."c",
      "g"."a",
      "g"."b",
      "g"."c"
  )
ORDER BY
  4
LIMIT
  10
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
        [0] SEARCH TABLE g USING PRIMARY KEY (a=?) (~1 row)
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "a",
  "b",
  "c",
  "a",
  "b",
  "c"
FROM
  (
    SELECT
      "COL_0" as "a",
      "COL_1" as "b",
      "COL_2" as "c",
      "COL_3" as "a",
      "COL_4" as "b",
      "COL_5" as "c"
    FROM
      "_tmp_157142034380639894_0136"
    GROUP BY
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4",
      "COL_5"
  )
ORDER BY
  4
LIMIT
  10
''
plan:
    [0] SCAN TABLE _tmp_157142034380639894_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: raw-buckets-logical-select-subquery
-- SKIP_FOR: 2rsX1
-- SQL:
explain (raw, buckets, logical) SELECT * FROM b WHERE b.id IN (SELECT val FROM c where id = 5);
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (b.id::int -> id, b.val::int -> val)
  selection (b.id::int in ROW($0))
    scan b
subquery $0:
  motion [policy: segment([ref(val)]), program: ReshardIfNeeded]
    scan
      projection (c.val::int -> val)
        selection (c.id::int = 5::int)
          scan c
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT "c"."val" FROM "c" WHERE "c"."id" = 5::int
''
plan:
    [0] SEARCH TABLE c USING PRIMARY KEY (id=?) (~1 row)
''
buckets = [219]
''
╭─────────────────────────────────╮
│ 2. Query (DYN-FILTERED STORAGE) │
╰─────────────────────────────────╯
''
SELECT "b"."id", "b"."val" FROM "b" WHERE "b"."id" in ( SELECT "COL_0" FROM "_tmp_13231959227628902151_0136" )
''
plan:
    [0] SEARCH TABLE b USING PRIMARY KEY (id=?) (~24 rows)
    [0] EXECUTE LIST SUBQUERY 1
    [1] SCAN TABLE _tmp_13231959227628902151_0136 (~1048576 rows)
''
buckets <= [1-3000]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: buckets-does-not-intersperse-with-raw-for-transactions
-- SKIP_FOR: 2rsX1
-- SQL:
explain (raw, buckets)
do $$ begin
    if true then
        update b set val = id where id = 1;
    end if;
end $$;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────────╮
│ 1.1. If cond (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────────╯
''
SELECT true::bool as "cond"
''
plan:
    [0] TRIVIAL
''
╭───────────────────────────────────────────────────╮
│ 1.2. If body: Query (CONST-FILTERED STORAGE, 1/1) │
╰───────────────────────────────────────────────────╯
''
UPDATE "b" SET "val" = "b"."id" WHERE "b"."id" = 1::int
''
plan:
    [0] SEARCH TABLE b USING PRIMARY KEY (id=?) (~1 row)
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: raw-buckets-logical-select-in-order-by-dyn-filtered
-- SQL:
EXPLAIN (raw, fmt, buckets, logical)
SELECT * FROM tt WHERE d IN (SELECT a FROM t ORDER BY 1);
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (tt.d::int -> d)
  selection (tt.d::int in ROW($0))
    scan tt
subquery $0:
  motion [policy: segment([ref(a)]), program: ReshardIfNeeded]
    scan
      projection (a::int)
        order by (1)
          motion [policy: full, program: ReshardIfNeeded]
            scan
              projection (t.a::int -> a)
                scan t
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "t"."a" FROM "t"
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT "COL_0" as "a" FROM "_tmp_15686095326258425568_0136" ORDER BY 1
''
plan:
    [0] SCAN TABLE _tmp_15686095326258425568_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
╭─────────────────────────────────╮
│ 3. Query (DYN-FILTERED STORAGE) │
╰─────────────────────────────────╯
''
SELECT
  "tt"."d"
FROM
  "tt"
WHERE
  "tt"."d" in (
    SELECT
      "COL_0"
    FROM
      "_tmp_17184605309414654587_1136"
  )
''
plan:
    [0] SEARCH TABLE tt USING PRIMARY KEY (d=?) (~24 rows)
    [0] EXECUTE LIST SUBQUERY 1
    [1] SCAN TABLE _tmp_17184605309414654587_1136 (~1048576 rows)
''
buckets <= [1-3000]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: raw-buckets-logical-select-dyn-filtered
-- SKIP_FOR: 2rsX1
-- SQL:
EXPLAIN (raw, fmt, buckets, logical)
SELECT * FROM tt WHERE d IN (SELECT a FROM t WHERE a = 5 AND c = 'lol');
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (tt.d::int -> d)
  selection (tt.d::int in ROW($0))
    scan tt
subquery $0:
  motion [policy: segment([ref(a)]), program: ReshardIfNeeded]
    scan
      projection (t.a::int -> a)
        selection (
          (t.a::int = 5::int and t.c::text = 'lol'::text)
        )
          scan t
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT "t"."a" FROM "t" WHERE "t"."a" = 5::int and "t"."c" = 'lol'::text
''
plan:
    [0] SEARCH TABLE t USING PRIMARY KEY (c=? AND a=?) (~1 row)
''
buckets = [442]
''
╭─────────────────────────────────╮
│ 2. Query (DYN-FILTERED STORAGE) │
╰─────────────────────────────────╯
''
SELECT
  "tt"."d"
FROM
  "tt"
WHERE
  "tt"."d" in (
    SELECT
      "COL_0"
    FROM
      "_tmp_17421884011639704895_0136"
  )
''
plan:
    [0] SEARCH TABLE tt USING PRIMARY KEY (d=?) (~24 rows)
    [0] EXECUTE LIST SUBQUERY 1
    [1] SCAN TABLE _tmp_17421884011639704895_0136 (~1048576 rows)
''
buckets <= [1-3000]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]


-- TEST: raw-buckets-logical-select-order-by-dyn-filtered
-- SKIP_FOR: 2rsX1
-- SQL:
EXPLAIN (raw, fmt, buckets, logical)
SELECT * FROM tt WHERE d IN (SELECT a FROM t WHERE a = 5 AND c = 'lol') ORDER BY 1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (d::int)
  order by (1)
    motion [policy: full, program: ReshardIfNeeded]
      scan
        projection (tt.d::int -> d)
          selection (tt.d::int in ROW($0))
            scan tt
subquery $0:
  motion [policy: segment([ref(a)]), program: ReshardIfNeeded]
    scan
      projection (t.a::int -> a)
        selection (
          (t.a::int = 5::int and t.c::text = 'lol'::text)
        )
          scan t
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT "t"."a" FROM "t" WHERE "t"."a" = 5::int and "t"."c" = 'lol'::text
''
plan:
    [0] SEARCH TABLE t USING PRIMARY KEY (c=? AND a=?) (~1 row)
''
buckets = [442]
''
╭─────────────────────────────────╮
│ 2. Query (DYN-FILTERED STORAGE) │
╰─────────────────────────────────╯
''
SELECT
  "tt"."d"
FROM
  "tt"
WHERE
  "tt"."d" in (
    SELECT
      "COL_0"
    FROM
      "_tmp_17421884011639704895_0136"
  )
''
plan:
    [0] SEARCH TABLE tt USING PRIMARY KEY (d=?) (~24 rows)
    [0] EXECUTE LIST SUBQUERY 1
    [1] SCAN TABLE _tmp_17421884011639704895_0136 (~1048576 rows)
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 3. Query (ROUTER) │
╰───────────────────╯
''
SELECT "COL_0" as "d" FROM "_tmp_10787063316738723192_1136" ORDER BY 1
''
plan:
    [0] SCAN TABLE _tmp_10787063316738723192_1136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]


-- TEST: raw-buckets-cte
-- SKIP_FOR: 2rsX1
-- SQL:
EXPLAIN (RAW, FMT, BUCKETS) WITH cte1 (a) AS (SELECT "d" FROM "tt" WHERE "d" = 1),
cte2 (b) AS (SELECT * FROM cte1 UNION ALL SELECT "d" FROM "tt" WHERE "d" = 2)
SELECT b FROM cte2;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "cte2"."a" as "b"
FROM
  (
    SELECT
      *
    FROM
      (
        SELECT
          "tt"."d" as "a"
        FROM
          "tt"
        WHERE
          "tt"."d" = 1::int
      ) as "cte1"
    UNION ALL
    SELECT
      "tt"."d"
    FROM
      "tt"
    WHERE
      "tt"."d" = 2::int
  ) as "cte2"
''
plan:
    [1] SEARCH TABLE tt USING PRIMARY KEY (d=?) (~1 row)
    [2] SEARCH TABLE tt USING PRIMARY KEY (d=?) (~1 row)
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets = [1410, 1934]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "cte2"."COL_0" as "b"
FROM
  (
    SELECT
      "COL_0"
    FROM
      "_tmp_5314867776996900872_0136"
  ) as "cte2"
''
plan:
    [0] SCAN TABLE _tmp_5314867776996900872_0136 (~1048576 rows)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1410, 1934]

-- TEST: raw-buckets-logical-simple-select-dyn-filtered-x/k
-- SKIP_FOR: 2rsX1
-- SQL:
EXPLAIN (raw, fmt, buckets, logical)
SELECT * FROM tt WHERE d IN (1, 2, 3) AND d = (SELECT MIN(d) FROM tt);
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (tt.d::int -> d)
  selection (
    (
      tt.d::int in ROW(1::int, 2::int, 3::int)
      and tt.d::int = ROW($0)
    )
  )
    scan tt
subquery $0:
  motion [policy: segment([ref(col_1)]), program: ReshardIfNeeded]
    scan
      projection (min(min_1::int)::int -> col_1)
        motion [policy: full, program: ReshardIfNeeded]
          projection (min(tt.d::int::int)::int -> min_1)
            scan tt
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT min ("tt"."d"::int) as "min_1" FROM "tt"
''
plan:
    [0] SCAN TABLE tt (~1048576 rows)
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT min ("COL_0") as "col_1" FROM "_tmp_13737569284422530290_0136"
''
plan:
    [0] SEARCH TABLE _tmp_13737569284422530290_0136 USING PRIMARY KEY (~1048576 rows)
''
buckets = any
''
╭─────────────────────────────────────────╮
│ 3. Query (DYN-FILTERED STORAGE, <= 1/1) │
╰─────────────────────────────────────────╯
''
SELECT
  "tt"."d"
FROM
  "tt"
WHERE
  "tt"."d" in (1::int, 2::int, 3::int)
  and "tt"."d" = (
    SELECT
      "COL_0"
    FROM
      "_tmp_7752600402223527222_1136"
  )
''
plan:
    [0] SEARCH TABLE tt USING PRIMARY KEY (d=?) (~1 row)
    [0] EXECUTE SCALAR SUBQUERY 1
    [1] SCAN TABLE _tmp_7752600402223527222_1136 (~1048576 rows)
    [0] EXECUTE LIST SUBQUERY 2
''
buckets <= [1410, 1934, 1958]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]


-- TEST: raw-buckets-select-with-dyn-filtered
-- SQL:
EXPLAIN (raw, fmt, buckets, logical)
SELECT * FROM tt WHERE d = 1 AND d = 2 and d = (SELECT MIN(d) FROM tt);
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (tt.d::int -> d)
  selection (
    (
      tt.d::int = 1::int
      and tt.d::int = 2::int
      and tt.d::int = ROW($0)
    )
  )
    scan tt
subquery $0:
  motion [policy: segment([ref(col_1)]), program: ReshardIfNeeded]
    scan
      projection (min(min_1::int)::int -> col_1)
        motion [policy: full, program: ReshardIfNeeded]
          projection (min(tt.d::int::int)::int -> min_1)
            scan tt
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT min ("tt"."d"::int) as "min_1" FROM "tt"
''
plan:
    [0] SCAN TABLE tt (~1048576 rows)
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT min ("COL_0") as "col_1" FROM "_tmp_13737569284422530290_0136"
''
plan:
    [0] SEARCH TABLE _tmp_13737569284422530290_0136 USING PRIMARY KEY (~1048576 rows)
''
buckets = any
''
╭─────────────────────────────────╮
│ 3. Query (DYN-FILTERED STORAGE) │
╰─────────────────────────────────╯
''
SELECT
  "tt"."d"
FROM
  "tt"
WHERE
  "tt"."d" = 1::int
  and "tt"."d" = 2::int
  and "tt"."d" = (
    SELECT
      "COL_0"
    FROM
      "_tmp_10861478095534191766_1136"
  )
''
plan:
    [0] SEARCH TABLE tt USING PRIMARY KEY (d=?) (~1 row)
    [0] EXECUTE SCALAR SUBQUERY 1
    [1] SCAN TABLE _tmp_10861478095534191766_1136 (~1048576 rows)
''
buckets <= [1-3000]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: raw-buckets-select-with-empty-buckets
-- SQL:
EXPLAIN (raw, buckets, logical)
SELECT * FROM tt WHERE d = 1 AND d = 2;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (tt.d::int -> d)
  selection ((tt.d::int = 1::int and tt.d::int = 2::int))
    scan tt
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭───────────────────╮
│ 1. Query (ROUTER) │
╰───────────────────╯
''
SELECT "tt"."d" FROM "tt" WHERE "tt"."d" = 1::int and "tt"."d" = 2::int
''
plan:
    [0] SEARCH TABLE tt USING PRIMARY KEY (d=?) (~1 row)
''
buckets = []
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = []

-- TEST: raw-buckets-delete
-- SKIP_FOR: 2rsX1
-- SQL:
EXPLAIN (RAW, FMT, BUCKETS) DELETE FROM testing_space WHERE id IN (10, 15, 42);
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "testing_space"."id" as "pk_col_0"
FROM
  "testing_space"
WHERE
  "testing_space"."id" in (10::int, 15::int, 42::int)
''
plan:
    [0] SEARCH TABLE testing_space USING PRIMARY KEY (id=?) (~3 rows)
    [0] EXECUTE LIST SUBQUERY 1
''
buckets = [626, 1403, 2426]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [626, 1403, 2426]

-- TEST: raw-buckets-update
-- SKIP_FOR: 2rsX1
-- SQL:
EXPLAIN (RAW, FMT, BUCKETS) UPDATE testing_space SET product_units = product_units + 10 WHERE id IN (10, 15, 42);
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "testing_space"."product_units" + 10::int as "col_0",
  "testing_space"."id" as "col_1"
FROM
  "testing_space"
WHERE
  "testing_space"."id" in (10::int, 15::int, 42::int)
''
plan:
    [0] SEARCH TABLE testing_space USING PRIMARY KEY (id=?) (~3 rows)
    [0] EXECUTE LIST SUBQUERY 1
''
buckets = [626, 1403, 2426]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [626, 1403, 2426]

-- TEST: order-by-position-fmt-values
-- SQL:
EXPLAIN (raw, fmt)
SELECT * FROM (VALUES (1)) ORDER BY 1;
-- EXPECTED:
╭───────────────────╮
│ 1. Query (ROUTER) │
╰───────────────────╯
''
VALUES (1::int)
''
plan:
    [0] TRIVIAL
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "COL_0" as "COLUMN_1"
FROM
  (
    SELECT
      *
    FROM
      (
        (
          SELECT
            "COL_0"
          FROM
            "_tmp_2988718420758940377_0136"
        )
      ) as "unnamed_subquery"
  )
ORDER BY
  1
''
plan:
    [0] SCAN TABLE _tmp_2988718420758940377_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR ORDER BY


-- TEST: raw-select-double-inf
-- SQL:
EXPLAIN (raw)
SELECT 'inf'::double;
-- EXPECTED:
╭───────────────────╮
│ 1. Query (ROUTER) │
╰───────────────────╯
''
SELECT inf::double as "col_1"
''
plan:
    [0] TRIVIAL

-- TEST: raw-select-double-nan
-- SQL:
EXPLAIN (raw)
SELECT 'nan'::double;
-- EXPECTED:
╭───────────────────╮
│ 1. Query (ROUTER) │
╰───────────────────╯
''
SELECT NaN::double as "col_1"
''
plan:
    [0] TRIVIAL

-- TEST: raw-buckets-fmt-wrapped
-- SKIP_FOR: 2rsX1
-- SQL:
explain (raw, buckets, verbose, fmt)
select d from tt where d in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                             13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23);
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "tt"."d"
FROM
  "tt"
WHERE
  "tt"."d" in (
    1::int,
    2::int,
    3::int,
    4::int,
    5::int,
    6::int,
    7::int,
    8::int,
    9::int,
    10::int,
    11::int,
    12::int,
    13::int,
    14::int,
    15::int,
    16::int,
    17::int,
    18::int,
    19::int,
    20::int,
    21::int,
    22::int,
    23::int
  )
''
plan:
    [0] SEARCH TABLE tt USING PRIMARY KEY (d=?) (~22 rows)
    [0] EXECUTE LIST SUBQUERY 1
''
buckets = [
  219, 246, 509, 626, 653, 680, 799, 1403,
  1410, 1418, 1439, 1488, 1514, 1860, 1934,
  1948, 1958, 2312, 2564, 2640, 2752, 2802,
  2852
]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [
  219, 246, 509, 626, 653, 680, 799, 1403,
  1410, 1418, 1439, 1488, 1514, 1860, 1934,
  1948, 1958, 2312, 2564, 2640, 2752, 2802,
  2852
]

-- TEST: raw-buckets-fmt-truncated
-- SKIP_FOR: 2rsX1
-- SQL:
explain (raw, buckets, fmt)
select d from tt where d in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                             13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23);
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "tt"."d"
FROM
  "tt"
WHERE
  "tt"."d" in (
    1::int,
    2::int,
    3::int,
    4::int,
    5::int,
    6::int,
    7::int,
    8::int,
    9::int,
    10::int,
    11::int,
    12::int,
    13::int,
    14::int,
    15::int,
    16::int,
    17::int,
    18::int,
    19::int,
    20::int,
    21::int,
    22::int,
    23::int
  )
''
plan:
    [0] SEARCH TABLE tt USING PRIMARY KEY (d=?) (~22 rows)
    [0] EXECUTE LIST SUBQUERY 1
''
buckets = [219, 246, 509, 626, 653, 680, ... (17 more)]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [219, 246, 509, 626, 653, 680, ... (17 more)]
