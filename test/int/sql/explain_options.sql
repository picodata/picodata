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
explain (raw, buckets) select * from t join t on true group by 1, 2, 3, 4, 5, 6 order by 4 limit 5;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "t"."a", "t"."b", "t"."c", "t"."bucket_id" FROM "t"
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
''
buckets = [1-3000]
''
╭──────────────────────────╮
│ 2. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "gr_expr_1", "gr_expr_2", "gr_expr_3", "gr_expr_4", "gr_expr_5", "gr_expr_6" FROM ( SELECT "t"."a" as "gr_expr_1", "t"."b" as "gr_expr_2", "t"."c" as "gr_expr_3", "t"."COL_0" as "gr_expr_4", "t"."COL_1" as "gr_expr_5", "t"."COL_2" as "gr_expr_6" FROM "t" INNER JOIN ( SELECT "COL_0", "COL_1", "COL_2", "COL_3" FROM "_tmp_8554073533927061987_0136" ) as "t" ON CAST(true AS bool) GROUP BY "t"."a", "t"."b", "t"."c", "t"."COL_0", "t"."COL_1", "t"."COL_2" ) ORDER BY 4 LIMIT 5
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
        [0] SCAN TABLE _tmp_8554073533927061987_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = [1-3000]
''
╭───────────────────╮
│ 3. Query (ROUTER) │
╰───────────────────╯
''
SELECT "a", "b", "c", "a", "b", "c" FROM ( SELECT "COL_0" as "a", "COL_1" as "b", "COL_2" as "c", "COL_3" as "a", "COL_4" as "b", "COL_5" as "c" FROM ( SELECT "COL_0", "COL_1", "COL_2", "COL_3", "COL_4", "COL_5" FROM "_tmp_12303710340300335502_1136" ) GROUP BY "COL_0", "COL_1", "COL_2", "COL_3", "COL_4", "COL_5" ) ORDER BY 4 LIMIT 5
''
plan:
    [0] SCAN TABLE _tmp_12303710340300335502_1136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1-3000]

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
buckets = [1-3000]
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
  (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2"
    FROM
      "_tmp_10708443887562185739_0136"
  )
GROUP BY
  "COL_0",
  "COL_1",
  "COL_2"
''
plan:
    [0] SCAN TABLE _tmp_10708443887562185739_0136 (~1048576 rows)
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
      "_tmp_1775831832684500176_1136"
  )
ORDER BY
'  3'
LIMIT
'  5'
''
plan:
    [2] SCAN TABLE t (~1048576 rows)
    [3] SCAN TABLE _tmp_1775831832684500176_1136 (~1048576 rows)
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
        "_tmp_10014680312475178822_2136"
    )
  )
ORDER BY
'  3'
LIMIT
'  5'
''
plan:
    [0] SCAN TABLE _tmp_10014680312475178822_2136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = unknown

-- TEST: buckets-raw-fmt-delete
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
  "t"."a" = CAST(5 AS int)
  and "t"."c" = CAST('lol' AS string)
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
  "t"."b" + CAST(1 AS int) as "col_0",
  "t"."c" as "col_1",
  "t"."a" as "col_2"
FROM
  "t"
WHERE
  "t"."a" = CAST(42 AS int)
  and "t"."c" = CAST('kek' AS string)
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
      t.c::string -> col_1,
      t.a::int -> col_2
    )
      selection (
        (
          t.a::int = 42::int
          and t.c::string = 'kek'::string
        )
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
  "t"."b" + CAST(1 AS int) as "col_0",
  "t"."c" as "col_1",
  "t"."a" as "col_2"
FROM
  "t"
WHERE
  "t"."a" = CAST(42 AS int)
  and "t"."c" = CAST('kek' AS string)
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
-- SQL:
explain (buckets, logical, raw, fmt) delete from t where a = 5 and c = 'lol';
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
delete from t
  motion [policy: local, program: [PrimaryKey(0, 1), ReshardIfNeeded]]
    projection (t.c::string -> pk_col_0, t.a::int -> pk_col_1)
      selection (
        (
          t.a::int = 5::int
          and t.c::string = 'lol'::string
        )
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
  "t"."a" = CAST(5 AS int)
  and "t"."c" = CAST('lol' AS string)
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
      value ROW(2::int, 2::int, '2'::string)
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭───────────────────╮
│ 1. Query (ROUTER) │
╰───────────────────╯
''
VALUES ( CAST(2 AS int), CAST(2 AS int), CAST('2' AS string) )
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
          group by (gr_expr_1::int) output (gr_expr_1::int)
            motion [policy: full, program: ReshardIfNeeded]
              limit 1
                projection (gr_expr_1::int)
                  order by (gr_expr_1::int)
                    scan
                      projection (t.a::int -> gr_expr_1)
                        group by (t.a::int) output (t.a::int -> a, t.b::double -> b, t.c::string -> c, t.bucket_id::int -> bucket_id)
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
buckets = [1-3000]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT "a" FROM ( SELECT "COL_0" as "a" FROM ( SELECT "COL_0" FROM "_tmp_6750925285242178588_0136" ) GROUP BY "COL_0" ) ORDER BY "a" LIMIT 1
''
plan:
    [0] SCAN TABLE _tmp_6750925285242178588_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1-3000]

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
          group by (gr_expr_1::int) output (gr_expr_1::int)
            motion [policy: full, program: ReshardIfNeeded]
              limit 1
                projection (gr_expr_1::int)
                  order by (gr_expr_1::int)
                    scan
                      projection (t.a::int -> gr_expr_1)
                        group by (t.a::int) output (t.a::int -> a, t.b::double -> b, t.c::string -> c, t.bucket_id::int -> bucket_id)
                          scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1-3000]

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
SELECT "COL_0" as "d", "COL_1" as "d", "COL_2" as "d" FROM ( SELECT "COL_0", "COL_1", "COL_2" FROM "_tmp_12357788242454060125_0136" ) ORDER BY 1
''
plan:
    [0] SCAN TABLE _tmp_12357788242454060125_0136 (~1048576 rows)
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
          group by (gr_expr_1::int) output (gr_expr_1::int)
            motion [policy: full, program: ReshardIfNeeded]
              limit 1
                projection (gr_expr_1::int)
                  order by (gr_expr_1::int)
                    scan
                      projection (t.a::int -> gr_expr_1)
                        group by (t.a::int) output (t.a::int -> a, t.b::double -> b, t.c::string -> c, t.bucket_id::int -> bucket_id)
                          scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1-3000]

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
                      group by (gr_expr_1::double) output (gr_expr_1::double)
                        motion [policy: full, program: ReshardIfNeeded]
                          projection (t.b::double -> gr_expr_1)
                            group by (t.b::double) output (t.a::int -> a, t.b::double -> b, t.c::string -> c, t.bucket_id::int -> bucket_id)
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
buckets = [1-3000]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT "COL_0" as "b" FROM ( SELECT "COL_0" FROM "_tmp_6336902104418448325_0136" ) GROUP BY "COL_0"
''
plan:
    [0] SCAN TABLE _tmp_6336902104418448325_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = any
''
╭─────────────────────────────────╮
│ 3. Query (DYN-FILTERED STORAGE) │
╰─────────────────────────────────╯
''
SELECT "a" FROM ( SELECT "t"."a" FROM "t" UNION ALL SELECT "COL_0" FROM "_tmp_11718628551685952233_1136" ) ORDER BY 1 LIMIT 1
''
plan:
    [1] SCAN TABLE t (~1048576 rows)
    [1] USE TEMP B-TREE FOR ORDER BY
    [2] SCAN TABLE _tmp_11718628551685952233_1136 (~1048576 rows)
    [2] USE TEMP B-TREE FOR ORDER BY
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 4. Query (ROUTER) │
╰───────────────────╯
''
SELECT "COL_0" as "a" FROM ( SELECT "COL_0" FROM "_tmp_2686742979206912190_2136" ) ORDER BY 1 LIMIT 1
''
plan:
    [0] SCAN TABLE _tmp_2686742979206912190_2136 (~1048576 rows)
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
buckets = unknown

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
          group by (gr_expr_1::int) output (gr_expr_1::int)
            motion [policy: full, program: ReshardIfNeeded]
              limit 1
                projection (gr_expr_1::int)
                  order by (1)
                    scan
                      projection (t.a::int -> gr_expr_1)
                        group by (t.a::int) output (
                          t.a::int -> a,
                          t.b::double -> b,
                          t.c::string -> c,
                          t.bucket_id::int -> bucket_id,
                          tt.b::double -> b
                        )
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
buckets = [1-3000]
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
      INNER JOIN (
        SELECT
          "COL_0"
        FROM
          "_tmp_11220547791858563238_0136"
      ) as "tt" ON "tt"."COL_0" = "t"."a"
    GROUP BY
      "t"."a"
  )
ORDER BY
'  1'
LIMIT
'  1'
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
        [0] SCAN TABLE _tmp_11220547791858563238_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = [1-3000]
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
      (
        SELECT
          "COL_0"
        FROM
          "_tmp_5693774806165816034_1136"
      )
    GROUP BY
      "COL_0"
  )
ORDER BY
'  1'
LIMIT
'  1'
''
plan:
    [0] SCAN TABLE _tmp_5693774806165816034_1136 (~1048576 rows)
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
buckets = [1-3000]

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
SELECT "t"."c" as "pk_col_0", "t"."a" as "pk_col_1" FROM "t" WHERE "t"."a" = CAST(1 AS int) and "t"."c" = CAST('2' AS string) or "t"."a" in ( CAST(1 AS int), CAST(2 AS int), CAST(3 AS int) )
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
SELECT "t"."a" FROM "t" WHERE "t"."a" = CAST(1 AS int) and "t"."c" = CAST('2' AS string) UNION select cast(null as int) as "col_1" where false
''
plan:
    [1] SEARCH TABLE t USING PRIMARY KEY (c=? AND a=?) (~1 row)
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
SELECT "_pico_table"."id" FROM "_pico_table" WHERE "_pico_table"."id" = CAST(1 AS int)
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
buckets = [1-3000]
''
──────────────────────────────────────────────────────────────────────
 # Context                                                            
──────────────────────────────────────────────────────────────────────
''
sql_vdbe_opcode_max = 45000
sql_motion_row_max = 5000

-- TEST: buckets-raw-block-delete-select
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
SELECT "tt"."d" FROM "tt" WHERE "tt"."d" = CAST(42 AS int)
''
plan:
    [0] SEARCH TABLE tt USING PRIMARY KEY (d=?) (~1 row)
''
╭────────────────────────────────────────╮
│ 2. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
DELETE FROM "tt" WHERE "tt"."d" = CAST(42 AS int)
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
SELECT CAST ("g"."c" as int) as "col_1" FROM "g" WHERE CAST(true AS bool)
''
projection (g.c::string::int -> col_1)
  selection (true::bool)
    scan g
''
╭──────────────────────────────────────────╮
│ 2. If cond (CONST-FILTERED STORAGE, 1/1) │
╰──────────────────────────────────────────╯
''
SELECT CAST(:var_1 AS int) = CAST(300 AS int) as "cond"
''
projection (:var_1::int = 300::int -> cond)
''
╭──────────────────────────────────────────╮
│ 3. If body (CONST-FILTERED STORAGE, 1/1) │
╰──────────────────────────────────────────╯
''
INSERT INTO "tt" ("d", "bucket_id") VALUES (CAST(42 AS int), 2426)
''
insert into tt on conflict: fail
  values
    value ROW(42::int)
''
╭────────────────────────────────────────╮
│ 4. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
INSERT INTO "tt" ("d", "bucket_id") VALUES (CAST(42 AS int), 2426)
''
insert into tt on conflict: fail
  values
    value ROW(42::int)
''
──────────────────────────────────────────────────────────────────────
 # Forward                                                            
──────────────────────────────────────────────────────────────────────
''
forward analysis (on > ro_to_rw > off):
  forward = off

-- TEST: raw-logical-forward-buckets-context-block-selects
-- SQL:
EXPLAIN (raw, logical, forward, buckets, context)
DO $$ BEGIN
    LET a1 = (SELECT a FROM g WHERE b = 2.5);
    LET a2 = (SELECT b FROM t WHERE a = 42 and c = 'kek' ORDER BY 1 DESC LIMIT 1);
    RETURN QUERY SELECT a FROM g;

    IF a2 THEN
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
SELECT "g"."a" FROM "g" WHERE "g"."b" = CAST(2.5 AS decimal)
''
projection (g.a::int -> a)
  selection (g.b::double = 2.5::decimal)
    scan g
''
╭───────────────────────────────────────────╮
│ 2. Let "a2" (CONST-FILTERED STORAGE, 1/1) │
╰───────────────────────────────────────────╯
''
SELECT "b" FROM ( SELECT "t"."b" FROM "t" WHERE "t"."a" = CAST(42 AS int) and "t"."c" = CAST('kek' AS string) ) ORDER BY 1 DESC LIMIT 1
''
limit 1
  projection (b::double)
    order by (1 desc)
      scan
        projection (t.b::double -> b)
          selection ((t.a::int = 42::int and t.c::string = 'kek'::string))
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
╭──────────────────────────────────────────╮
│ 4. If cond (CONST-FILTERED STORAGE, 1/1) │
╰──────────────────────────────────────────╯
''
SELECT CAST(:a2 AS double) as "cond"
''
projection (:a2::double -> cond)
''
╭──────────────────────────────────────────╮
│ 5. If body (CONST-FILTERED STORAGE, 1/1) │
╰──────────────────────────────────────────╯
''
UPDATE "t" SET "b" = CAST(5.5 AS decimal) WHERE "t"."a" = CAST(42 AS int) and "t"."c" = CAST('kek' AS string)
''
update t (b = col_0)
  projection (5.5::decimal -> col_0, t.c::string -> col_1, t.a::int -> col_2)
    selection ((t.a::int = 42::int and t.c::string = 'kek'::string))
      scan t
''
╭──────────────────────────────────────────╮
│ 6. If cond (CONST-FILTERED STORAGE, 1/1) │
╰──────────────────────────────────────────╯
''
SELECT CAST(:a1 AS int) = CAST(-1 AS int) as "cond"
''
projection (:a1::int = -1::int -> cond)
''
╭──────────────────────────────────────────╮
│ 7. If body (CONST-FILTERED STORAGE, 1/1) │
╰──────────────────────────────────────────╯
''
INSERT INTO "t" ("a", "c", "bucket_id") VALUES (CAST(42 AS int), CAST('kek' AS string), 2873)
''
insert into t on conflict: fail
  values
    value ROW(42::int, 'kek'::string)
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭───────────────────────────────────────────╮
│ 1. Let "a1" (CONST-FILTERED STORAGE, 1/1) │
╰───────────────────────────────────────────╯
''
SELECT "g"."a" FROM "g" WHERE "g"."b" = CAST(2.5 AS decimal)
''
plan:
    [0] SCAN TABLE g (~262144 rows)
''
╭───────────────────────────────────────────╮
│ 2. Let "a2" (CONST-FILTERED STORAGE, 1/1) │
╰───────────────────────────────────────────╯
''
SELECT "b" FROM ( SELECT "t"."b" FROM "t" WHERE "t"."a" = CAST(42 AS int) and "t"."c" = CAST('kek' AS string) ) ORDER BY 1 DESC LIMIT 1
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
╭──────────────────────────────────────────╮
│ 4. If cond (CONST-FILTERED STORAGE, 1/1) │
╰──────────────────────────────────────────╯
''
SELECT CAST(:a2 AS double) as "cond"
''
plan:
    [0] TRIVIAL
''
╭──────────────────────────────────────────╮
│ 5. If body (CONST-FILTERED STORAGE, 1/1) │
╰──────────────────────────────────────────╯
''
UPDATE "t" SET "b" = CAST(5.5 AS decimal) WHERE "t"."a" = CAST(42 AS int) and "t"."c" = CAST('kek' AS string)
''
plan:
    [0] SEARCH TABLE t USING PRIMARY KEY (c=? AND a=?) (~1 row)
''
╭──────────────────────────────────────────╮
│ 6. If cond (CONST-FILTERED STORAGE, 1/1) │
╰──────────────────────────────────────────╯
''
SELECT CAST(:a1 AS int) = CAST(-1 AS int) as "cond"
''
plan:
    [0] TRIVIAL
''
╭──────────────────────────────────────────╮
│ 7. If body (CONST-FILTERED STORAGE, 1/1) │
╰──────────────────────────────────────────╯
''
INSERT INTO "t" ("a", "c", "bucket_id") VALUES (CAST(42 AS int), CAST('kek' AS string), 2873)
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
  "b" = CAST(2.0 AS decimal)
WHERE
  "t"."a" = CAST(42 AS int)
  and "t"."c" = CAST('lol' AS string)
''
update t (b = col_0)
  projection (
    2.0::decimal -> col_0,
    t.c::string -> col_1,
    t.a::int -> col_2
  )
    selection (
      (
        t.a::int = 42::int
        and t.c::string = 'lol'::string
      )
    )
      scan t
''
╭────────────────────────────────────────╮
│ 2. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
DELETE FROM
  "t"
WHERE
  "t"."a" = CAST(42 AS int)
  and "t"."c" = CAST('lol' AS string)
''
delete from t
  projection (t.c::string -> pk_col_0, t.a::int -> pk_col_1)
    selection (
      (
        t.a::int = 42::int
        and t.c::string = 'lol'::string
      )
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
  (
    CAST(42 AS int),
    CAST(2.5 AS decimal),
    CAST('lol' AS string),
'    739'
  )
''
insert into t on conflict: fail
  values
    value ROW(42::int, 2.5::decimal, 'lol'::string)
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
  "b" = CAST(2.0 AS decimal)
WHERE
  "t"."a" = CAST(42 AS int)
  and "t"."c" = CAST('lol' AS string)
''
plan:
    [0] SEARCH TABLE t USING PRIMARY KEY (c=? AND a=?) (~1 row)
''
╭────────────────────────────────────────╮
│ 2. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
DELETE FROM
  "t"
WHERE
  "t"."a" = CAST(42 AS int)
  and "t"."c" = CAST('lol' AS string)
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
  (
    CAST(42 AS int),
    CAST(2.5 AS decimal),
    CAST('lol' AS string),
'    739'
  )
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
SELECT CAST(1 AS int) as "col_1"
''
projection (1::int -> col_1)
''
╭────────────────────────────────────────────╮
│ 2. Let "var" (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────────╯
''
SELECT "t"."a" FROM "t" WHERE "t"."a" = CAST(42 AS int) and "t"."c" = CAST('lol' AS string)
''
projection (t.a::int -> a)
  selection ((t.a::int = 42::int and t.c::string = 'lol'::string))
    scan t
''
╭───────────────────────────────────────────────╮
│ 3. Return query (CONST-FILTERED STORAGE, 1/1) │
╰───────────────────────────────────────────────╯
''
SELECT CAST(:var AS int) as "col_1"
''
projection (:var::int -> col_1)
''
╭────────────────────────────────────────╮
│ 4. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
UPDATE "t" SET "b" = CAST(2.0 AS decimal) WHERE "t"."a" = CAST(42 AS int) and "t"."c" = CAST('lol' AS string)
''
update t (b = col_0)
  projection (2.0::decimal -> col_0, t.c::string -> col_1, t.a::int -> col_2)
    selection ((t.a::int = 42::int and t.c::string = 'lol'::string))
      scan t
''
╭────────────────────────────────────────╮
│ 5. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
DELETE FROM "t" WHERE "t"."a" = CAST(42 AS int) and "t"."c" = CAST('lol' AS string)
''
delete from t
  projection (t.c::string -> pk_col_0, t.a::int -> pk_col_1)
    selection ((t.a::int = 42::int and t.c::string = 'lol'::string))
      scan t
''
╭────────────────────────────────────────╮
│ 6. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
INSERT INTO "t" ("a", "b", "c", "bucket_id") VALUES ( CAST(42 AS int), CAST(2.5 AS decimal), CAST('lol' AS string), 739 )
''
insert into t on conflict: fail
  values
    value ROW(42::int, 2.5::decimal, 'lol'::string)
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭───────────────────────────────────────────────────────╮
│ 1. **Unused** let "var" (CONST-FILTERED STORAGE, 1/1) │
╰───────────────────────────────────────────────────────╯
''
SELECT CAST(1 AS int) as "col_1"
''
plan:
    [0] TRIVIAL
''
╭────────────────────────────────────────────╮
│ 2. Let "var" (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────────╯
''
SELECT "t"."a" FROM "t" WHERE "t"."a" = CAST(42 AS int) and "t"."c" = CAST('lol' AS string)
''
plan:
    [0] SEARCH TABLE t USING PRIMARY KEY (c=? AND a=?) (~1 row)
''
╭───────────────────────────────────────────────╮
│ 3. Return query (CONST-FILTERED STORAGE, 1/1) │
╰───────────────────────────────────────────────╯
''
SELECT CAST(:var AS int) as "col_1"
''
plan:
    [0] TRIVIAL
''
╭────────────────────────────────────────╮
│ 4. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
UPDATE "t" SET "b" = CAST(2.0 AS decimal) WHERE "t"."a" = CAST(42 AS int) and "t"."c" = CAST('lol' AS string)
''
plan:
    [0] SEARCH TABLE t USING PRIMARY KEY (c=? AND a=?) (~1 row)
''
╭────────────────────────────────────────╮
│ 5. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
DELETE FROM "t" WHERE "t"."a" = CAST(42 AS int) and "t"."c" = CAST('lol' AS string)
''
plan:
    [0] SEARCH TABLE t USING PRIMARY KEY (c=? AND a=?) (~1 row)
''
╭────────────────────────────────────────╮
│ 6. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
INSERT INTO "t" ("a", "b", "c", "bucket_id") VALUES ( CAST(42 AS int), CAST(2.5 AS decimal), CAST('lol' AS string), 739 )
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
buckets = [1-3000]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT "COL_0" as "a", "COL_1" as "b", "COL_2" as "c" FROM ( SELECT "COL_0", "COL_1", "COL_2" FROM "_tmp_2758788183884433110_0136" ) ORDER BY 1 LIMIT 1000
''
plan:
    [0] SCAN TABLE _tmp_2758788183884433110_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1-3000]

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
SELECT "t"."a", "t"."b", "t"."c", "g".* FROM "t" INNER JOIN "g" ON "t"."a" = "g"."a" and "g"."c" = CAST('lol' AS string)
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
        [0] SEARCH TABLE g USING PRIMARY KEY (a=?) (~1 row)
''
buckets = [1-3000]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1-3000]

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
      and "g"."c" = CAST('lol' AS string)
    GROUP BY
      "t"."a",
      "t"."b",
      "t"."c",
      "g"."a",
      "g"."b",
      "g"."c"
  )
ORDER BY
'  4'
LIMIT
'  10'
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
        [0] SEARCH TABLE g USING PRIMARY KEY (a=?) (~1 row)
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = [1-3000]
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
      (
        SELECT
          "COL_0",
          "COL_1",
          "COL_2",
          "COL_3",
          "COL_4",
          "COL_5"
        FROM
          "_tmp_7812755374194076184_0136"
      )
    GROUP BY
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4",
      "COL_5"
  )
ORDER BY
'  4'
LIMIT
'  10'
''
plan:
    [0] SCAN TABLE _tmp_7812755374194076184_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1-3000]

-- TEST: raw-buckets-logical-select-subquery
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
SELECT "c"."val" FROM "c" WHERE "c"."id" = CAST(5 AS int)
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
SELECT "b"."id", "b"."val" FROM "b" WHERE "b"."id" in ( SELECT "COL_0" FROM "_tmp_11862588026286075466_0136" )
''
plan:
    [0] SEARCH TABLE b USING PRIMARY KEY (id=?) (~24 rows)
    [0] EXECUTE LIST SUBQUERY 1
    [1] SCAN TABLE _tmp_11862588026286075466_0136 (~1048576 rows)
''
buckets <= [1-3000]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = unknown

-- TEST: buckets-does-not-intersperse-with-raw-for-transactions
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
╭──────────────────────────────────────────╮
│ 1. If cond (CONST-FILTERED STORAGE, 1/1) │
╰──────────────────────────────────────────╯
''
SELECT CAST(true AS bool) as "cond"
''
plan:
    [0] TRIVIAL
''
╭──────────────────────────────────────────╮
│ 2. If body (CONST-FILTERED STORAGE, 1/1) │
╰──────────────────────────────────────────╯
''
UPDATE "b" SET "val" = "b"."id" WHERE "b"."id" = CAST(1 AS int)
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
buckets = [1-3000]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "COL_0" as "a"
FROM
  (
    SELECT
      "COL_0"
    FROM
      "_tmp_11035079382586487614_0136"
  )
ORDER BY
'  1'
''
plan:
    [0] SCAN TABLE _tmp_11035079382586487614_0136 (~1048576 rows)
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
      "_tmp_10955078849476425956_1136"
  )
''
plan:
    [0] SEARCH TABLE tt USING PRIMARY KEY (d=?) (~24 rows)
    [0] EXECUTE LIST SUBQUERY 1
    [1] SCAN TABLE _tmp_10955078849476425956_1136 (~1048576 rows)
''
buckets <= [1-3000]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = unknown

-- TEST: raw-buckets-logical-select-dyn-filtered
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
          (
            t.a::int = 5::int
            and t.c::string = 'lol'::string
          )
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
  "t"."a"
FROM
  "t"
WHERE
  "t"."a" = CAST(5 AS int)
  and "t"."c" = CAST('lol' AS string)
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
      "_tmp_17111086162118482306_0136"
  )
''
plan:
    [0] SEARCH TABLE tt USING PRIMARY KEY (d=?) (~24 rows)
    [0] EXECUTE LIST SUBQUERY 1
    [1] SCAN TABLE _tmp_17111086162118482306_0136 (~1048576 rows)
''
buckets <= [1-3000]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = unknown


-- TEST: raw-buckets-logical-select-order-by-dyn-filtered
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
          (
            t.a::int = 5::int
            and t.c::string = 'lol'::string
          )
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
  "t"."a"
FROM
  "t"
WHERE
  "t"."a" = CAST(5 AS int)
  and "t"."c" = CAST('lol' AS string)
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
      "_tmp_17111086162118482306_0136"
  )
''
plan:
    [0] SEARCH TABLE tt USING PRIMARY KEY (d=?) (~24 rows)
    [0] EXECUTE LIST SUBQUERY 1
    [1] SCAN TABLE _tmp_17111086162118482306_0136 (~1048576 rows)
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 3. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "COL_0" as "d"
FROM
  (
    SELECT
      "COL_0"
    FROM
      "_tmp_5264661124255812437_1136"
  )
ORDER BY
'  1'
''
plan:
    [0] SCAN TABLE _tmp_5264661124255812437_1136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = unknown


-- TEST: raw-buckets-cte
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
          "tt"."d" = CAST(1 AS int)
      ) as "cte1"
    UNION ALL
    SELECT
      "tt"."d"
    FROM
      "tt"
    WHERE
      "tt"."d" = CAST(2 AS int)
  ) as "cte2"
''
plan:
    [1] SEARCH TABLE tt USING PRIMARY KEY (d=?) (~1 row)
    [2] SEARCH TABLE tt USING PRIMARY KEY (d=?) (~1 row)
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets = [1410,1934]
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
      "_tmp_15450967935293219502_0136"
  ) as "cte2"
''
plan:
    [0] SCAN TABLE _tmp_15450967935293219502_0136 (~1048576 rows)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1410,1934]

-- TEST: raw-buckets-logical-simple-select-dyn-filtered-x/k
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
SELECT min (CAST ("tt"."d" as int)) as "min_1" FROM "tt"
''
plan:
    [0] SCAN TABLE tt (~1048576 rows)
''
buckets = [1-3000]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  min ("COL_0") as "col_1"
FROM
  (
    SELECT
      "COL_0"
    FROM
      "_tmp_6332101395320054053_0136"
  )
''
plan:
    [0] SEARCH TABLE _tmp_6332101395320054053_0136 USING PRIMARY KEY (~1048576 rows)
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
  "tt"."d" in (
    CAST(1 AS int),
    CAST(2 AS int),
    CAST(3 AS int)
  )
  and "tt"."d" = (
    SELECT
      "COL_0"
    FROM
      "_tmp_12450157650986780114_1136"
  )
''
plan:
    [0] SEARCH TABLE tt USING PRIMARY KEY (d=?) (~1 row)
    [0] EXECUTE SCALAR SUBQUERY 1
    [1] SCAN TABLE _tmp_12450157650986780114_1136 (~1048576 rows)
    [0] EXECUTE LIST SUBQUERY 2
''
buckets <= [1410,1934,1958]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = unknown


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
SELECT min (CAST ("tt"."d" as int)) as "min_1" FROM "tt"
''
plan:
    [0] SCAN TABLE tt (~1048576 rows)
''
buckets = [1-3000]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  min ("COL_0") as "col_1"
FROM
  (
    SELECT
      "COL_0"
    FROM
      "_tmp_6332101395320054053_0136"
  )
''
plan:
    [0] SEARCH TABLE _tmp_6332101395320054053_0136 USING PRIMARY KEY (~1048576 rows)
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
  "tt"."d" = CAST(1 AS int)
  and "tt"."d" = CAST(2 AS int)
  and "tt"."d" = (
    SELECT
      "COL_0"
    FROM
      "_tmp_2619068961665415764_1136"
  )
''
plan:
    [0] SEARCH TABLE tt USING PRIMARY KEY (d=?) (~1 row)
    [0] EXECUTE SCALAR SUBQUERY 1
    [1] SCAN TABLE _tmp_2619068961665415764_1136 (~1048576 rows)
''
buckets <= [1-3000]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = unknown

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
SELECT "tt"."d" FROM "tt" WHERE "tt"."d" = CAST(1 AS int) and "tt"."d" = CAST(2 AS int)
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
  "testing_space"."id" in (
    CAST(10 AS int),
    CAST(15 AS int),
    CAST(42 AS int)
  )
''
plan:
    [0] SEARCH TABLE testing_space USING PRIMARY KEY (id=?) (~3 rows)
    [0] EXECUTE LIST SUBQUERY 1
''
buckets = [626,1403,2426]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [626,1403,2426]

-- TEST: raw-buckets-update
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
  "testing_space"."product_units" + CAST(10 AS int) as "col_0",
  "testing_space"."id" as "col_1"
FROM
  "testing_space"
WHERE
  "testing_space"."id" in (
    CAST(10 AS int),
    CAST(15 AS int),
    CAST(42 AS int)
  )
''
plan:
    [0] SEARCH TABLE testing_space USING PRIMARY KEY (id=?) (~3 rows)
    [0] EXECUTE LIST SUBQUERY 1
''
buckets = [626,1403,2426]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [626,1403,2426]
