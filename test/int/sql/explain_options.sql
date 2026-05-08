-- TEST: explain-setup
-- SQL:
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS tt;
DROP TABLE IF EXISTS g;
CREATE TABLE t (a INT, b DOUBLE, c TEXT, PRIMARY KEY (c, a));
CREATE TABLE tt (d INT PRIMARY KEY);
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
╭────────────────────╮
│ 1. Query (STORAGE) │
╰────────────────────╯
''
SELECT "t"."a", "t"."b", "t"."c", "t"."bucket_id" FROM "t"
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
''
╭────────────────────╮
│ 2. Query (STORAGE) │
╰────────────────────╯
''
SELECT "gr_expr_1", "gr_expr_2", "gr_expr_3", "gr_expr_4", "gr_expr_5", "gr_expr_6" FROM ( SELECT "t"."a" as "gr_expr_1", "t"."b" as "gr_expr_2", "t"."c" as "gr_expr_3", "t"."COL_0" as "gr_expr_4", "t"."COL_1" as "gr_expr_5", "t"."COL_2" as "gr_expr_6" FROM "t" INNER JOIN ( SELECT "COL_0", "COL_1", "COL_2", "COL_3" FROM "TMP_8554073533927061987_0136" ) as "t" ON CAST(true AS bool) GROUP BY "t"."a", "t"."b", "t"."c", "t"."COL_0", "t"."COL_1", "t"."COL_2" ) ORDER BY 4 LIMIT 5
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
        [0] SCAN TABLE TMP_8554073533927061987_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
    [0] USE TEMP B-TREE FOR ORDER BY
''
╭───────────────────╮
│ 3. Query (ROUTER) │
╰───────────────────╯
''
SELECT "a", "b", "c", "a", "b", "c" FROM ( SELECT "COL_0" as "a", "COL_1" as "b", "COL_2" as "c", "COL_3" as "a", "COL_4" as "b", "COL_5" as "c" FROM ( SELECT "COL_0", "COL_1", "COL_2", "COL_3", "COL_4", "COL_5" FROM "TMP_12303710340300335502_1136" ) GROUP BY "COL_0", "COL_1", "COL_2", "COL_3", "COL_4", "COL_5" ) ORDER BY 4 LIMIT 5
''
plan:
    [0] SCAN TABLE TMP_12303710340300335502_1136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
    [0] USE TEMP B-TREE FOR ORDER BY
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
╭────────────────────╮
│ 1. Query (STORAGE) │
╰────────────────────╯
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
      "TMP_10708443887562185739_0136"
  )
GROUP BY
  "COL_0",
  "COL_1",
  "COL_2"
''
plan:
    [0] SCAN TABLE TMP_10708443887562185739_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
╭────────────────────╮
│ 3. Query (STORAGE) │
╰────────────────────╯
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
      "TMP_1775831832684500176_1136"
  )
ORDER BY
'  3'
LIMIT
'  5'
''
plan:
    [2] SCAN TABLE t (~1048576 rows)
    [3] SCAN TABLE TMP_1775831832684500176_1136 (~1048576 rows)
    [1] COMPOUND SUBQUERIES 2 AND 3 USING TEMP B-TREE (UNION)
    [0] SCAN SUBQUERY 1 (~1 row)
    [0] USE TEMP B-TREE FOR ORDER BY
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
        "TMP_10014680312475178822_2136"
    )
  )
ORDER BY
'  3'
LIMIT
'  5'
''
plan:
    [0] SCAN TABLE TMP_10014680312475178822_2136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR ORDER BY
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
╭─────────────────────────────╮
│ 1. Query (FILTERED STORAGE) │
╰─────────────────────────────╯
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
╭─────────────────────────────╮
│ 1. Query (FILTERED STORAGE) │
╰─────────────────────────────╯
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
╭─────────────────────────────╮
│ 1. Query (FILTERED STORAGE) │
╰─────────────────────────────╯
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
╭─────────────────────────────╮
│ 1. Query (FILTERED STORAGE) │
╰─────────────────────────────╯
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
╭────────────────────╮
│ 1. Query (STORAGE) │
╰────────────────────╯
''
SELECT "gr_expr_1" FROM ( SELECT "t"."a" as "gr_expr_1" FROM "t" GROUP BY "t"."a" ) ORDER BY "gr_expr_1" LIMIT 1
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT "a" FROM ( SELECT "COL_0" as "a" FROM ( SELECT "COL_0" FROM "TMP_6750925285242178588_0136" ) GROUP BY "COL_0" ) ORDER BY "a" LIMIT 1
''
plan:
    [0] SCAN TABLE TMP_6750925285242178588_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
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
╭────────────────────╮
│ 1. Query (STORAGE) │
╰────────────────────╯
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
SELECT "COL_0" as "d", "COL_1" as "d", "COL_2" as "d" FROM ( SELECT "COL_0", "COL_1", "COL_2" FROM "TMP_12357788242454060125_0136" ) ORDER BY 1
''
plan:
    [0] SCAN TABLE TMP_12357788242454060125_0136 (~1048576 rows)
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
╭────────────────────╮
│ 1. Query (STORAGE) │
╰────────────────────╯
''
SELECT "t"."b" as "gr_expr_1" FROM "t" GROUP BY "t"."b"
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT "COL_0" as "b" FROM ( SELECT "COL_0" FROM "TMP_6336902104418448325_0136" ) GROUP BY "COL_0"
''
plan:
    [0] SCAN TABLE TMP_6336902104418448325_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
╭────────────────────╮
│ 3. Query (STORAGE) │
╰────────────────────╯
''
SELECT "a" FROM ( SELECT "t"."a" FROM "t" UNION ALL SELECT "COL_0" FROM "TMP_11718628551685952233_1136" ) ORDER BY 1 LIMIT 1
''
plan:
    [1] SCAN TABLE t (~1048576 rows)
    [1] USE TEMP B-TREE FOR ORDER BY
    [2] SCAN TABLE TMP_11718628551685952233_1136 (~1048576 rows)
    [2] USE TEMP B-TREE FOR ORDER BY
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
╭───────────────────╮
│ 4. Query (ROUTER) │
╰───────────────────╯
''
SELECT "COL_0" as "a" FROM ( SELECT "COL_0" FROM "TMP_2686742979206912190_2136" ) ORDER BY 1 LIMIT 1
''
plan:
    [0] SCAN TABLE TMP_2686742979206912190_2136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR ORDER BY
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
╭────────────────────╮
│ 1. Query (STORAGE) │
╰────────────────────╯
''
SELECT "t"."b" FROM "t"
''
plan:
    [0] SCAN TABLE t (~1048576 rows)
''
╭────────────────────╮
│ 2. Query (STORAGE) │
╰────────────────────╯
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
          "TMP_11220547791858563238_0136"
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
        [0] SCAN TABLE TMP_11220547791858563238_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
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
          "TMP_5693774806165816034_1136"
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
    [0] SCAN TABLE TMP_5693774806165816034_1136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
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
╭────────────────────╮
│ 1. Query (STORAGE) │
╰────────────────────╯
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
╭─────────────────────────────╮
│ 1. Query (FILTERED STORAGE) │
╰─────────────────────────────╯
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
╭────────────────────────────────────╮
│ 1. Return query (FILTERED STORAGE) │
╰────────────────────────────────────╯
''
SELECT "tt"."d" FROM "tt" WHERE "tt"."d" = CAST(42 AS int)
''
plan:
    [0] SEARCH TABLE tt USING PRIMARY KEY (d=?) (~1 row)
''
╭─────────────────────────────╮
│ 2. Query (FILTERED STORAGE) │
╰─────────────────────────────╯
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
╭───────────────────────────────────╮
│ 1. Let "var_1" (FILTERED STORAGE) │
╰───────────────────────────────────╯
''
SELECT CAST ("g"."c" as int) as "col_1" FROM "g" WHERE CAST(true AS bool)
''
projection (g.c::string::int -> col_1)
  selection (true::bool)
    scan g
''
╭───────────────────────────────╮
│ 2. If cond (FILTERED STORAGE) │
╰───────────────────────────────╯
''
SELECT CAST(:var_1 AS int) = CAST(300 AS int) as "cond"
''
projection (:var_1::int = 300::int -> cond)
''
╭───────────────────────────────╮
│ 3. If body (FILTERED STORAGE) │
╰───────────────────────────────╯
''
INSERT INTO "tt" ("d", "bucket_id") VALUES (CAST(42 AS int), 2426)
''
insert into tt on conflict: fail
  values
    value ROW(42::int)
''
╭─────────────────────────────╮
│ 4. Query (FILTERED STORAGE) │
╰─────────────────────────────╯
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
╭────────────────────────────────╮
│ 1. Let "a1" (FILTERED STORAGE) │
╰────────────────────────────────╯
''
SELECT "g"."a" FROM "g" WHERE "g"."b" = CAST(2.5 AS decimal)
''
projection (g.a::int -> a)
  selection (g.b::double = 2.5::decimal)
    scan g
''
╭────────────────────────────────╮
│ 2. Let "a2" (FILTERED STORAGE) │
╰────────────────────────────────╯
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
╭────────────────────────────────────╮
│ 3. Return query (FILTERED STORAGE) │
╰────────────────────────────────────╯
''
SELECT "g"."a" FROM "g"
''
projection (g.a::int -> a)
  scan g
''
╭───────────────────────────────╮
│ 4. If cond (FILTERED STORAGE) │
╰───────────────────────────────╯
''
SELECT CAST(:a2 AS double) as "cond"
''
projection (:a2::double -> cond)
''
╭───────────────────────────────╮
│ 5. If body (FILTERED STORAGE) │
╰───────────────────────────────╯
''
UPDATE "t" SET "b" = CAST(5.5 AS decimal) WHERE "t"."a" = CAST(42 AS int) and "t"."c" = CAST('kek' AS string)
''
update t (b = col_0)
  projection (5.5::decimal -> col_0, t.c::string -> col_1, t.a::int -> col_2)
    selection ((t.a::int = 42::int and t.c::string = 'kek'::string))
      scan t
''
╭───────────────────────────────╮
│ 6. If cond (FILTERED STORAGE) │
╰───────────────────────────────╯
''
SELECT CAST(:a1 AS int) = CAST(-1 AS int) as "cond"
''
projection (:a1::int = -1::int -> cond)
''
╭───────────────────────────────╮
│ 7. If body (FILTERED STORAGE) │
╰───────────────────────────────╯
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
╭────────────────────────────────╮
│ 1. Let "a1" (FILTERED STORAGE) │
╰────────────────────────────────╯
''
SELECT "g"."a" FROM "g" WHERE "g"."b" = CAST(2.5 AS decimal)
''
plan:
    [0] SCAN TABLE g (~262144 rows)
''
╭────────────────────────────────╮
│ 2. Let "a2" (FILTERED STORAGE) │
╰────────────────────────────────╯
''
SELECT "b" FROM ( SELECT "t"."b" FROM "t" WHERE "t"."a" = CAST(42 AS int) and "t"."c" = CAST('kek' AS string) ) ORDER BY 1 DESC LIMIT 1
''
plan:
    [0] SEARCH TABLE t USING PRIMARY KEY (c=? AND a=?) (~1 row)
''
╭────────────────────────────────────╮
│ 3. Return query (FILTERED STORAGE) │
╰────────────────────────────────────╯
''
SELECT "g"."a" FROM "g"
''
plan:
    [0] SCAN TABLE g (~1048576 rows)
''
╭───────────────────────────────╮
│ 4. If cond (FILTERED STORAGE) │
╰───────────────────────────────╯
''
SELECT CAST(:a2 AS double) as "cond"
''
plan:
    [0] TRIVIAL
''
╭───────────────────────────────╮
│ 5. If body (FILTERED STORAGE) │
╰───────────────────────────────╯
''
UPDATE "t" SET "b" = CAST(5.5 AS decimal) WHERE "t"."a" = CAST(42 AS int) and "t"."c" = CAST('kek' AS string)
''
plan:
    [0] SEARCH TABLE t USING PRIMARY KEY (c=? AND a=?) (~1 row)
''
╭───────────────────────────────╮
│ 6. If cond (FILTERED STORAGE) │
╰───────────────────────────────╯
''
SELECT CAST(:a1 AS int) = CAST(-1 AS int) as "cond"
''
plan:
    [0] TRIVIAL
''
╭───────────────────────────────╮
│ 7. If body (FILTERED STORAGE) │
╰───────────────────────────────╯
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
╭─────────────────────────────╮
│ 1. Query (FILTERED STORAGE) │
╰─────────────────────────────╯
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
╭─────────────────────────────╮
│ 2. Query (FILTERED STORAGE) │
╰─────────────────────────────╯
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
╭─────────────────────────────╮
│ 3. Query (FILTERED STORAGE) │
╰─────────────────────────────╯
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
╭─────────────────────────────╮
│ 1. Query (FILTERED STORAGE) │
╰─────────────────────────────╯
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
╭─────────────────────────────╮
│ 2. Query (FILTERED STORAGE) │
╰─────────────────────────────╯
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
╭─────────────────────────────╮
│ 3. Query (FILTERED STORAGE) │
╰─────────────────────────────╯
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
