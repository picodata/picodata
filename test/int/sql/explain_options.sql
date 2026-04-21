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
# Raw plan
''
1. Query (ROUTER):
SELECT * FROM "_pico_table"
+----------+-------+------+----------------------------------------+
| selectid | order | from | detail                                 |
+==================================================================+
| 0        | 0     | 0    | SCAN TABLE _pico_table (~1048576 rows) |
+----------+-------+------+----------------------------------------+
''
# Buckets
''
buckets = any

-- TEST: raw-buckets-join-many
-- SQL:
explain (raw, buckets) select * from t join t on true group by 1, 2, 3, 4, 5, 6 order by 4 limit 5;
-- EXPECTED:
# Raw plan
''
1. Query (STORAGE):
SELECT "t"."a", "t"."b", "t"."c", "t"."bucket_id" FROM "t"
+----------+-------+------+------------------------------+
| selectid | order | from | detail                       |
+========================================================+
| 0        | 0     | 0    | SCAN TABLE t (~1048576 rows) |
+----------+-------+------+------------------------------+
''
2. Query (STORAGE):
SELECT "gr_expr_1", "gr_expr_2", "gr_expr_3", "gr_expr_4", "gr_expr_5", "gr_expr_6" FROM ( SELECT "t"."a" as "gr_expr_1", "t"."b" as "gr_expr_2", "t"."c" as "gr_expr_3", "t"."COL_0" as "gr_expr_4", "t"."COL_1" as "gr_expr_5", "t"."COL_2" as "gr_expr_6" FROM "t" INNER JOIN ( SELECT "COL_0", "COL_1", "COL_2", "COL_3" FROM "TMP_16168701359278756412_0136" ) as "t" ON CAST(true AS bool) GROUP BY "t"."a", "t"."b", "t"."c", "t"."COL_0", "t"."COL_1", "t"."COL_2" ) ORDER BY 4 LIMIT 5
+----------+-------+------+----------------------------------------------------------+
| selectid | order | from | detail                                                   |
+====================================================================================+
| 0        | 0     | 0    | SCAN TABLE t (~1048576 rows)                             |
|----------+-------+------+----------------------------------------------------------|
| 0        | 1     | 1    | SCAN TABLE TMP_16168701359278756412_0136 (~1048576 rows) |
|----------+-------+------+----------------------------------------------------------|
| 0        | 0     | 0    | USE TEMP B-TREE FOR GROUP BY                             |
|----------+-------+------+----------------------------------------------------------|
| 0        | 0     | 0    | USE TEMP B-TREE FOR ORDER BY                             |
+----------+-------+------+----------------------------------------------------------+
''
3. Query (ROUTER):
SELECT "a", "b", "c", "a", "b", "c" FROM ( SELECT "COL_0" as "a", "COL_1" as "b", "COL_2" as "c", "COL_3" as "a", "COL_4" as "b", "COL_5" as "c" FROM ( SELECT "COL_0", "COL_1", "COL_2", "COL_3", "COL_4", "COL_5" FROM "TMP_16395273730977733505_0136" ) GROUP BY "COL_0", "COL_1", "COL_2", "COL_3", "COL_4", "COL_5" ) ORDER BY 4 LIMIT 5
+----------+-------+------+----------------------------------------------------------+
| selectid | order | from | detail                                                   |
+====================================================================================+
| 0        | 0     | 0    | SCAN TABLE TMP_16395273730977733505_0136 (~1048576 rows) |
|----------+-------+------+----------------------------------------------------------|
| 0        | 0     | 0    | USE TEMP B-TREE FOR GROUP BY                             |
|----------+-------+------+----------------------------------------------------------|
| 0        | 0     | 0    | USE TEMP B-TREE FOR ORDER BY                             |
+----------+-------+------+----------------------------------------------------------+
''
# Buckets
''
buckets = [1-3000]

-- TEST: raw-buckets-union-many
-- SQL:
explain (raw, buckets, fmt) select * from t union select * from t group by 1, 2, 3 order by 3 limit 5;
-- EXPECTED:
# Raw plan
''
1. Query (STORAGE):
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
+----------+-------+------+------------------------------+
| selectid | order | from | detail                       |
+========================================================+
| 0        | 0     | 0    | SCAN TABLE t (~1048576 rows) |
+----------+-------+------+------------------------------+
''
2. Query (ROUTER):
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
      "TMP_1873893905113759248_0136"
  )
GROUP BY
  "COL_0",
  "COL_1",
  "COL_2"
+----------+-------+------+---------------------------------------------------------+
| selectid | order | from | detail                                                  |
+===================================================================================+
| 0        | 0     | 0    | SCAN TABLE TMP_1873893905113759248_0136 (~1048576 rows) |
|----------+-------+------+---------------------------------------------------------|
| 0        | 0     | 0    | USE TEMP B-TREE FOR GROUP BY                            |
+----------+-------+------+---------------------------------------------------------+
''
3. Query (STORAGE):
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
      "TMP_11382294507056677330_0136"
  )
ORDER BY
'  3'
LIMIT
'  5'
+----------+-------+------+----------------------------------------------------------+
| selectid | order | from | detail                                                   |
+====================================================================================+
| 2        | 0     | 0    | SCAN TABLE t (~1048576 rows)                             |
|----------+-------+------+----------------------------------------------------------|
| 3        | 0     | 0    | SCAN TABLE TMP_11382294507056677330_0136 (~1048576 rows) |
|----------+-------+------+----------------------------------------------------------|
| 1        | 0     | 0    | COMPOUND SUBQUERIES 2 AND 3 USING TEMP B-TREE (UNION)    |
|----------+-------+------+----------------------------------------------------------|
| 0        | 0     | 0    | SCAN SUBQUERY 1 (~1 row)                                 |
|----------+-------+------+----------------------------------------------------------|
| 0        | 0     | 0    | USE TEMP B-TREE FOR ORDER BY                             |
+----------+-------+------+----------------------------------------------------------+
''
4. Query (ROUTER):
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
        "TMP_3518291152574438074_0136"
    )
  )
ORDER BY
'  3'
LIMIT
'  5'
+----------+-------+------+---------------------------------------------------------+
| selectid | order | from | detail                                                  |
+===================================================================================+
| 0        | 0     | 0    | SCAN TABLE TMP_3518291152574438074_0136 (~1048576 rows) |
|----------+-------+------+---------------------------------------------------------|
| 0        | 0     | 0    | USE TEMP B-TREE FOR ORDER BY                            |
+----------+-------+------+---------------------------------------------------------+
''
# Buckets
''
buckets = unknown

-- TEST: buckets-raw-fmt-delete
-- SQL:
explain (buckets, raw, fmt) delete from t where a = 5 and c = 'lol';
-- EXPECTED:
# Raw plan
''
1. Query (FILTERED STORAGE):
SELECT
  "t"."c" as "pk_col_0",
  "t"."a" as "pk_col_1"
FROM
  "t"
WHERE
  "t"."a" = CAST(5 AS int)
  and "t"."c" = CAST('lol' AS string)
+----------+-------+------+---------------------------------------------------------+
| selectid | order | from | detail                                                  |
+===================================================================================+
| 0        | 0     | 0    | SEARCH TABLE t USING PRIMARY KEY (c=? AND a=?) (~1 row) |
+----------+-------+------+---------------------------------------------------------+
''
# Buckets
''
buckets = [442]

-- TEST: buckets-raw-fmt-update
-- SQL:
explain (buckets, raw, fmt) update t set b = b + 1 where a = 42 and c = 'kek';
-- EXPECTED:
# Raw plan
''
1. Query (FILTERED STORAGE):
SELECT
  "t"."b" + CAST(1 AS int) as "col_0",
  "t"."c" as "col_1",
  "t"."a" as "col_2"
FROM
  "t"
WHERE
  "t"."a" = CAST(42 AS int)
  and "t"."c" = CAST('kek' AS string)
+----------+-------+------+---------------------------------------------------------+
| selectid | order | from | detail                                                  |
+===================================================================================+
| 0        | 0     | 0    | SEARCH TABLE t USING PRIMARY KEY (c=? AND a=?) (~1 row) |
+----------+-------+------+---------------------------------------------------------+
''
# Buckets
''
buckets = [2873]
