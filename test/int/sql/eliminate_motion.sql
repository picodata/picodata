-- TEST: init-1
-- SQL:
CREATE table t1 (a INT PRIMARY KEY, b INT);
INSERT INTO t1 VALUES (1, 2), (2, 3), (5, 5), (7, 2), (10, 3);

-- TEST: group-by-1
-- SQL:
SELECT count(*) FROM t1 WHERE a = 1 GROUP BY a;
-- EXPECTED:
1

-- TEST: group-by-1-explain
-- SQL:
EXPLAIN SELECT count(*) FROM t1 WHERE a = 1 GROUP BY a;
-- EXPECTED:
# Logical plan
''
projection (count(*)::int -> col_1)
  group by (t1.a::int) output (t1.a::int -> a, t1.bucket_id::int -> bucket_id, t1.b::int -> b)
    selection (t1.a::int = 1::int)
      scan t1
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets = [1934]

-- TEST: group-by-2
-- SQL:
SELECT count(*) FROM t1 WHERE a = 1 GROUP BY a LIMIT 1;
-- EXPECTED:
1

-- TEST: group-by-2-explain
-- SQL:
EXPLAIN SELECT count(*) FROM t1 WHERE a = 1 GROUP BY a LIMIT 1;
-- EXPECTED:
# Logical plan
''
limit 1
  projection (count(*)::int -> col_1)
    group by (t1.a::int) output (t1.a::int -> a, t1.bucket_id::int -> bucket_id, t1.b::int -> b)
      selection (t1.a::int = 1::int)
        scan t1
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets = [1934]

-- TEST: group-by-dnf-1
-- SQL:
SELECT count(*) FROM t1 WHERE a = 1 AND (a < 2 OR a > 3) GROUP BY a;
-- EXPECTED:
1

-- TEST: group-by-dnf-1-explain
-- SQL:
EXPLAIN SELECT count(*) FROM t1 WHERE a = 1 AND (a < 2 OR a > 3) GROUP BY a;
-- EXPECTED:
# Logical plan
''
projection (count(*)::int -> col_1)
  group by (t1.a::int) output (t1.a::int -> a, t1.bucket_id::int -> bucket_id, t1.b::int -> b)
    selection ((t1.a::int = 1::int and (t1.a::int < 2::int or t1.a::int > 3::int)))
      scan t1
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets = [1934]

-- TEST: group-by-dnf-2
-- SQL:
SELECT count(*) FROM t1 WHERE a = 1 AND (a < 2 OR a > 3) AND b = 2 GROUP BY a;
-- EXPECTED:
1

-- TEST: group-by-dnf-2-explain
-- SQL:
EXPLAIN SELECT count(*) FROM t1 WHERE a = 1 AND (a < 2 OR a > 3) AND b = 2 GROUP BY a;
-- EXPECTED:
# Logical plan
''
projection (count(*)::int -> col_1)
  group by (t1.a::int) output (t1.a::int -> a, t1.bucket_id::int -> bucket_id, t1.b::int -> b)
    selection ((t1.a::int = 1::int and (t1.a::int < 2::int or t1.a::int > 3::int) and t1.b::int = 2::int))
      scan t1
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets = [1934]

-- TEST: group-by-3
-- SQL:
SELECT count(*) FROM t1 WHERE a = 1 AND a < 10 AND b = 2 GROUP BY a;
-- EXPECTED:
1

-- TEST: group-by-3-explain
-- SQL:
EXPLAIN SELECT count(*) FROM t1 WHERE a = 1 AND a < 10 AND b = 2 GROUP BY a;
-- EXPECTED:
# Logical plan
''
projection (count(*)::int -> col_1)
  group by (t1.a::int) output (t1.a::int -> a, t1.bucket_id::int -> bucket_id, t1.b::int -> b)
    selection ((t1.a::int = 1::int and t1.a::int < 10::int and t1.b::int = 2::int))
      scan t1
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets = [1934]

-- TEST: group-by-4
-- SQL:
SELECT count(*) FROM t1 WHERE a = 1 OR a = 2 GROUP BY a;
-- EXPECTED:
1, 1

-- TEST: group-by-4-explain
-- SQL:
EXPLAIN SELECT count(*) FROM t1 WHERE a = 1 OR a = 2 GROUP BY a;
-- EXPECTED:
# Logical plan
''
projection (sum(count_1::int)::int -> col_1)
  group by (gr_expr_1::int) output (gr_expr_1::int, count_1::int)
    motion [policy: full, program: ReshardIfNeeded]
      projection (t1.a::int -> gr_expr_1, count(*)::int -> count_1)
        group by (t1.a::int) output (t1.a::int -> a, t1.bucket_id::int -> bucket_id, t1.b::int -> b)
          selection (t1.a::int = 1::int or t1.a::int = 2::int)
            scan t1
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets <= [1410,1934]

-- TEST: group-by-5
-- SQL:
SELECT count(*) FROM t1 WHERE a = 1 AND a < 10 AND a = 2 GROUP BY a;
-- EXPECTED:

-- TEST: group-by-5-explain
-- SQL:
EXPLAIN SELECT count(*) FROM t1 WHERE a = 1 AND a < 10 AND a = 2 GROUP BY a;
-- EXPECTED:
# Logical plan
''
projection (sum(count_1::int)::int -> col_1)
  group by (gr_expr_1::int) output (gr_expr_1::int, count_1::int)
    motion [policy: full, program: ReshardIfNeeded]
      projection (t1.a::int -> gr_expr_1, count(*)::int -> count_1)
        group by (t1.a::int) output (t1.a::int -> a, t1.bucket_id::int -> bucket_id, t1.b::int -> b)
          selection ((t1.a::int = 1::int and t1.a::int < 10::int and t1.a::int = 2::int))
            scan t1
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets <= []

-- TEST: distinct-1
-- SQL:
SELECT DISTINCT b FROM t1 WHERE a = 5 AND (a > 4 OR a < 4);
-- EXPECTED:
5

-- TEST: distinct-1-explain
-- SQL:
EXPLAIN SELECT DISTINCT b FROM t1 WHERE a = 5 AND (a > 4 OR a < 4);
-- EXPECTED:
# Logical plan
''
projection (t1.b::int -> b)
  selection ((t1.a::int = 5::int and (t1.a::int > 4::int or t1.a::int < 4::int)))
    scan t1
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets = [219]

-- TEST: init-2
-- SQL:
DROP TABLE t1;
CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT) DISTRIBUTED BY (a);
CREATE INDEX t_idx ON t(b, id);
INSERT INTO t VALUES (1, 2, 3), (3, 2, 1), (4, 4, 5), (6, 1, 7), (10, 1, 1), (11, 1, 1);

-- TEST: order-by-1
-- SQL:
SELECT * FROM t WHERE a = 1 AND b = 1 ORDER BY id LIMIT 1;
-- EXPECTED:
10, 1, 1

-- TEST: order-by-1-explain
-- SQL:
EXPLAIN SELECT * FROM t WHERE a = 1 AND b = 1 ORDER BY id LIMIT 1;
-- EXPECTED:
# Logical plan
''
limit 1
  projection (id::int, a::int, b::int)
    order by (id::int)
      scan
        projection (t.id::int -> id, t.a::int -> a, t.b::int -> b)
          selection ((t.a::int = 1::int and t.b::int = 1::int))
            scan t
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets = [1934]

-- TEST: order-by-2
-- SQL:
SELECT id FROM t WHERE a = 1 ORDER BY id;
-- EXPECTED:
6, 10, 11

-- TEST: order-by-2-explain
-- SQL:
EXPLAIN SELECT id FROM t WHERE a = 1 ORDER BY id;
-- EXPECTED:
# Logical plan
''
projection (id::int)
  order by (id::int)
    scan
      projection (t.id::int -> id)
        selection (t.a::int = 1::int)
          scan t
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets = [1934]

-- TEST: limit-1
-- SQL:
SELECT * FROM t WHERE a = 4 LIMIT 1;
-- EXPECTED:
4, 4, 5

-- TEST: limit-1-explain
-- SQL:
EXPLAIN SELECT * FROM t WHERE a = 4 LIMIT 1;
-- EXPECTED:
# Logical plan
''
limit 1
  projection (t.id::int -> id, t.a::int -> a, t.b::int -> b)
    selection (t.a::int = 4::int)
      scan t
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets = [2752]

-- TEST: agg-1
-- SQL:
SELECT sum(b), count(*), min(id) FROM t WHERE a = 1;
-- EXPECTED:
9, 3, 6

-- TEST: agg-1-explain
-- SQL:
EXPLAIN SELECT sum(b), count(*), min(id) FROM t WHERE a = 1;
-- EXPECTED:
# Logical plan
''
projection (sum(t.b::int::int)::decimal -> col_1, count(*)::int -> col_2, min(t.id::int::int)::int -> col_3)
  selection (t.a::int = 1::int)
    scan t
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets = [1934]

-- TEST: having-1
-- SQL:
SELECT b, count(*) FROM t WHERE a = 1 GROUP BY b HAVING count(*) > 1;
-- EXPECTED:
1, 2

-- TEST: having-1-explain
-- SQL:
EXPLAIN SELECT b, count(*) FROM t WHERE a = 1 GROUP BY b HAVING count(*) > 1;
-- EXPECTED:
# Logical plan
''
projection (t.b::int -> b, count(*)::int -> col_1)
  having (count(*)::int > 1::int)
    group by (t.b::int) output (t.id::int -> id, t.bucket_id::int -> bucket_id, t.a::int -> a, t.b::int -> b)
      selection (t.a::int = 1::int)
        scan t
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets = [1934]

-- TEST: subquery-1
-- SQL:
SELECT id FROM (SELECT * FROM t WHERE a = 4) sub ORDER BY id LIMIT 1;
-- EXPECTED:
4

-- TEST: subquery-1-explain
-- SQL:
EXPLAIN SELECT id FROM (SELECT * FROM t WHERE a = 4) sub ORDER BY id LIMIT 1;
-- EXPECTED:
# Logical plan
''
limit 1
  projection (id::int)
    order by (id::int)
      scan
        projection (sub.id::int -> id)
          scan sub
            projection (t.id::int -> id, t.a::int -> a, t.b::int -> b)
              selection (t.a::int = 4::int)
                scan t
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets = [2752]

-- TEST: no-optimize-1
-- SQL:
SELECT id FROM t WHERE b = 1 ORDER BY id LIMIT 1;
-- EXPECTED:
3

-- TEST: no-optimize-1-explain
-- SQL:
EXPLAIN SELECT id FROM t WHERE b = 1 ORDER BY id LIMIT 1;
-- EXPECTED:
# Logical plan
''
limit 1
  projection (id::int)
    order by (id::int)
      motion [policy: full, program: ReshardIfNeeded]
        limit 1
          projection (id::int)
            order by (id::int)
              scan
                projection (t.id::int -> id)
                  selection (t.b::int = 1::int)
                    scan t
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets = [1-3000]

-- TEST: no-optimize-2
-- SQL:
SELECT id FROM t ORDER BY id LIMIT 1;
-- EXPECTED:
1

-- TEST: no-optimize-2-explain
-- SQL:
EXPLAIN SELECT id FROM t ORDER BY id LIMIT 1;
-- EXPECTED:
# Logical plan
''
limit 1
  projection (id::int)
    order by (id::int)
      motion [policy: full, program: ReshardIfNeeded]
        limit 1
          projection (id::int)
            order by (id::int)
              scan
                projection (t.id::int -> id)
                  scan t
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets = [1-3000]

-- TEST: no-optimize-3
-- SQL:
SELECT id FROM t WHERE a = 1 OR a = 4 ORDER BY id LIMIT 1;
-- EXPECTED:
4

-- TEST: no-optimize-3-explain
-- SQL:
EXPLAIN SELECT id FROM t WHERE a = 1 OR a = 4 ORDER BY id LIMIT 1;
-- EXPECTED:
# Logical plan
''
limit 1
  projection (id::int)
    order by (id::int)
      motion [policy: full, program: ReshardIfNeeded]
        limit 1
          projection (id::int)
            order by (id::int)
              scan
                projection (t.id::int -> id)
                  selection (t.a::int = 1::int or t.a::int = 4::int)
                    scan t
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets <= [1934,2752]

-- TEST: sub-1
-- SQL:
SELECT * FROM (SELECT * FROM t WHERE a = 4) s WHERE b = 5 ORDER BY a LIMIT 1;
-- EXPECTED:
4, 4, 5

-- TEST: sub-1-explain
-- SQL:
EXPLAIN SELECT * FROM (SELECT * FROM t WHERE a = 4) s WHERE b = 5 ORDER BY a LIMIT 1;
-- EXPECTED:
# Logical plan
''
limit 1
  projection (id::int, a::int, b::int)
    order by (a::int)
      scan
        projection (s.id::int -> id, s.a::int -> a, s.b::int -> b)
          selection (s.b::int = 5::int)
            scan s
              projection (t.id::int -> id, t.a::int -> a, t.b::int -> b)
                selection (t.a::int = 4::int)
                  scan t
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets = [2752]

-- TEST: cte-1
-- SQL:
WITH cte AS (SELECT * FROM t WHERE a = 1) SELECT id FROM cte ORDER BY id LIMIT 1;
-- EXPECTED:
6

-- TEST: cte-1-explain
-- SQL:
EXPLAIN WITH cte AS (SELECT * FROM t WHERE a = 1) SELECT id FROM cte ORDER BY id LIMIT 1;
-- EXPECTED:
# Logical plan
''
limit 1
  projection (id::int)
    order by (id::int)
      scan
        projection (cte.id::int -> id)
          scan cte cte($0)
subquery $0:
  projection (t.id::int -> id, t.a::int -> a, t.b::int -> b)
    selection (t.a::int = 1::int)
      scan t
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets = [1934]

-- TEST: cte-2
-- SQL:
WITH cte AS (SELECT * FROM t) SELECT id FROM cte WHERE a = 2 ORDER BY id LIMIT 1;
-- EXPECTED:
1

-- TEST: cte-2-explain
-- SQL:
EXPLAIN WITH cte AS (SELECT * FROM t) SELECT id FROM cte WHERE a = 2 ORDER BY id LIMIT 1;
-- EXPECTED:
# Logical plan
''
limit 1
  projection (id::int)
    order by (id::int)
      scan
        projection (cte.id::int -> id)
          selection (cte.a::int = 2::int)
            scan cte cte($0)
subquery $0:
  motion [policy: full, program: ReshardIfNeeded]
    projection (t.id::int -> id, t.a::int -> a, t.b::int -> b)
      scan t
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets = [1-3000]

-- TEST: cte-3
-- SQL:
WITH cte AS (SELECT count(*) as cnt FROM t WHERE a = 1) SELECT cnt FROM cte;
-- EXPECTED:
3

-- TEST: cte-3-explain
-- SQL:
EXPLAIN WITH cte AS (SELECT count(*) as cnt FROM t WHERE a = 1) SELECT cnt FROM cte;
-- EXPECTED:
# Logical plan
''
projection (cte.cnt::int -> cnt)
  scan cte cte($0)
subquery $0:
  projection (count(*)::int -> cnt)
    selection (t.a::int = 1::int)
      scan t
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets = [1934]

-- TEST: cte-4
-- SQL:
WITH cte AS (SELECT * FROM t WHERE a = 1)
SELECT * FROM cte c1 JOIN cte c2 ON c1.b = c2.b
ORDER BY c1.id LIMIT 1;
-- EXPECTED:
6, 1, 7, 6, 1, 7

-- TEST: cte-4-explain
-- SQL:
EXPLAIN WITH cte AS (SELECT * FROM t WHERE a = 1)
SELECT * FROM cte c1 JOIN cte c2 ON c1.b = c2.b
ORDER BY c1.id LIMIT 1;
-- EXPECTED:
# Logical plan
''
limit 1
  projection (id::int, a::int, b::int, id::int, a::int, b::int)
    order by (id::int)
      motion [policy: full, program: ReshardIfNeeded]
        limit 1
          projection (id::int, a::int, b::int, id::int, a::int, b::int)
            order by (id::int)
              scan
                projection (c1.id::int -> id, c1.a::int -> a, c1.b::int -> b, c2.id::int -> id, c2.a::int -> a, c2.b::int -> b)
                  join on (c1.b::int = c2.b::int)
                    scan cte c1($0)
                    motion [policy: full, program: ReshardIfNeeded]
                      scan cte c2($0)
subquery $0:
  projection (t.id::int -> id, t.a::int -> a, t.b::int -> b)
    selection (t.a::int = 1::int)
      scan t
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets <= [1934]

-- TEST: cte-5
-- SQL:
WITH cte AS (SELECT * FROM t WHERE a = 4) SELECT id FROM (SELECT * FROM cte) s WHERE b = 5 ORDER BY id LIMIT 1;
-- EXPECTED:
4

-- TEST: cte-5-explain
-- SQL:
EXPLAIN WITH cte AS (SELECT * FROM t WHERE a = 4) SELECT id FROM (SELECT * FROM cte) s WHERE b = 5 ORDER BY id LIMIT 1;
-- EXPECTED:
# Logical plan
''
limit 1
  projection (id::int)
    order by (id::int)
      scan
        projection (s.id::int -> id)
          selection (s.b::int = 5::int)
            scan s
              projection (cte.id::int -> id, cte.a::int -> a, cte.b::int -> b)
                scan cte cte($0)
subquery $0:
  projection (t.id::int -> id, t.a::int -> a, t.b::int -> b)
    selection (t.a::int = 4::int)
      scan t
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets = [2752]

-- TEST: no-optimize-union-all
-- SQL:
SELECT id FROM t WHERE a = 1 UNION ALL SELECT id FROM t WHERE a = 4 ORDER BY id LIMIT 1;
-- EXPECTED:
4

-- TEST: no-optimize-union-all-explain
-- SQL:
EXPLAIN SELECT id FROM t WHERE a = 1 UNION ALL SELECT id FROM t WHERE a = 4 ORDER BY id LIMIT 1;
-- EXPECTED:
# Logical plan
''
limit 1
  projection (id::int)
    order by (id::int)
      motion [policy: full, program: ReshardIfNeeded]
        limit 1
          projection (id::int)
            order by (id::int)
              scan
                union all
                  projection (t.id::int -> id)
                    selection (t.a::int = 1::int)
                      scan t
                  projection (t.id::int -> id)
                    selection (t.a::int = 4::int)
                      scan t
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets <= [1934,2752]

-- TEST: no-optimize-union
-- SQL:
SELECT id FROM t WHERE a = 1 UNION SELECT id FROM t WHERE a = 4 ORDER BY id LIMIT 1;
-- EXPECTED:
4

-- TEST: no-optimize-union-explain
-- SQL:
EXPLAIN SELECT id FROM t WHERE a = 1 UNION SELECT id FROM t WHERE a = 4 ORDER BY id LIMIT 1;
-- EXPECTED:
# Logical plan
''
limit 1
  projection (id::int)
    order by (id::int)
      scan
        motion [policy: full, program: RemoveDuplicates]
          limit 1
            projection (id::int)
              order by (id::int)
                scan
                  union
                    projection (t.id::int -> id)
                      selection (t.a::int = 1::int)
                        scan t
                    projection (t.id::int -> id)
                      selection (t.a::int = 4::int)
                        scan t
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets <= [1934,2752]

-- TEST: no-optimize-join
-- SQL:
SELECT t1.id FROM t t1 JOIN t t2 ON t1.b = t2.b WHERE t1.a = 1 AND t2.a = 4 ORDER BY t1.id LIMIT 1;
-- EXPECTED:

-- TEST: no-optimize-join-explain
-- SQL:
EXPLAIN SELECT t1.id FROM t t1 JOIN t t2 ON t1.b = t2.b WHERE t1.a = 1 AND t2.a = 4 ORDER BY t1.id LIMIT 1;
-- EXPECTED:
# Logical plan
''
limit 1
  projection (id::int)
    order by (id::int)
      scan
        projection (t1.id::int -> id)
          selection ((t1.a::int = 1::int and t2.a::int = 4::int))
            join on (t1.b::int = t2.b::int)
              scan t -> t1
              motion [policy: full, program: ReshardIfNeeded]
                projection (t2.id::int -> id, t2.bucket_id::int -> bucket_id, t2.a::int -> a, t2.b::int -> b)
                  scan t -> t2
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets = [1-3000]

-- TEST: no-optimize-except
-- SQL:
SELECT id FROM t WHERE a = 1 EXCEPT SELECT id FROM t WHERE a = 4 ORDER BY id LIMIT 1;
-- EXPECTED:
6

-- TEST: no-optimize-except-explain
-- SQL:
EXPLAIN SELECT id FROM t WHERE a = 1 EXCEPT SELECT id FROM t WHERE a = 4 ORDER BY id LIMIT 1;
-- EXPECTED:
# Logical plan
''
limit 1
  projection (id::int)
    order by (id::int)
      motion [policy: full, program: ReshardIfNeeded]
        limit 1
          projection (id::int)
            order by (id::int)
              scan
                except
                  projection (t.id::int -> id)
                    selection (t.a::int = 1::int)
                      scan t
                  motion [policy: full, program: ReshardIfNeeded]
                    projection (t.id::int -> id)
                      selection (t.a::int = 4::int)
                        scan t
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets <= [1934,2752]

-- TEST: distinct-2
-- SQL:
SELECT DISTINCT b FROM t WHERE a = 4;
-- EXPECTED:
5

-- TEST: distinct-2-explain
-- SQL:
EXPLAIN SELECT DISTINCT b FROM t WHERE a = 4;
-- EXPECTED:
# Logical plan
''
projection (t.b::int -> b)
  selection (t.a::int = 4::int)
    scan t
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets = [2752]

-- TEST: scalar-agg-1
-- SQL:
SELECT count(*) FROM t WHERE a = 1 AND a = 2;
-- EXPECTED:
null

-- TEST: scalar-agg-2
-- SQL:
EXPLAIN SELECT count(*) FROM t WHERE a = 1 AND a = 2;
-- EXPECTED:
# Logical plan
''
projection (sum(count_1::int)::int -> col_1)
  motion [policy: full, program: ReshardIfNeeded]
    projection (count(*)::int -> count_1)
      selection ((t.a::int = 1::int and t.a::int = 2::int))
        scan t
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
''
# Buckets
''
buckets <= []

-- TEST: init-3
-- SQL:
DROP TABLE t;
CREATE TABLE t0 (id INT PRIMARY KEY, a INT, b INT) DISTRIBUTED BY (a);
INSERT INTO t0 VALUES (1, 2, 3), (3, 2, 1), (4, 4, 5), (6, 1, 7), (10, 1, 1), (11, 1, 1);

-- TEST: distinct-3
-- SQL:
SELECT DISTINCT b FROM t0 WHERE a = 1;
-- EXPECTED:
7, 1

-- TEST: distinct-3-explain
-- SQL:
EXPLAIN (RAW) SELECT DISTINCT b FROM t0 WHERE a = 1;
-- EXPECTED:
1. Query (FILTERED STORAGE):
SELECT DISTINCT "t0"."b" FROM "t0" WHERE "t0"."a" = CAST(1 AS int)
+----------+-------+------+------------------------------+
| selectid | order | from | detail                       |
+========================================================+
| 0        | 0     | 0    | SCAN TABLE t0 (~262144 rows) |
|----------+-------+------+------------------------------|
| 0        | 0     | 0    | USE TEMP B-TREE FOR DISTINCT |
+----------+-------+------+------------------------------+
