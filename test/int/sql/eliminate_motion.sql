-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (count(*)::int -> col_1)
  group by (t1.a::int)
    selection (t1.a::int = 1::int)
      scan t1
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 1
  projection (count(*)::int -> col_1)
    group by (t1.a::int)
      selection (t1.a::int = 1::int)
        scan t1
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (count(*)::int -> col_1)
  group by (t1.a::int)
    selection ((t1.a::int = 1::int and (t1.a::int < 2::int or t1.a::int > 3::int)))
      scan t1
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (count(*)::int -> col_1)
  group by (t1.a::int)
    selection ((t1.a::int = 1::int and (t1.a::int < 2::int or t1.a::int > 3::int) and t1.b::int = 2::int))
      scan t1
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (count(*)::int -> col_1)
  group by (t1.a::int)
    selection ((t1.a::int = 1::int and t1.a::int < 10::int and t1.b::int = 2::int))
      scan t1
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: group-by-4
-- SQL:
SELECT count(*) FROM t1 WHERE a = 1 OR a = 2 GROUP BY a;
-- UNORDERED:
1, 1

-- TEST: group-by-4-explain
-- SQL:
EXPLAIN SELECT count(*) FROM t1 WHERE a = 1 OR a = 2 GROUP BY a;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (sum(count_1::int::int)::int::int -> col_1)
  group by (gr_expr_1::int)
    motion [policy: full, program: ReshardIfNeeded]
      projection (t1.a::int -> gr_expr_1, count(*)::int -> count_1)
        group by (t1.a::int)
          selection (t1.a::int = 1::int or t1.a::int = 2::int)
            scan t1
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1410, 1934]

-- TEST: group-by-5
-- SQL:
SELECT count(*) FROM t1 WHERE a = 1 AND a < 10 AND a = 2 GROUP BY a;
-- EXPECTED:

-- TEST: group-by-5-explain
-- SQL:
EXPLAIN SELECT count(*) FROM t1 WHERE a = 1 AND a < 10 AND a = 2 GROUP BY a;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (count(*)::int -> col_1)
  group by (t1.a::int)
    selection ((t1.a::int = 1::int and t1.a::int < 10::int and t1.a::int = 2::int))
      scan t1
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = []

-- TEST: distinct-1
-- SQL:
SELECT DISTINCT b FROM t1 WHERE a = 5 AND (a > 4 OR a < 4);
-- EXPECTED:
5

-- TEST: distinct-1-explain
-- SQL:
EXPLAIN SELECT DISTINCT b FROM t1 WHERE a = 5 AND (a > 4 OR a < 4);
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t1.b::int -> b)
  selection ((t1.a::int = 5::int and (t1.a::int > 4::int or t1.a::int < 4::int)))
    scan t1
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 1
  projection (id::int, a::int, b::int)
    order by (id::int)
      scan
        projection (t.id::int -> id, t.a::int -> a, t.b::int -> b)
          selection ((t.a::int = 1::int and t.b::int = 1::int))
            scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (id::int)
  order by (id::int)
    scan
      projection (t.id::int -> id)
        selection (t.a::int = 1::int)
          scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 1
  projection (t.id::int -> id, t.a::int -> a, t.b::int -> b)
    selection (t.a::int = 4::int)
      scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (sum(t.b::int::int)::decimal -> col_1, count(*)::int -> col_2, min(t.id::int::int)::int -> col_3)
  selection (t.a::int = 1::int)
    scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t.b::int -> b, count(*)::int -> col_1)
  having (count(*)::int > 1::int)
    group by (t.b::int)
      selection (t.a::int = 1::int)
        scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: no-optimize-2
-- SQL:
SELECT id FROM t ORDER BY id LIMIT 1;
-- EXPECTED:
1

-- TEST: no-optimize-2-explain
-- SQL:
EXPLAIN SELECT id FROM t ORDER BY id LIMIT 1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: no-optimize-3
-- SQL:
SELECT id FROM t WHERE a = 1 OR a = 4 ORDER BY id LIMIT 1;
-- EXPECTED:
4

-- TEST: no-optimize-3-explain
-- SQL:
EXPLAIN SELECT id FROM t WHERE a = 1 OR a = 4 ORDER BY id LIMIT 1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934, 2752]

-- TEST: sub-1
-- SQL:
SELECT * FROM (SELECT * FROM t WHERE a = 4) s WHERE b = 5 ORDER BY a LIMIT 1;
-- EXPECTED:
4, 4, 5

-- TEST: sub-1-explain
-- SQL:
EXPLAIN SELECT * FROM (SELECT * FROM t WHERE a = 4) s WHERE b = 5 ORDER BY a LIMIT 1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: cte-3
-- SQL:
WITH cte AS (SELECT count(*) as cnt FROM t WHERE a = 1) SELECT cnt FROM cte;
-- EXPECTED:
3

-- TEST: cte-3-explain
-- SQL:
EXPLAIN WITH cte AS (SELECT count(*) as cnt FROM t WHERE a = 1) SELECT cnt FROM cte;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (cte.cnt::int -> cnt)
  scan cte cte($0)
subquery $0:
  projection (count(*)::int -> cnt)
    selection (t.a::int = 1::int)
      scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
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
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: cte-5
-- SQL:
WITH cte AS (SELECT * FROM t WHERE a = 4) SELECT id FROM (SELECT * FROM cte) s WHERE b = 5 ORDER BY id LIMIT 1;
-- EXPECTED:
4

-- TEST: cte-5-explain
-- SQL:
EXPLAIN WITH cte AS (SELECT * FROM t WHERE a = 4) SELECT id FROM (SELECT * FROM cte) s WHERE b = 5 ORDER BY id LIMIT 1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934, 2752]

-- TEST: no-optimize-union
-- SQL:
SELECT id FROM t WHERE a = 1 UNION SELECT id FROM t WHERE a = 4 ORDER BY id LIMIT 1;
-- EXPECTED:
4

-- TEST: no-optimize-union-explain
-- SQL:
EXPLAIN SELECT id FROM t WHERE a = 1 UNION SELECT id FROM t WHERE a = 4 ORDER BY id LIMIT 1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934, 2752]

-- TEST: no-optimize-join
-- SQL:
SELECT t1.id FROM t t1 JOIN t t2 ON t1.b = t2.b WHERE t1.a = 1 AND t2.a = 4 ORDER BY t1.id LIMIT 1;
-- EXPECTED:

-- TEST: no-optimize-join-explain
-- SQL:
EXPLAIN SELECT t1.id FROM t t1 JOIN t t2 ON t1.b = t2.b WHERE t1.a = 1 AND t2.a = 4 ORDER BY t1.id LIMIT 1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: no-optimize-except
-- SQL:
SELECT id FROM t WHERE a = 1 EXCEPT SELECT id FROM t WHERE a = 4 ORDER BY id LIMIT 1;
-- EXPECTED:
6

-- TEST: no-optimize-except-explain
-- SQL:
EXPLAIN SELECT id FROM t WHERE a = 1 EXCEPT SELECT id FROM t WHERE a = 4 ORDER BY id LIMIT 1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934, 2752]

-- TEST: distinct-2
-- SQL:
SELECT DISTINCT b FROM t WHERE a = 4;
-- EXPECTED:
5

-- TEST: distinct-2-explain
-- SQL:
EXPLAIN SELECT DISTINCT b FROM t WHERE a = 4;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t.b::int -> b)
  selection (t.a::int = 4::int)
    scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [2752]

-- TEST: scalar-agg-1
-- SQL:
SELECT count(*) FROM t WHERE a = 1 AND a = 2;
-- EXPECTED:
0

-- TEST: scalar-agg-2
-- SQL:
EXPLAIN SELECT count(*) FROM t WHERE a = 1 AND a = 2;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (count(*)::int -> col_1)
  selection ((t.a::int = 1::int and t.a::int = 2::int))
    scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = []

-- TEST: init-3
-- SQL:
DROP TABLE t;
CREATE TABLE t0 (id INT PRIMARY KEY, a INT, b INT) DISTRIBUTED BY (a);
INSERT INTO t0 VALUES (1, 2, 3), (3, 2, 1), (4, 4, 5), (6, 1, 7), (10, 1, 1), (11, 1, 1);

-- TEST: distinct-3
-- SQL:
SELECT DISTINCT b FROM t0 WHERE a = 1;
-- UNORDERED:
7, 1

-- TEST: distinct-3-explain
-- SKIP_FOR: 1rsX1
-- SQL:
EXPLAIN (RAW) SELECT DISTINCT b FROM t0 WHERE a = 1;
-- EXPECTED:
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/2) │
╰────────────────────────────────────────╯
''
SELECT DISTINCT "t0"."b" FROM "t0" WHERE "t0"."a" = 1::int
''
plan:
    [0] SCAN TABLE t0 (~262144 rows)
    [0] USE TEMP B-TREE FOR DISTINCT

-- TEST: init-single-bucket
-- SQL:
CREATE TABLE sb_t (a INT PRIMARY KEY, b INT);
CREATE TABLE sb_t2 (a INT PRIMARY KEY, b INT);
CREATE TABLE sb_g (a INT PRIMARY KEY, b INT) DISTRIBUTED GLOBALLY;
INSERT INTO sb_t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 5), (7, 7);
INSERT INTO sb_t2 VALUES (1, 10), (2, 21), (3, 30), (4, 41), (5, 50);
INSERT INTO sb_g VALUES (1, 100);

-- TEST: param-key-union-all-of-counts
-- SQL:
SELECT count(*) FROM sb_t WHERE a = $1 UNION ALL SELECT count(*) FROM sb_t2 WHERE a = $1;
-- PARAMS:
1
-- EXPECTED:
1, 1

-- TEST: param-key-union-all-of-counts-no-rows
-- SQL:
SELECT count(*) FROM sb_t WHERE a = $1 UNION ALL SELECT count(*) FROM sb_t2 WHERE a = $1;
-- PARAMS:
100
-- EXPECTED:
0, 0

-- TEST: param-key-union-all-of-counts-null
-- SQL:
SELECT count(*) FROM sb_t WHERE a = $1 UNION ALL SELECT count(*) FROM sb_t2 WHERE a = $1;
-- PARAMS:
null
-- EXPECTED:
0, 0

-- TEST: param-key-union-all-of-counts-explain
-- SQL:
EXPLAIN SELECT count(*) FROM sb_t WHERE a = $1 UNION ALL SELECT count(*) FROM sb_t2 WHERE a = $1;
-- PARAMS:
1
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
union all
  projection (count(*)::int -> col_1)
    selection (sb_t.a::int = 1::int)
      scan sb_t
  projection (count(*)::int -> col_1)
    selection (sb_t2.a::int = 1::int)
      scan sb_t2
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: param-key-join-count
-- SQL:
SELECT count(*) FROM (SELECT * FROM sb_t WHERE a = $1) x JOIN (SELECT * FROM sb_t2 WHERE a = $1) y ON true;
-- PARAMS:
2
-- EXPECTED:
1

-- TEST: colocated-subquery-literal
-- SQL:
SELECT a, b FROM sb_t WHERE a = 3 AND b IN (SELECT b FROM sb_t2 WHERE a = 3);
-- EXPECTED:
3, 30

-- TEST: colocated-subquery-literal-explain
-- SQL:
EXPLAIN SELECT a, b FROM sb_t WHERE a = 3 AND b IN (SELECT b FROM sb_t2 WHERE a = 3);
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (sb_t.a::int -> a, sb_t.b::int -> b)
  selection ((sb_t.a::int = 3::int and sb_t.b::int in ROW($0)))
    scan sb_t
subquery $0:
  scan
    projection (sb_t2.b::int -> b)
      selection (sb_t2.a::int = 3::int)
        scan sb_t2
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1958]

-- TEST: colocated-subquery-param
-- SQL:
SELECT a, b FROM sb_t WHERE a = $1 AND b IN (SELECT b FROM sb_t2 WHERE a = $1);
-- PARAMS:
1
-- EXPECTED:
1, 10

-- TEST: colocated-subquery-param-no-match
-- SQL:
SELECT count(*) FROM (SELECT a FROM sb_t WHERE a = $1 AND b IN (SELECT b FROM sb_t2 WHERE a = $1)) s;
-- PARAMS:
2
-- EXPECTED:
0

-- TEST: colocated-subquery-select-list
-- SQL:
SELECT b, (SELECT b FROM sb_t2 WHERE a = $1) FROM sb_t WHERE a = $1;
-- PARAMS:
2
-- EXPECTED:
20, 21

-- TEST: colocated-not-exists-reads-nothing
-- SQL:
SELECT count(*) FROM (SELECT a FROM sb_t WHERE a = $1 AND NOT EXISTS (SELECT 1 FROM sb_t2 WHERE a = $1 AND a = $2)) s;
-- PARAMS:
1, 2
-- EXPECTED:
1

-- TEST: colocated-not-exists-reads-the-bucket
-- SQL:
SELECT count(*) FROM (SELECT a FROM sb_t WHERE a = $1 AND NOT EXISTS (SELECT 1 FROM sb_t2 WHERE a = $1 AND a = $2)) s;
-- PARAMS:
1, 1
-- EXPECTED:
0

-- TEST: colocated-subquery-many-rows
-- SQL:
SELECT a FROM sb_t WHERE a = 1 AND b = (SELECT b FROM sb_t2 WHERE a = 1 UNION ALL SELECT b FROM sb_t2 WHERE a = 1);
-- ERROR:
Expression subquery returned more than 1 row

-- TEST: subquery-param-pin-stays-inside
-- SQL:
SELECT count(*) FROM (SELECT a FROM sb_t WHERE b = $1 AND NOT EXISTS (SELECT 1 FROM sb_t2 WHERE a = $1 AND a = 5)) s;
-- PARAMS:
7
-- EXPECTED:
1

-- TEST: subquery-param-pin-stays-inside-same-value
-- SQL:
SELECT count(*) FROM (SELECT a FROM sb_t WHERE b = $1 AND NOT EXISTS (SELECT 1 FROM sb_t2 WHERE a = $1 AND a = 5)) s;
-- PARAMS:
5
-- EXPECTED:
0

-- TEST: subquery-contradiction-stays-inside
-- SQL:
SELECT a FROM sb_t WHERE b = 10 AND NOT EXISTS (SELECT 1 FROM sb_t2 WHERE a = 1 AND a = 2);
-- EXPECTED:
1

-- TEST: pinned-sws-literal
-- SQL:
SELECT (SELECT count(*) FROM sb_t WHERE a = 1) > 0;
-- EXPECTED:
true

-- TEST: pinned-sws-literal-explain
-- SQL:
EXPLAIN SELECT (SELECT count(*) FROM sb_t WHERE a = 1) > 0;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (ROW($0) > 0::int -> col_1)
subquery $0:
  scan
    projection (count(*)::int -> col_1)
      selection (sb_t.a::int = 1::int)
        scan sb_t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: pinned-sws-exists-star
-- SQL:
SELECT EXISTS (SELECT * FROM sb_t WHERE a = $1);
-- PARAMS:
1
-- EXPECTED:
true

-- TEST: pinned-sws-exists-star-absent
-- SQL:
SELECT EXISTS (SELECT * FROM sb_t WHERE a = $1);
-- PARAMS:
100
-- EXPECTED:
false

-- TEST: pinned-sws-two-subqueries
-- SQL:
SELECT EXISTS (SELECT 1 FROM sb_t WHERE a = $1) AND (SELECT b FROM sb_t2 WHERE a = $1) > 0;
-- PARAMS:
1
-- EXPECTED:
true

-- TEST: pinned-sws-global-and-sharded
-- SQL:
SELECT (SELECT b FROM sb_g WHERE a = 1), (SELECT count(*) FROM sb_t WHERE a = $1);
-- PARAMS:
1
-- EXPECTED:
100, 1

-- TEST: pinned-sws-chain
-- SQL:
SELECT c + 1 FROM (SELECT (SELECT b FROM sb_t WHERE a = $1) AS c) WHERE c > 0 ORDER BY 1;
-- PARAMS:
1
-- EXPECTED:
11

-- TEST: pinned-sws-chain-explain
-- SQL:
EXPLAIN SELECT c + 1 FROM (SELECT (SELECT b FROM sb_t WHERE a = 1) AS c) WHERE c > 0 ORDER BY 1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (col_1::int)
  order by (1)
    scan
      projection (unnamed_subquery.c::int + 1::int -> col_1)
        selection (unnamed_subquery.c::int > 0::int)
          scan unnamed_subquery
            projection (ROW($0) -> c)
subquery $0:
  scan
    projection (sb_t.b::int -> b)
      selection (sb_t.a::int = 1::int)
        scan sb_t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: pinned-sws-chain-count
-- SQL:
SELECT count(*) FROM (SELECT (SELECT b FROM sb_t WHERE a = $1) AS c) WHERE c > 0;
-- PARAMS:
1
-- EXPECTED:
1

-- TEST: pinned-sws-cte
-- SQL:
WITH x AS (SELECT (SELECT b FROM sb_t WHERE a = $1) AS c) SELECT c FROM x;
-- PARAMS:
1
-- EXPECTED:
10

-- TEST: pinned-sws-no-bucket
-- SQL:
SELECT (SELECT b FROM sb_t WHERE a = 1 AND a = 2);
-- EXPECTED:
null

-- TEST: pinned-sws-empty-next-to-one-bucket
-- SQL:
SELECT (SELECT count(*) FROM sb_t WHERE a = $1 AND a = $2), (SELECT b FROM sb_t2 WHERE a = $1);
-- PARAMS:
1, 2
-- EXPECTED:
0, 10

-- TEST: window-single-bucket
-- SQL:
SELECT b, row_number() OVER (ORDER BY b) FROM sb_t WHERE a = 1;
-- EXPECTED:
10, 1

-- TEST: window-single-bucket-explain
-- SQL:
EXPLAIN SELECT b, row_number() OVER (ORDER BY b) FROM sb_t WHERE a = 1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (sb_t.b::int -> b, row_number() over (order by (sb_t.b::int asc) ) -> col_1)
  selection (sb_t.a::int = 1::int)
    scan sb_t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: union-single-bucket
-- SQL:
SELECT b FROM sb_t WHERE a = $1 UNION SELECT b FROM sb_t2 WHERE a = $1;
-- PARAMS:
1
-- EXPECTED:
10

-- TEST: union-single-bucket-explain
-- SQL:
EXPLAIN SELECT b FROM sb_t WHERE a = 1 UNION SELECT b FROM sb_t2 WHERE a = 1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
union
  projection (sb_t.b::int -> b)
    selection (sb_t.a::int = 1::int)
      scan sb_t
  projection (sb_t2.b::int -> b)
    selection (sb_t2.a::int = 1::int)
      scan sb_t2
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: except-single-bucket
-- SQL:
SELECT b FROM sb_t WHERE a = 5 EXCEPT SELECT b FROM sb_t2 WHERE a = 5;
-- EXPECTED:
5

-- TEST: except-single-bucket-explain
-- SQL:
EXPLAIN SELECT b FROM sb_t WHERE a = 5 EXCEPT SELECT b FROM sb_t2 WHERE a = 5;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
except
  projection (sb_t.b::int -> b)
    selection (sb_t.a::int = 5::int)
      scan sb_t
  projection (sb_t2.b::int -> b)
    selection (sb_t2.a::int = 5::int)
      scan sb_t2
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [219]

-- TEST: colocated-subquery-update
-- SQL:
UPDATE sb_t SET b = (SELECT b FROM sb_t2 WHERE a = $1) WHERE a = $1;
-- PARAMS:
2

-- TEST: colocated-subquery-update-check
-- SQL:
SELECT b FROM sb_t WHERE a = 2;
-- EXPECTED:
21

-- TEST: colocated-subquery-update-literal
-- SQL:
UPDATE sb_t SET b = (SELECT b FROM sb_t2 WHERE a = 4) WHERE a = 4;

-- TEST: colocated-subquery-update-literal-check
-- SQL:
SELECT b FROM sb_t WHERE a = 4;
-- EXPECTED:
41

-- TEST: colocated-subquery-update-explain
-- SQL:
EXPLAIN UPDATE sb_t SET b = (SELECT b FROM sb_t2 WHERE a = 4) WHERE a = 4;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
update sb_t (b = col_0)
  motion [policy: local, program: ReshardIfNeeded]
    projection (ROW($0) -> col_0, sb_t.a::int -> col_1)
      selection (sb_t.a::int = 4::int)
        scan sb_t
subquery $0:
  scan
    projection (sb_t2.b::int -> b)
      selection (sb_t2.a::int = 4::int)
        scan sb_t2
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [2752]

-- TEST: colocated-subquery-delete
-- SQL:
DELETE FROM sb_t WHERE a = $1 AND b IN (SELECT b FROM sb_t2 WHERE a = $1);
-- PARAMS:
3

-- TEST: colocated-subquery-delete-check
-- SQL:
SELECT count(*) FROM sb_t WHERE a = 3;
-- EXPECTED:
0

-- TEST: colocated-subquery-over-empty-scalar-aggregate
-- SQL:
SELECT c, (SELECT b FROM sb_t2 WHERE a = $1) FROM (SELECT count(*) AS c FROM sb_t WHERE a = $1 AND a = $2) s;
-- PARAMS:
2, 1002
-- EXPECTED:
0, 21

-- TEST: colocated-subquery-over-scalar-aggregate-on-another-bucket
-- SQL:
SELECT c, (SELECT b FROM sb_t2 WHERE a = 2) FROM (SELECT count(*) AS c FROM sb_t WHERE a = 2 AND bucket_id = 1) s;
-- EXPECTED:
0, 21

-- TEST: colocated-filter-over-empty-scalar-aggregate
-- SQL:
SELECT count(*) FROM (SELECT c FROM (SELECT count(*) AS c FROM sb_t WHERE a = $1 AND a = $2) s WHERE c < (SELECT b FROM sb_t2 WHERE a = $1)) x;
-- PARAMS:
2, 1002
-- EXPECTED:
1

-- TEST: except-over-empty-scalar-aggregate
-- SQL:
SELECT count(*) FROM (SELECT count(*) AS c FROM sb_t WHERE a = $1 AND a = $2 EXCEPT SELECT b - 21 FROM sb_t2 WHERE a = $1) x;
-- PARAMS:
2, 1002
-- EXPECTED:
0

-- TEST: colocated-subquery-of-window-with-volatile-function
-- SQL:
SELECT pico_instance_uuid() IS NOT NULL, row_number() OVER (ORDER BY b), (SELECT b FROM sb_t2 WHERE a = $1) FROM sb_t WHERE a = $1;
-- PARAMS:
2
-- EXPECTED:
true, 1, 21

-- TEST: colocated-subquery-of-window-under-join
-- SQL:
SELECT w.s FROM (SELECT b, row_number() OVER () AS rn, (SELECT b FROM sb_t2 WHERE a = $1) AS s FROM sb_t WHERE a = $1) w JOIN sb_g ON true;
-- PARAMS:
2
-- EXPECTED:
21

-- TEST: colocated-subquery-of-grouped-window-under-join
-- SQL:
SELECT w.s FROM (SELECT b, (SELECT b FROM sb_t2 WHERE a = $1) AS s, row_number() OVER () AS rn FROM sb_t WHERE a = $1 GROUP BY b) w JOIN sb_g ON true;
-- PARAMS:
2
-- EXPECTED:
21

-- TEST: colocated-subquery-of-window-in-place
-- SQL:
SELECT row_number() OVER (ORDER BY b), (SELECT b FROM sb_t2 WHERE a = $1) FROM sb_t WHERE a = $1;
-- PARAMS:
2
-- EXPECTED:
1, 21

-- TEST: subquery-of-global-filter-rejecting-all-rows
-- SQL:
SELECT count(*) FROM sb_g WHERE a = NULL AND b IN (SELECT b FROM sb_t WHERE a = 1);
-- EXPECTED:
0

-- TEST: having-subquery-union-on-another-bucket
-- SQL:
SELECT b FROM sb_t WHERE a = 1 GROUP BY b HAVING b IN (SELECT b - 11 FROM sb_t2 WHERE a = $1 UNION SELECT b - 11 FROM sb_t2 WHERE a = $1);
-- PARAMS:
2
-- EXPECTED:
10

-- TEST: having-subquery-select-without-scan-on-another-bucket
-- SQL:
SELECT b FROM sb_t WHERE a = 1 GROUP BY b HAVING b < (SELECT (SELECT b FROM sb_t2 WHERE a = $1));
-- PARAMS:
2
-- EXPECTED:
10

-- TEST: having-subquery-window-on-another-bucket
-- SQL:
SELECT b FROM sb_t WHERE a = 1 GROUP BY b HAVING b / 10 IN (SELECT row_number() OVER () FROM sb_t2 WHERE a = $1);
-- PARAMS:
2
-- EXPECTED:
10

-- TEST: having-subquery-over-global-table
-- SQL:
SELECT a FROM sb_g GROUP BY a HAVING a < (SELECT (SELECT b FROM sb_t2 WHERE a = $1));
-- PARAMS:
2
-- EXPECTED:
1

-- TEST: join-condition-subquery-select-without-scan-on-another-bucket
-- SQL:
SELECT sb_t.a FROM sb_t JOIN sb_t2 ON sb_t.a = sb_t2.a AND sb_t.b < (SELECT (SELECT b FROM sb_t2 WHERE a = $1)) + 0;
-- PARAMS:
5
-- UNORDERED:
1, 2, 4, 5

-- TEST: join-condition-subquery-union-on-another-bucket
-- SQL:
SELECT sb_t.a FROM sb_t JOIN sb_t2 ON sb_t.a = sb_t2.a AND sb_t.b < (SELECT b FROM sb_t2 WHERE a = $1 UNION SELECT b FROM sb_t2 WHERE a = $1) + 0;
-- PARAMS:
5
-- UNORDERED:
1, 2, 4, 5

-- TEST: join-condition-subquery-window-on-another-bucket
-- SQL:
SELECT sb_t.a FROM sb_t JOIN sb_t2 ON sb_t.a = sb_t2.a AND sb_t.b > (SELECT row_number() OVER () FROM sb_t2 WHERE a = $1) + 0;
-- PARAMS:
5
-- UNORDERED:
1, 2, 4, 5

-- TEST: join-condition-subquery-over-global-tables
-- SQL:
SELECT g1.a FROM sb_g g1 JOIN sb_g g2 ON g1.a = g2.a AND g1.b > (SELECT (SELECT b FROM sb_t2 WHERE a = $1)) + 0;
-- PARAMS:
5
-- EXPECTED:
1

-- TEST: left-join-condition-subquery-with-global-outer
-- SQL:
SELECT sb_g.a, sb_t.b FROM sb_g LEFT JOIN sb_t ON sb_g.a = sb_t.a AND sb_t.b IN (SELECT b - 40 FROM sb_t2 WHERE a = $1 UNION SELECT b - 40 FROM sb_t2 WHERE a = $1);
-- PARAMS:
5
-- EXPECTED:
1, 10
