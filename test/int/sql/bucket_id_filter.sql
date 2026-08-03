-- TEST: init
-- SQL:
CREATE TABLE t (a INT PRIMARY KEY, b INT);
INSERT INTO t VALUES (1, 2), (2, 3), (5, 5), (7, 2), (10, 3);
CREATE TABLE g (id INT PRIMARY KEY, grp INT) DISTRIBUTED BY (grp);
INSERT INTO g VALUES (10, 1), (20, 1), (30, 1), (40, 2);

-- TEST: multi-row-order-by-without-reduce-stage
-- SQL:
SELECT id FROM g WHERE bucket_id = 1934 ORDER BY id DESC;
-- EXPECTED:
30,
20,
10

-- TEST: multi-row-order-by-explain
-- SQL:
EXPLAIN SELECT id FROM g WHERE bucket_id = 1934 ORDER BY id DESC;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (id::int)
  order by (id::int desc)
    scan
      projection (g.id::int -> id)
        selection (g.bucket_id::int = 1934::int)
          scan g
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: const-returns-the-row
-- SQL:
SELECT a FROM t WHERE bucket_id = 1934;
-- EXPECTED:
1

-- TEST: const-explain
-- SQL:
EXPLAIN SELECT a FROM t WHERE bucket_id = 1934;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t.a::int -> a)
  selection (t.bucket_id::int = 1934::int)
    scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: const-explain-raw
-- SQL:
EXPLAIN (RAW) SELECT a FROM t WHERE bucket_id = 1934;
-- EXPECTED:
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/2) │
╰────────────────────────────────────────╯
''
SELECT "t"."a" FROM "t" WHERE "t"."bucket_id" = CAST(1934 AS int)
''
plan:
    [0] SEARCH TABLE t USING COVERING INDEX bucket_id (bucket_id=?) (~10 rows)

-- TEST: order-by-needs-no-reduce-stage
-- SQL:
SELECT a FROM t WHERE bucket_id = 1934 ORDER BY a;
-- EXPECTED:
1

-- TEST: order-by-explain
-- SQL:
EXPLAIN SELECT a FROM t WHERE bucket_id = 1934 ORDER BY a;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (a::int)
  order by (a::int)
    scan
      projection (t.a::int -> a)
        selection (t.bucket_id::int = 1934::int)
          scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: limit-explain
-- SQL:
EXPLAIN SELECT a FROM t WHERE bucket_id = 1934 LIMIT 1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 1
  projection (t.a::int -> a)
    selection (t.bucket_id::int = 1934::int)
      scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: aggregate-over-a-single-bucket
-- SQL:
SELECT count(*) FROM g WHERE bucket_id = 1934;
-- EXPECTED:
3

-- TEST: aggregate-out-of-range-still-returns-a-row
-- SQL:
SELECT count(*) FROM g WHERE bucket_id = 999999;
-- EXPECTED:
0

-- TEST: aggregate-anded-with-sharding-key
-- SQL:
SELECT count(*) FROM g WHERE bucket_id = 1934 AND grp = 1;
-- EXPECTED:
3

-- TEST: in-list-explain
-- SQL:
EXPLAIN SELECT a FROM t WHERE bucket_id IN (1934, 1591);
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t.a::int -> a)
  selection (t.bucket_id::int in ROW(1934::int, 1591::int))
    scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1591,1934]

-- TEST: anded-with-unrelated-filter-explain
-- SQL:
EXPLAIN SELECT a FROM t WHERE bucket_id = 1934 AND b = 2;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t.a::int -> a)
  selection ((t.bucket_id::int = 1934::int and t.b::int = 2::int))
    scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: anded-with-sharding-key-order-by
-- SQL:
SELECT id FROM g WHERE bucket_id = 1934 AND grp = 1 ORDER BY id DESC;
-- EXPECTED:
30,
20,
10

-- TEST: ored-arms-order-by
-- SQL:
SELECT id FROM g WHERE (bucket_id = 1934 AND grp = 1) OR grp = 2 ORDER BY id DESC;
-- EXPECTED:
40,
30,
20,
10

-- TEST: join-on-bucket-id-row-form-explain
-- SQL:
EXPLAIN SELECT t1.a FROM t AS t1 JOIN t AS t2 ON t1.bucket_id = 1934 AND t2.bucket_id = 1934;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t1.a::int -> a)
  join on (ROW(t1.bucket_id::int, t2.bucket_id::int) = ROW(1934::int, 1934::int))
    scan t -> t1
    scan t -> t2
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: out-of-range-returns-nothing
-- SQL:
SELECT a FROM t WHERE bucket_id = 999999;
-- EXPECTED:

-- TEST: out-of-range-explain
-- SQL:
EXPLAIN SELECT a FROM t WHERE bucket_id = 999999;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t.a::int -> a)
  selection (t.bucket_id::int = 999999::int)
    scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = []

-- TEST: whole-decimal-const-routes-to-the-bucket
-- SQL:
SELECT a FROM t WHERE bucket_id = 1934.0;
-- EXPECTED:
1

-- TEST: whole-decimal-const-explain
-- SQL:
EXPLAIN SELECT a FROM t WHERE bucket_id = 1934.0;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t.a::int -> a)
  selection (t.bucket_id::int = 1934.0::decimal)
    scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: whole-double-const-routes-to-the-bucket
-- SQL:
SELECT a FROM t WHERE bucket_id = 1934e0;
-- EXPECTED:
1

-- TEST: whole-double-const-explain
-- SQL:
EXPLAIN SELECT a FROM t WHERE bucket_id = 1934e0;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t.a::int -> a)
  selection (t.bucket_id::int = 1934::double)
    scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: fractional-decimal-const-explain
-- SQL:
EXPLAIN SELECT a FROM t WHERE bucket_id = 1934.5;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t.a::int -> a)
  selection (t.bucket_id::int = 1934.5::decimal)
    scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: fractional-double-const-explain
-- SQL:
EXPLAIN SELECT a FROM t WHERE bucket_id = 1.9345e3;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t.a::int -> a)
  selection (t.bucket_id::int = 1934.5::double)
    scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: fractional-const-out-of-range-aggregate-returns-a-single-row
-- SQL:
SELECT count(*) FROM g WHERE bucket_id = 999999.5;
-- EXPECTED:
0

-- TEST: negative-decimal-const-returns-nothing
-- SQL:
SELECT a FROM t WHERE bucket_id = -1.5;
-- EXPECTED:

-- TEST: negative-decimal-const-explain
-- SQL:
EXPLAIN SELECT a FROM t WHERE bucket_id = -1.5;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t.a::int -> a)
  selection (t.bucket_id::int = -1.5::decimal)
    scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = []

-- TEST: string-const-routes-to-the-bucket
-- SQL:
SELECT a FROM t WHERE bucket_id = '1934';
-- EXPECTED:
1

-- TEST: string-const-explain
-- SQL:
EXPLAIN SELECT a FROM t WHERE bucket_id = '1934';
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t.a::int -> a)
  selection (t.bucket_id::int = 1934::int)
    scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: ored-with-unrelated-filter-explain
-- SQL:
EXPLAIN SELECT a FROM t WHERE bucket_id = 1934 OR b > 100;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t.a::int -> a)
  selection (t.bucket_id::int = 1934::int or t.b::int > 100::int)
    scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1-3000]

-- TEST: cast-is-not-a-bucket-predicate-explain
-- SQL:
EXPLAIN SELECT a FROM t WHERE cast(bucket_id as string) = '1934';
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t.a::int -> a)
  selection (t.bucket_id::int::string = '1934'::string)
    scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1-3000]

-- TEST: parameter-routes-at-execution-time
-- SQL:
SELECT a FROM t WHERE bucket_id = ?;
-- PARAMS:
1934
-- EXPECTED:
1

-- TEST: delete-explain
-- SQL:
EXPLAIN DELETE FROM t WHERE bucket_id = 1934;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
delete from t
  motion [policy: local, program: [PrimaryKey(0), ReshardIfNeeded]]
    projection (t.a::int -> pk_col_0)
      selection (t.bucket_id::int = 1934::int)
        scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: init-motion
-- SQL:
CREATE TABLE outer_t (a INT PRIMARY KEY);
INSERT INTO outer_t VALUES (10), (20), (30), (40);
CREATE TABLE inner_t (id INT PRIMARY KEY, grp INT) DISTRIBUTED BY (grp);
INSERT INTO inner_t VALUES (10, 1), (20, 1), (30, 1), (40, 2);

-- TEST: bucket-id-above-resharding-motion
-- SQL:
SELECT i.id FROM (SELECT a FROM outer_t) AS o
JOIN inner_t AS i ON o.a = i.id
WHERE i.bucket_id = 1934
ORDER BY i.id;
-- EXPECTED:
10,
20,
30

-- TEST: parameter-explain
-- SQL:
EXPLAIN SELECT a FROM t WHERE bucket_id = ?;
-- PARAMS:
1934
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t.a::int -> a)
  selection (t.bucket_id::int = 1934::int)
    scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: parameter-out-of-range-returns-nothing
-- SQL:
SELECT a FROM t WHERE bucket_id = ?;
-- PARAMS:
999999
-- EXPECTED:

-- TEST: parameter-out-of-range-explain
-- SQL:
EXPLAIN SELECT a FROM t WHERE bucket_id = ?;
-- PARAMS:
999999
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t.a::int -> a)
  selection (t.bucket_id::int = 999999::int)
    scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = []

-- TEST: parameter-zero-returns-nothing
-- SQL:
SELECT a FROM t WHERE bucket_id = ?;
-- PARAMS:
0
-- EXPECTED:

-- TEST: parameter-in-list
-- SQL:
SELECT a FROM t WHERE bucket_id IN (?, ?) ORDER BY a;
-- PARAMS:
1934,
1410
-- EXPECTED:
1,
2

-- TEST: parameter-anded-with-sharding-key
-- SQL:
SELECT a FROM t WHERE bucket_id = ? AND a = ?;
-- PARAMS:
1934,
1
-- EXPECTED:
1

-- TEST: parameter-conflicting-with-sharding-key
-- SQL:
SELECT a FROM t WHERE bucket_id = ? AND a = ?;
-- PARAMS:
1934,
2
-- EXPECTED:

-- TEST: parameter-order-by
-- SQL:
SELECT id FROM g WHERE bucket_id = ? ORDER BY id DESC;
-- PARAMS:
1934
-- EXPECTED:
30,
20,
10

-- TEST: parameter-order-by-explain
-- SQL:
EXPLAIN SELECT id FROM g WHERE bucket_id = ? ORDER BY id DESC;
-- PARAMS:
1934
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (id::int)
  order by (id::int desc)
    scan
      projection (g.id::int -> id)
        selection (g.bucket_id::int = 1934::int)
          scan g
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: parameter-limit
-- SQL:
SELECT a FROM t WHERE bucket_id = ? LIMIT 1;
-- PARAMS:
1934
-- EXPECTED:
1

-- TEST: parameter-aggregate
-- SQL:
SELECT count(*) FROM g WHERE bucket_id = ?;
-- PARAMS:
1934
-- EXPECTED:
3

-- TEST: parameter-aggregate-out-of-range-still-returns-a-row
-- SQL:
SELECT count(*) FROM g WHERE bucket_id = ?;
-- PARAMS:
999999
-- EXPECTED:
0

-- TEST: parameter-null-aggregate-returns-a-single-row
-- SQL:
SELECT count(*) FROM g WHERE bucket_id = ?;
-- PARAMS:
null
-- EXPECTED:
0

-- TEST: parameter-null-returns-nothing
-- SQL:
SELECT id FROM g WHERE bucket_id = ?;
-- PARAMS:
null
-- EXPECTED:

-- TEST: parameter-whole-decimal-routes-to-the-bucket
-- SQL:
SELECT a FROM t WHERE bucket_id = ?;
-- PARAMS:
Decimal('1934')
-- EXPECTED:
1

-- TEST: parameter-fractional-decimal-explain
-- SQL:
EXPLAIN SELECT a FROM t WHERE bucket_id = ?;
-- PARAMS:
Decimal('1934.5')
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t.a::int -> a)
  selection (t.bucket_id::int = 1934.5::decimal)
    scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: parameter-whole-double-routes-to-the-bucket
-- SQL:
SELECT a FROM t WHERE bucket_id = ?;
-- PARAMS:
1934.0
-- EXPECTED:
1

-- TEST: parameter-fractional-double-explain
-- SQL:
EXPLAIN SELECT a FROM t WHERE bucket_id = ?;
-- PARAMS:
1934.5
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t.a::int -> a)
  selection (t.bucket_id::int = 1934.5::double)
    scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: parameter-fractional-out-of-range-aggregate-returns-a-single-row
-- SQL:
SELECT count(*) FROM g WHERE bucket_id = ?;
-- PARAMS:
999999.5
-- EXPECTED:
0

-- TEST: parameter-null-explain
-- SQL:
EXPLAIN SELECT count(*) FROM g WHERE bucket_id = ?;
-- PARAMS:
null
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (count(*)::int -> col_1)
  selection (g.bucket_id::int = NULL::unknown)
    scan g
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = []

-- TEST: parameter-above-resharding-motion
-- SQL:
SELECT i.id FROM (SELECT a FROM outer_t) AS o
JOIN inner_t AS i ON o.a = i.id
WHERE i.bucket_id = ?
ORDER BY i.id;
-- PARAMS:
1934
-- EXPECTED:
10,
20,
30

-- TEST: block-without-bucket-id-is-not-routable
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT a FROM t;
END $$;
-- ERROR:
transaction cannot be executed on all buckets

-- TEST: block-return-query-on-bucket-id
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT a FROM t WHERE bucket_id = 1934;
END $$;
-- EXPECTED:
1

-- TEST: block-two-statements-same-bucket
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT a FROM t WHERE bucket_id = 1934;
  RETURN QUERY SELECT b FROM t WHERE bucket_id = 1934;
END $$;
-- EXPECTED:
1,
2

-- TEST: block-bucket-id-agrees-with-sharding-key
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT a FROM t WHERE bucket_id = 1934;
  RETURN QUERY SELECT b FROM t WHERE a = 1;
END $$;
-- EXPECTED:
1,
2

-- TEST: block-different-buckets-is-rejected
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT a FROM t WHERE bucket_id = 1934;
  RETURN QUERY SELECT a FROM t WHERE bucket_id = 1410;
END $$;
-- ERROR:
different buckets

-- TEST: block-bucket-id-disagrees-with-sharding-key
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT a FROM t WHERE bucket_id = 1934;
  RETURN QUERY SELECT b FROM t WHERE a = 2;
END $$;
-- ERROR:
different buckets

-- TEST: block-bucket-id-in-list-is-rejected
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT a FROM t WHERE bucket_id IN (1934, 1410);
END $$;
-- ERROR:
transaction can only be executed on a single bucket, got \[1410, 1934\]

-- TEST: block-bucket-id-out-of-range-is-rejected
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT a FROM t WHERE bucket_id = 999999;
END $$;
-- ERROR:
transaction can only be executed on a single bucket, got \[\]

-- TEST: block-bucket-id-ored-with-unrelated-filter-is-rejected
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT a FROM t WHERE bucket_id = 1934 OR b > 100;
END $$;
-- ERROR:
transaction cannot be executed on all buckets

-- TEST: block-bucket-id-with-parameter
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT a FROM t WHERE bucket_id = $1;
END $$;
-- PARAMS:
1934
-- EXPECTED:
1

-- TEST: block-let-on-bucket-id
-- SQL:
DO $$
BEGIN
  LET v = (SELECT a FROM t WHERE bucket_id = 1934);
  RETURN QUERY SELECT v + 100;
END $$;
-- EXPECTED:
101

-- TEST: block-order-by-on-bucket-id
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT id FROM g WHERE bucket_id = 1934 ORDER BY id DESC;
END $$;
-- EXPECTED:
30,
20,
10

-- TEST: block-aggregate-on-bucket-id-is-allowed
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT count(*) FROM g WHERE bucket_id = 1934;
END $$;
-- EXPECTED:
3

-- TEST: block-aggregate-on-sharding-key-is-allowed
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT count(*) FROM g WHERE grp = 1;
END $$;
-- EXPECTED:
3

-- TEST: init-block-dml
-- SQL:
CREATE TABLE blk (a INT PRIMARY KEY, b INT);
INSERT INTO blk VALUES (1, 10), (2, 20);

-- TEST: block-update-on-bucket-id
-- SQL:
DO $$
BEGIN
  UPDATE blk SET b = b + 1 WHERE bucket_id = 1934;
END $$;

-- TEST: block-update-on-bucket-id-check
-- SQL:
SELECT a, b FROM blk ORDER BY a;
-- EXPECTED:
1, 11,
2, 20

-- TEST: block-let-and-dml-on-bucket-id
-- SQL:
DO $$
BEGIN
  LET v = (SELECT b FROM blk WHERE bucket_id = 1410);
  UPDATE blk SET b = v * 2 WHERE bucket_id = 1410;
END $$;

-- TEST: block-let-and-dml-on-bucket-id-check
-- SQL:
SELECT a, b FROM blk ORDER BY a;
-- EXPECTED:
1, 11,
2, 40

-- TEST: block-delete-on-bucket-id
-- SQL:
DO $$
BEGIN
  DELETE FROM blk WHERE bucket_id = 1934;
END $$;

-- TEST: block-delete-on-bucket-id-check
-- SQL:
SELECT a, b FROM blk ORDER BY a;
-- EXPECTED:
2, 40

-- TEST: forward-ro-to-rw-on-bucket-id
-- SQL:
SELECT a FROM t WHERE bucket_id = 1934 OPTION (FORWARD = RO_TO_RW);
-- EXPECTED:
1

-- TEST: forward-ro-to-rw-on-bucket-id-order-by
-- SQL:
SELECT id FROM g WHERE bucket_id = 1934 ORDER BY id DESC OPTION (FORWARD = RO_TO_RW);
-- EXPECTED:
30,
20,
10

-- TEST: forward-ro-to-rw-with-parameter
-- SQL:
SELECT a FROM t WHERE bucket_id = ? OPTION (FORWARD = RO_TO_RW);
-- PARAMS:
1934
-- EXPECTED:
1

-- TEST: forward-ro-to-rw-in-block
-- SQL:
DO $$
BEGIN
  RETURN QUERY SELECT a FROM t WHERE bucket_id = 1934;
END $$ OPTION (FORWARD = RO_TO_RW);
-- EXPECTED:
1

-- TEST: forward-off-on-empty-bucket-set
-- SQL:
SELECT a FROM t WHERE bucket_id = 999999 OPTION (FORWARD = OFF);

-- TEST: forward-off-on-conflicting-bucket-ids
-- SQL:
SELECT a FROM t WHERE bucket_id = 1934 AND bucket_id = 1410 OPTION (FORWARD = OFF);

-- TEST: forward-on-is-always-satisfiable
-- SQL:
SELECT a FROM t WHERE bucket_id = 1934 OPTION (FORWARD = ON);
-- EXPECTED:
1

-- TEST: forward-ro-to-rw-without-bucket-id-is-rejected
-- SQL:
SELECT a FROM t WHERE b = 2 OPTION (FORWARD = RO_TO_RW);
-- ERROR:
cannot satisfy "forward = ro_to_rw": buckets span multiple nodes, try using "forward = on" instead

-- TEST: forward-ro-to-rw-with-bucket-id-ored-is-rejected
-- SQL:
SELECT a FROM t WHERE bucket_id = 1934 OR b = 2 OPTION (FORWARD = RO_TO_RW);
-- ERROR:
cannot satisfy "forward = ro_to_rw": buckets span multiple nodes, try using "forward = on" instead
