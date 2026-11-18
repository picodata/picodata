-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- TEST: invalid-bucket-calculation-gl2640
-- SQL:
CREATE TABLE t (a INT, b DECIMAL, PRIMARY KEY (b));


-- TEST: invalid-bucket-calculation-gl2640.2
-- SQL:
EXPLAIN SELECT * FROM t WHERE b = 1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t.a::int -> a, t.b::decimal -> b)
  selection (t.b::decimal = 1::int)
    scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: invalid-bucket-calculation-gl2640.3
-- SQL:
EXPLAIN SELECT * FROM t WHERE b = 1::decimal;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t.a::int -> a, t.b::decimal -> b)
  selection (t.b::decimal = 1::decimal)
    scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [2135]

-- TEST: invalid-bucket-calculation-gl2640.4
-- SQL:
EXPLAIN SELECT * FROM t WHERE b = 1.0;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t.a::int -> a, t.b::decimal -> b)
  selection (t.b::decimal = 1.0::decimal)
    scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [712]

-- TEST: invalid-bucket-calculation-gl2640.5
-- SQL:
EXPLAIN INSERT INTO t VALUES (1, 1);
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
insert into t on conflict: fail
  motion [policy: segment([ref("COLUMN_2")]), program: ReshardIfNeeded]
    values
      value ROW(1::int, 1::int)
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1934]

-- TEST: invalid-bucket-calculation-gl2640.6
-- SQL:
EXPLAIN INSERT INTO t VALUES (1, 1.0);
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
insert into t on conflict: fail
  motion [policy: segment([ref("COLUMN_2")]), program: ReshardIfNeeded]
    values
      value ROW(1::int, 1.0::decimal)
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [712]

-- TEST: mixed-representations-gl2640.1
-- SQL:
EXPLAIN SELECT * FROM t WHERE b = 1 AND b = 1.0;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t.a::int -> a, t.b::decimal -> b)
  selection ((t.b::decimal = 1::int and t.b::decimal = 1.0::decimal))
    scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = []

-- TEST: mixed-representations-gl2640.1.1
-- SQL:
EXPLAIN SELECT * FROM t WHERE b = 1.00;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t.a::int -> a, t.b::decimal -> b)
  selection (t.b::decimal = 1.00::decimal)
    scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [2158]

-- TEST: mixed-representations-gl2640.2
-- SQL:
EXPLAIN SELECT * FROM t WHERE b = 1.0 OR b = 1.00;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t.a::int -> a, t.b::decimal -> b)
  selection (t.b::decimal = 1.0::decimal or t.b::decimal = 1.00::decimal)
    scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [712, 2158]

-- TEST: mixed-representations-gl2640.3
-- SQL:
EXPLAIN SELECT * FROM (SELECT b FROM t WHERE b = 1.0) AS l
LEFT JOIN (SELECT b FROM t WHERE b = 1.000) AS r ON l.b = r.b;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (l.b::decimal -> b, r.b::decimal -> b)
  left join on (l.b::decimal = r.b::decimal)
    scan l
      projection (t.b::decimal -> b)
        selection (t.b::decimal = 1.0::decimal)
          scan t
    scan r
      projection (t.b::decimal -> b)
        selection (t.b::decimal = 1.000::decimal)
          scan t
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [712]

-- TEST: transitive-another-type-gl2640.1
-- SQL:
CREATE TABLE t2 (a INT PRIMARY KEY, c INT);

-- TEST: transitive-another-type-gl2640.2
-- SQL:
EXPLAIN SELECT * FROM t2 WHERE a = c AND c = 1.0;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t2.a::int -> a, t2.c::int -> c)
  selection ((t2.a::int = t2.c::int and t2.c::int = 1.0::decimal))
    scan t2
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]
