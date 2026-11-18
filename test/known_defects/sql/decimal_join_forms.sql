-- TEST-MATRIX: pgproto-2rsX1, iproto-2rsX1

-- A decimal sharding key is hashed by its stored form (gl2640): `1.0` and
-- `1.00` are one value in buckets 712 and 2158, on different replicasets
-- here. A join of two decimal keys is planned as co-located, without a
-- motion, so each replicaset joins only its own rows and the pair is lost:
-- the counts are 0, not 1. `.0` shows the pair found through a motion.
-- In `.3` and `.4` the key `dl.a` is pinned to `1.0`, and the key `dr.a` is
-- equal to it only through its secondary column `b`, equal to `1.00`.

-- TEST: decimal-join-forms-gl2640
-- SQL:
CREATE TABLE dl (k INT, a DECIMAL, b DECIMAL, PRIMARY KEY (a));
CREATE TABLE dr (k INT, a DECIMAL, b DECIMAL, PRIMARY KEY (a));
INSERT INTO dl VALUES (1, 1.0, 1.0);
INSERT INTO dr VALUES (2, 1.00, 1.00);

-- TEST: decimal-join-forms-gl2640.0
-- SQL:
SELECT dl.k, dr.k FROM dl JOIN dr ON true;
-- EXPECTED:
1, 2

-- TEST: decimal-join-forms-gl2640.1
-- SQL:
EXPLAIN SELECT dl.k, dr.k FROM dl JOIN dr ON dl.a = dr.a;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (dl.k::int -> k, dr.k::int -> k)
  join on (dl.a::decimal = dr.a::decimal)
    scan dl
    scan dr
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: decimal-join-forms-gl2640.2
-- SQL:
SELECT count(*) FROM dl JOIN dr ON dl.a = dr.a;
-- EXPECTED:
0

-- TEST: decimal-join-forms-gl2640.3
-- SQL:
EXPLAIN SELECT dl.k, dr.k FROM dl JOIN dr
ON dl.a = 1.0 AND dr.a = dr.b AND dr.b = 1.00;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (dl.k::int -> k, dr.k::int -> k)
  join on (ROW(dr.b::decimal, dl.a::decimal, dr.a::decimal) = ROW(1.00::decimal, 1.0::decimal, dr.b::decimal))
    scan dl
    scan dr
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [712]

-- TEST: decimal-join-forms-gl2640.4
-- SQL:
SELECT count(*) FROM dl JOIN dr
ON dl.a = 1.0 AND dr.a = dr.b AND dr.b = 1.00;
-- EXPECTED:
0
