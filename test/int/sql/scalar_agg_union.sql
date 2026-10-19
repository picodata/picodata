-- TEST-MATRIX: pgproto-1rsX1, iproto-2rsX1

-- A one-stage scalar aggregate must run on exactly one node, even when its
-- input is empty. Its input is on the single shard of `a = 1`, but UNION ALL
-- sends the statement to every shard the other side touches as well.
-- With two replicasets `a = 1` and `a = 2` land on different ones.
-- A GROUP BY yields no rows on empty input, so extra shards are harmless.

-- TEST: init
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t (a INT PRIMARY KEY) DISTRIBUTED BY (a);
INSERT INTO t VALUES (1), (2), (3), (4), (5), (6), (7), (8);
DROP TABLE IF EXISTS g;
CREATE TABLE g (a INT PRIMARY KEY) DISTRIBUTED GLOBALLY;
INSERT INTO g VALUES (10), (20);

-- TEST: scalar-agg-top-level
-- SQL:
SELECT count(*) FROM t WHERE a = 1;
-- EXPECTED:
1

-- TEST: scalar-agg-under-union-all
-- SQL:
SELECT count(*) FROM t WHERE a = 1
UNION ALL
SELECT a FROM t WHERE a <> 1;
-- UNORDERED:
1,
2,
3,
4,
5,
6,
7,
8

-- TEST: scalar-agg-join-under-union-all
-- SQL:
SELECT count(*)
FROM (SELECT a FROM t WHERE a = 1) AS l
JOIN (SELECT a FROM t WHERE a = 1) AS r ON l.a = r.a
UNION ALL
SELECT a FROM t WHERE a <> 1;
-- UNORDERED:
1,
2,
3,
4,
5,
6,
7,
8

-- TEST: scalar-agg-join-under-union-all-other-shard
-- SQL:
SELECT count(*)
FROM (SELECT a FROM t WHERE a = 1) AS l
JOIN (SELECT a FROM t WHERE a = 1) AS r ON l.a = r.a
UNION ALL
SELECT a FROM t WHERE a = 2;
-- UNORDERED:
1,
2

-- TEST: scalar-sum-empty-input-under-union-all
-- SQL:
SELECT sum(a) FROM t WHERE a = 100
UNION ALL
SELECT a FROM t WHERE a = 1;
-- UNORDERED:
NULL,
1

-- TEST: scalar-agg-under-nested-union-all
-- SQL:
SELECT count(*) FROM t WHERE a = 1
UNION ALL
SELECT a FROM t WHERE a = 1
UNION ALL
SELECT a FROM t WHERE a <> 1;
-- UNORDERED:
1,
1,
2,
3,
4,
5,
6,
7,
8

-- TEST: scalar-agg-in-where-subquery
-- SQL:
SELECT a FROM t WHERE a = (SELECT count(*) FROM t WHERE a = 1);
-- EXPECTED:
1

-- TEST: group-by-agg-under-union-all
-- SQL:
SELECT count(*) FROM t WHERE a = 1 GROUP BY a
UNION ALL
SELECT a FROM t WHERE a <> 1;
-- UNORDERED:
1,
2,
3,
4,
5,
6,
7,
8

-- TEST: scalar-agg-in-reused-cte
-- SQL:
WITH c(n) AS (SELECT count(*) FROM t WHERE a = 1)
SELECT n FROM c
UNION ALL
SELECT n FROM c
UNION ALL
SELECT a FROM t WHERE a <> 1;
-- UNORDERED:
1,
1,
2,
3,
4,
5,
6,
7,
8

-- TEST: scalar-agg-as-join-input
-- SQL:
SELECT t.a FROM (SELECT count(*) AS n FROM t WHERE a = 1) AS s
JOIN t ON s.n < t.a;
-- UNORDERED:
2,
3,
4,
5,
6,
7,
8

-- TEST: scalar-agg-where-false
-- SQL:
SELECT count(*) FROM t WHERE false;
-- EXPECTED:
0

-- TEST: scalar-agg-contradiction
-- SQL:
SELECT count(*) FROM t WHERE a = 1 AND a = 2;
-- EXPECTED:
0

-- TEST: scalar-agg-where-false-under-union-all
-- SQL:
SELECT count(*) FROM t WHERE false
UNION ALL
SELECT a FROM t WHERE a <> 1;
-- UNORDERED:
0,
2,
3,
4,
5,
6,
7,
8

-- TEST: scalar-agg-contradiction-under-union-all
-- SQL:
SELECT count(*) FROM t WHERE a = 1 AND a = 2
UNION ALL
SELECT a FROM t WHERE a <> 1;
-- UNORDERED:
0,
2,
3,
4,
5,
6,
7,
8

-- TEST: scalar-agg-in-select-subquery
-- SQL:
SELECT (SELECT count(*) FROM t WHERE a = 1) FROM t;
-- UNORDERED:
1,
1,
1,
1,
1,
1,
1,
1

-- TEST: scalar-agg-order-by-limit-in-where-subquery
-- SQL:
SELECT a FROM t
WHERE a = (SELECT count(*) FROM t WHERE a = 1 ORDER BY 1 LIMIT 1);
-- EXPECTED:
1

-- TEST: scalar-agg-in-in-subquery
-- SQL:
SELECT a FROM t WHERE a IN (SELECT count(*) FROM t WHERE a = 1);
-- EXPECTED:
1

-- TEST: scalar-agg-in-not-in-subquery
-- SQL:
SELECT a FROM t WHERE a NOT IN (SELECT count(*) FROM t WHERE a = 1);
-- UNORDERED:
2,
3,
4,
5,
6,
7,
8

-- TEST: scalar-agg-having-under-union-all
-- SQL:
SELECT count(*) FROM t WHERE a = 1 HAVING count(*) < 5
UNION ALL
SELECT a FROM t WHERE a <> 1;
-- UNORDERED:
1,
2,
3,
4,
5,
6,
7,
8

-- TEST: scalar-agg-over-scalar-agg
-- SQL:
SELECT count(*) FROM (SELECT count(*) FROM t WHERE a = 1)
UNION ALL
SELECT a FROM t WHERE a <> 1;
-- UNORDERED:
1,
2,
3,
4,
5,
6,
7,
8

-- TEST: scalar-agg-cte-in-union-all-and-in
-- SQL:
WITH c(n) AS (SELECT count(*) FROM t WHERE a = 1)
SELECT n FROM c
UNION ALL
SELECT a FROM t WHERE a IN (SELECT n FROM c);
-- UNORDERED:
1,
1

-- TEST: scalar-agg-under-union-all-same-bucket
-- SQL:
SELECT count(*) FROM t WHERE a = 1
UNION ALL
SELECT a FROM t WHERE a = 1;
-- UNORDERED:
1,
1

-- TEST: scalar-aggs-in-both-union-all-branches
-- SQL:
SELECT count(*) FROM t WHERE a = 1
UNION ALL
SELECT count(*) FROM t WHERE a = 2;
-- UNORDERED:
1,
1

-- TEST: scalar-agg-under-union-distinct
-- SQL:
SELECT count(*) FROM t WHERE a = 100
UNION
SELECT a FROM t WHERE a <> 1;
-- UNORDERED:
0,
2,
3,
4,
5,
6,
7,
8

-- TEST: scalar-agg-in-subquery-under-union-all
-- SQL:
SELECT * FROM (SELECT count(*) FROM t WHERE a = 1)
UNION ALL
SELECT a FROM t WHERE a <> 1;
-- UNORDERED:
1,
2,
3,
4,
5,
6,
7,
8

-- TEST: scalar-agg-under-union-all-with-global
-- SQL:
SELECT count(*) FROM t WHERE a = 1
UNION ALL
SELECT a FROM g;
-- UNORDERED:
1,
10,
20

-- TEST: scalar-agg-missing-key-under-union-all
-- SQL:
SELECT count(*) FROM t WHERE a = 100
UNION ALL
SELECT a FROM t WHERE a <> 1;
-- UNORDERED:
0,
2,
3,
4,
5,
6,
7,
8

-- TEST: bucket-proof-init
-- SQL:
DELETE FROM t;
INSERT INTO t VALUES (1), (2), (10208);
DELETE FROM g;
INSERT INTO g VALUES (2);

-- TEST: bucket-proof-real-hash
-- SQL:
SELECT a, bucket_id FROM t ORDER BY a;
-- EXPECTED:
1, 1934,
2, 1410,
10208, 1934

-- TEST: bucket-proof-data
-- SQL:
DELETE FROM t WHERE a <> 2;

-- TEST: bucket-proof-contradictory-union
-- SQL:
SELECT count(*) FROM (
    SELECT a FROM t WHERE a = 1 AND a = 10208
    UNION ALL
    SELECT a FROM t WHERE a = 2
) u;
-- EXPECTED:
1

-- TEST: bucket-proof-contradictory-or
-- SQL:
SELECT count(*) FROM t WHERE (a = 1 AND a = 10208) OR a = 2;
-- EXPECTED:
1

-- TEST: bucket-proof-contradictory-join
-- SQL:
SELECT count(*)
FROM (SELECT a FROM t WHERE a = 1 AND a = 10208) l
JOIN (SELECT a FROM t WHERE a = 2) r ON l.a = r.a;
-- EXPECTED:
0

-- TEST: bucket-proof-false-conjunct
-- SQL:
SELECT count(*) FROM t
WHERE (a = 1 AND a = 10208 AND false) OR a = 2;
-- EXPECTED:
1

-- TEST: bucket-proof-common-key
-- SQL:
SELECT count(*) FROM t WHERE (a = 1 AND a = 10208) OR a = 1;
-- EXPECTED:
0

-- TEST: bucket-proof-empty-count
-- SQL:
SELECT count(*) FROM t WHERE false UNION ALL SELECT a FROM t;
-- UNORDERED:
0,
2

-- TEST: bucket-proof-empty-sum
-- SQL:
SELECT sum(a) FROM t WHERE false UNION ALL SELECT a FROM t;
-- UNORDERED:
NULL,
2

-- TEST: bucket-proof-global-false
-- SQL:
SELECT count(*)
FROM (SELECT a FROM t WHERE false) l
JOIN g ON l.a = g.a
UNION ALL
SELECT a FROM t WHERE a = 2;
-- UNORDERED:
0,
2

-- TEST: bucket-proof-global-contradiction
-- SQL:
SELECT count(*)
FROM (SELECT a FROM t WHERE a = 1 AND a = 10208) l
JOIN g ON l.a = g.a
UNION ALL
SELECT a FROM t WHERE a = 2;
-- UNORDERED:
0,
2

-- TEST: bucket-proof-composite-init
-- SQL:
CREATE TABLE composite (a INT, b INT, PRIMARY KEY (a, b))
DISTRIBUTED BY (a, b);
INSERT INTO composite VALUES (1, 2);

-- TEST: bucket-proof-composite-union
-- SQL:
SELECT count(*) FROM (
    SELECT a FROM composite WHERE (a, b) = (1, 2)
    UNION ALL
    SELECT a FROM composite WHERE (a, b) = (1, 2)
) u;
-- EXPECTED:
2

-- TEST: bucket-proof-composite-join
-- SQL:
SELECT count(*)
FROM (SELECT a, b FROM composite WHERE (a, b) = (1, 2)) l
JOIN (SELECT a, b FROM composite WHERE (a, b) = (1, 2)) r
ON (l.a, l.b) = (r.a, r.b);
-- EXPECTED:
1

-- TEST: reset-t
-- SQL:
DELETE FROM t;
INSERT INTO t VALUES (1), (2), (3), (4), (5), (6), (7), (8);

-- TEST: scalar-agg-under-union-under-union-all
-- A UNION is materialized by its full motion. Under UNION ALL it must be read
-- on exactly one shard, whichever holds the bucket of `a = 1`.
-- SQL:
SELECT count(*) FROM t WHERE a = 1
UNION
SELECT a FROM t WHERE a = 1
UNION ALL
SELECT a FROM t;
-- UNORDERED:
1,
1,
2,
3,
4,
5,
6,
7,
8

-- TEST: scalar-agg-under-union-under-two-union-alls
-- SQL:
SELECT count(*) FROM t WHERE a = 1
UNION
SELECT a FROM t WHERE a = 1
UNION ALL
SELECT a FROM t
UNION ALL
SELECT a FROM t;
-- UNORDERED:
1,
1,
1,
2,
2,
3,
3,
4,
4,
5,
5,
6,
6,
7,
7,
8,
8

-- TEST: scalar-agg-over-union-under-union-all
-- SQL:
SELECT count(*) FROM (
    SELECT a FROM t
    UNION
    SELECT a FROM t
    UNION ALL
    SELECT a FROM t WHERE a = 1
) AS u;
-- EXPECTED:
9

-- TEST: union-under-union-all-swapped-names
-- UNION branches match by position, not by name: the motion over the UNION
-- keeps the column order of its first branch.
-- SQL:
SELECT a AS p, a + 100 AS q FROM t WHERE a = 1
UNION
SELECT a + 100 AS q, a AS p FROM t WHERE a = 1
UNION ALL
SELECT a AS q, a + 1000 AS p FROM t WHERE a <= 2;
-- UNORDERED:
1, 101,
101, 1,
1, 1001,
2, 1002
