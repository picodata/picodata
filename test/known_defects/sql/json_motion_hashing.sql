-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- Equal JSON numbers spelled differently, `42` and `42.0`, are one value to
-- storage. A segment motion sends a row to the bucket of its key's hash, and
-- the router hashes a JSON value by its MessagePack bytes, which keep the
-- spelling: `42` is an integer, `42.0` a decimal with scale 1. So equal values
-- land in different buckets and never meet. Each EXPECTED block records the
-- current, wrong, result.
--
-- json-motion-hashing-except: both inputs sit on one node, so both are
-- reshuffled by the first column. The EXCEPT then runs on the buckets of the
-- left rows only, with the right rows filtered down to those buckets, so it
-- fails even on one replicaset. Correct: no rows.
--
-- json-motion-hashing-window-partition: the input is reshuffled by the
-- PARTITION BY columns and each storage computes the partitions it holds, so
-- one partition of six equal values splits by storage. Correct: a count of 6
-- in every row.
--
-- json-motion-hashing-join-on-reshuffled-key: each side is reshuffled by its
-- first column, JSON, by an EXCEPT against an empty table. The planner then
-- takes both as distributed by that column and joins them where they are,
-- with no motion, so `42` meets only the spellings that hashed to its
-- storage. Correct: ids 1 to 5.

-- TEST: json-motion-hashing-setup
-- SQL:
CREATE TABLE t (id INT PRIMARY KEY, data JSON);
CREATE TABLE u (id INT PRIMARY KEY, data JSON);
CREATE TABLE e (id INT PRIMARY KEY, data JSON);
CREATE TABLE w (id INT PRIMARY KEY, data JSON);
INSERT INTO t VALUES (0, '42'::json);
INSERT INTO u VALUES (1, '42.0'::json), (2, '42.00'::json), (3, '42.000'::json),
                     (4, '42.0000'::json), (5, '42.00000'::json);
INSERT INTO w VALUES (0, '42'::json), (1, '42.0'::json), (2, '42.00'::json),
                     (3, '42.000'::json), (4, '42.0000'::json), (5, '42.00000'::json);

-- TEST: json-motion-hashing-storage-equality
-- SQL:
SELECT count(*) FROM w WHERE data = '42'::json;
-- EXPECTED:
6

-- TEST: json-motion-hashing-except
-- SQL:
SELECT DISTINCT data FROM t EXCEPT SELECT DISTINCT data FROM u;
-- EXPECTED:
42

-- TEST: json-motion-hashing-window-partition
-- SKIP_FOR: 1rsX1
-- SQL:
SELECT id, count(*) OVER (PARTITION BY data) FROM w ORDER BY id;
-- EXPECTED:
0, 3,
1, 3,
2, 3,
3, 3,
4, 3,
5, 3

-- TEST: json-motion-hashing-join-on-reshuffled-key
-- SKIP_FOR: 1rsX1
-- SQL:
SELECT b.id FROM
  (SELECT DISTINCT data, id FROM t EXCEPT SELECT DISTINCT data, id FROM e) a
  JOIN (SELECT DISTINCT data, id FROM u EXCEPT SELECT DISTINCT data, id FROM e) b
  ON a.data = b.data
ORDER BY b.id;
-- EXPECTED:
3,
4
