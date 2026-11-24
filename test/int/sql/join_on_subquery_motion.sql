-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- Sharded by id; the join below goes by k, so t2 is copied everywhere,
-- while t1 and the subquery table stay sharded.

-- TEST: init
-- SQL:
CREATE TABLE jsq (id INT PRIMARY KEY, k INT, v INT, f INT);
INSERT INTO jsq VALUES
    (1, 1, 16, 2), (2, 1, 15, 2), (3, 1, 14, 2), (4, 1, 13, 2),
    (5, 1, 12, 2), (6, 1, 11, 2), (7, 1, 10, 2), (8, 1, 9, 2),
    (9, 1, 8, 2), (10, 1, 7, 2), (11, 1, 6, 2), (12, 1, 5, 2),
    (13, 1, 4, 2), (14, 1, 3, 2), (15, 1, 2, 2), (16, 1, 1, 2);

-- TEST: join-on-subquery-co-sharded-with-moved-inner
-- SQL:
SELECT count(*) FROM jsq AS t1
JOIN jsq AS t2
  ON t1.k = t2.k
 AND (t2.id IN (SELECT id FROM jsq AS t3 WHERE t3.f = 2) OR t1.f = 1);
-- EXPECTED:
256

-- TEST: join-on-subquery-resharded-by-moved-inner
-- SQL:
SELECT count(*) FROM jsq AS t1
JOIN jsq AS t2
  ON t1.k = t2.k
 AND (t2.id IN (SELECT v FROM jsq AS t3) OR t1.f = 1);
-- EXPECTED:
256

-- TEST: join-on-subquery-reads-outer
-- SQL:
SELECT count(*) FROM jsq AS t1
JOIN jsq AS t2
  ON t1.k = t2.k
 AND (t1.id IN (SELECT id FROM jsq AS t3 WHERE t3.f = 2) OR t2.f = 1);
-- EXPECTED:
256

-- TEST: join-on-subquery-matches-new-inner-key
-- SQL:
SELECT count(*) FROM jsq AS t1
JOIN jsq AS t2
  ON t1.id = t2.v
 AND (t2.v IN (SELECT id FROM jsq AS t3 WHERE t3.f = 2) OR t1.f = 1);
-- EXPECTED:
16

-- TEST: join-on-subquery-resharded-by-new-inner-key
-- SQL:
SELECT count(*) FROM jsq AS t1
JOIN jsq AS t2
  ON t1.id = t2.v
 AND (t2.v IN (SELECT v FROM jsq AS t3) OR t1.f = 1);
-- EXPECTED:
16

-- TEST: join-on-subquery-matches-old-inner-key
-- SQL:
SELECT count(*) FROM jsq AS t1
JOIN jsq AS t2
  ON t1.id = t2.v
 AND (t2.id IN (SELECT id FROM jsq AS t3) OR t1.f = 1);
-- EXPECTED:
16

-- TEST: join-on-subquery-without-or
-- SQL:
SELECT count(*) FROM jsq AS t1
JOIN jsq AS t2 ON t1.k = t2.k AND t2.id IN (SELECT id FROM jsq AS t3);
-- EXPECTED:
256

-- TEST: left-join-on-subquery-reads-moved-inner
-- SQL:
SELECT count(t2.id) FROM jsq AS t1
LEFT JOIN jsq AS t2 ON t1.k = t2.k AND t2.id IN (SELECT id FROM jsq AS t3);
-- EXPECTED:
256

-- TEST: join-on-subquery-by-bucket-id
-- SQL:
SELECT count(*) FROM jsq AS t1
JOIN jsq AS t2
  ON t1.k = t2.k
 AND t2.bucket_id IN (SELECT bucket_id FROM jsq AS t3);
-- EXPECTED:
256

-- TEST: join-on-aggregate-subquery
-- SQL:
SELECT count(*) FROM jsq AS t1
JOIN jsq AS t2 ON t1.k = t2.k AND t2.id IN (SELECT max(id) FROM jsq AS t3);
-- EXPECTED:
16
