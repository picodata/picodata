-- TEST: window2
-- SQL:
DROP TABLE IF EXISTS t2;
CREATE TABLE t2(a INT PRIMARY KEY, b INT, c INT);
INSERT INTO t2 VALUES(0, 0, 0);
INSERT INTO t2 VALUES(1, 1, 1);
INSERT INTO t2 VALUES(2, 0, 2);
INSERT INTO t2 VALUES(3, 1, 0);
INSERT INTO t2 VALUES(4, 0, 1);
INSERT INTO t2 VALUES(5, 1, 2);
INSERT INTO t2 VALUES(6, 0, 0);


-- TEST: window2-4.2
-- SQL:
SELECT a, sum(a) OVER (PARTITION BY b) FROM t2 ORDER BY a;
-- EXPECTED:
0, 12, 1, 9, 2, 12, 3, 9, 4, 12, 5, 9, 6, 12

-- TEST: window2-4.3
-- SQL:
SELECT a, sum(a) OVER () FROM t2 ORDER BY a;
-- EXPECTED:
0, 21, 1, 21, 2, 21, 3, 21, 4, 21, 5, 21, 6, 21

-- TEST: window2-4.4
-- SQL:
SELECT a, sum(a) OVER (ORDER BY a) FROM t2;
-- EXPECTED:
0, 0, 1, 1, 2, 3, 3, 6, 4, 10, 5, 15, 6, 21

-- TEST: window2-4.5
-- SQL:
SELECT a, sum(a) OVER (PARTITION BY b ORDER BY a) FROM t2 ORDER BY a;
-- EXPECTED:
0, 0, 1, 1, 2, 2, 3, 4, 4, 6, 5, 9, 6, 12

-- TEST: window2-4.6
-- SQL:
SELECT a, sum(a) OVER (PARTITION BY c ORDER BY a) FROM t2 ORDER BY a;
-- EXPECTED:
0, 0, 1, 1, 2, 2, 3, 3, 4, 5, 5, 7, 6, 9

-- TEST: window2-4.7
-- SQL:
SELECT a, sum(a) OVER (PARTITION BY b ORDER BY a DESC) FROM t2
ORDER BY a;
-- EXPECTED:
0, 12, 1, 9, 2, 12, 3, 8, 4, 10, 5, 5, 6, 6

-- TEST: window2-4.8
-- SQL:
SELECT a,
    sum(a) OVER (PARTITION BY b ORDER BY a DESC),
    sum(a) OVER (PARTITION BY c ORDER BY a)
FROM t2 ORDER BY a
-- EXPECTED:
0, 12, 0,
1, 9, 1,
2, 12, 2,
3, 8, 3,
4, 10, 5,
5, 5, 7,
6, 6, 9

-- TEST: window2-4.9
-- SQL:
SELECT a,
    sum(a) OVER (ORDER BY a),
    avg(cast(a as double)) OVER (ORDER BY a)
FROM t2 ORDER BY a;
-- EXPECTED:
0, 0, 0.0,
1, 1, 0.5,
2, 3, 1.0,
3, 6, 1.5,
4, 10, 2.0,
5, 15, 2.5,
6, 21, 3.0

-- TEST: window2-4.10.1
-- SQL:
SELECT a,
    count(*) OVER (ORDER BY a DESC),
    group_concat(cast(a as text), '.') OVER (ORDER BY a DESC)
FROM t2 ORDER BY a DESC;
-- EXPECTED:
6, 1, '6',
5, 2, '6.5',
4, 3, '6.5.4',
3, 4, '6.5.4.3',
2, 5, '6.5.4.3.2',
1, 6, '6.5.4.3.2.1',
0, 7, '6.5.4.3.2.1.0'

-- TEST: window2-4.10.2
-- SQL:
SELECT a,
    count(*) OVER (ORDER BY a DESC),
    group_concat(cast(a as text), '.') OVER (ORDER BY a DESC)
FROM t2 ORDER BY a DESC;
-- EXPECTED:
6, 1, '6',
5, 2, '6.5',
4, 3, '6.5.4',
3, 4, '6.5.4.3',
2, 5, '6.5.4.3.2',
1, 6, '6.5.4.3.2.1',
0, 7, '6.5.4.3.2.1.0'
