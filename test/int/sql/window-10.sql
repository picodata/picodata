-- TEST: initialization
-- SQL:
DROP TABLE IF EXISTS t5;
CREATE TABLE t5(a INTEGER PRIMARY KEY, b TEXT, c TEXT, d INTEGER);
INSERT INTO t5 VALUES(1, 'A', 'one',   5);
INSERT INTO t5 VALUES(2, 'B', 'two',   4);
INSERT INTO t5 VALUES(3, 'A', 'three', 3);
INSERT INTO t5 VALUES(4, 'B', 'four',  2);
INSERT INTO t5 VALUES(5, 'A', 'five',  1);

-- TEST: window-3.3
-- SQL:
SELECT a, count(*) OVER abc, count(*) OVER def FROM t5
WINDOW abc AS (ORDER BY a),
def AS (ORDER BY a DESC)
ORDER BY a;
-- EXPECTED:
1,
1,
5,
2,
2,
4,
3,
3,
3,
4,
4,
2,
5,
5,
1

-- TEST: window-3.5.1
-- SQL:
SELECT a, max(c) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING)
FROM t5;
-- EXPECTED:
1,
NULL,
2,
NULL,
3,
NULL,
4,
NULL,
5,
NULL

-- TEST: window-3.5.2
-- SQL:
SELECT a, max(c) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)
FROM t5;
-- EXPECTED:
1,
NULL,
2,
one,
3,
two,
4,
three,
5,
four

-- TEST: window-3.5.3
-- SQL:
SELECT a, max(c) OVER (ORDER BY a ROWS BETWEEN 0 PRECEDING AND 0 PRECEDING)
FROM t5;
-- EXPECTED:
1,
one,
2,
two,
3,
three,
4,
four,
5,
five

-- TEST: window-3.6.1
-- SQL:
SELECT a, max(c) OVER (ORDER BY a ROWS BETWEEN 2 FOLLOWING AND 1 FOLLOWING)
FROM t5;
-- EXPECTED:
1,
NULL,
2,
NULL,
3,
NULL,
4,
NULL,
5,
NULL

-- TEST: window-3.6.2
-- SQL:
SELECT a, max(c) OVER (ORDER BY a ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
FROM t5;
-- EXPECTED:
1,
two,
2,
three,
3,
four,
4,
five,
5,
NULL

-- TEST: window-3.6.3
-- SQL:
SELECT a, max(c) OVER (ORDER BY a ROWS BETWEEN 0 FOLLOWING AND 0 FOLLOWING)
FROM t5;
-- EXPECTED:
1,
one,
2,
two,
3,
three,
4,
four,
5,
five
