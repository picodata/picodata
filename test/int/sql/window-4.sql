-- TEST: window4
-- SQL:
DROP TABLE IF EXISTS t1;
CREATE TABLE t1(x INT PRIMARY KEY, y INT);
INSERT INTO t1 VALUES(1, 2);
INSERT INTO t1 VALUES(3, 4);
INSERT INTO t1 VALUES(5, 6);
INSERT INTO t1 VALUES(7, 8);
INSERT INTO t1 VALUES(9, 10);


-- TEST: window4-7.1.2
-- SQL:
SELECT * FROM t1 WHERE row_number() OVER (ORDER BY y);
-- ERROR:
rule parsing error

-- TEST: window4-7.1.2
-- SQL:
SELECT count(*) FROM t1 GROUP BY y HAVING row_number()
OVER (ORDER BY y);
-- ERROR:
rule parsing error

-- TEST: window4-7.1.4
-- SQL:
SELECT count(*) FROM t1 GROUP BY row_number() OVER (ORDER BY y);
-- ERROR:
rule parsing error

-- TEST: window4-7.1.5
-- SQL:
SELECT count(*) FROM t1 LIMIT row_number() OVER ();
-- ERROR:
rule parsing error

-- TEST: window4-7.1.6
-- SQL:
SELECT f(x) OVER (ORDER BY y) FROM t1;
-- ERROR:
F() may not be used as a window function

-- TEST: window4-7.1.7
-- SQL:
SELECT max(x) OVER abc FROM t1 WINDOW def AS (ORDER BY y);
-- ERROR:
Window with name abc not found

-- TEST: window4-7.2
-- SQL:
SELECT
    row_number() OVER win,
    sum(y) OVER win,
    max(x) OVER win
FROM t1
WINDOW win AS (ORDER BY x)
-- EXPECTED:
1, 2, 1,
2, 6, 3,
3, 12, 5,
4, 20, 7,
5, 30, 9

-- TEST: window4-7.3
-- SQL:
SELECT row_number() OVER (ORDER BY x) FROM t1;
-- EXPECTED:
1, 2, 3, 4, 5

-- TEST: window4-7.4
-- SQL:
SELECT
    row_number() OVER win,
    sum(x) OVER win
FROM t1
WINDOW win AS (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
-- EXPECTED:
1, 1,
2, 4,
3, 9,
4, 16,
5, 25
