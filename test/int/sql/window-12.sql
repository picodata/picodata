-- TEST: window12
-- SQL:
DROP TABLE IF EXISTS t6;
CREATE TABLE t6(x INT PRIMARY KEY, y INT);
INSERT INTO t6 VALUES(1, 2);
INSERT INTO t6 VALUES(3, 4);
INSERT INTO t6 VALUES(5, 6);
INSERT INTO t6 VALUES(7, 8);
INSERT INTO t6 VALUES(9, 10);

-- TEST: window12-1.1
-- SQL:
SELECT 1 + (sum(x) OVER (ORDER BY x))::int FROM t6;
-- EXPECTED:
2, 5, 10, 17, 26


-- TEST: window12-1.2
-- SQL:
SELECT (sum(y) OVER (ORDER BY x))::int + (sum(x) OVER (ORDER BY x))::int FROM t6;
-- EXPECTED:
3, 10, 21, 36, 55


-- TEST: window12-1.3
-- SQL:
SELECT (sum(x) OVER (ORDER BY x))::int * (count(y) OVER (ORDER BY x))::int FROM t6;
-- EXPECTED:
1, 8, 27, 64, 125


-- TEST: window12-2.1
-- SQL:
select count() over (partition by 1 + false) from t6;
-- ERROR:
sbroad: could not resolve operator overload for +(unsigned, bool)


-- TEST: window12-2.2
-- SQL:
 SELECT count() OVER (PARTITION BY 'hello' - 3) FROM t6;
-- ERROR:
sbroad: could not resolve operator overload for -(text, unsigned)


-- TEST: window12-2.3
-- SQL:
SELECT count(*) OVER (ORDER BY current_date || 5) FROM t6;
-- ERROR:
sbroad: could not resolve operator overload for ||(datetime, unsigned)


-- TEST: window12-2.4
-- SQL:
SELECT count(*) OVER (ORDER BY '2025‑01‑01'::datetime - 'abc') FROM t6;
-- ERROR:
sbroad: could not resolve operator overload for -(datetime, text)


-- TEST: window12-2.5
-- SQL:
SELECT avg(x) FILTER (WHERE x > '2025‑01‑01'::datetime) OVER () FROM t6;
-- ERROR:
sbroad: could not resolve operator overload for >(int, datetime)


-- TEST: window12-2.6
-- SQL:
SELECT  sum(x)  OVER ( ORDER BY x ROWS BETWEEN 1 + '1' PRECEDING AND 1 FOLLOWING) from t6
-- ERROR:
sbroad: could not resolve operator overload for +(unsigned, text)
