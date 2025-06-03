-- TEST: init
-- SQL:
CREATE TABLE t1(pk INT PRIMARY KEY, a TEXT, b TEXT, c TEXT);
INSERT INTO t1 VALUES(1, 'a', 'b', 'c');
INSERT INTO t1 VALUES(2, 'A', 'B', 'C');
INSERT INTO t1 VALUES(3, 'a', 'b', 'c');
INSERT INTO t1 VALUES(4, 'A', 'B', 'C');

-- TEST: distinct-2.1
-- SQL:
SELECT DISTINCT a, b FROM t1;
-- EXPECTED(SORTED):
'A', 'B',
'a', 'b'

-- TEST: distinct-2.2
-- SQL:
SELECT DISTINCT b, a FROM t1;
-- EXPECTED(SORTED):
'B', 'A',
'b', 'a'

-- TEST: distinct-2.3
-- SQL:
SELECT DISTINCT a, b, c FROM t1;
-- EXPECTED(SORTED):
'A', 'B', 'C',
'a', 'b', 'c'

-- TEST: distinct-2.4
-- TBD: https://git.picodata.io/core/picodata/-/issues/1924
SELECT DISTINCT a, b FROM t1 ORDER BY a, b, c;
-- EXPECTED(SORTED):
'A', 'B', 'C',
'a', 'b', 'c'

-- TEST: distinct-2.5
-- SQL:
SELECT DISTINCT b FROM t1 WHERE a = 'a';
-- EXPECTED:
'b'

-- TEST: distinct-2.6
-- SQL:
SELECT DISTINCT b FROM t1 ORDER BY b;
-- EXPECTED:
'B',
'b'

-- TEST: distinct-2.7
-- SQL:
SELECT DISTINCT a FROM t1 ORDER BY a;
-- EXPECTED(SORTED):
'A',
'a'

-- TEST: distinct-2.8
-- TBD: https://git.picodata.io/core/picodata/-/issues/1925
SELECT DISTINCT b COLLATE nocase FROM t1;
-- EXPECTED:
'b'

-- TEST: distinct-2.9
-- TBD: https://git.picodata.io/core/picodata/-/issues/1925
SELECT DISTINCT b COLLATE nocase FROM t1 ORDER BY b COLLATE nocase;
-- EXPECTED:
'b'

-- TEST: distinct-2.A
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT (SELECT DISTINCT o.a FROM t1 AS i) FROM t1 AS o ORDER BY rowid;
-- EXPECTED:
'a', 'A', 'a', 'A'

-- TEST: init
-- SQL:
CREATE TABLE t3(pk INT PRIMARY KEY, a INTEGER NULL, b INTEGER NULL, c INT);
INSERT INTO t3 VALUES
(1, null, null, 1),
(2, null, null, 2),
(3, null, 3, 4),
(4, null, 3, 5),
(5, 6, null, 7),
(6, 6, null, 8);

-- TEST: distinct-3.0
-- SQL:
SELECT DISTINCT a, b FROM t3 ORDER BY a, b;
-- EXPECTED:


-- TEST: init
-- TBD: (zeroblob)
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1(a INTEGER PRIMARY KEY);
INSERT INTO t1 VALUES(3);
INSERT INTO t1 VALUES(2);
INSERT INTO t1 VALUES(1);
INSERT INTO t1 VALUES(2);
INSERT INTO t1 VALUES(3);
INSERT INTO t1 VALUES(1);
CREATE TABLE t2(x);
INSERT INTO t2
SELECT DISTINCT
CASE a WHEN 1 THEN x'0000000000'
WHEN 2 THEN zeroblob(5)
ELSE 'xyzzy' END
FROM t1;

-- TEST: distinct-4.1
-- TBD: (quote)
SELECT quote(x) FROM t2 ORDER BY 1;
-- EXPECTED:
''xyzzy'', 'X'0000000000''

-- TEST: init
-- SQL:
DROP TABLE IF EXISTS t1;
CREATE TABLE t1(pk INT PRIMARY KEY, x INT);
INSERT INTO t1 VALUES(1, 3),(2, 1),(3, 5),(4, 2),(5, 6),(6, 4),(7, 5),(8, 1),(9, 3);

-- TEST: distinct-5.1
-- SQL:
SELECT DISTINCT x FROM t1 ORDER BY x ASC;
-- EXPECTED:
1, 2, 3, 4, 5, 6

-- TEST: distinct-5.2
-- SQL:
SELECT DISTINCT x FROM t1 ORDER BY x DESC;
-- EXPECTED:
6, 5, 4, 3, 2, 1

-- TEST: distinct-5.3
-- SQL:
SELECT DISTINCT x FROM t1 ORDER BY x;
-- EXPECTED:
1, 2, 3, 4, 5, 6

-- TEST: distinct-5.4
-- SQL:
SELECT DISTINCT x FROM t1 ORDER BY x ASC;
-- EXPECTED:
1, 2, 3, 4, 5, 6

-- TEST: distinct-5.5
-- SQL:
SELECT DISTINCT x FROM t1 ORDER BY x DESC;
-- EXPECTED:
6, 5, 4, 3, 2, 1

-- TEST: distinct-5.6
-- SQL:
SELECT DISTINCT x FROM t1 ORDER BY x;
-- EXPECTED:
1, 2, 3, 4, 5, 6

-- TEST: distinct-7.0
-- SQL:
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;
DROP TABLE IF EXISTS t5;
CREATE TABLE t1(a INTEGER PRIMARY KEY);
CREATE TABLE t3(a INTEGER PRIMARY KEY);
CREATE TABLE t4(x INT PRIMARY KEY);
CREATE TABLE t5(pk INT PRIMARY KEY, y INT);
INSERT INTO t1 VALUES(2);
INSERT INTO t3 VALUES(2);
INSERT INTO t4 VALUES(2);
INSERT INTO t5 VALUES(1, 1), (2, 2), (3, 2);

-- TEST: distinct-7.1
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
WITH t2(b) AS (SELECT DISTINCT y FROM t5 ORDER BY y)
SELECT * FROM
t4 JOIN t3 ON TRUE JOIN t1 ON TRUE
WHERE (t1.a=t3.a) AND (SELECT count(*) FROM t2 AS y WHERE t4.x!='abc')=t1.a
-- EXPECTED:
2, 2, 2

-- TEST: init
-- SQL:
CREATE TABLE person (pk INT PRIMARY KEY, pid INT) ;
INSERT INTO person VALUES (1, 1), (2, 10), (3, 10);

-- TEST: distinct-8.0
-- SQL:
SELECT DISTINCT pid FROM person where pid = 10;
-- EXPECTED:
10

-- TEST: distinct-9.0
-- SQL:
DROP TABLE IF EXISTS t1;
CREATE TABLE t1(pk INT PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t1 VALUES(1, 'a', 'a');
INSERT INTO t1 VALUES(2, 'a', 'b');
INSERT INTO t1 VALUES(3, 'a', 'c');
INSERT INTO t1 VALUES(4, 'b', 'a');
INSERT INTO t1 VALUES(5, 'b', 'b');
INSERT INTO t1 VALUES(6, 'b', 'c');
INSERT INTO t1 VALUES(7, 'a', 'a');
INSERT INTO t1 VALUES(8, 'b', 'b');
INSERT INTO t1 VALUES(9, 'A', 'A');
INSERT INTO t1 VALUES(10, 'B', 'B');


-- TEST: distinct-9.1
-- SQL:
SELECT DISTINCT a, b FROM t1 ORDER BY a, b
-- EXPECTED:
'A', 'A', 'B', 'B', 'a', 'a', 'a', 'b', 'a', 'c', 'b', 'a', 'b', 'b', 'b', 'c'

-- TEST: distinct-10.1
-- TBD: (DISTINCT without projection)
SELECT  DISTINCT
1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
1,  1,  1,  1,  1
ORDER  BY
'x','x','x','x','x','x','x','x','x','x',
'x','x','x','x','x','x','x','x','x','x',
'x','x','x','x','x','x','x','x','x','x',
'x','x','x','x','x','x','x','x','x','x',
'x','x','x','x','x','x','x','x','x','x',
'x','x','x','x','x','x','x','x','x','x',
'x','x','x','x';
-- EXPECTED:
1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1
