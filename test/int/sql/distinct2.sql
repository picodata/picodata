-- TEST: init
-- TBD: (CREATE TABLE AS + AUTO PRIMARY KEY)
CREATE TABLE t1(x INTEGER PRIMARY KEY);
INSERT INTO t1 VALUES(0),(1),(2);
CREATE TABLE t2 AS
SELECT DISTINCT a.x AS aa, b.x AS bb
FROM t1 a, t1 b;

-- TEST: distinct2-100
-- TBD: https://git.picodata.io/core/picodata/-/issues/1924
SELECT *, '|' FROM t2 ORDER BY aa, bb;
-- EXPECTED:
0, 0, '|', 0, 1, '|', 0, 2, '|', 1, 0, '|', 1, 1, '|', 1, 2, '|', 2, 0, '|', 2, 1, '|', 2, 2, '|'

-- TEST: init
-- TBD: (CREATE TABLE AS + AUTO PRIMARY KEY)
DROP TABLE IF EXISTS t2;
CREATE TABLE t2 AS
SELECT DISTINCT a.x AS aa, b.x AS bb
FROM t1 a, t1 b
WHERE a.x IN t1 AND b.x IN t1;

-- TEST: distinct2-110
-- TBD: https://git.picodata.io/core/picodata/-/issues/1924
SELECT *, '|' FROM t2 ORDER BY aa, bb;
-- EXPECTED:
0, 0, '|', 0, 1, '|', 0, 2, '|', 1, 0, '|', 1, 1, '|', 1, 2, '|', 2, 0, '|', 2, 1, '|', 2, 2, '|'

-- TEST: init
-- TBD: https://git.picodata.io/core/picodata/-/issues/1929
CREATE TABLE t102 (i0 TEXT PRIMARY KEY);
INSERT INTO t102 VALUES ('0'),('1'),('2');
DROP TABLE IF EXISTS t2;
CREATE TABLE t2 AS
SELECT DISTINCT *
FROM t102 AS t0
JOIN t102 AS t4 ON (t2.i0 IN t102)
NATURAL JOIN t102 AS t3
JOIN t102 AS t1 ON (t0.i0 IN t102)
JOIN t102 AS t2 ON (t2.i0=+t0.i0 OR (t0.i0<>500 AND t2.i0=t1.i0));

-- TEST: distinct2-120
-- TBD: https://git.picodata.io/core/picodata/-/issues/1929
SELECT *, '|' FROM t2 ORDER BY 1, 2, 3, 4, 5;
-- EXPECTED:
0, 0, 0, 0, '|', 0, 0, 1, 0, '|', 0, 0, 1, 1, '|', 0, 0, 2, 0, '|', 0, 0, 2, 2, '|', 0, 1, 0, 0, '|', 0, 1, 1, 0, '|', 0, 1, 1, 1, '|', 0, 1, 2, 0, '|', 0, 1, 2, 2, '|', 0, 2, 0, 0, '|', 0, 2, 1, 0, '|', 0, 2, 1, 1, '|', 0, 2, 2, 0, '|', 0, 2, 2, 2, '|', 1, 0, 0, 0, '|', 1, 0, 0, 1, '|', 1, 0, 1, 1, '|', 1, 0, 2, 1, '|', 1, 0, 2, 2, '|', 1, 1, 0, 0, '|', 1, 1, 0, 1, '|', 1, 1, 1, 1, '|', 1, 1, 2, 1, '|', 1, 1, 2, 2, '|', 1, 2, 0, 0, '|', 1, 2, 0, 1, '|', 1, 2, 1, 1, '|', 1, 2, 2, 1, '|', 1, 2, 2, 2, '|', 2, 0, 0, 0, '|', 2, 0, 0, 2, '|', 2, 0, 1, 1, '|', 2, 0, 1, 2, '|', 2, 0, 2, 2, '|', 2, 1, 0, 0, '|', 2, 1, 0, 2, '|', 2, 1, 1, 1, '|', 2, 1, 1, 2, '|', 2, 1, 2, 2, '|', 2, 2, 0, 0, '|', 2, 2, 0, 2, '|', 2, 2, 1, 1, '|', 2, 2, 1, 2, '|', 2, 2, 2, 2, '|'

-- TEST: init
-- SQL:
CREATE TABLE t4(a INT PRIMARY KEY, b INT NULL,c INT NULL, d INT NULL, e INT NULL, f INT NULL, g INT NULL, h INT NULL, i INT NULL, j INT NULL);
INSERT INTO t4 VALUES(0,1,2,3,4,5,6,7,8,9);
INSERT INTO t4 SELECT a + 10, b, c, d, e, f, g, h, i, j FROM t4;
INSERT INTO t4 SELECT a + 100, b, c, d, e, f, g, h, i, j FROM t4;

-- TEST: distinct2-400
-- SQL:
SELECT DISTINCT a,b,c FROM t4 WHERE a=0 AND b=1;
-- EXPECTED:
0, 1, 2

-- TEST: distinct2-410
-- SQL:
SELECT DISTINCT a,b,c,d FROM t4 WHERE a=0 AND b=1;
-- EXPECTED:
0, 1, 2, 3

-- TEST: distinct2-411
-- SQL:
SELECT DISTINCT d,a,b,c FROM t4 WHERE a=0 AND b=1;
-- EXPECTED:
3, 0, 1, 2

-- TEST: distinct2-420
-- SQL:
SELECT DISTINCT a,b,c,d,e FROM t4 WHERE a=0 AND b=1;
-- EXPECTED:
0, 1, 2, 3, 4

-- TEST: distinct2-430
-- SQL:
SELECT DISTINCT a,b,c,d,e,f FROM t4 WHERE a=0 AND b=1;
-- EXPECTED:
0, 1, 2, 3, 4, 5

-- TEST: init
-- SQL:
CREATE TABLE t6a(x INTEGER PRIMARY KEY);
INSERT INTO t6a VALUES(1);
CREATE TABLE t6b(y INTEGER PRIMARY KEY);
INSERT INTO t6b VALUES(2),(3);

-- TEST: distinct2-600
-- SQL:
SELECT DISTINCT x, x FROM t6a JOIN t6b ON TRUE;
-- EXPECTED:
1, 1

-- TEST: init
-- TBD: https://git.picodata.io/core/picodata/-/issues/1931
CREATE TABLE t7(a, b, c);
WITH s(i) AS (
SELECT 1 UNION ALL SELECT i+1 FROM s WHERE (i+1)<200
)
INSERT INTO t7 SELECT i/100, i/50, i FROM s;

-- TEST: distinct2-700
-- TBD: https://git.picodata.io/core/picodata/-/issues/1931
SELECT DISTINCT a, b FROM t7;
-- EXPECTED:
0, 0, 0, 1, 1, 2, 1, 3

-- TEST: distinct2-720
-- TBD: https://git.picodata.io/core/picodata/-/issues/1931
SELECT DISTINCT a, b+1 FROM t7;
-- EXPECTED:
0, 1, 0, 2, 1, 3, 1, 4

-- TEST: distinct2-730
-- TBD: https://git.picodata.io/core/picodata/-/issues/1931
SELECT DISTINCT a, b+1 FROM t7;
-- EXPECTED:
0, 1, 0, 2, 1, 3, 1, 4

-- TEST: init
-- TBD: https://git.picodata.io/core/picodata/-/issues/1931
CREATE TABLE t8(a, b, c);
WITH s(i) AS (
SELECT 1 UNION ALL SELECT i+1 FROM s WHERE (i+1)<100
)
INSERT INTO t8 SELECT i/40, i/20, i/40 FROM s;

-- TEST: distinct2-800
-- TBD: https://git.picodata.io/core/picodata/-/issues/1931
SELECT DISTINCT a, b, c FROM t8;
-- EXPECTED:
0, 0, 0, 0, 1, 0, 1, 2, 1, 1, 3, 1, 2, 4, 2

-- TEST: distinct2-820
-- TBD: https://git.picodata.io/core/picodata/-/issues/1931
SELECT DISTINCT a, b, c FROM t8 WHERE b=3;
-- EXPECTED:
1, 3, 1

-- TEST: distinct2-830
-- TBD: https://git.picodata.io/core/picodata/-/issues/1931
SELECT DISTINCT a, b, c FROM t8 WHERE b=3;
-- EXPECTED:
1, 3, 1

-- TEST: distinct2-900
-- SQL:
CREATE TABLE t9(v TEXT PRIMARY KEY);
INSERT INTO t9 VALUES
('abcd'), ('Abcd'), ('aBcd'), ('ABcd'), ('abCd'), ('AbCd'), ('aBCd'),
('ABCd'), ('abcD'), ('AbcD'), ('aBcD'), ('ABcD'), ('abCD'), ('AbCD'),
('aBCD'), ('ABCD'),
('wxyz'), ('Wxyz'), ('wXyz'), ('WXyz'), ('wxYz'), ('WxYz'), ('wXYz'),
('WXYz'), ('wxyZ'), ('WxyZ'), ('wXyZ'), ('WXyZ'), ('wxYZ'), ('WxYZ'),
('wXYZ'), ('WXYZ');

-- TEST: distinct2-910
-- TBD: https://git.picodata.io/core/picodata/-/issues/1925
SELECT DISTINCT v COLLATE NOCASE, v FROM t9 ORDER BY +v;
-- EXPECTED:
'ABCD', 'ABCD', 'ABCd', 'ABCd', 'ABcD', 'ABcD', 'ABcd', 'ABcd', 'AbCD', 'AbCD', 'AbCd', 'AbCd', 'AbcD', 'AbcD', 'Abcd', 'Abcd', 'WXYZ', 'WXYZ', 'WXYz', 'WXYz', 'WXyZ', 'WXyZ', 'WXyz', 'WXyz', 'WxYZ', 'WxYZ', 'WxYz', 'WxYz', 'WxyZ', 'WxyZ', 'Wxyz', 'Wxyz', 'aBCD', 'aBCD', 'aBCd', 'aBCd', 'aBcD', 'aBcD', 'aBcd', 'aBcd', 'abCD', 'abCD', 'abCd', 'abCd', 'abcD', 'abcD', 'abcd', 'abcd', 'wXYZ', 'wXYZ', 'wXYz', 'wXYz', 'wXyZ', 'wXyZ', 'wXyz', 'wXyz', 'wxYZ', 'wxYZ', 'wxYz', 'wxYz', 'wxyZ', 'wxyZ', 'wxyz', 'wxyz'

-- TEST: distinct2-920
-- TBD: https://git.picodata.io/core/picodata/-/issues/1925
SELECT DISTINCT v COLLATE NOCASE, v FROM t9 ORDER BY +v;
-- EXPECTED:
'ABCD', 'ABCD', 'ABCd', 'ABCd', 'ABcD', 'ABcD', 'ABcd', 'ABcd', 'AbCD', 'AbCD', 'AbCd', 'AbCd', 'AbcD', 'AbcD', 'Abcd', 'Abcd', 'WXYZ', 'WXYZ', 'WXYz', 'WXYz', 'WXyZ', 'WXyZ', 'WXyz', 'WXyz', 'WxYZ', 'WxYZ', 'WxYz', 'WxYz', 'WxyZ', 'WxyZ', 'Wxyz', 'Wxyz', 'aBCD', 'aBCD', 'aBCd', 'aBCd', 'aBcD', 'aBcD', 'aBcd', 'aBcd', 'abCD', 'abCD', 'abCd', 'abCd', 'abcD', 'abcD', 'abcd', 'abcd', 'wXYZ', 'wXYZ', 'wXYz', 'wXYz', 'wXyZ', 'wXyZ', 'wXyz', 'wXyz', 'wxYZ', 'wxYZ', 'wxYz', 'wxYz', 'wxyZ', 'wxyZ', 'wxyz', 'wxyz'

-- TEST: init
-- TBD: https://git.picodata.io/core/picodata/-/issues/1930
CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER);
CREATE TABLE t2(x INTEGER PRIMARY KEY, y INTEGER);
WITH RECURSIVE c(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM c WHERE x<49)
INSERT INTO t1(b) SELECT x/10 - 1 FROM c;
WITH RECURSIVE c(x) AS (VALUES(-1) UNION ALL SELECT x+1 FROM c WHERE x<19)
INSERT INTO t2(x,y) SELECT x, 1 FROM c;

-- TEST: distinct2-1000
-- TBD: https://git.picodata.io/core/picodata/-/issues/1930
SELECT DISTINCT y FROM t1, t2 WHERE b=x AND b<>-1;
-- EXPECTED:
1, 1

-- TEST: init
-- TBD: https://git.picodata.io/core/picodata/-/issues/1930
CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER);
CREATE INDEX t1b ON t1(b);
CREATE TABLE t2(x INTEGER PRIMARY KEY, y INTEGER);
CREATE INDEX t2y ON t2(y);
WITH RECURSIVE c(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM c WHERE x<49)
INSERT INTO t1(b) SELECT -(x/10 - 1) FROM c;
WITH RECURSIVE c(x) AS (VALUES(-1) UNION ALL SELECT x+1 FROM c WHERE x<19)
INSERT INTO t2(x,y) SELECT -x, 1 FROM c;

-- TEST: distinct2-1010
-- TBD: https://git.picodata.io/core/picodata/-/issues/1930
SELECT DISTINCT y FROM t1, t2 WHERE b=x AND b<>1 ORDER BY y DESC;
-- EXPECTED:
1, 1

-- TEST: distinct2-1020
-- TBD: https://git.picodata.io/core/picodata/-/issues/1930
CREATE TABLE t1(a, b);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<100)
INSERT INTO t1(a, b) SELECT 1, 'no' FROM c;
INSERT INTO t1(a, b) VALUES(1, 'yes');
CREATE TABLE t2(x PRIMARY KEY);
INSERT INTO t2 VALUES('yes');

-- TEST: distinct2-1020
-- TBD: https://git.picodata.io/core/picodata/-/issues/1930
SELECT DISTINCT a FROM t1, t2 WHERE x=b;
-- EXPECTED:
1, 1

-- TEST: distinct2-2000
-- TBD: (INSERT with partial column specification)
CREATE TABLE t0 (c0 INT , c1 INT, c2 INT, PRIMARY KEY (c0, c1));
CREATE TABLE t1 (c2 INT PRIMARY KEY);
INSERT INTO t0(c2) VALUES (0),(1),(3),(4),(5),(6),(7),(8),(9),(10),(11);
INSERT INTO t0(c1) VALUES ('a');
INSERT INTO t1 VALUES (0);

-- TEST: distinct2-2030
-- TBD: https://git.picodata.io/core/picodata/-/issues/1930
CREATE TABLE t2(a, b, c);
WITH c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<64)
INSERT INTO t2 SELECT 'one', i%2, 'one' FROM c;
WITH c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<64)
INSERT INTO t2 SELECT 'two', i%2, 'two' FROM c;
CREATE TABLE t3(x INTEGER PRIMARY KEY);
INSERT INTO t3 VALUES(1);

-- TEST: distinct2-2040
-- TBD: https://git.picodata.io/core/picodata/-/issues/1930
SELECT DISTINCT a, b, x FROM t3 CROSS JOIN t2 ORDER BY a, +b;
-- EXPECTED:
'one', 0, 1, 'one', 1, 1, 'two', 0, 1, 'two', 1, 1

-- TEST: init
-- SQL:
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 INT, c1 INT NOT NULL, c2 TEXT, PRIMARY KEY (c0, c1));
INSERT INTO t0 VALUES (1, 1, NULL), (2, 1, NULL), (3, 1, NULL), (4, 1, NULL), (5, 1, NULL), (6, 1, NULL), (7, 1, NULL), (8, 1, NULL), (9, 1, NULL), (10, 1, NULL), (11, 1, NULL);
INSERT INTO t0 VALUES(12, 1, 'a');

-- TEST: distinct2-3010
-- TBD: https://git.picodata.io/core/picodata/-/issues/1928
SELECT DISTINCT * FROM t0 WHERE NULL IS t0.c0;
-- EXPECTED:


-- TEST: distinct2-3030
-- TBD: https://git.picodata.io/core/picodata/-/issues/1928
SELECT DISTINCT * FROM t0 WHERE NULL IS c0;
-- EXPECTED:


-- TEST: distinct2-4010
-- TBD: https://git.picodata.io/core/picodata/-/issues/1925
CREATE TABLE t1(a, b COLLATE RTRIM);
INSERT INTO t1 VALUES(1, ''), (2, ' '), (3, '  ');

-- TEST: distinct2-4020
-- TBD: https://git.picodata.io/core/picodata/-/issues/1925
SELECT b FROM t1 UNION SELECT 1;
-- EXPECTED:
1

-- TEST: distinct2-5010
-- TBD: https://git.picodata.io/core/picodata/-/issues/1930
CREATE TABLE cnt(a);
WITH RECURSIVE cnt2(x) AS (
VALUES(1) UNION ALL SELECT x+1 FROM cnt2 WHERE x<50
)
INSERT INTO cnt SELECT x FROM cnt2;

-- TEST: distinct2-5010
-- TBD: https://git.picodata.io/core/picodata/-/issues/1932
SELECT DISTINCT abs(random())%5 AS r FROM cnt ORDER BY r;
-- EXPECTED:
0, 1, 2, 3, 4

-- TEST: distinct2-5030
-- TBD: https://git.picodata.io/core/picodata/-/issues/1932
SELECT abs(random())%5 AS r FROM cnt GROUP BY 1 ORDER BY 1;
-- EXPECTED:
0, 1, 2, 3, 4

-- TEST: distinct2-5040
-- TBD: https://git.picodata.io/core/picodata/-/issues/1930
SELECT a FROM cnt WHERE a>45 GROUP BY 1;
-- EXPECTED:
46, 47, 48, 49, 50
