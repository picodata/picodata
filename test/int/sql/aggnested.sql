-- TEST: init
-- SQL:
CREATE TABLE t1 (a1 INT PRIMARY KEY);
CREATE TABLE t2 (b1 INT PRIMARY KEY);
INSERT INTO t1 VALUES (1), (2), (3);
INSERT INTO t2 VALUES (4), (5);

-- TEST: aggnested-1.1
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT (SELECT string_agg(a1::text,'x') FROM t2) FROM t1;
-- EXPECTED:
'1x2x3'

-- TEST: aggnested-1.2
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT
(SELECT string_agg(a1::text,'x') || '-' || string_agg(b1::text,'y') FROM t2)
FROM t1;
-- EXPECTED:
'1x2x3-4y5',

-- TEST: aggnested-1.3
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT (SELECT string_agg(b1::text, a1::text) FROM t2) FROM t1;
-- EXPECTED:
'415',
'425',
'435'

-- TEST: aggnested-1.4
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT (SELECT string_agg(a1::text, b1::text) FROM t2) FROM t1;
-- EXPECTED:
'151',
'252',
'353'

-- TEST: init
-- SQL:
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1 (
        A1 INTEGER NOT NULL,
        A2 INTEGER NOT NULL,
        A3 INTEGER NOT NULL,
        A4 INTEGER NOT NULL,
        PRIMARY KEY(A1)
);
INSERT INTO t1 VALUES(1, 11, 111, 1111);
INSERT INTO t1 VALUES(2, 22, 222, 2222);
INSERT INTO t1 VALUES(3, 33, 333, 3333);
CREATE TABLE t2 (
        B1 INTEGER NOT NULL,
        B2 INTEGER NOT NULL,
        B3 INTEGER NOT NULL,
        B4 INTEGER NOT NULL,
        PRIMARY KEY(B1)
);
INSERT INTO t2 VALUES(1,88, 888, 8888);
INSERT INTO t2 VALUES(2,99, 999, 9999);

-- TEST: aggnested-2.0
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT
        (SELECT GROUP_CONCAT(CASE WHEN a1=1 THEN 'A' ELSE 'B' END) FROM t2),
        t1.* 
FROM t1;
-- EXPECTED:
'A','B','B 1 11 111 1111'

-- TEST: init
-- SQL:
CREATE TABLE AAA (
aaa_id       INTEGER PRIMARY KEY
);
CREATE TABLE RRR (
rrr_id      INTEGER     PRIMARY KEY,
rrr_date    INTEGER     NOT NULL,
rrr_aaa     INTEGER
);
CREATE TABLE TTT (
ttt_id      INTEGER PRIMARY KEY,
target_aaa  INTEGER NOT NULL,
source_aaa  INTEGER NOT NULL
);
insert into AAA (aaa_id) values (2);
insert into TTT (ttt_id, target_aaa, source_aaa)
values (4469, 2, 2);
insert into TTT (ttt_id, target_aaa, source_aaa)
values (4476, 2, 1);
insert into RRR (rrr_id, rrr_date, rrr_aaa)
values (0, 0, NULL);
insert into RRR (rrr_id, rrr_date, rrr_aaa)
values (2, 4312, 2);

-- TEST: aggnested-3.0
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT i.aaa_id,
        (SELECT sum(CASE WHEN (t.source_aaa = i.aaa_id) THEN 1 ELSE 0 END)
        FROM TTT t
        ) AS segfault
FROM
(
        SELECT curr.rrr_aaa as aaa_id
        FROM RRR curr
        INNER JOIN AAA a ON (curr.rrr_aaa = aaa_id)
        LEFT JOIN RRR r ON (r.rrr_id <> 0 AND r.rrr_date < curr.rrr_date)
        GROUP BY curr.rrr_id
        HAVING r.rrr_date IS NULL
) i;
-- EXPECTED:
2, 1

-- TEST: init
-- SQL:
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1 (
id1 INTEGER PRIMARY KEY,
value1 INTEGER
);
INSERT INTO t1 VALUES(4469,2),(4476,1);
CREATE TABLE t2 (
id2 INTEGER PRIMARY KEY,
value2 INTEGER
);
INSERT INTO t2 VALUES(0,1),(2,2);

-- TEST: aggnested-3.1
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT
        (SELECT sum(value2=xyz) FROM t2)
FROM
(
        SELECT curr.value1 as xyz
        FROM t1 AS curr
        LEFT JOIN t1 AS other ON TRUE
        GROUP BY curr.id1
) i;
-- EXPECTED:
1, 1


-- TEST: init
-- SQL:
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1 (
        pk INT PRIMARY KEY,
        id1 INTEGER,
        value1 INTEGER,
        x1 INTEGER
);
INSERT INTO t1 VALUES (1, 4469,2,98), (2, 4469,1,99), (3, 4469,3,97);
CREATE TABLE t2 (
        value2 INTEGER PRIMARY KEY
);
INSERT INTO t2 VALUES (1);

-- TEST: aggnested-3.2
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT
        (SELECT sum(value2<>xyz) FROM t2)
FROM
        (SELECT value1 as xyz, max(x1) AS pqr
FROM t1
GROUP BY id1);
-- EXPECTED:
1, 0

-- TEST: init
-- SQL:
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1(pk INT PRIMARY KEY, id1 INT, value1 INT);
INSERT INTO t1 VALUES (1, 4469,2), (2, 4469,1);
CREATE TABLE t2 (value2 INT PRIMARY KEY);
INSERT INTO t2 VALUES(1);

-- TEST: aggnested-3.3
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT (SELECT sum(value2=value1) FROM t2), max(value1)
FROM t1
GROUP BY id1;
-- EXPECTED:
0, 2

-- TEST: init
-- SQL:
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1(pk INT PRIMARY KEY, id1 INT, value1 INT);
INSERT INTO t1 VALUES (1, 4469, 12), (2, 4469, 11), (3, 4470, 34);
CREATE INDEX t1id1 ON t1 (id1);
CREATE TABLE t2 (pk INT PRIMARY KEY, value2 INT);
INSERT INTO t2 VALUES (1, 12), (2, 34), (3, 34);
INSERT INTO t2 SELECT value2 + pk, value2 FROM t2;

-- TEST: aggnested-3.11
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT
        max(value1),
        (SELECT count(*) FROM t2 WHERE value2=max(value1))
FROM t1
GROUP BY id1;
-- EXPECTED:
12, 2, 34, 4

-- TEST: aggnested-3.12
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT
        max(value1),
        (SELECT count(*) FROM t2 WHERE value2=value1)
FROM t1
GROUP BY id1;
-- EXPECTED:
12, 2, 34, 4

-- TEST: aggnested-3.13
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT
        value1,
        (SELECT sum(value2=value1) FROM t2)
FROM t1;
-- EXPECTED:
12, 2, 11, 0, 34, 4

-- TEST: aggnested-3.14
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT
        value1,
        (SELECT sum(value2=value1) FROM t2)
FROM t1
WHERE value1 IN (SELECT max(value1) FROM t1 GROUP BY id1);
-- EXPECTED:
12, 2, 34, 4

-- TEST: aggnested-3.16
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT
        max(value1),
        (SELECT sum(value2=value1) FROM t2)
FROM t1
GROUP BY id1;
-- EXPECTED:
12, 2, 34, 4

-- TEST: init
-- SQL:
DROP TABLE IF EXISTS aa;
DROP TABLE IF EXISTS bb;
CREATE TABLE aa(x INT PRIMARY KEY);  INSERT INTO aa(x) VALUES(123);
CREATE TABLE bb(y INT PRIMARY KEY);  INSERT INTO bb(y) VALUES(456);

-- TEST: aggnested-4.1
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT (SELECT sum(x+(SELECT y)) FROM bb) FROM aa;
-- EXPECTED:
579

-- TEST: aggnested-4.2
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT (SELECT sum(x+y) FROM bb) FROM aa;
-- EXPECTED:
579

-- TEST: init
-- SQL:
DROP TABLE IF EXISTS tx;
DROP TABLE IF EXISTS ty;
CREATE TABLE tx(x INT PRIMARY KEY);
INSERT INTO tx VALUES(1),(2),(3),(4),(5);
CREATE TABLE ty(y INT PRIMARY KEY);
INSERT INTO ty VALUES(91),(92),(93);

-- TEST: aggnested-4.3
-- SQL:
SELECT min((SELECT count(y) FROM ty)) FROM tx;
-- EXPECTED:
3

-- TEST: aggnested-4.4
-- SQL:
SELECT
        max((SELECT a FROM (SELECT count(*) AS a FROM ty) AS s))
FROM tx;
-- EXPECTED:
3

-- TEST: aggnested-5.0
-- SQL:
CREATE TABLE x1(a INT PRIMARY KEY, b INT);
INSERT INTO x1 VALUES(1, 2);
CREATE TABLE x2(pk INT PRIMARY KEY, x INT);
INSERT INTO x2 VALUES (1, NULL), (2, NULL), (3, NULL);

-- TEST: aggnested-5.1
-- SQL:
SELECT
        (SELECT total((SELECT b FROM x1)))
FROM x2;
-- EXPECTED:
2.0, 2.0, 2.0

-- TEST: aggnested-5.2
-- SQL:
SELECT ( SELECT total( (SELECT 2 FROM x1) ) ) FROM x2;
-- EXPECTED:
2.0, 2.0, 2.0

-- TEST: aggnested-5.3
-- SQL:
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1(a INT PRIMARY KEY);
CREATE TABLE t2(b INT PRIMARY KEY);

-- TEST: aggnested-5.4
-- TBD: https://git.picodata.io/core/picodata/-/issues/1483
SELECT(
        SELECT max(b) LIMIT (
                SELECT total( (SELECT a FROM t1) )
        )
)
FROM t2;
-- EXPECTED:


-- TEST: init
-- SQL:
CREATE TABLE a(b INT PRIMARY KEY);

-- TEST: aggnested-5.5
-- TBD: https://git.picodata.io/core/picodata/-/issues/1483
WITH c AS(SELECT a)
SELECT(
        SELECT(SELECT string_agg(b, b)
        LIMIT(
                SELECT 0.100000 * AVG(DISTINCT(SELECT 0 FROM a ORDER BY b, b, b))
        )
)
FROM a GROUP BY b, b, b)
FROM a EXCEPT SELECT b FROM a ORDER BY b, b, b;

-- TEST: aggnested-6.0
-- SQL:
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1(a TEXT PRIMARY KEY);
CREATE TABLE t2(pk INT PRIMARY KEY, b INT);
INSERT INTO t1 VALUES('x');
INSERT INTO t2 VALUES(1, 1);

-- TEST: aggnested-6.1.1
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT (
        SELECT t2.b FROM (SELECT t2.b AS c FROM t1)
        GROUP BY 1
        HAVING t2.b
)
FROM t2 GROUP BY 'constant_string';
-- EXPECTED:
1

-- TEST: aggnested-6.1.2
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT (
        SELECT c FROM (SELECT t2.b AS c FROM t1)
        GROUP BY c
        HAVING t2.b
)
FROM t2 GROUP BY 'constant_string';
-- EXPECTED:
1

-- TEST: aggnested-6.2.0
-- SQL
UPDATE t2 SET b=0;

-- TEST: aggnested-6.2.1
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT (
        SELECT t2.b FROM (SELECT t2.b AS c FROM t1)
        GROUP BY 1
        HAVING t2.b
)
FROM t2 GROUP BY 'constant_string';
-- EXPECTED:


-- TEST: aggnested-6.2.2
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT (
        SELECT c FROM
        (SELECT t2.b AS c FROM t1)
        GROUP BY c
        HAVING t2.b
)
FROM t2 GROUP BY 'constant_string';
-- EXPECTED:


-- TEST: aggnested-7.0
-- SQL:
CREATE TABLE invoice (
        id INTEGER PRIMARY KEY,
        amount DOUBLE NULL,
        name VARCHAR(100) NULL
);
INSERT INTO invoice VALUES
(1, 4.0, 'Michael'),
(2, 15.0, 'Bara'),
(3, 4.0, 'Michael'),
(4, 6.0, 'John');

-- TEST: aggnested-7.1
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT sum(amount), name
from invoice
group by name
having (select v > 6 from (select sum(amount) v) t);
-- EXPECTED:
15.0, 'Bara', 8.0, 'Michael'

-- TEST: aggnested-7.2
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT (select 1 from (select sum(amount))) FROM invoice;
-- EXPECTED:
1

-- TEST: init
-- SQL:
DROP TABLE IF EXISTS t1;
CREATE TABLE t1(x INT PRIMARY KEY);
INSERT INTO t1 VALUES(100);
INSERT INTO t1 VALUES(20);
INSERT INTO t1 VALUES(3);

-- TEST: aggnested-8.0
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT (SELECT y FROM (SELECT sum(x) AS y) AS t2 ) FROM t1;
-- EXPECTED:
123

-- TEST: aggnested-8.1
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT (
        SELECT y FROM (
                SELECT z AS y FROM (SELECT sum(x) AS z) AS t2
        )
) FROM t1;
-- EXPECTED:
123

-- TEST: aggnested-8.2
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT (
        SELECT a FROM (
                SELECT y AS a FROM (
                        SELECT z AS y FROM (SELECT sum(x) AS z) AS t2
                )
        )
) FROM t1;
-- EXPECTED:
123

-- TEST: aggnested-9.1
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
WITH out(i, j, k) AS (
VALUES(1234, 5678, 9012)
)
SELECT (
        SELECT (
                SELECT min(abc) = ( SELECT ( SELECT 1234 fROM (SELECT abc) ) )
                FROM (
                        SELECT sum( out.i ) + ( SELECT sum( out.i ) ) AS abc FROM (SELECT out.j)
                )
        )
) FROM out;
-- EXPECTED:
0

-- TEST: init
-- SQL:
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1(a INT PRIMARY KEY);
CREATE TABLE t2(b INT PRIMARY KEY);
INSERT INTO t1 VALUES(1), (2), (3);
INSERT INTO t2 VALUES(4), (5), (6);

-- TEST: aggnested-9.2
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT (
        SELECT min(y) + (SELECT x) FROM (
                SELECT sum(a) AS x, b AS y FROM t2
        )
)
FROM t1;
-- EXPECTED:
10

-- TEST: aggnested-9.3
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT (
        SELECT min(y) + (SELECT (SELECT x)) FROM (
                SELECT sum(a) AS x, b AS y FROM t2
        )
)
FROM t1;
-- EXPECTED:
10

-- TEST: aggnested-9.4
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT (
        SELECT (SELECT x) FROM (
                SELECT sum(a) AS x, b AS y FROM t2
        ) GROUP BY y
)
FROM t1;
-- EXPECTED:
6

-- TEST: aggnested-9.5
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT (
        SELECT (SELECT (SELECT x)) FROM (
                SELECT sum(a) AS x, b AS y FROM t2
        ) GROUP BY y
)
FROM t1;
-- EXPECTED:
6

-- TEST: init
-- SQL:
DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;
CREATE TABLE t0(c1 INT PRIMARY KEY, c2 INT);  INSERT INTO t0 VALUES(1,2);
CREATE TABLE t1(c3 INT PRIMARY KEY, c4 INT);  INSERT INTO t1 VALUES(3,4);

-- TEST: aggnested-10.1
-- TBD: https://git.picodata.io/core/picodata/-/issues/1926
SELECT * FROM t0
WHERE
EXISTS (
        SELECT 1 FROM t1
        GROUP BY c3
        HAVING (
                SELECT count(*) FROM (SELECT 1 UNION ALL SELECT sum(DISTINCT c1) )
        )
)
BETWEEN 1 AND 1;
-- EXPECTED:
1, 2
