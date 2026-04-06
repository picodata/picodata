-- TEST: simple-init
-- SQL:
CREATE TABLE s(a INT PRIMARY KEY, b INT);
CREATE TABLE t(a INT PRIMARY KEY, b INT);
INSERT INTO s VALUES (1, 10);
INSERT INTO t VALUES (10, 1);

-- TEST: simple-query-1
-- SQL:
SELECT * FROM s JOIN t ON s.b = t.a UNION SELECT * FROM t JOIN s ON t.b = s.a;
-- EXPECTED:
1, 10, 10, 1, 10, 1, 1, 10

-- TEST: simple-drop
-- SQL:
DROP TABLE s;
DROP TABLE t;
CREATE TABLE s(a INT PRIMARY KEY, b INT);
CREATE TABLE t(a INT PRIMARY KEY, b INT);
INSERT INTO s VALUES (1, 10);
INSERT INTO t VALUES (10, 1);

-- TEST: simple-query-2
-- SQL:
SELECT * FROM s JOIN t ON s.b = t.a UNION SELECT * FROM t JOIN s ON t.b = s.a;
-- EXPECTED:
1, 10, 10, 1, 10, 1, 1, 10

-- TEST: simple-query-3
-- SQL:
SELECT * FROM s JOIN t ON s.b = t.a UNION SELECT * FROM t JOIN s ON t.b = s.a;
-- EXPECTED:
1, 10, 10, 1, 10, 1, 1, 10

-- TEST: complex-init
-- SQL:
CREATE TABLE t1(a INT PRIMARY KEY, b INT, c INT);
CREATE TABLE t2 (a INT PRIMARY KEY, b INT, c INT);
INSERT INTO t1 VALUES (1, 1, 1);
INSERT INTO t2 VALUES (10, 10, 10);

-- TEST: complex-query-1
-- SQL:
SELECT d.b, e.b, g.c, d.c
FROM t2 d
    JOIN t2 h
        ON d.b = h.a
    JOIN t1 e
        ON true
    JOIN t1 g
        ON true
    UNION
        SELECT e.c, 0, count(g.c), min(i.a)
        FROM t1 d
            JOIN t2 h
                ON h.a = d.c
            JOIN t2 e
                ON true
            JOIN t1 g
                ON true
            JOIN t1 i
                ON g.a > (
                    SELECT count(a) f FROM t1 HAVING 1 > min(a) ORDER BY f LIMIT 1
                )
        GROUP BY e.c
-- EXPECTED:
10, 1, 1, 10

-- TEST: complex-drop
-- SQL:
DROP TABLE t1;
DROP TABLE t2;
CREATE TABLE t1(a INT PRIMARY KEY, b INT, c INT);
CREATE TABLE t2 (a INT PRIMARY KEY, b INT, c INT);
INSERT INTO t1 VALUES (1, 1, 1);
INSERT INTO t2 VALUES (10, 10, 10);

-- TEST: complex-query-2
-- SQL:
SELECT d.b, e.b, g.c, d.c
FROM t2 d
    JOIN t2 h
        ON d.b = h.a
    JOIN t1 e
        ON true
    JOIN t1 g
        ON true
    UNION
        SELECT e.c, 0, count(g.c), min(i.a)
        FROM t1 d
            JOIN t2 h
                ON h.a = d.c
            JOIN t2 e
                ON true
            JOIN t1 g
                ON true
            JOIN t1 i
                ON g.a > (
                    SELECT count(a) f FROM t1 HAVING 1 > min(a) ORDER BY f LIMIT 1
                )
        GROUP BY e.c
-- EXPECTED:
10, 1, 1, 10

-- TEST: complex-query-3
-- SQL:
SELECT d.b, e.b, g.c, d.c
FROM t2 d
    JOIN t2 h
        ON d.b = h.a
    JOIN t1 e
        ON true
    JOIN t1 g
        ON true
    UNION
        SELECT e.c, 0, count(g.c), min(i.a)
        FROM t1 d
            JOIN t2 h
                ON h.a = d.c
            JOIN t2 e
                ON true
            JOIN t1 g
                ON true
            JOIN t1 i
                ON g.a > (
                    SELECT count(a) f FROM t1 HAVING 1 > min(a) ORDER BY f LIMIT 1
                )
        GROUP BY e.c
-- EXPECTED:
10, 1, 1, 10
