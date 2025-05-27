-- TEST: initialization
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t(a INT PRIMARY KEY, b INT);
INSERT INTO t VALUES(1, 1);
INSERT INTO t VALUES(2, 1);
INSERT INTO t VALUES(3, 2);
INSERT INTO t VALUES(4, 3);
DROP TABLE IF EXISTS tb;
CREATE TABLE tb(a INT PRIMARY KEY, b BOOLEAN);
INSERT INTO tb VALUES(1, true);
INSERT INTO tb VALUES(2, true);
INSERT INTO tb VALUES(3, false);

-- TEST: reference-under-case-expression
-- SQL:
SELECT CASE a WHEN 1 THEN 42 WHEN 2 THEN 69 ELSE 0 END AS c FROM t ORDER BY c;
-- EXPECTED:
0,
0,
42,
69

-- TEST: reference-under-when-without-case-expression
-- SQL:
SELECT CASE WHEN a <= 2 THEN true ELSE false END AS c FROM t ORDER BY c;
-- EXPECTED:
false,
false,
true,
true

-- TEST: reference-under-else-without-case-expression
-- SQL:
SELECT CASE WHEN false THEN 42::INT ELSE a END AS c FROM t ORDER BY c;
-- EXPECTED:
1,
2,
3,
4

-- TEST: reference-under-when-without-case-expression-and-else
-- SQL:
SELECT CASE WHEN a <= 4 THEN 42 END AS c FROM t ORDER BY c;
-- EXPECTED:
42,
42,
42,
42

-- TEST: case-under-where-clause
-- SQL:
SELECT * FROM t WHERE CASE WHEN true THEN 5::INT END = 5 ORDER BY 1;
-- EXPECTED:
1, 1, 2, 1, 3, 2, 4, 3

-- TEST: case-under-where-clause-subtree
-- SQL:
SELECT * FROM t WHERE true and CASE WHEN true THEN 5::INT END = 5 ORDER BY 1;
-- EXPECTED:
1, 1, 2, 1, 3, 2, 4, 3

-- TEST: not-in-simple
-- SQL:
SELECT a FROM t WHERE a NOT IN (1, 3) ORDER BY 1;
-- EXPECTED:
2,
4

-- TEST: not-in-redundant
-- SQL:
SELECT a FROM t WHERE a NOT IN (1, 2) AND TRUE ORDER BY 1;
-- EXPECTED:
3,
4

-- TEST: not-in-under-join
-- SQL:
SELECT a FROM t JOIN (SELECT b from t) new ON t.b = new.b AND a NOT IN (1, 2) AND TRUE ORDER BY 1;
-- EXPECTED:
3,
4

-- TEST: not-in-simple
-- SQL:
SELECT a FROM t WHERE a NOT IN (1, 3) ORDER BY 1;
-- EXPECTED:
2,
4

-- TEST: not-in-redundant
-- SQL:
SELECT a FROM t WHERE a NOT IN (1, 2) AND TRUE ORDER BY 1;
-- EXPECTED:
3,
4

-- TEST: not-in-under-join
-- SQL:
SELECT a FROM t JOIN (SELECT b from t) new ON t.b = new.b AND a NOT IN (1, 2) AND TRUE ORDER BY 1;
-- EXPECTED:
3,
4

-- TEST: parentheses-under-cast-with-not
-- SQL:
SELECT (NOT TRUE)::TEXT
-- EXPECTED:
'FALSE'

-- TEST: parentheses-under-cast-with-concat
-- SQL:
SELECT ('1' || '2')::INT
-- EXPECTED:
12

-- TEST: parentheses-under-is-null
-- SQL:
SELECT (TRUE OR FALSE) IS NULL
-- EXPECTED:
false

-- TEST: parentheses-under-arithmetic
-- SQL:
SELECT 1 + (2 < 3)
-- ERROR:
could not resolve operator overload for +(unsigned, bool)

-- TEST: parentheses-under-arithmetic-with-not
-- SQL:
SELECT (NOT 1) + NULL
-- ERROR:
argument of NOT must be type boolean, not type unsigned

-- TEST: parentheses-under-arithmetic-with-between
-- SQL:
SELECT 1 + (1 BETWEEN 1 AND 1)
-- ERROR:
could not resolve operator overload for +(unsigned, bool)

-- TEST: parentheses-under-concat
-- SQL:
SELECT (NOT 1) || '1'
-- ERROR:
argument of NOT must be type boolean, not type unsigned

-- TEST: parentheses-under-divide
-- SQL:
SELECT 8 / (4 / 2)
-- EXPECTED:
4

-- TEST: parentheses-under-subtract
-- SQL:
SELECT 2 - (4 - 8)
-- EXPECTED:
6

-- TEST: parentheses-under-multiply
-- SQL:
SELECT 2 * (3 + 5)
-- EXPECTED:
16

-- TEST: parentheses-under-bool
-- SQL:
SELECT 1 = (2 = FALSE)
-- ERROR:
could not resolve operator overload for =(unsigned, bool)

-- TEST: parentheses-under-like
-- SQL:
SELECT (NOT NULL) LIKE 'a'
-- ERROR:
could not resolve function overload for like(bool, text, text)

-- TEST: parentheses-under-not-with-and
-- SQL:
SELECT NOT (FALSE AND TRUE)
-- EXPECTED:
true

-- TEST: parentheses-under-not-with-or
-- SQL:
SELECT NOT (TRUE OR TRUE)
-- EXPECTED:
false

-- TEST: parentheses-under-and
-- SQL:
SELECT FALSE AND (FALSE OR TRUE)
-- EXPECTED:
false

-- TEST: having-with-boolean-column
-- SQL:
SELECT sum(a) FROM tb GROUP BY b HAVING b;
-- EXPECTED:
3

-- TEST: select-distinct-asterisk
-- SQL:
SELECT DISTINCT * FROM t ORDER BY 1
-- EXPECTED:
1, 1, 2, 1, 3, 2, 4, 3

-- TEST: select-asterisk-with-group-by
-- SQL:
SELECT * FROM t GROUP BY a, b ORDER BY 1
-- EXPECTED:
1, 1, 2, 1, 3, 2, 4, 3

-- TEST: test-creatinon-with-json-type
-- SQL:
CREATE TABLE s (a INT PRIMARY KEY, b JSON);

-- TEST: test-dml-with-json-type
-- SQL:
INSERT INTO s VALUES (1, '{
    "glossary": {
        "title": "example glossary",
		"GlossDiv": {
            "title": "S",
			"GlossList": {
                "GlossEntry": {
                    "ID": "SGML",
					"SortAs": "SGML",
					"GlossTerm": "Standard Generalized Markup Language",
					"Acronym": "SGML",
					"Abbrev": "ISO 8879:1986",
					"GlossDef": {
                        "para": "A meta-markup language, used to create markup languages such as DocBook.",
						"GlossSeeAlso": ["GML", "XML"]
                    },
					"GlossSee": "markup"
                }
            }
        }
    }
}');
-- ERROR:
as a value of type map, consider using explicit type casts

-- TEST: test-json-is-not-keyword-1
-- SQL:
CREATE TABLE tc (JSON int primary key);
INSERT INTO tc (JSON) VALUES(1);

-- TEST: test-json-is-not-keyword-2
-- SQL:
SELECT * FROM tc
-- EXPECTED:
1

-- TEST: test-cte-caching-1
-- SQL:
with cte as (select 1) select 1 = (select * from cte), (select * from cte);
-- EXPECTED:
true, 1

-- TEST: test-cte-caching-2
-- SQL:
with cte as (select 1) select 1 = (select * from cte), (select * from cte);
-- EXPECTED:
true, 1

-- TEST: test-between-caching-1
-- SQL:
select (select 1) between 1 and 2
-- EXPECTED:
true

-- TEST: test-between-caching-2
-- SQL:
select (select 1) between 1 and 2
-- EXPECTED:
true

-- TEST: test-cte-union-caching-1
-- SQL:
with cte(escape) as (select '#') select escape from cte union all select escape from cte
-- EXPECTED:
'#', '#'

-- TEST: test-cte-union-caching-2
-- SQL:
with cte(escape) as (select '#') select escape from cte union all select escape from cte
-- EXPECTED:
'#', '#'

-- TEST: test-qualified-references-1
-- SQL:
SELECT "t".* FROM (SELECT 1) AS "t"
-- EXPECTED:
1

-- TEST: test-qualified-references-2
-- SQL:
SELECT "no_such_table".* FROM (SELECT 1)
-- ERROR:
sbroad: table 'no_such_table' not found

-- TEST: test-qualified-references-3
-- SQL:
SELECT "t1".* FROM t AS t1;
-- EXPECTED:
1, 1, 2, 1, 3, 2, 4, 3

-- TEST: test-json-is-not-keyword-3
-- SQL:
DROP TABLE tc;

-- TEST: test-filter-keyword-1
-- SQL:
select 1 as int
-- EXPECTED:
1

-- TEST: test-filter-keyword-2
-- SQL:
CREATE TABLE int8 (int int, bigint smallint, uuid text, primary key (bigint, uuid));
INSERT INTO int8 VALUES (8, 1, 'kek'), (10, 5, 'lol');

-- TEST: test-filter-keyword-3
-- SQL:
SELECT uuid from int8;
-- EXPECTED:
'kek', 'lol'

-- TEST: test-filter-keyword-4
-- SQL:
SELECT CAST(int as int) from int8;
-- EXPECTED:
8, 10

-- TEST: test-filter-keyword-5
-- SQL:
UPDATE int8 set int = 9;

-- TEST: test-filter-keyword-6
-- SQL:
CREATE UNIQUE INDEX unsigned ON int8 USING HASH (bigint, uuid)

-- TEST: test-filter-keyword-7
-- SQL:
CREATE PROCEDURE array (string)
LANGUAGE SQL
AS $$
    INSERT INTO int8 VALUES (10, 7, 'kek2')
$$

-- TEST: test-filter-keyword-8
-- SQL:
DELETE FROM int8 where uuid = 'kek';

-- TEST: test-filter-keyword-9
-- SQL:
SELECT * FROM int8;
-- EXPECTED:
9, 5, 'lol'

-- TEST: test-check-compare-order
-- SQL:
SELECT * FROM t ORDER BY a desc;
-- EXPECTED:
1, 1, 2, 1, 3, 2, 4, 3

-- TEST: test-order-by-nulls-init-1
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t(a INT PRIMARY KEY, b INT, c INT);
INSERT INTO t VALUES(1, 2, 1), (2, 1, 2);
INSERT INTO t (a, c) VALUES(3, 3), (4, 3);

-- TEST: test-order-by-nulls-1
-- SQL:
SELECT c, b FROM t ORDER BY b NULLS FIRST, c ;
-- EXPECTED:
3, nil, 3, nil, 2, 1, 1, 2

-- TEST: test-order-by-nulls-2
-- SQL:
SELECT c, b FROM t ORDER BY b NULLS LAST;
-- EXPECTED:
2, 1, 1, 2, 3, nil, 3, nil

-- TEST: test-order-by-nulls-3
-- SQL:
SELECT c, b FROM t ORDER BY b DESC NULLS FIRST;
-- EXPECTED:
3, nil, 3, nil, 1, 2, 2, 1

-- TEST: test-order-by-nulls-4
-- SQL:
SELECT c, b FROM t ORDER BY b DESC NULLS LAST;
-- EXPECTED:
1, 2, 2, 1, 3, nil, 3, nil

-- TEST: test-order-by-nulls-5
-- SQL:
SELECT a, b FROM t ORDER BY b DESC NULLS FIRST, a DESC;
-- EXPECTED:
4, nil, 3, nil, 1, 2, 2, 1

-- TEST: test-order-by-nulls-6
-- SQL:
SELECT a, b FROM t ORDER BY b DESC NULLS LAST, a DESC;
-- EXPECTED:
1, 2, 2, 1, 4, nil, 3, nil

-- TEST: test-order-by-nulls-init-2
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t(a INT PRIMARY KEY, b INT, c INT);
INSERT INTO t (a) VALUES(1), (2);
INSERT INTO t (a, c) VALUES(3, 4), (4, 3);
INSERT INTO t (a, b) VALUES(5, 8), (6, 7);
INSERT INTO t VALUES(7, 6, 8), (8, 5, 7);

-- TEST: test-order-by-nulls-7
-- SQL:
SELECT b, c FROM t ORDER BY b NULLS FIRST, c NULLS LAST;
-- EXPECTED:
nil, 3, nil, 4, nil, nil, nil, nil, 5, 7, 6, 8, 7, nil, 8, nil

-- TEST: test-order-by-nulls-init-3
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t(a INT PRIMARY KEY, b INT);
INSERT INTO t (a) VALUES(1), (2);
INSERT INTO t (a, b) VALUES(3, 1), (4, 2);

DROP TABLE IF EXISTS t1;
CREATE TABLE t1(a INT PRIMARY KEY, b INT);
INSERT INTO t1 (a) VALUES(1), (2);
INSERT INTO t1 (a, b) VALUES(3, 1), (4, 2);

-- TEST: test-order-by-nulls-8
-- SQL:
SELECT * FROM t ORDER BY b + (SELECT b FROM t1 ORDER BY b NULLS LAST LIMIT 1) NULLS FIRST;
-- EXPECTED:
1, nil, 2, nil, 3, 1, 4, 2
