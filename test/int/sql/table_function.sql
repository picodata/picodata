-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- TEST: init
-- SQL:
CREATE TABLE t (id INT PRIMARY KEY, n INT, arr INT ARRAY);
INSERT INTO t VALUES
  (1, 2, ARRAY[10, 20]),
  (2, 0, ARRAY[]),
  (3, 1, NULL),
  (4, 3, ARRAY[30]);

-- TEST: generate-series
-- SQL:
SELECT * FROM generate_series(1, 3);
-- EXPECTED:
1, 2, 3

-- TEST: generate-series-step
-- SQL:
SELECT value FROM generate_series(10, 1, -4);
-- EXPECTED:
10, 6, 2

-- TEST: generate-series-empty
-- SQL:
SELECT * FROM generate_series(3, 1);
-- EXPECTED:

-- TEST: generate-series-decimal
-- SQL:
SELECT g.value FROM generate_series(1.5, 3) AS g;
-- EXPECTED:
Decimal('1.5'), Decimal('2.5')

-- TEST: generate-series-double
-- SQL:
SELECT * FROM generate_series(1, 2.5::double);
-- ERROR:
could not resolve function overload for generate_series\(numeric, double\)

-- TEST: generate-series-ordinality
-- SQL:
SELECT * FROM generate_series(5, 7) WITH ORDINALITY;
-- EXPECTED:
5, 1, 6, 2, 7, 3

-- TEST: generate-series-parameters
-- SQL:
SELECT * FROM generate_series($1, $2);
-- PARAMS:
1, 2
-- EXPECTED:
1, 2

-- TEST: generate-series-zero-step
-- SQL:
SELECT * FROM generate_series(1, 3, 0);
-- ERROR:
step

-- TEST: generate-series-aggregate
-- SQL:
SELECT count(*), sum(value) FROM generate_series(1, 100);
-- EXPECTED:
100, Decimal('5050')

-- TEST: generate-series-subquery
-- SQL:
SELECT id FROM t WHERE id IN (SELECT value FROM generate_series(2, 3)) ORDER BY id;
-- EXPECTED:
2, 3

-- TEST: generate-series-subquery-column
-- SQL:
SELECT s.value FROM (SELECT g.value FROM generate_series(1, 2) g) s;
-- EXPECTED:
1, 2

-- TEST: unnest-array-literal
-- SQL:
SELECT * FROM unnest(ARRAY['a', 'b']) WITH ORDINALITY;
-- EXPECTED:
'a', 1, 'b', 2

-- TEST: lateral-generate-series
-- SQL:
SELECT t.id, g.value FROM t JOIN generate_series(1, t.n) g ON true ORDER BY 1, 2;
-- EXPECTED:
1, 1, 1, 2, 3, 1, 4, 1, 4, 2, 4, 3

-- TEST: lateral-generate-series-left-join
-- SQL:
SELECT t.id, g.value
FROM t
LEFT JOIN generate_series(1, t.n) g ON g.value > 1
ORDER BY 1, 2;
-- EXPECTED:
1, 2, 2, None, 3, None, 4, 2, 4, 3

-- TEST: lateral-unnest
-- SQL:
SELECT t.id, u.value, u.ordinality
FROM t
JOIN unnest(t.arr) WITH ORDINALITY u ON true
ORDER BY 1, 3;
-- EXPECTED:
1, 10, 1, 1, 20, 2, 4, 30, 1

-- TEST: lateral-unnest-arithmetic
-- SQL:
SELECT u.value + 1 FROM t JOIN unnest(t.arr) u ON true ORDER BY 1;
-- EXPECTED:
11, 21, 31

-- TEST: unnest-alias-of-local-name
-- SQL:
SELECT s."VALUE"
FROM (SELECT u.value AS "VALUE" FROM t JOIN unnest(t.arr) u ON true) s
ORDER BY 1;
-- EXPECTED:
10, 20, 30

-- TEST: lateral-unnest-arithmetic-in-filter
-- SQL:
SELECT t.id FROM t JOIN unnest(t.arr) u ON u.value * 2 > 30 ORDER BY 1;
-- EXPECTED:
1, 4

-- TEST: lateral-function-chain
-- SQL:
SELECT a.value AS a, b.value AS b
FROM generate_series(1, 3) a
JOIN generate_series(a.value, 3) b ON true
ORDER BY 1, 2;
-- EXPECTED:
1, 1, 1, 2, 1, 3, 2, 2, 2, 3, 3, 3

-- TEST: lateral-aggregate
-- SQL:
SELECT count(*), sum(g.value) FROM t JOIN generate_series(1, t.n) g ON true;
-- EXPECTED:
6, Decimal('10')

-- TEST: lateral-group-by
-- SQL:
SELECT g.value, count(*)
FROM t
JOIN generate_series(1, t.n) g ON true
GROUP BY g.value
ORDER BY 1;
-- EXPECTED:
1, 3, 2, 2, 3, 1

-- TEST: lateral-sharding-key-filter
-- SQL:
SELECT g.value
FROM t
JOIN generate_series(1, t.n) g ON true
WHERE t.id = 4 AND g.value > 1
ORDER BY 1;
-- EXPECTED:
2, 3

-- TEST: lateral-after-motion
-- SQL:
SELECT s.id, g.value
FROM (SELECT id, n FROM t ORDER BY id LIMIT 1) s
JOIN generate_series(1, s.n) g ON true
ORDER BY 2;
-- EXPECTED:
1, 1, 1, 2

-- TEST: lateral-values
-- SQL:
SELECT v."COLUMN_1", g.value
FROM (VALUES (2), (1)) v
JOIN generate_series(1, v."COLUMN_1") g ON true
ORDER BY 1, 2;
-- EXPECTED:
1, 1, 2, 1, 2, 2

-- TEST: lateral-cte
-- SQL:
WITH c AS (SELECT n FROM t WHERE id = 1)
SELECT g.value
FROM c
JOIN generate_series(1, c.n) g ON true
ORDER BY 1;
-- EXPECTED:
1, 2

-- TEST: subquery-in-arguments
-- SQL:
SELECT * FROM generate_series(1, (SELECT max(n) FROM t));
-- EXPECTED:
1, 2, 3

-- TEST: subquery-in-arguments-of-lateral-function
-- SQL:
SELECT t.id, g.value
FROM t
JOIN generate_series(t.n, (SELECT max(n) FROM t)) g ON true
WHERE t.id < 3
ORDER BY 1, 2;
-- EXPECTED:
1, 2, 1, 3, 2, 0, 2, 1, 2, 2, 2, 3

-- TEST: lateral-cross-join
-- SQL:
SELECT t.id, g.value
FROM t
CROSS JOIN generate_series(t.id, 5) g
WHERE t.id IN (1, 4)
ORDER BY 1, 2;
-- EXPECTED:
1, 1, 1, 2, 1, 3, 1, 4, 1, 5, 4, 4, 4, 5

-- TEST: asterisk-over-lateral-join
-- SQL:
SELECT * FROM t JOIN generate_series(1, t.n) g ON true WHERE t.id = 1 ORDER BY 4;
-- EXPECTED:
1, 2, [10, 20], 1, 1, 2, [10, 20], 2

-- TEST: asterisk-over-lateral-join-limit
-- SQL:
SELECT * FROM t JOIN generate_series(1, t.n) g ON true ORDER BY 1, 4 LIMIT 3;
-- EXPECTED:
1, 2, [10, 20], 1, 1, 2, [10, 20], 2, 3, 1, None, 1

-- TEST: qualified-asterisk-over-lateral-join
-- SQL:
SELECT t.id, g.* FROM t JOIN generate_series(1, t.n) g ON true WHERE t.id = 1 ORDER BY 2;
-- EXPECTED:
1, 1, 1, 2

-- TEST: asterisk-over-lateral-unnest-ordinality
-- SQL:
SELECT * FROM t JOIN unnest(t.arr) WITH ORDINALITY u ON true WHERE t.id = 1 ORDER BY 5;
-- EXPECTED:
1, 2, [10, 20], 10, 1, 1, 2, [10, 20], 20, 2

-- TEST: asterisk-over-lateral-join-with-subquery
-- SQL:
SELECT *
FROM t
JOIN generate_series(1, (SELECT max(n) FROM t)) g ON true
WHERE t.id = 1
ORDER BY 4;
-- EXPECTED:
1, 2, [10, 20], 1, 1, 2, [10, 20], 2, 1, 2, [10, 20], 3

-- TEST: function-on-outer-side-of-left-join
-- SQL:
SELECT * FROM generate_series(1, 3) g LEFT JOIN t ON g.value = t.id ORDER BY 1;
-- EXPECTED:
1, 1, 2, [10, 20], 2, 2, 0, [], 3, 3, 1, None

-- TEST: count-with-function-on-outer-side-of-left-join
-- SQL:
SELECT count(*) FROM generate_series(1, 3) g LEFT JOIN t ON g.value = t.id;
-- EXPECTED:
3

-- TEST: cte-over-asterisk
-- SQL:
WITH c AS (SELECT * FROM generate_series(1, 3))
SELECT c1.value, c2.value
FROM c c1
JOIN c c2 ON c1.value = c2.value
ORDER BY 1;
-- EXPECTED:
1, 1, 2, 2, 3, 3

-- TEST: self-join-of-functions
-- SQL:
SELECT *
FROM generate_series(1, 3) g
JOIN generate_series(1, 3) h ON g.value = h.value
WHERE h.value > 1
ORDER BY 1;
-- EXPECTED:
2, 2, 3, 3

-- TEST: lateral-reference-to-the-right
-- SQL:
SELECT * FROM generate_series(1, t.n) g JOIN t ON true;
-- ERROR:
could not resolve reference "t"

-- TEST: unknown-table-function
-- SQL:
SELECT * FROM no_such_function(1);
-- ERROR:
table function "no_such_function" does not exist

-- TEST: volatile-function-in-arguments
-- SQL:
SELECT * FROM unnest(ARRAY[pico_instance_uuid()]);
-- ERROR:
volatile functions are not allowed in functions in FROM

-- TEST: volatile-function-in-subquery-of-arguments
-- SQL:
SELECT t.id, u.value
FROM t CROSS JOIN unnest(ARRAY[(SELECT pico_instance_uuid())]) u;
-- ERROR:
volatile functions are not allowed in functions in FROM
