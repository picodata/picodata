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

-- TEST: unnest-of-untyped-array
-- SQL:
select
    name as "table",
    value['name']::text as "column",
    value['field_type']::text as "type"
from _pico_table cross join unnest(_pico_table.format)
where name = '_pico_table'
order by "table", "column";
-- EXPECTED:
'_pico_table', 'description', 'string',
'_pico_table', 'distribution', 'map',
'_pico_table', 'engine', 'string',
'_pico_table', 'format', 'array',
'_pico_table', 'id', 'unsigned',
'_pico_table', 'name', 'string',
'_pico_table', 'operable', 'boolean',
'_pico_table', 'opts', 'array',
'_pico_table', 'owner', 'unsigned',
'_pico_table', 'schema_version', 'unsigned'

-- TEST: index-parts-with-ordinality
-- SQL:
SELECT i.name, p.ordinality, p.value[1]::text
FROM _pico_index i CROSS JOIN unnest(i.parts) WITH ORDINALITY p
WHERE i.table_id = (SELECT id FROM _pico_table WHERE name = '_pico_index')
ORDER BY 1, 2;
-- EXPECTED:
'_pico_index_id', 1, 'table_id',
'_pico_index_id', 2, 'id',
'_pico_index_name', 1, 'name'

-- TEST: filter-on-untyped-map-element
-- SQL:
SELECT i.name
FROM _pico_index i CROSS JOIN unnest(i.opts) o
WHERE i.table_id = (SELECT id FROM _pico_table WHERE name = '_pico_table')
  AND o.value['unique']::bool
ORDER BY 1;
-- EXPECTED:
'_pico_table_id', '_pico_table_name'

-- TEST: untyped-elements-of-mixed-types
-- SQL:
SELECT ordinality, value
FROM _pico_instance CROSS JOIN unnest(current_state) WITH ORDINALITY
WHERE name = 'default_1_1'
ORDER BY 1;
-- EXPECTED:
1, 'Online', 2, 1

-- TEST: unnest-of-null-array-gives-no-rows
-- SQL:
SELECT u.name, a.value::text
FROM _pico_user u CROSS JOIN unnest(u.auth) WITH ORDINALITY a
WHERE a.ordinality = 1 AND u.name IN ('admin', 'guest', 'pico_service', 'public')
ORDER BY 1;
-- EXPECTED:
'admin', 'md5',
'guest', 'md5',
'pico_service', 'chap-sha1'

-- TEST: untyped-element-needs-a-cast
-- SQL:
SELECT value = 1 FROM _pico_instance CROSS JOIN unnest(current_state);
-- ERROR:
unexpected expression of type any, explicit cast to the actual type is required

-- TEST: untyped-element-with-a-cast
-- SQL:
SELECT ordinality, value::text = 'Online'
FROM _pico_instance CROSS JOIN unnest(current_state) WITH ORDINALITY
WHERE name = 'default_1_1'
ORDER BY 1;
-- EXPECTED:
1, true, 2, false

-- TEST: unnest-of-several-arrays
-- SQL:
SELECT * FROM unnest(ARRAY[1, 2, 3], ARRAY['a', 'b']) WITH ORDINALITY ORDER BY 3;
-- EXPECTED:
1, 'a', 1,
2, 'b', 2,
3, None, 3

-- TEST: unnest-of-null-and-array
-- SQL:
SELECT * FROM unnest(NULL, ARRAY[true]);
-- EXPECTED:
None, True

-- TEST: unnest-of-several-arrays-arithmetic
-- SQL:
SELECT u.value_1, u.value_1 + u.value_2 FROM unnest(ARRAY[1, 2], ARRAY[10]) u ORDER BY 1;
-- EXPECTED:
1, 11,
2, None

-- TEST: unnest-of-several-arrays-and-parameter
-- SQL:
SELECT * FROM unnest(ARRAY[1, 2], $1) ORDER BY 1;
-- PARAMS:
['x']
-- EXPECTED:
1, 'x',
2, None

-- TEST: unnest-of-a-repeated-parameter
-- SQL:
SELECT * FROM unnest($1::int[], $1) ORDER BY 1;
-- PARAMS:
[1, 2]
-- EXPECTED:
1, 1,
2, 2

-- TEST: lateral-unnest-of-several-arrays
-- SQL:
SELECT t.id, u.value_1, u.value_2, u.ordinality
FROM t CROSS JOIN unnest(t.arr, ARRAY[1]) WITH ORDINALITY u
ORDER BY 1, 4;
-- EXPECTED:
1, 10, 1, 1,
1, 20, None, 2,
2, None, 1, 1,
3, None, 1, 1,
4, 30, 1, 1

-- TEST: lateral-left-join-unnest-of-several-arrays
-- SQL:
SELECT t.id, u.value_1, u.value_2, u.ordinality
FROM t LEFT JOIN unnest(t.arr, ARRAY[5]) WITH ORDINALITY u ON u.value_1 > 10
ORDER BY 1;
-- EXPECTED:
1, 20, None, 2,
2, None, None, None,
3, None, None, None,
4, 30, 5, 1

-- TEST: unnest-of-several-arrays-longer-second
-- SQL:
SELECT * FROM unnest(ARRAY[1], ARRAY['a', 'b', 'c']) ORDER BY 2;
-- EXPECTED:
1, 'a',
None, 'b',
None, 'c'

-- TEST: unnest-of-several-empty-arrays
-- SQL:
SELECT * FROM unnest(ARRAY[], NULL);
-- EXPECTED:

-- TEST: join-of-unnests-of-several-arrays-by-ordinality
-- SQL:
SELECT a.value_1, a.value_2, b.value_2
FROM unnest(ARRAY[1, 2, 3], ARRAY['a', 'b']) WITH ORDINALITY a
JOIN unnest(ARRAY[10, 20], ARRAY['x', 'y', 'z']) WITH ORDINALITY b
  ON a.ordinality = b.ordinality;
-- UNORDERED:
1, 'a', 'x',
2, 'b', 'y',
3, None, 'z'

-- TEST: subquery-in-arguments-of-unnest-of-several-arrays
-- SQL:
SELECT * FROM unnest((SELECT arr FROM t WHERE id = 1), ARRAY['a']) ORDER BY 1;
-- EXPECTED:
10, 'a',
20, None

-- TEST: unnest-of-several-arrays-in-subquery-filter
-- SQL:
SELECT id FROM t
WHERE id IN (SELECT value_2 FROM unnest(ARRAY['a', 'b'], ARRAY[1, 4]))
ORDER BY 1;
-- EXPECTED:
1, 4

-- TEST: insert-from-unnest-of-several-arrays
-- SQL:
CREATE TABLE pairs (id INT PRIMARY KEY, name TEXT);
INSERT INTO pairs SELECT value_1, value_2 FROM unnest(ARRAY[1, 2], ARRAY['a']);

-- TEST: select-inserted-from-unnest-of-several-arrays
-- SQL:
SELECT * FROM pairs ORDER BY 1;
-- EXPECTED:
1, 'a',
2, None

-- TEST: unnest-of-several-arrays-has-no-value-column
-- SQL:
SELECT value FROM unnest(ARRAY[1], ARRAY[2]);
-- ERROR:
column with name "value" not found

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

-- TEST: unnest-of-null
-- SQL:
SELECT * FROM unnest(NULL);
-- EXPECTED:

-- TEST: unnest-of-non-array-argument
-- SQL:
SELECT * FROM unnest(ARRAY[1], 2);
-- ERROR:
could not resolve function overload for unnest\(int\[\], int\)

-- TEST: unnest-of-empty-array
-- SQL:
SELECT * FROM unnest(ARRAY[]);
-- EXPECTED:


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
