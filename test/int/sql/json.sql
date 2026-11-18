-- TEST-MATRIX: pgproto-2rsX2, iproto-2rsX2

-- TEST: init-kinds
-- SQL:
DROP TABLE IF EXISTS t_kinds;
CREATE TABLE t_kinds (id INTEGER PRIMARY KEY, data JSON);
INSERT INTO t_kinds VALUES (1, '{"a":1}'::json);
INSERT INTO t_kinds VALUES (2, CAST('[1,2,3]' AS json));
INSERT INTO t_kinds VALUES (3, '42'::json);
INSERT INTO t_kinds VALUES (4, '"hello"'::json);
INSERT INTO t_kinds VALUES (5, 'true'::json);
INSERT INTO t_kinds VALUES (6, 'null'::json);

-- TEST: select-all-kinds
-- SQL:
SELECT id, data FROM t_kinds ORDER BY id;
-- EXPECTED:
1, {"a": 1},
2, [1, 2, 3],
3, 42,
4, 'hello',
5, true,
6, null

-- TEST: json-cast-to-text
-- SQL:
SELECT id, CAST(data AS TEXT) FROM t_kinds ORDER BY id;
-- EXPECTED:
1, '{"a": 1}',
2, '[1, 2, 3]',
3, '42',
4, '"hello"',
5, 'true',
6, 'null'

-- TEST: json-cast-to-text-pg-operator
-- SQL:
SELECT data::text FROM t_kinds WHERE id = 1;
-- EXPECTED:
'{"a": 1}'

-- TEST: json-cast-to-text-is-concatenable
-- SQL:
SELECT data::text || '!' FROM t_kinds WHERE id = 1;
-- EXPECTED:
'{"a": 1}!'

-- TEST: init-cast-forms
-- SQL:
DROP TABLE IF EXISTS t_cast;
CREATE TABLE t_cast (id INTEGER PRIMARY KEY, data JSON);
INSERT INTO t_cast VALUES (1, '{"a":1}'::json);
INSERT INTO t_cast VALUES (2, CAST('{"a":1}' AS json));

-- TEST: cast-forms-equivalent
-- SQL:
SELECT id, data FROM t_cast ORDER BY id;
-- EXPECTED:
1, {"a": 1},
2, {"a": 1}

-- TEST: init-null
-- SQL:
DROP TABLE IF EXISTS t_null;
CREATE TABLE t_null (id INTEGER PRIMARY KEY, data JSON);
INSERT INTO t_null VALUES (1, 'null'::json);
INSERT INTO t_null VALUES (2, NULL);
INSERT INTO t_null VALUES (3, NULL::json);

-- TEST: json-null-is-a-present-value
-- SQL:
SELECT id, data IS NULL FROM t_null ORDER BY id;
-- EXPECTED:
1, false,
2, true,
3, true

-- TEST: json-raw-literals
-- SQL:
EXPLAIN (RAW) SELECT '"O''Brien"'::json AS s, 'null'::json IS NULL AS n;
-- EXPECTED:
╭───────────────────╮
│ 1. Query (ROUTER) │
╰───────────────────╯
''
SELECT '"O''Brien"'::json as "s", 'null'::json is null as "n"
''
plan:
    [0] TRIVIAL

-- TEST: init-block
-- SQL:
DROP TABLE IF EXISTS t_block;
CREATE TABLE t_block (id INTEGER PRIMARY KEY, data JSON);
DO $$ BEGIN INSERT INTO t_block VALUES (1, 'null'::json); END $$;
DO $$ BEGIN INSERT INTO t_block VALUES (2, '{"a":1}'::json); END $$;

-- TEST: json-null-in-anonymous-block-stays-json
-- SQL:
SELECT id, data IS NULL FROM t_block ORDER BY id;
-- EXPECTED:
1, false,
2, false

-- TEST: json-in-anonymous-block-object-roundtrip
-- SQL:
SELECT data FROM t_block WHERE id = 2;
-- EXPECTED:
{"a": 1}

-- TEST: init-equality
-- SQL:
DROP TABLE IF EXISTS t_eq;
CREATE TABLE t_eq (id INTEGER PRIMARY KEY, data JSON);
INSERT INTO t_eq VALUES
  (1, '{"a":1}'::json),
  (2, '{"a":2}'::json),
  (3, '{"a":1}'::json);

-- TEST: json-equality-filter
-- SQL:
SELECT id FROM t_eq WHERE data = '{"a":1}'::json ORDER BY id;
-- EXPECTED:
1,
3

-- TEST: init-count
-- SQL:
DROP TABLE IF EXISTS t_count;
CREATE TABLE t_count (id INTEGER PRIMARY KEY, data JSON);
INSERT INTO t_count VALUES (1, '{"a":1}'::json), (2, NULL), (3, '[1,2]'::json);

-- TEST: json-count-skips-null
-- SQL:
SELECT count(data) FROM t_count;
-- EXPECTED:
2

-- TEST: json-count-distinct
-- SQL:
SELECT count(DISTINCT data) FROM t_count;
-- EXPECTED:
2

-- TEST: json-count-over-window
-- SQL:
SELECT id, count(data) OVER () FROM t_count ORDER BY id;
-- EXPECTED:
1, 2,
2, 2,
3, 2

-- TEST: init-set-ops
-- SQL:
DROP TABLE IF EXISTS t_set;
CREATE TABLE t_set (id INTEGER PRIMARY KEY, data JSON);
INSERT INTO t_set VALUES
  (1, '{"a":1}'::json),
  (2, '{"a":2}'::json),
  (3, '{"a":1}'::json);

-- TEST: json-union-dedups
-- SQL:
SELECT data FROM t_set UNION SELECT data FROM t_set ORDER BY data;
-- EXPECTED:
{"a": 1},
{"a": 2}

-- TEST: json-join-key
-- SQL:
SELECT a.id, b.id
FROM t_set a
JOIN t_set b ON a.data = b.data
ORDER BY a.id, b.id;
-- EXPECTED:
1, 1,
1, 3,
2, 2,
3, 1,
3, 3

-- TEST: init-order
-- SQL:
DROP TABLE IF EXISTS t_order;
CREATE TABLE t_order (id INTEGER PRIMARY KEY, data JSON);
INSERT INTO t_order VALUES (1, '3'::json), (2, '1'::json), (3, '2'::json);

-- TEST: json-order-by
-- SQL:
SELECT data FROM t_order ORDER BY data;
-- EXPECTED:
1,
2,
3

-- TEST: init-numbers
-- SQL:
DROP TABLE IF EXISTS t_num;
CREATE TABLE t_num (id INTEGER PRIMARY KEY, data JSON);
INSERT INTO t_num VALUES
  (1, '42'::json),
  (2, '42.0'::json),
  (3, '4.2e1'::json),
  (4, '43'::json);

-- TEST: json-constants-order-true
-- SQL:
SELECT id FROM t_num WHERE '1'::json < '2'::json ORDER BY id;
-- EXPECTED:
1,
2,
3,
4

-- TEST: json-constants-order-false
-- SQL:
SELECT id FROM t_num WHERE '1'::json >= '2'::json ORDER BY id;
-- EXPECTED:

-- TEST: json-constants-order-by-class
-- SQL:
SELECT id
FROM t_num
WHERE '"a"'::json < '1'::json
  AND '1'::json < 'true'::json
  AND '[2]'::json < '[1,1]'::json
ORDER BY id;
-- EXPECTED:
1,
2,
3,
4

-- TEST: json-constants-equal-across-number-spellings
-- SQL:
SELECT id
FROM t_num
WHERE '42'::json = '42.0'::json
  AND '42.0'::json = '4.2e1'::json
ORDER BY id;
-- EXPECTED:
1,
2,
3,
4

-- TEST: json-constants-not-equal
-- SQL:
SELECT id FROM t_num WHERE '42'::json <> '42.0'::json ORDER BY id;
-- EXPECTED:

-- TEST: json-column-equals-either-spelling
-- SQL:
SELECT id FROM t_num WHERE data = '42.0'::json ORDER BY id;
-- EXPECTED:
1,
2,
3

-- TEST: json-two-spellings-of-one-number-do-not-contradict
-- SQL:
SELECT id
FROM t_num
WHERE data = '42'::json
  AND data = '42.0'::json
ORDER BY id;
-- EXPECTED:
1,
2,
3

-- TEST: json-different-numbers-contradict
-- SQL:
SELECT id FROM t_num WHERE data = '42'::json AND data = '43'::json ORDER BY id;
-- EXPECTED:

-- TEST: json-union-dedups-number-spellings
-- SQL:
SELECT count(*) FROM (
  SELECT data FROM t_num WHERE id = 1
  UNION
  SELECT data FROM t_num WHERE id = 2
  UNION
  SELECT data FROM t_num WHERE id = 3
);
-- EXPECTED:
1

-- TEST: json-union-with-null
-- SQL:
SELECT '{}'::json AS j
UNION
SELECT NULL::json
UNION ALL
SELECT CAST(NULL AS json)
ORDER BY 1;
-- EXPECTED:
null,
null,
{}

-- TEST: json-union-with-bare-null
-- SQL:
SELECT '{}'::json AS j UNION SELECT NULL ORDER BY 1;
-- EXPECTED:
null,
{}

-- TEST: json-union-all-bare-null-first
-- SQL:
SELECT NULL AS j UNION ALL SELECT '{}'::json UNION ALL SELECT NULL ORDER BY 1;
-- EXPECTED:
null,
null,
{}

-- TEST: json-union-with-integer-rejected
-- SQL:
SELECT '{}'::json AS j UNION SELECT 1;
-- ERROR:
Unable to unify inconsistent types: Json and Integer

-- TEST: json-union-typed-null-with-integer-rejected
-- SQL:
SELECT NULL::json UNION SELECT 1;
-- ERROR:
Unable to unify inconsistent types: Json and Integer

-- TEST: init-extract
-- SQL:
DROP TABLE IF EXISTS t_extract;
CREATE TABLE t_extract (id INTEGER PRIMARY KEY, data JSON);
INSERT INTO t_extract VALUES (1, '{"a":{"b":"c"},"d":[10,20,30],"f":42}'::json);

-- TEST: json-extract-path-object
-- SQL:
SELECT JSON_EXTRACT_PATH(data, 'a') FROM t_extract WHERE id = 1;
-- EXPECTED:
{"b": "c"}

-- TEST: json-extract-path-nested-scalar
-- SQL:
SELECT JSON_EXTRACT_PATH(data, 'a', 'b') FROM t_extract WHERE id = 1;
-- EXPECTED:
'c'

-- TEST: json-extract-path-array
-- SQL:
SELECT JSON_EXTRACT_PATH(data, 'd') FROM t_extract WHERE id = 1;
-- EXPECTED:
[10, 20, 30]

-- TEST: json-extract-path-scalar
-- SQL:
SELECT JSON_EXTRACT_PATH(data, 'f') FROM t_extract WHERE id = 1;
-- EXPECTED:
42

-- TEST: json-extract-path-missing
-- SQL:
SELECT JSON_EXTRACT_PATH(data, 'missing') FROM t_extract WHERE id = 1;
-- EXPECTED:
null

-- TEST: json-extract-path-array-index
-- SQL:
SELECT
  JSON_EXTRACT_PATH(data, 'd', '0'),
  JSON_EXTRACT_PATH(data, 'd', '1'),
  JSON_EXTRACT_PATH(data, 'd', '-1')
FROM t_extract
WHERE id = 1;
-- EXPECTED:
10, 20, 30

-- TEST: json-extract-path-explicit-text-key-cast
-- SQL:
SELECT JSON_EXTRACT_PATH(data, 'd', 1::text) FROM t_extract WHERE id = 1;
-- EXPECTED:
20

-- TEST: json-extract-path-raw-json
-- SQL:
EXPLAIN (RAW) SELECT JSON_EXTRACT_PATH(data, 'd', '1') FROM t_extract;
-- EXPECTED:
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "t_extract"."data" [ 'd'::text ] [ '1'::text ] :: json as "col_1" FROM "t_extract"
''
plan:
    [0] SCAN TABLE t_extract (~1048576 rows)

-- TEST: json-extract-path-raw-map
-- SQL:
EXPLAIN (RAW)
SELECT JSON_EXTRACT_PATH(distribution, 'ShardedImplicitly', '1')
FROM _pico_table;
-- EXPECTED:
╭───────────────────╮
│ 1. Query (ROUTER) │
╰───────────────────╯
''
SELECT "_pico_table"."distribution"::json [ 'ShardedImplicitly'::text ] [ '1'::text ] :: json as "col_1" FROM "_pico_table"
''
plan:
    [0] SCAN TABLE _pico_table (~1048576 rows)

-- TEST: json-extract-path-raw-path-parameters
-- SQL:
EXPLAIN (RAW) SELECT JSON_EXTRACT_PATH(data, $1, $2) FROM t_extract;
-- PARAMS:
'd', '1'
-- EXPECTED:
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "t_extract"."data" [ 'd'::text ] [ '1'::text ] :: json as "col_1" FROM "t_extract"
''
plan:
    [0] SCAN TABLE t_extract (~1048576 rows)

-- TEST: json-extract-path-raw-untyped-document-parameter
-- SKIP_FOR: iproto
-- SQL:
EXPLAIN (RAW) SELECT JSON_EXTRACT_PATH($1, $2, $3);
-- PARAMS:
'{"d": [10, 20, 30]}', 'd', '1'
-- EXPECTED:
╭───────────────────╮
│ 1. Query (ROUTER) │
╰───────────────────╯
''
SELECT '{"d": [10, 20, 30]}'::json [ 'd'::text ] [ '1'::text ] :: json as "col_1"
''
plan:
    [0] TRIVIAL

-- TEST: json-extract-path-raw-nested
-- SQL:
EXPLAIN (RAW)
SELECT JSON_EXTRACT_PATH(JSON_EXTRACT_PATH(data, 'a'), 'b')
FROM t_extract;
-- EXPECTED:
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "t_extract"."data" [ 'a'::text ] :: json [ 'b'::text ] :: json as "col_1" FROM "t_extract"
''
plan:
    [0] SCAN TABLE t_extract (~1048576 rows)

-- TEST: json-extract-path-raw-group-by-alias
-- SQL:
EXPLAIN (RAW)
SELECT JSON_EXTRACT_PATH(data, 'a') AS extracted
FROM t_extract
GROUP BY extracted
ORDER BY extracted;
-- EXPECTED:
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "t_extract"."data" [ 'a'::text ] :: json as "gr_expr_1" FROM "t_extract" GROUP BY "t_extract"."data" [ 'a'::text ] :: json
''
plan:
    [0] SCAN TABLE t_extract (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT "extracted" FROM ( SELECT "COL_0" as "extracted" FROM "_tmp_4117842499118603289_0136" GROUP BY "COL_0" ) ORDER BY "extracted"
''
plan:
    [0] SCAN TABLE _tmp_4117842499118603289_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY

-- TEST: json-extract-path-requires-path
-- SQL:
EXPLAIN (RAW) SELECT JSON_EXTRACT_PATH(data) FROM t_extract;
-- ERROR:
could not resolve function overload for json_extract_path\(json\)

-- TEST: json-extract-path-rejects-integer-source
-- SQL:
EXPLAIN (RAW) SELECT JSON_EXTRACT_PATH(id, 'a') FROM t_extract;
-- ERROR:
could not resolve function overload for json_extract_path\(int, text\)

-- TEST: json-extract-path-rejects-text-source
-- SQL:
EXPLAIN (RAW) SELECT JSON_EXTRACT_PATH(data::text, 'a') FROM t_extract;
-- ERROR:
could not resolve function overload for json_extract_path\(text, text\)

-- TEST: json-extract-path-rejects-boolean-key
-- SQL:
EXPLAIN (RAW) SELECT JSON_EXTRACT_PATH(data, 'd', true) FROM t_extract;
-- ERROR:
could not resolve function overload for json_extract_path\(json, text, bool\)

-- TEST: json-extract-path-rejects-integer-key-json
-- SQL:
EXPLAIN (RAW) SELECT JSON_EXTRACT_PATH(data, 'd', 1) FROM t_extract;
-- ERROR:
could not resolve function overload for json_extract_path\(json, text, int\)

-- TEST: json-extract-path-rejects-integer-key-map
-- SQL:
EXPLAIN (RAW)
SELECT JSON_EXTRACT_PATH(distribution, 'ShardedImplicitly', 1)
FROM _pico_table;
-- ERROR:
could not resolve function overload for json_extract_path\(map, text, int\)

-- TEST: json-extract-path-rejects-integer-column-key
-- SQL:
EXPLAIN (RAW) SELECT JSON_EXTRACT_PATH(data, 'd', id) FROM t_extract;
-- ERROR:
could not resolve function overload for json_extract_path\(json, text, int\)

-- TEST: json-extract-path-rejects-integer-parameter-key
-- SQL:
EXPLAIN (RAW) SELECT JSON_EXTRACT_PATH(data, 'd', $1::int) FROM t_extract;
-- PARAMS:
1
-- ERROR:
could not resolve function overload for json_extract_path\(json, text, int\)

-- TEST: init-array
-- SQL:
DROP TABLE IF EXISTS t_arr;
CREATE TABLE t_arr (id INTEGER PRIMARY KEY, data JSON);
INSERT INTO t_arr VALUES (1, '[10,20,30]'::json);

-- TEST: json-subscript-array-zero-based
-- SQL:
SELECT data[0], data[1], data[2] FROM t_arr WHERE id = 1;
-- EXPECTED:
10, 20, 30

-- TEST: json-subscript-array-negative
-- SQL:
SELECT data[-1], data[-2], data[-3] FROM t_arr WHERE id = 1;
-- EXPECTED:
30, 20, 10

-- TEST: json-subscript-array-out-of-range
-- SQL:
SELECT data[3], data[-4] FROM t_arr WHERE id = 1;
-- EXPECTED:
null, null

-- TEST: json-subscript-array-text-key
-- SQL:
SELECT data['0'], data['1'], data['-1'] FROM t_arr WHERE id = 1;
-- EXPECTED:
10, 20, 30

-- TEST: json-subscript-array-invalid-text-key
-- SQL:
SELECT data['10kek'], data['abc'], data[''] FROM t_arr WHERE id = 1;
-- EXPECTED:
null, null, null

-- TEST: init-object
-- SQL:
DROP TABLE IF EXISTS t_obj;
CREATE TABLE t_obj (id INTEGER PRIMARY KEY, data JSON);
INSERT INTO t_obj VALUES (1, '{"0":"a","1":"b","10":"c"}'::json);

-- TEST: json-subscript-object-numeric-key
-- SQL:
SELECT data[0], data[1], data[10] FROM t_obj WHERE id = 1;
-- EXPECTED:
'a', 'b', 'c'

-- TEST: json-subscript-object-text-key
-- SQL:
SELECT data['0'], data['10'] FROM t_obj WHERE id = 1;
-- EXPECTED:
'a', 'c'

-- TEST: json-subscript-object-missing-key
-- SQL:
SELECT data[5], data['missing'] FROM t_obj WHERE id = 1;
-- EXPECTED:
null, null

-- TEST: init-path
-- SQL:
DROP TABLE IF EXISTS t_path;
CREATE TABLE t_path (id INTEGER PRIMARY KEY, data JSON);
INSERT INTO t_path VALUES (1, '{"d":[10,20,30]}'::json);

-- TEST: json-subscript-object-array-path
-- SQL:
SELECT data['d'][0], data['d'][-1] FROM t_path WHERE id = 1;
-- EXPECTED:
10, 30

-- TEST: init-mixed
-- SQL:
DROP TABLE IF EXISTS t_mixed;
CREATE TABLE t_mixed (id INTEGER PRIMARY KEY, arr INTEGER[], data JSON);
INSERT INTO t_mixed VALUES (1, ARRAY[10,20,30], '[10,20,30]'::json);

-- TEST: native-array-one-based
-- SQL:
SELECT arr[1], arr[2], arr[3], arr[0], arr[4] FROM t_mixed WHERE id = 1;
-- EXPECTED:
10, 20, 30, null, null

-- TEST: init-grouping
-- SQL:
DROP TABLE IF EXISTS t_group;
CREATE TABLE t_group (id INTEGER PRIMARY KEY, data JSON);
INSERT INTO t_group VALUES
  (1, '{"a":1}'::json),
  (2, '{"a":2}'::json),
  (3, '{"a":1}'::json),
  (4, '[1,2]'::json),
  (5, NULL);

-- TEST: json-distinct
-- SQL:
SELECT DISTINCT data FROM t_group ORDER BY 1;
-- EXPECTED:
null,
[1, 2],
{"a": 1},
{"a": 2}

-- TEST: json-distinct-normalizes-key-order
-- SQL:
SELECT DISTINCT data FROM (
  SELECT '{"bb":1,"a":2}'::json AS data FROM t_group
  UNION ALL
  SELECT '{"a":2,"bb":1}'::json AS data FROM t_group
);
-- EXPECTED:
{"a": 2, "bb": 1}

-- TEST: json-group-by
-- SQL:
SELECT data, count(*) FROM t_group GROUP BY data ORDER BY 1;
-- EXPECTED:
null, 1,
[1, 2], 1,
{"a": 1}, 2,
{"a": 2}, 1

-- TEST: json-group-by-having
-- SQL:
SELECT data, count(*) FROM t_group GROUP BY data HAVING count(*) > 1;
-- EXPECTED:
{"a": 1}, 2

-- TEST: json-group-by-subscript
-- SQL:
SELECT data['a'], count(*) FROM t_group GROUP BY data['a'] ORDER BY 1;
-- EXPECTED:
null, 2,
1, 2,
2, 1

-- TEST: init-number-spellings
-- SQL:
DROP TABLE IF EXISTS t_spell;
CREATE TABLE t_spell (id INTEGER PRIMARY KEY, data JSON);
INSERT INTO t_spell VALUES
  (0, '42'::json),
  (1, '42.0'::json),
  (2, '42.00'::json),
  (3, '42.000'::json),
  (4, '42.0000'::json),
  (5, '42.00000'::json),
  (6, '43'::json);

-- TEST: json-group-by-number-spellings
-- SQL:
SELECT count(*) FROM t_spell GROUP BY data ORDER BY 1;
-- EXPECTED:
1,
6

-- TEST: json-count-distinct-number-spellings
-- SQL:
SELECT count(DISTINCT data) FROM t_spell;
-- EXPECTED:
2

-- TEST: init-reject
-- SQL:
DROP TABLE IF EXISTS t_reject;
CREATE TABLE t_reject (id INTEGER PRIMARY KEY, data JSON);
INSERT INTO t_reject VALUES (1, '{"a":1}'::json);

-- TEST: json-primary-key-rejected
-- SQL:
CREATE TABLE bad_pk (id JSON PRIMARY KEY) DISTRIBUTED GLOBALLY;
-- ERROR:
JSON columns are not indexable

-- TEST: json-composite-primary-key-rejected
-- SQL:
CREATE TABLE bad_pk2 (id INT, j JSON, PRIMARY KEY (j)) DISTRIBUTED GLOBALLY;
-- ERROR:
JSON columns are not indexable

-- TEST: json-sharding-key-rejected
-- SQL:
CREATE TABLE bad_shard (id INT PRIMARY KEY, j JSON) DISTRIBUTED BY (j);
-- ERROR:
not of scalar type

-- TEST: json-array-column-rejected
-- SQL:
CREATE TABLE bad_arr (id INT PRIMARY KEY, a json[]);
-- ERROR:
unsupported type: json cannot be used as an array element type

-- TEST: json-array-cast-rejected
-- SQL:
SELECT ARRAY[]::json[] FROM t_reject;
-- ERROR:
unsupported type: json cannot be used as an array element type

-- TEST: json-array-literal-rejected
-- SQL:
SELECT ARRAY['{"a":1}'::json] FROM t_reject;
-- ERROR:
json cannot be used as an array element type

-- TEST: init-index-table
-- SQL:
DROP TABLE IF EXISTS idxt;
CREATE TABLE idxt (id INT PRIMARY KEY, j JSON) DISTRIBUTED GLOBALLY;

-- TEST: json-index-rejected
-- SQL:
CREATE INDEX ix ON idxt (j);
-- ERROR:
index type tree does not support column type json

-- TEST: json-unique-index-rejected
-- SQL:
CREATE UNIQUE INDEX ixu ON idxt (j);
-- ERROR:
index type tree does not support column type json

-- TEST: json-literal-unquoted-key-rejected
-- SQL:
SELECT '{"userId": 1, userName: "Alice"}'::json;
-- ERROR:
Failed to parse JSON

-- TEST: json-literal-trailing-comma-rejected
-- SQL:
SELECT '[1, 2,]'::json;
-- ERROR:
Failed to parse JSON

-- TEST: json-literal-trailing-garbage-rejected
-- SQL:
SELECT '{"a":1} garbage'::json;
-- ERROR:
Failed to parse JSON

-- TEST: json-literal-truncated-rejected
-- SQL:
SELECT '{"a":'::json;
-- ERROR:
Failed to parse JSON

-- TEST: json-literal-coerced-invalid-gives-reason
-- SQL:
INSERT INTO t_kinds VALUES (7, '[1, 2,]');
-- ERROR:
failed to parse '\[1, 2,\]' as a value of type json: Failed to parse JSON: .+, consider using explicit type casts

-- TEST: init-pgkey
-- SQL:
DROP TABLE IF EXISTS t_pgkey;
CREATE TABLE t_pgkey (id INTEGER PRIMARY KEY, data JSON);
INSERT INTO t_pgkey VALUES (1, '{"a": "b", "b": [2, 3]}'::json);

-- TEST: json-subscript-key-cast-to-text-keeps-quotes
-- SQL:
SELECT data[data['a']::text] FROM t_pgkey WHERE id = 1;
-- EXPECTED:
null

-- TEST: json-subscript-key-unquoted-by-substring
-- SQL:
SELECT data[substring(data['a']::text from '"(.*)"')] FROM t_pgkey WHERE id = 1;
-- EXPECTED:
[2, 3]

-- TEST: init-params
-- SQL:
DROP TABLE IF EXISTS t_params;
CREATE TABLE t_params (id INTEGER PRIMARY KEY, data JSON);
INSERT INTO t_params VALUES (1, '{"a": 1, "b": [10, 20]}');
INSERT INTO t_params VALUES (2, '{"a": 2}');

-- TEST: json-param-cast-object
-- SQL:
SELECT $1::json;
-- PARAMS:
'{"b": 2, "a": 1}'
-- EXPECTED:
{"a": 1, "b": 2}

-- TEST: json-param-cast-array
-- SQL:
SELECT CAST($1 AS json);
-- PARAMS:
'[1, 2]'
-- EXPECTED:
[1, 2]

-- TEST: json-param-cast-scalar
-- SQL:
SELECT $1::json;
-- PARAMS:
'42'
-- EXPECTED:
42

-- TEST: json-param-cast-null
-- SQL:
SELECT $1::json;
-- PARAMS:
nil
-- EXPECTED:
null

-- TEST: json-param-cast-invalid
-- SQL:
SELECT $1::json;
-- PARAMS:
'{bad'
-- ERROR:
(Failed to parse JSON|is not a valid jsonb)

-- TEST: json-param-insert-cast
-- SQL:
INSERT INTO t_params VALUES (3, $1::json);
-- PARAMS:
'{"c": 3}'

-- TEST: json-param-insert-null
-- SQL:
INSERT INTO t_params VALUES (4, $1);
-- PARAMS:
nil

-- TEST: json-param-insert-wrong-type
-- SQL:
INSERT INTO t_params VALUES (5, $1);
-- PARAMS:
42
-- ERROR:
INSERT column at position 2 is of type json, but expression is of type int

-- TEST: json-param-insert-untyped-text
-- SKIP_FOR: iproto
-- SQL:
INSERT INTO t_params VALUES (5, $1);
-- PARAMS:
'{"d": 5}'

-- TEST: json-param-insert-untyped-text-iproto
-- SKIP_FOR: pgproto
-- SQL:
INSERT INTO t_params VALUES (5, $1);
-- PARAMS:
'{"d": 5}'
-- ERROR:
INSERT column at position 2 is of type json, but expression is of type text

-- TEST: json-param-update-cast
-- SQL:
UPDATE t_params SET data = $1::json WHERE id = 4;
-- PARAMS:
'{"e": 4}'

-- TEST: json-param-update-untyped-text
-- SKIP_FOR: iproto
-- SQL:
UPDATE t_params SET data = $1 WHERE id = 3;
-- PARAMS:
'{"c": 30}'

-- TEST: json-param-update-untyped-text-iproto
-- SKIP_FOR: pgproto
-- SQL:
UPDATE t_params SET data = $1 WHERE id = 3;
-- PARAMS:
'{"c": 30}'
-- ERROR:
column "data" is of type json, but expression is of type text

-- TEST: json-param-compare-cast
-- SQL:
SELECT id FROM t_params WHERE data = $1::json;
-- PARAMS:
'{"a": 2}'
-- EXPECTED:
2

-- TEST: json-param-compare-untyped-text
-- SKIP_FOR: iproto
-- SQL:
SELECT id FROM t_params WHERE data = $1;
-- PARAMS:
'{"a": 2}'
-- EXPECTED:
2

-- TEST: json-param-compare-untyped-text-iproto
-- SKIP_FOR: pgproto
-- SQL:
SELECT id FROM t_params WHERE data = $1;
-- PARAMS:
'{"a": 2}'
-- ERROR:
could not resolve operator overload for =\(json, text\)

-- TEST: json-param-subscript-key
-- SQL:
SELECT data[$1] FROM t_params WHERE id = 1;
-- PARAMS:
'a'
-- EXPECTED:
1

-- TEST: json-param-subscript-index
-- SQL:
SELECT data['b'][$1] FROM t_params WHERE id = 1;
-- PARAMS:
1
-- EXPECTED:
20

-- TEST: json-param-extract-path
-- SQL:
SELECT JSON_EXTRACT_PATH(data, $1) FROM t_params WHERE id = 1;
-- PARAMS:
'b'
-- EXPECTED:
[10, 20]

-- TEST: json-param-union-cast-null
-- SQL:
SELECT '{}'::json AS j UNION SELECT $1::json ORDER BY 1;
-- PARAMS:
nil
-- EXPECTED:
null,
{}

-- TEST: json-param-rows
-- SKIP_FOR: iproto
-- SQL:
SELECT id, data FROM t_params ORDER BY id;
-- EXPECTED:
1, {"a": 1, "b": [10, 20]},
2, {"a": 2},
3, {"c": 30},
4, {"e": 4},
5, {"d": 5}

-- TEST: json-param-rows-iproto
-- SKIP_FOR: pgproto
-- SQL:
SELECT id, data FROM t_params ORDER BY id;
-- EXPECTED:
1, {"a": 1, "b": [10, 20]},
2, {"a": 2},
3, {"c": 3},
4, {"e": 4}
