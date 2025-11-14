-- TEST: test_arbitrary_expr
-- SQL:
DROP TABLE IF EXISTS arithmetic_space;
CREATE TABLE arithmetic_space (id int primary key, a int, b int, c int, d int, e int, f int, boolean_col bool, string_col string, number_col double);
INSERT INTO "arithmetic_space"
("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
VALUES (1, 1, 2, 3, -1, -1, -1, true, '123', 1),
        (2, 2, 4, 6, -2, -2, -2, true, '123', 2),
        (3, 3, 6, 9, -3, -3, -3, true, '123', 3),
        (4, 4, 8, 12, -4, -4, -4, true, '123', 4),
        (5, 5, 10, 15, -5, -5, -5, true, '123', 5),
        (6, 6, 12, 18, -6, -6, -6, true, '123', 6),
        (7, 7, 14, 21, -7, -7, -7, true, '123', 7),
        (8, 8, 16, 24, -8, -8, -8, true, '123', 8),
        (9, 9, 18, 27, -9, -9, -9, true, '123', 9),
        (10, 10, 20, 30, -10, -10, -10, true, '123', 10);

-- TEST: test_arbitrary_invalid-1
-- SQL:
select "id" + 1 as "alias" > a from "arithmetic_space";
-- ERROR:
rule parsing error

-- TEST: test_arbitrary_invalid-2
-- SQL:
'select id" + 1 as "alias" > "a" is not null from "arithmetic_space"';
-- ERROR:
rule parsing error

-- TEST: test_arbitrary_invalid-3
-- SQL:
select "a" + "b" and true from "arithmetic_space";
-- ERROR:
could not resolve operator overload for and\(int, bool\)

-- TEST: test_arbitrary_invalid-4
-- SQL:
SELECT
    CASE "id"
        WHEN 1 THEN 'first'::text
        ELSE 42
    END "case_result"
FROM "arithmetic_space";
-- ERROR:
CASE/THEN types text and int cannot be matched

-- TEST: test_arbitrary_valid-1
-- SQL:
select "id" from "arithmetic_space";
-- EXPECTED:
1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- TEST: test_arbitrary_valid-2
-- SQL:
select "id" - 9 > 0 from "arithmetic_space";
-- EXPECTED:
False, False, False, False, False, False, False, False, False, True

-- TEST: test_arbitrary_valid-3
-- SQL:
select "id" + "b" > "id" + "b", "id" + "b" > "id" + "b" as "cmp" from "arithmetic_space";
-- EXPECTED:
False, False, False, False, False, False, False,
False, False, False, False, False, False, False,
False, False, False, False, False, False,

-- TEST: test_arbitrary_valid-4
-- SQL:
select 0 = "id" + "f", 0 = "id" + "f" as "cmp" from "arithmetic_space";
-- EXPECTED:
True, True, True, True, True, True,
True, True, True, True, True, True,
True, True, True, True, True, True,
True, True

-- TEST: test_arbitrary_valid-5
-- SQL:
select 1 > 0, 1 > 0 as "cmp" from "arithmetic_space";
-- EXPECTED:
True, True, True, True, True, True,
True, True, True, True, True, True,
True, True, True, True, True, True,
True, True

-- TEST: test_arbitrary_valid-6
-- SQL:
select
    "id" between "id" - 1 and "id" * 4,
    "id" between "id" - 1 and "id" * 4 as "between"
from
    "arithmetic_space";
-- EXPECTED:
True, True, True, True, True, True,
True, True, True, True, True, True,
True, True, True, True, True, True,
True, True

-- TEST: test_arbitrary_valid-7
-- SQL:
select
    "id" between "id" - 1 and "id" * 4,
    "id" between "id" - 1 and "id" * 4 as "between"
from
    "arithmetic_space";
-- EXPECTED:
True, True, True, True, True, True,
True, True, True, True, True, True,
True, True, True, True, True, True,
True, True

-- TEST: test_arbitrary_valid-8
-- SQL:
SELECT "COLUMN_1" FROM (VALUES (1));
-- EXPECTED:
1

-- TEST: test_arbitrary_valid-9
-- SQL:
SELECT CAST("COLUMN_1" as int) FROM (VALUES (1));
-- EXPECTED:
1

-- TEST: test_arbitrary_valid-10
-- SQL:
SELECT "COLUMN_1" as "колонка" FROM (VALUES (1));
-- EXPECTED:
1

-- TEST: test_arbitrary_valid-11
-- SQL:
SELECT
    CASE "id"
        WHEN 1 THEN 'first'
        WHEN 2 THEN 'second'
        ELSE '42'
    END "case_result"
FROM "arithmetic_space";
-- EXPECTED:
'first', 'second', '42', '42', '42', '42', '42', '42', '42', '42'

-- TEST: test_arbitrary_valid-13
-- SQL:
SELECT
    "id",
    CASE
        WHEN "id" = 7 THEN 'first'
        WHEN "id" / 2 < 4 THEN 'second'
    END "case_result"
FROM "arithmetic_space";
-- EXPECTED:
1, 'second',
2, 'second',
3, 'second',
4, 'second',
5, 'second',
6, 'second',
7, 'first',
8, null,
9, null,
10, null,

-- TEST: test_arbitrary_valid-14
-- SQL:
SELECT
    "id",
    CASE
        WHEN false THEN 0
        WHEN "id" < 3 THEN 1
        WHEN "id" > 3 AND "id" < 8 THEN 2
        ELSE
            CASE
                WHEN "id" = 8 THEN 3
                WHEN "id" = 9 THEN 4
                ELSE 0
            END
    END
FROM "arithmetic_space";
-- EXPECTED:
1, 1, 2, 1, 3, 0, 4, 2, 5, 2, 6, 2, 7, 2, 8, 3, 9, 4, 10, 0

-- TEST: test_values-1
-- SQL:
VALUES (8, 8, null), (9, 9, 'hello');
-- EXPECTED:
8, 8, null, 9, 9, 'hello'

-- TEST: test_values-2
-- SQL:
VALUES (9, 9, 'hello'), (8, 8, null);
-- EXPECTED:
9, 9, 'hello', 8, 8, null

-- TEST: test_index_current_state_from_pico_instance
-- SQL:
SELECT DISTINCT current_state[1] FROM _pico_instance;
-- EXPECTED:
'Online'

-- TEST: test_arithmetic_expr_as_an_index
-- SQL:
SELECT DISTINCT current_state[1 + 2 - 2] FROM _pico_instance;
-- EXPECTED:
'Online'

-- TEST: test_out_of_bounds_index_returns_null-1
-- SQL:
SELECT DISTINCT current_state[9223372036854775807] FROM _pico_instance;
-- EXPECTED:
null

-- TEST: test_out_of_bounds_index_returns_null-2
-- SQL:
SELECT DISTINCT current_state[0] FROM _pico_instance;
-- EXPECTED:
null

-- TEST: test_out_of_bounds_index_returns_null-3
-- SQL:
SELECT DISTINCT current_state[-9223372036854775808] FROM _pico_instance;
-- EXPECTED:
null

-- TEST: test_subquery_as_an_index-1
-- SQL:
SELECT DISTINCT current_state[(SELECT 1)] FROM _pico_instance;
-- EXPECTED:
'Online'

-- TEST: test_subquery_as_an_index-2
-- SQL:
SELECT DISTINCT current_state[(SELECT count(*) FROM _pico_instance) / count(*)] FROM _pico_instance;
-- EXPECTED:
'Online'

-- TEST: test_index_with_between
-- SQL:
SELECT DISTINCT current_state[1]::text BETWEEN 'Ofline' AND 'Online' FROM _pico_instance;
-- EXPECTED:
true

-- TEST: test_filter_with_index
WITH online_instances AS (
  SELECT uuid FROM _pico_instance WHERE current_state[1]::text = 'Online'
), all_instances AS (
  SELECT uuid FROM _pico_instance
)
SELECT (SELECT count(*) FROM online_instances) = (SELECT count(*) FROM all_instances);
-- EXPECTED:
true

-- TEST: test_between_filter_with_subuery_and_index
-- SQL:
WITH all_instances1 AS (
SELECT * FROM _pico_instance WHERE current_state[(SELECT 1)]::text BETWEEN 'Offline' AND 'Online'
), all_instances2 AS (
SELECT * FROM _pico_instance
)
SELECT (SELECT count(*) FROM all_instances1) = (SELECT count(*) FROM all_instances2);
-- EXPECTED:
true

-- TEST: test_index_chain_from_pico_index
-- SQL:
SELECT DISTINCT parts[1][2] FROM _pico_index ORDER BY 1;
-- EXPECTED:
'integer',
'string',
'unsigned'

-- TEST: test_filter_with_index_chain
-- SQL:
WITH string_and_numeric_indexes AS (
  SELECT name FROM _pico_index WHERE parts[1][2]::text IN ('string', 'integer', 'unsigned')
), all_indexes AS (
  SELECT name FROM _pico_index
)
SELECT (SELECT count(*) FROM string_and_numeric_indexes) = (SELECT count(*) FROM all_indexes);
-- EXPECTED:
true

-- TEST: test_index_explain
-- SQL:
EXPLAIN SELECT current_state[1] FROM _pico_instance;
-- EXPECTED:
projection ("_pico_instance"."current_state"::array[1::int] -> "col_1")
    scan "_pico_instance"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = any

-- TEST: test_index_explain_raw
-- SQL:
EXPLAIN (raw) SELECT current_state[1] FROM _pico_instance;
-- EXPECTED:
1. Query (ROUTER):
SELECT "_pico_instance"."current_state" [ CAST($1 AS int) ] as "col_1" FROM "_pico_instance"
+----------+-------+------+-------------------------------------------+
| selectid | order | from | detail                                    |
+=====================================================================+
| 0        | 0     | 0    | SCAN TABLE _pico_instance (~1048576 rows) |
+----------+-------+------+-------------------------------------------+
''

-- TEST: test_index_returns_any
-- SQL:
SELECT current_state[2] + 1 FROM _pico_instance;
-- ERROR:
could not resolve operator overload for \+\(any, int\)

-- TEST: test_index_cannot_be_string
-- SQL:
SELECT current_state['lol'::text] FROM _pico_instance;
-- ERROR:
could not resolve operator overload for \[\]\(array, text\)

-- TEST: test_cannot_index_expr_of_random_type
-- SQL:
SELECT (1 + 1)[1];
-- ERROR:
cannot index expression of type int

-- TEST: test_index_with_maps_prepare
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t (id int primary key, txt text);

-- TEST: test_index_with_maps-1
-- SQL:
SELECT
  distribution['ShardedImplicitly'][1][1],
  distribution['ShardedImplicitly'][2],
  distribution['ShardedImplicitly'][3]
FROM _pico_table
WHERE name = 't';
-- EXPECTED:
'id', 'murmur3', 'default',

-- TEST: test_index_with_maps-2
-- SQL:
WITH murmur_tables AS (
  SELECT * FROM _pico_table WHERE distribution['ShardedImplicitly'][2]::text = 'murmur3'
), sharded_tables AS (
  SELECT * FROM _pico_table WHERE distribution['ShardedImplicitly'] IS NOT NULL
)
SELECT (SELECT count(*) FROM murmur_tables) = (SELECT count(*) FROM sharded_tables);
-- EXPECTED:
true

-- TEST: test_map_index_must_be_a_string
-- SQL:
SELECT distribution[1]['ShardedImplicitly'] FROM _pico_table WHERE name = 't';
-- ERROR:
could not resolve operator overload for \[\]\(map, int, text\)

-- TEST: test_subsequent_indexes_must_be_strings_or_integers-1
-- SQL:
SELECT distribution['ShardedImplicitly'][1][false] FROM _pico_table WHERE name = 't';
-- ERROR:
could not resolve operator overload for \[\]\(map, text, int, bool\)

-- TEST: test_subsequent_indexes_must_be_strings_or_integers-2
-- SQL:
SELECT distribution['ShardedImplicitly'][1]['whatever'] FROM _pico_table WHERE name = 't';
-- EXPECTED:
null

-- TEST: test_large_index_string
-- SQL:
SELECT
  distribution['This string contains exactly 257 characters. xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']
FROM _pico_table
WHERE name = 't';
-- EXPECTED:
null

-- TEST: test_large_index_chain
-- SQL:
SELECT
  distribution['ShardedImplicitly'][1][2][3][4][5][6][7][8][9][10][11][12][13][14][15]
FROM _pico_table
WHERE name = 't';
-- EXPECTED:
null

-- TEST: test_index_of_unknown_type_is_inferred_to_text_for_maps
-- SQL:
SELECT distribution[NULL] FROM _pico_table WHERE name = 't';
-- EXPECTED:
null

-- TEST: test_index_of_unknown_type_is_inferred_to_int_for_arrays
-- SQL:
SELECT DISTINCT current_state[NULL] FROM _pico_instance;
-- EXPECTED:
null

-- TEST: test_parsing_semicolons_in_literals
-- SQL:
select ';' union select ';';
-- EXPECTED:
';'

-- TEST: test_parsing_semicolons_in_quoted_identifiers
-- SQL:
select 1 as "kek;"
-- EXPECTED:
1

-- TEST: test_quoted_identifiers_work_with_semicolons
-- SQL:
CREATE TABLE "t;" ("id;" int primary key, "a;" int, "b;" int);
INSERT INTO "t;" ("id;", "a;", "b;") VALUES (1,2,3);

-- TEST: test_quoted_identifiers_with_semicolons_in_select
-- SQL:
SELECT "t;"."id;", "a;" + "b;" AS "a+b;" FROM "t;";
-- EXPECTED:
1,5

-- TEST: test_quoted_identifiers_with_semicolons_in_rename
-- SQL:
ALTER TABLE "t;" RENAME TO "tt;";

-- TEST: test_quoted_identifiers_with_semicolons_in_delete
-- SQL:
DELETE FROM "tt;" WHERE "id;" = "id;";

-- TEST: test_quoted_identifiers_with_semicolons_in_drop

-- SQL:
DROP TABLE "tt;";

