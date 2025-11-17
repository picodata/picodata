-- TEST: insert
-- SQL:
DROP TABLE IF EXISTS integer_table;
DROP TABLE IF EXISTS unsigned_table;
CREATE TABLE integer_table (key INTEGER PRIMARY KEY, value INTEGER);
CREATE TABLE unsigned_table (key UNSIGNED PRIMARY KEY, value UNSIGNED);


-- TEST: test_insert_integer_max_value_into_int_table
-- SQL:
INSERT INTO integer_table SELECT 1, 9223372036854775807;

-- TEST: test_insert_integer_max_value_into_unsigned_table
-- SQL:
INSERT INTO unsigned_table SELECT 1, 9223372036854775807;

-- TEST: test_insert_integer_max_value_into_int_table
-- SQL:
INSERT INTO integer_table SELECT 2, 9223372036854775806;

-- TEST: test_insert_integer_max_value_into_unsigned_table
-- SQL:
INSERT INTO unsigned_table SELECT 2, 9223372036854775806;


-- TEST: test_insert_value_above_max_value_into_int_table
-- SQL:
INSERT INTO integer_table SELECT 3, 9223372036854775808;
-- ERROR:
sbroad: invalid query: value doesn\'t fit into integer range

-- TEST: test_insert_value_above_max_value_into_unsigned_table
-- SQL:
INSERT INTO unsigned_table SELECT 3, 9223372036854775808;
-- ERROR:
sbroad: invalid query: value doesn\'t fit into integer range

-- TEST: test_insert_value_above_max_value_into_int_table#2
-- SQL:
INSERT INTO integer_table SELECT 3, 9223372036854775807 * 2;
-- ERROR:
failed decoding i64: out of range integral type conversion attempted

-- TEST: test_insert_value_above_max_value_into_unsigned_table#2
-- SQL:
INSERT INTO unsigned_table SELECT 3, 9223372036854775807 * 2;
-- ERROR:
failed decoding i64: out of range integral type conversion attempted

-- TEST: test_insert_value_above_max_value_into_int_table#3
-- SQL:
INSERT INTO integer_table SELECT 3, 18446744073709551614;
-- ERROR:
sbroad: invalid query: value doesn\'t fit into integer range

-- TEST: test_insert_value_above_max_value_into_unsigned_table#3
-- SQL:
INSERT INTO unsigned_table SELECT 3, 18446744073709551614;
-- ERROR:
sbroad: invalid query: value doesn\'t fit into integer range

-- TEST: test_insert_value_above_max_value_into_int_table#4
-- SQL:
INSERT INTO integer_table SELECT 3, '9223372036854775808'::int;
-- ERROR:
failed decoding i64: out of range integral type conversion attempted

-- TEST: test_insert_value_above_max_value_into_unsigned_table#4
-- SQL:
INSERT INTO unsigned_table SELECT 3, '9223372036854775808'::int;
-- ERROR:
failed decoding i64: out of range integral type conversion attempted


-- TEST: test_insert_value_above_max_value_into_int_table_via_values
-- SQL:
INSERT INTO integer_table VALUES(3, 9223372036854775808);
-- ERROR:
sbroad: invalid query: value doesn\'t fit into integer range

-- TEST: test_insert_value_above_max_value_into_unsigned_table_via_values
-- SQL:
INSERT INTO unsigned_table VALUES(3, 9223372036854775808);
-- ERROR:
sbroad: invalid query: value doesn\'t fit into integer range

-- TEST: test_insert_value_above_max_value_into_int_table_via_values#2
-- SQL:
INSERT INTO integer_table VALUES(3, 9223372036854775807 * 2);
-- ERROR:
failed decoding i64: out of range integral type conversion attempted

-- TEST: test_insert_value_above_max_value_into_unsigned_table_via_values#2
-- SQL:
INSERT INTO unsigned_table VALUES(3, 9223372036854775807 * 2);
-- ERROR:
failed decoding i64: out of range integral type conversion attempted

-- TEST: test_insert_value_above_max_value_into_int_table_via_values#3
-- SQL:
INSERT INTO integer_table VALUES(3, 18446744073709551614);
-- ERROR:
sbroad: invalid query: value doesn\'t fit into integer range

-- TEST: test_insert_value_above_max_value_into_unsigned_table_via_values#3
-- SQL:
INSERT INTO unsigned_table VALUES(3, 18446744073709551614);
-- ERROR:
sbroad: invalid query: value doesn\'t fit into integer range

-- TEST: test_insert_value_above_max_value_into_int_table_via_values#4
-- SQL:
INSERT INTO integer_table VALUES(3, '9223372036854775808'::int);
-- ERROR:
failed decoding i64: out of range integral type conversion attempted

-- TEST: test_insert_value_above_max_value_into_unsigned_table_via_values#4
-- SQL:
INSERT INTO unsigned_table VALUES(3, '9223372036854775808'::int);
-- ERROR:
failed decoding i64: out of range integral type conversion attempted


-- TEST: test_insertions_of_large_values_into_int_table_was_not_successfull
-- SQL:
SELECT value FROM integer_table;
-- UNORDERED:
9223372036854775806, 9223372036854775807

-- TEST: test_insertions_of_large_values_into_unsigned_table_was_not_successfull
-- SQL:
SELECT value FROM integer_table;
-- UNORDERED:
9223372036854775806, 9223372036854775807

-- TEST: test_still_can_work_with_large_values_but_it_is_useless_since_values_from_tables_are_smaller
-- SQL:
SELECT value FROM integer_table WHERE value > 9223372036854775807 * 2;
-- UNORDERED:
