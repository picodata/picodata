-- TEST: basic_add_column
-- SQL:
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table(id INT PRIMARY KEY, name TEXT);
ALTER TABLE test_table ADD COLUMN age INT;

-- TEST: add_column_null_explicit
-- SQL:
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table(id INT PRIMARY KEY, name TEXT);
ALTER TABLE test_table ADD COLUMN status TEXT NULL;
INSERT INTO test_table(id, name, status) VALUES (1, 'Test', NULL);

-- TEST: verify_null_explicit
-- SQL:
SELECT * FROM test_table;
-- EXPECTED:
1, 'Test', NULL

-- TEST: add_column_null_implicit
-- SQL:
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table(id INT PRIMARY KEY, name TEXT);
ALTER TABLE test_table ADD COLUMN status TEXT;
INSERT INTO test_table(id, name) VALUES (1, 'Test');

-- TEST: verify_null_implicit
-- SQL:
SELECT * FROM test_table;
-- EXPECTED:
1, 'Test', NULL

-- TEST: add_column_not_null_to_empty_table
-- SQL:
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table(id INT PRIMARY KEY, name TEXT);
ALTER TABLE test_table ADD COLUMN required_field INT NOT NULL;
INSERT INTO test_table(id, name, required_field) VALUES (1, 'Test', 42);

-- TEST: verify_not_null_empty_table
-- SQL:
SELECT * FROM test_table;
-- EXPECTED:
1, 'Test', 42

-- TEST: add_column_not_null_to_non_empty_table
-- SQL:
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table(id INT PRIMARY KEY, name TEXT);
INSERT INTO test_table(id, name) VALUES (1, 'Test');
ALTER TABLE test_table ADD COLUMN required_field INT NOT NULL;
-- ERROR:
-
-
-
Tuple field 4 (required_field) required by space format is missing

-- TEST: add_multiple_columns_with_null_constraints
-- SQL:
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table(id INT PRIMARY KEY);
ALTER TABLE test_table ADD COLUMN first_name TEXT NULL, ADD COLUMN last_name TEXT NOT NULL, ADD COLUMN email TEXT;
INSERT INTO test_table VALUES (1, NULL, 'Kunkka', NULL);
INSERT INTO test_table VALUES (2, NULL, 'Pork', 'john@call.me');
INSERT INTO test_table VALUES (3, 'Brann', 'Bronzebeard', NULL);
INSERT INTO test_table VALUES (4, 'Jastor', 'Gallywix', 'jastor@gallywix.com');

-- TEST: verify_multiple_columns
-- SQL:
SELECT * FROM test_table ORDER BY id ASC;
-- EXPECTED:
1, NULL, 'Kunkka', NULL,
2, NULL, 'Pork', 'john@call.me',
3, 'Brann', 'Bronzebeard', NULL,
4, 'Jastor', 'Gallywix', 'jastor@gallywix.com'

-- TEST: add_column_to_nonexistent_table
-- SQL:
DROP TABLE IF EXISTS test_table;
ALTER TABLE test_table ADD COLUMN new_column INT;
-- ERROR:
-
table test_table does not exist

-- TEST: add_duplicate_column
-- SQL:
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table(id INT PRIMARY KEY, name TEXT);
ALTER TABLE test_table ADD COLUMN name INT;
-- ERROR:
-
-
column name already exists

-- TEST: add_column_with_reserved_keyword_name
-- SQL:
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table(id INT PRIMARY KEY);
ALTER TABLE test_table ADD COLUMN "table" TEXT NULL;
ALTER TABLE test_table ADD COLUMN "select" INT NOT NULL;
INSERT INTO test_table VALUES (1, NULL, 42);

-- TEST: verify_reserved_keywords
-- SQL:
SELECT id, "table", "select" FROM test_table;
-- EXPECTED:
1, NULL, 42

-- TEST: add_column_different_data_types
-- SQL:
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table(id INT PRIMARY KEY);
ALTER TABLE test_table ADD COLUMN col_int INT NULL;
ALTER TABLE test_table ADD COLUMN col_text TEXT NOT NULL;
ALTER TABLE test_table ADD COLUMN col_double DOUBLE NULL;
ALTER TABLE test_table ADD COLUMN col_decimal DECIMAL NULL;
ALTER TABLE test_table ADD COLUMN col_bool BOOLEAN NULL;
INSERT INTO test_table VALUES (1, NULL, 'text value', NULL, NULL, NULL);

-- TEST: verify_different_data_types
-- SQL:
SELECT * FROM test_table;
-- EXPECTED:
1, NULL, 'text value', NULL, NULL, NULL

-- TEST: add_column_names_with_special_characters
-- SQL:
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table(id INT PRIMARY KEY);
ALTER TABLE test_table ADD COLUMN "column-with-hyphen" TEXT NULL;
ALTER TABLE test_table ADD COLUMN "column with space" TEXT NULL;
ALTER TABLE test_table ADD COLUMN "column_with_underscore" TEXT NULL;

-- TEST: not_null_constraint_enforcement_setup
-- SQL:
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table(id INT PRIMARY KEY);
ALTER TABLE test_table ADD COLUMN required_field INT NOT NULL;

-- TEST: not_null_constraint_enforcement
-- SQL:
INSERT INTO test_table(id) VALUES (1);
-- ERROR:
column "required_field" must be specified

-- TEST: null_constraint_explicit_vs_implicit
-- SQL:
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table(id INT PRIMARY KEY);
ALTER TABLE test_table ADD COLUMN implicit_nullable INT;
ALTER TABLE test_table ADD COLUMN explicit_nullable INT NULL;
ALTER TABLE test_table ADD COLUMN explicit_non_nullable INT NOT NULL;
INSERT INTO test_table VALUES (1, NULL, NULL, 52);

-- TEST: verify_null_constraints
-- SQL:
SELECT * FROM test_table;
-- EXPECTED:
1, NULL, NULL, 52
