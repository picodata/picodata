-- TEST: selecting_works_after_rename_prepare
-- SQL:
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table(id INT PRIMARY KEY, status TEXT);
INSERT INTO test_table VALUES (1, 'hi'), (2, 'kitty');
SELECT * FROM test_table;
ALTER TABLE test_table RENAME COLUMN status TO age, RENAME COLUMN id TO name;

-- TEST: selecting_works_after_rename_verify
-- SQL:
SELECT name, age FROM test_table ORDER BY name;
-- EXPECTED:
1, 'hi', 2, 'kitty'

-- TEST: rename_nonexistent_field
-- SQL:
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table(id INT PRIMARY KEY, status TEXT);
ALTER TABLE test_table RENAME COLUMN doesnt_exist TO new_name;
-- ERROR:
-
-
column doesnt_exist does not exist

-- TEST: rename_to_conflicting_name
-- SQL:
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table(id INT PRIMARY KEY, status TEXT);
ALTER TABLE test_table RENAME COLUMN status TO id;
-- ERROR:
-
-
column id already exist

-- TEST: chain_rename
-- SQL:
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table(id INT PRIMARY KEY, status TEXT);
ALTER TABLE test_table RENAME COLUMN status TO intermediate_name, RENAME COLUMN intermediate_name TO new_name;
SELECT new_name FROM test_table;

-- TEST: chain_rename_cant_observe_old_name
-- SQL:
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table(id INT PRIMARY KEY, status TEXT);
ALTER TABLE test_table RENAME COLUMN status TO name1, RENAME COLUMN status TO name2;
-- ERROR:
-
-
column status does not exist


