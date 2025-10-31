-- TEST: rename_index
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t(a INT PRIMARY KEY, b TEXT);
CREATE INDEX i1 ON t (b);
INSERT INTO t VALUES (1, 'hello'), (2, 'kitty');
ALTER INDEX i1 RENAME TO i2;

-- TEST: verify_rename_index
-- SQL:
SELECT name FROM _pico_index WHERE name = 'i2';
-- EXPECTED:
'i2'

-- TEST: rename_nonexistent_index
-- SQL:
ALTER INDEX nonexistent_index RENAME TO new_index;
-- ERROR:
index nonexistent_index does not exist

-- TEST: rename_to_conflicting_name
-- SQL:
CREATE INDEX i1 ON t (b);
ALTER INDEX i2 RENAME TO i1;
-- ERROR:
-
index i1 already exists

-- TEST: chain_rename
-- SQL:
CREATE INDEX i3 ON t(b);
ALTER INDEX i3 RENAME TO i4;
ALTER INDEX i4 RENAME TO i5;


-- TEST: verify_chain_rename
-- SQL:
SELECT name FROM _pico_index WHERE name = 'i5';
-- EXPECTED:
'i5'

-- TEST: chain_rename_cant_observe_old_name
-- SQL:
ALTER INDEX i5 RENAME TO i6;
ALTER INDEX i5 RENAME TO i7;
-- ERROR:
-
index i5 does not exist

-- TEST: rename_existing_index_with_if_exists
-- SQL:
ALTER INDEX IF EXISTS i6 RENAME TO i7;

-- TEST: verify_rename_nonexistent_index_if_exists
-- SQL:
SELECT name FROM _pico_index WHERE name = 'i7';
-- EXPECTED:
'i7'

-- TEST: rename_nonexistent_index_if_exists
-- SQL:
ALTER INDEX IF EXISTS i8 RENAME TO i9;

-- TEST: check other options
-- SQL:
ALTER INDEX i7 RENAME TO i10 WAIT APPLIED GLOBALLY OPTION (TIMEOUT = 10.0);
ALTER  INDEX  i10  RENAME  TO  i11  WAIT  APPLIED LOCALLY  OPTION ( TIMEOUT=10.0 );
