-- TEST: initialization
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t(a INT PRIMARY KEY, b TEXT, c DOUBLE NOT NULL, d UNSIGNED, e INT NOT NULL);


-- TEST: alter
-- SQL:
ALTER TABLE t ALTER COLUMN d TYPE DECIMAL;
ALTER TABLE t ALTER d TYPE UNSIGNED;
ALTER TABLE t ALTER COLUMN d SET NOT NULL;
ALTER TABLE t ALTER COLUMN d DROP NOT NULL;
ALTER TABLE t ALTER d SET NOT NULL;
ALTER TABLE t ALTER d DROP NOT NULL;
-- ERROR:
sbroad: unsupported DDL: `ALTER TABLE _ ALTER COLUMN` is not yet supported
sbroad: unsupported DDL: `ALTER TABLE _ ALTER COLUMN` is not yet supported
sbroad: unsupported DDL: `ALTER TABLE _ ALTER COLUMN` is not yet supported
sbroad: unsupported DDL: `ALTER TABLE _ ALTER COLUMN` is not yet supported
sbroad: unsupported DDL: `ALTER TABLE _ ALTER COLUMN` is not yet supported
sbroad: unsupported DDL: `ALTER TABLE _ ALTER COLUMN` is not yet supported

-- TEST: rename
-- SQL:
ALTER TABLE t RENAME COLUMN c TO x;
ALTER TABLE t RENAME x TO c;


-- TEST: add (err)
-- SQL:
ALTER TABLE t ADD COLUMN IF NOT EXISTS k INT WAIT APPLIED GLOBALLY;
ALTER TABLE t ADD COLUMN IF NOT EXISTS i INT OPTION (timeout = 1000);
ALTER TABLE t ADD COLUMN IF NOT EXISTS f INT;
ALTER TABLE t ADD IF NOT EXISTS g INT;
-- ERROR:
unsupported action/entity: IF NOT EXISTS
unsupported action/entity: IF NOT EXISTS
unsupported action/entity: IF NOT EXISTS
unsupported action/entity: IF NOT EXISTS

-- TEST: add (ok)
-- SQL:
ALTER TABLE t ADD COLUMN h INT;
ALTER TABLE t ADD j INT;


-- TEST: drop
-- SQL:
ALTER TABLE t DROP COLUMN k WAIT APPLIED GLOBALLY;
ALTER TABLE t DROP COLUMN i OPTION (timeout = 1000);
ALTER TABLE t DROP COLUMN IF EXISTS f;
ALTER TABLE t DROP IF EXISTS g;
ALTER TABLE t DROP COLUMN h;
ALTER TABLE t DROP j;
-- ERROR:
sbroad: unsupported DDL: `ALTER TABLE _ DROP COLUMN` is not yet supported
sbroad: unsupported DDL: `ALTER TABLE _ DROP COLUMN` is not yet supported
sbroad: unsupported DDL: `ALTER TABLE _ DROP COLUMN` is not yet supported
sbroad: unsupported DDL: `ALTER TABLE _ DROP COLUMN` is not yet supported
sbroad: unsupported DDL: `ALTER TABLE _ DROP COLUMN` is not yet supported
sbroad: unsupported DDL: `ALTER TABLE _ DROP COLUMN` is not yet supported


-- TEST: multiple
-- SQL:
-- add only
ALTER TABLE t ADD COLUMN a1 INT, ADD COLUMN a2 TEXT;
-- drop only
ALTER TABLE t DROP COLUMN a1, DROP COLUMN a2;
-- rename only
ALTER TABLE t RENAME COLUMN b TO b_new, RENAME COLUMN d TO d_new;
ALTER TABLE t RENAME COLUMN b to b_new, RENAME COLUMN b_new to b;
-- alter only
ALTER TABLE t ALTER COLUMN c TYPE FLOAT, ALTER COLUMN e SET NOT NULL, ALTER COLUMN e DROP NOT NULL;
-- single column with renaming first then alterting nullability
ALTER TABLE t ADD COLUMN f INT, RENAME COLUMN f TO f_new, ALTER COLUMN f_new SET NOT NULL, DROP COLUMN f_new;
-- single column with altering type first then renaming
ALTER TABLE t ADD COLUMN f INT, ALTER COLUMN f TYPE DOUBLE, RENAME COLUMN f TO f_new, DROP COLUMN f_new;
-- multiple unique columns
ALTER TABLE t ADD COLUMN f INT, ALTER COLUMN b SET NOT NULL, RENAME COLUMN c to x, ALTER COLUMN d_new TYPE DECIMAL, ALTER COLUMN e DROP NOT NULL, DROP COLUMN e;
-- ERROR:
unsupported action/entity: IF NOT EXISTS
sbroad: unsupported DDL: ADD COLUMN is the only supported action in ALTER TABLE
sbroad: unsupported DDL: ADD COLUMN is the only supported action in ALTER TABLE
sbroad: unsupported DDL: ADD COLUMN is the only supported action in ALTER TABLE
sbroad: unsupported DDL: ADD COLUMN is the only supported action in ALTER TABLE
sbroad: unsupported DDL: ADD COLUMN is the only supported action in ALTER TABLE
sbroad: unsupported DDL: ADD COLUMN is the only supported action in ALTER TABLE
sbroad: unsupported DDL: ADD COLUMN is the only supported action in ALTER TABLE


-- TEST: timeout
-- SQL:
ALTER TABLE t ADD COLUMN IF NOT EXISTS i INT OPTION (timeout = 0);
ALTER TABLE t DROP COLUMN IF EXISTS i OPTION (timeout = 0);
ALTER TABLE t RENAME COLUMN i TO j OPTION (timeout = 0);
ALTER TABLE t ALTER COLUMN i SET NOT NULL OPTION (timeout = 0);
ALTER TABLE t ALTER COLUMN i TYPE DECIMAL OPTION (timeout = 0);
-- ERROR:
timeout
sbroad: unsupported DDL: `ALTER TABLE _ DROP COLUMN` is not yet supported
timeout
sbroad: unsupported DDL: `ALTER TABLE _ ALTER COLUMN` is not yet supported
sbroad: unsupported DDL: `ALTER TABLE _ ALTER COLUMN` is not yet supported


-- TEST: rename table
-- SQL:
ALTER TABLE nonexistent_table RENAME TO no_matter;
ALTER TABLE t RENAME TO t;
ALTER TABLE t RENAME TO public._pico_tier;
ALTER TABLE public.t RENAME TO _pico_tier;
ALTER TABLE public.t RENAME TO public._pico_tier;
-- ERROR:
table nonexistent_table does not exist
table t already exists
table _pico_tier already exists
table _pico_tier already exists
table _pico_tier already exists
