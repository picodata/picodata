-- TEST: initialization
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t(a INT PRIMARY KEY, b TEXT, c DOUBLE NOT NULL, d UNSIGNED, e INT NOT NULL);

-- TEST: alter_single
-- SQL:
ALTER TABLE t ALTER COLUMN d TYPE DECIMAL;
ALTER TABLE t ALTER d TYPE UNSIGNED;
ALTER TABLE t ALTER COLUMN d SET NOT NULL;
ALTER TABLE t ALTER COLUMN d DROP NOT NULL;
ALTER TABLE t ALTER d SET NOT NULL;
ALTER TABLE t ALTER d DROP NOT NULL;
-- ERROR:
sbroad: unsupported DDL: ALTER TABLE ADD is the only supported option
sbroad: unsupported DDL: ALTER TABLE ADD is the only supported option
sbroad: unsupported DDL: ALTER TABLE ADD is the only supported option
sbroad: unsupported DDL: ALTER TABLE ADD is the only supported option
sbroad: unsupported DDL: ALTER TABLE ADD is the only supported option
sbroad: unsupported DDL: ALTER TABLE ADD is the only supported option

-- TEST: alter_batch
-- SQL:
-- column with types only
ALTER TABLE t ALTER COLUMN d TYPE DECIMAL, e TYPE DECIMAL;
-- types only
ALTER TABLE t ALTER d TYPE UNSIGNED, e TYPE INT;
-- column with nulls forward
ALTER TABLE t ALTER COLUMN d SET NOT NULL, e SET NOT NULL;
-- column with nulls backward
ALTER TABLE t ALTER COLUMN d DROP NOT NULL, e DROP NOT NULL;
-- nulls forward
ALTER TABLE t ALTER d SET NOT NULL, e SET NOT NULL;
-- nulls backward
ALTER TABLE t ALTER d DROP NOT NULL, e DROP NOT NULL;
-- column with single column multiple nulls
ALTER TABLE t ALTER COLUMN d SET NOT NULL, d DROP NOT NULL;
-- single column multiple nulls
ALTER TABLE t ALTER d SET NOT NULL, d DROP NOT NULL;
-- column with single column multiple nulls single type
ALTER TABLE t ALTER COLUMN d TYPE INT, d SET NOT NULL, d DROP NOT NULL;
-- single column multiple nulls single type
ALTER TABLE t ALTER d TYPE INT, d SET NOT NULL, d DROP NOT NULL;
-- column with single column multiple nulls multiple types
ALTER TABLE t ALTER COLUMN d TYPE INT, d SET NOT NULL, d DROP NOT NULL, d TYPE UNSIGNED;
-- single column multiple nulls multiple types
ALTER TABLE t ALTER d TYPE INT, d SET NOT NULL, d DROP NOT NULL;
-- multiple columns multiple nulls multiple types
ALTER TABLE t ALTER d TYPE INT, d SET NOT NULL, d DROP NOT NULL, e TYPE INT, e SET NOT NULL, e DROP NOT NULL;
-- ERROR:
sbroad: unsupported DDL: ALTER TABLE ADD is the only supported option
sbroad: unsupported DDL: ALTER TABLE ADD is the only supported option
sbroad: unsupported DDL: ALTER TABLE ADD is the only supported option
sbroad: unsupported DDL: ALTER TABLE ADD is the only supported option
sbroad: unsupported DDL: ALTER TABLE ADD is the only supported option
sbroad: unsupported DDL: ALTER TABLE ADD is the only supported option
sbroad: unsupported DDL: ALTER TABLE ADD is the only supported option
sbroad: unsupported DDL: ALTER TABLE ADD is the only supported option
sbroad: unsupported DDL: ALTER TABLE ADD is the only supported option
sbroad: unsupported DDL: ALTER TABLE ADD is the only supported option
sbroad: unsupported DDL: ALTER TABLE ADD is the only supported option
sbroad: unsupported DDL: ALTER TABLE ADD is the only supported option
sbroad: unsupported DDL: ALTER TABLE ADD is the only supported option


-- TEST: rename_single
-- SQL:
ALTER TABLE t RENAME COLUMN c TO x;
ALTER TABLE t RENAME x TO c;
-- ERROR:
sbroad: unsupported DDL: ALTER TABLE ADD is the only supported option
sbroad: unsupported DDL: ALTER TABLE ADD is the only supported option

-- TEST: rename_batch
-- SQL:
ALTER TABLE t RENAME COLUMN a TO z, b TO y, c TO x, d TO w;
ALTER TABLE t RENAME z TO a, y TO b, x TO c, w TO d;
-- ERROR:
sbroad: unsupported DDL: ALTER TABLE ADD is the only supported option
sbroad: unsupported DDL: ALTER TABLE ADD is the only supported option


-- TEST: add_drop_single
-- SQL:
ALTER TABLE t ADD COLUMN IF NOT EXISTS f INT;
ALTER TABLE t DROP COLUMN IF EXISTS f;
ALTER TABLE t ADD IF NOT EXISTS g INT;
ALTER TABLE t DROP IF EXISTS g;
ALTER TABLE t ADD COLUMN h INT;
ALTER TABLE t DROP COLUMN h;
ALTER TABLE t ADD i INT;
ALTER TABLE t DROP i;
-- ERROR:
sbroad: unsupported DDL: ALTER TABLE is reserved for future use
sbroad: unsupported DDL: ALTER TABLE is reserved for future use
sbroad: unsupported DDL: ALTER TABLE is reserved for future use
sbroad: unsupported DDL: ALTER TABLE is reserved for future use
sbroad: unsupported DDL: ALTER TABLE is reserved for future use
sbroad: unsupported DDL: ALTER TABLE is reserved for future use
sbroad: unsupported DDL: ALTER TABLE is reserved for future use
sbroad: unsupported DDL: ALTER TABLE is reserved for future use

-- TEST: add_drop_batch
-- SQL:
ALTER TABLE t ADD COLUMN IF NOT EXISTS f INT, g INT, h INT, i INT;
ALTER TABLE t DROP COLUMN IF EXISTS f, g, h, i;
ALTER TABLE t ADD IF NOT EXISTS f INT, g INT, h INT, i INT;
ALTER TABLE t DROP IF EXISTS f, g, h, i;
ALTER TABLE t ADD COLUMN f INT, g INT, h INT, i INT;
ALTER TABLE t DROP COLUMN f, g, h, i;
ALTER TABLE t ADD f INT, g INT, h INT, i INT;
ALTER TABLE t DROP f, g, h, i;
-- ERROR:
sbroad: unsupported DDL: ALTER TABLE is reserved for future use
sbroad: unsupported DDL: ALTER TABLE is reserved for future use
sbroad: unsupported DDL: ALTER TABLE is reserved for future use
sbroad: unsupported DDL: ALTER TABLE is reserved for future use
sbroad: unsupported DDL: ALTER TABLE is reserved for future use
sbroad: unsupported DDL: ALTER TABLE is reserved for future use
sbroad: unsupported DDL: ALTER TABLE is reserved for future use
sbroad: unsupported DDL: ALTER TABLE is reserved for future use
