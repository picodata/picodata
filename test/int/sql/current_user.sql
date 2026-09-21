-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- TEST: init
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t (id INT PRIMARY KEY, owner STRING);
DROP TABLE IF EXISTS s;
CREATE TABLE s (owner STRING PRIMARY KEY, v INT) DISTRIBUTED BY (owner);
DROP TABLE IF EXISTS b;
CREATE TABLE b (owner STRING PRIMARY KEY, v INT) DISTRIBUTED BY (owner);

-- TEST: current-user-iproto
-- SKIP_FOR: pgproto
-- SQL:
SELECT current_user;
-- EXPECTED:
'pico_service'

-- TEST: current-user-pgproto
-- SKIP_FOR: iproto
-- SQL:
SELECT CURRENT_USER;
-- EXPECTED:
'postgres'

-- TEST: current-user-is-stable-within-query
-- SQL:
SELECT current_user = current_user, current_user IS NOT NULL;
-- EXPECTED:
true, true

-- TEST: current-user-type
-- SQL:
SELECT current_user::text = current_user, upper(current_user) = upper(current_user || '');
-- EXPECTED:
true, true

-- TEST: current-user-with-parentheses
-- SQL:
SELECT current_user();
-- ERROR:
rule parsing error

-- TEST: current-user-in-values
-- SQL:
SELECT count(*) FROM (VALUES (current_user), (current_user)) WHERE "COLUMN_1" = current_user;
-- EXPECTED:
2

-- TEST: current-user-in-insert
-- SQL:
INSERT INTO t VALUES (1, current_user), (2, 'someone_else'), (3, current_user);

-- TEST: current-user-in-where
-- SQL:
SELECT id FROM t WHERE owner = current_user ORDER BY id;
-- EXPECTED:
1, 3

-- TEST: current-user-in-group-by
-- SQL:
SELECT owner = current_user, count(*) FROM t GROUP BY owner = current_user ORDER BY 1;
-- EXPECTED:
false, 1,
true, 2

-- TEST: current-user-group-by-itself
-- SQL:
SELECT current_user = current_user, count(*) FROM t GROUP BY current_user;
-- EXPECTED:
true, 3

-- TEST: current-user-in-update
-- SQL:
UPDATE t SET owner = current_user WHERE owner <> current_user;

-- TEST: current-user-in-update-check
-- SQL:
SELECT count(*) FROM t WHERE owner = current_user;
-- EXPECTED:
3

-- TEST: current-user-in-delete
-- SQL:
DELETE FROM t WHERE owner = current_user AND id > 1;

-- TEST: current-user-in-delete-check
-- SQL:
SELECT id FROM t;
-- EXPECTED:
1

-- TEST: current-user-as-sharding-key
-- SQL:
INSERT INTO s VALUES (current_user, 42), ('someone_else', 0);

-- TEST: current-user-as-sharding-key-check
-- SQL:
SELECT v FROM s WHERE owner = current_user;
-- EXPECTED:
42

-- TEST: current-user-in-anonymous-block
-- SQL:
DO $$ BEGIN RETURN QUERY SELECT owner = current_user FROM t WHERE id = 1; END $$;
-- EXPECTED:
true

-- TEST: current-user-as-sharding-key-in-anonymous-block
-- SQL:
DO $$ BEGIN INSERT INTO b VALUES (current_user, 1); END $$;

-- TEST: current-user-as-sharding-key-in-anonymous-block-check
-- SQL:
SELECT v FROM b WHERE owner = current_user;
-- EXPECTED:
1

-- TEST: current-user-compared-with-int
-- SQL:
SELECT id FROM t WHERE id = current_user;
-- ERROR:
could not resolve operator overload for =\(int, text\)

-- TEST: current-user-in-arithmetic
-- SQL:
SELECT 1 + current_user;
-- ERROR:
could not resolve operator overload for \+\(int, text\)

-- TEST: current-user-in-case-with-int
-- SQL:
SELECT CASE WHEN false THEN 1 ELSE current_user END;
-- ERROR:
CASE/THEN types int and text cannot be matched

-- TEST: current-user-insert-into-int-column
-- SQL:
INSERT INTO t(id) VALUES (current_user);
-- ERROR:
INSERT column at position 1 is of type int, but expression is of type text

-- TEST: current-user-sharding-key-filter-with-limit-in-anonymous-block
-- SQL:
DO $$ BEGIN RETURN QUERY SELECT v FROM s WHERE owner = current_user LIMIT 1; END $$;
-- EXPECTED:
42

-- TEST: current-user-sharding-key-filter-with-aggregate-in-anonymous-block
-- SQL:
DO $$ BEGIN RETURN QUERY SELECT count(*) FROM s WHERE owner = current_user; END $$;
-- EXPECTED:
1
