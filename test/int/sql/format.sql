-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- TEST: init
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t (a INT PRIMARY KEY, q TEXT);
INSERT INTO t VALUES (1, 'x'), (2, NULL);

-- TEST: format-s
-- SQL:
SELECT format('Hello, %s!', 'World');
-- EXPECTED:
'Hello, World!'

-- TEST: format-s-null-is-empty
-- SQL:
SELECT format('[%s]', NULL);
-- EXPECTED:
'[]'

-- TEST: format-s-is-the-same-as-text-cast
-- SQL:
SELECT format('%s|%s|%s|%s|%s|%s', 1, 1.5, 1.5::double, true, '2023-07-07T12:34:56Z'::datetime, 'e7a46f5e-2f70-4c9a-9b0e-4f6b1d0a3c2e'::uuid)
    = 1::text || '|' || 1.5::text || '|' || 1.5::double::text || '|' || true::text || '|' || '2023-07-07T12:34:56Z'::datetime::text || '|' || 'e7a46f5e-2f70-4c9a-9b0e-4f6b1d0a3c2e'::uuid::text;
-- EXPECTED:
True

-- TEST: format-identifier
-- SQL:
SELECT format('%I.%I.%I', 't', 'My Table', 'select');
-- EXPECTED:
't."My Table"."select"'

-- TEST: format-literal
-- SQL:
SELECT format('%L|%L|%L', $1, 42, NULL);
-- PARAMS:
'O'Reilly'
-- EXPECTED:
'O''Reilly'|'42'|NULL

-- TEST: format-percent
-- SQL:
SELECT format('100%% of %s', 'it');
-- EXPECTED:
'100% of it'

-- TEST: format-null-format-string
-- SQL:
SELECT format(NULL, 'a');
-- EXPECTED:
None

-- TEST: format-columns
-- SQL:
SELECT format('%I.a = %s and %I.q = %L', 'My Table', a, 'My Table', q) FROM t ORDER BY 1;
-- EXPECTED:
"My Table".a = 1 and "My Table".q = 'x',
"My Table".a = 2 and "My Table".q = NULL

-- TEST: format-parameters
-- SQL:
SELECT format($1, $2, $3);
-- PARAMS:
'%s and %L', 'x', 'y'
-- EXPECTED:
x and 'y'

-- TEST: format-too-few-arguments
-- SQL:
SELECT format('%s %s', 'a');
-- ERROR:
too few arguments for format\(\)

-- TEST: format-null-identifier
-- SQL:
SELECT format('%I', NULL);
-- ERROR:
null values cannot be formatted as an SQL identifier

-- TEST: format-unterminated-specifier
-- SQL:
SELECT format('50%');
-- ERROR:
unterminated format\(\) type specifier

-- TEST: format-unrecognized-specifier
-- SQL:
SELECT format('%d', 1);
-- ERROR:
unrecognized format\(\) type specifier "d"

-- TEST: format-without-arguments
-- SQL:
SELECT format();
-- ERROR:
could not resolve function overload for format\(\)

-- TEST: format-with-127-arguments
-- SQL:
SELECT format('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
-- EXPECTED:
'111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111'

-- TEST: format-with-128-arguments
-- SQL:
SELECT format('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
-- ERROR:
format takes at most 127 arguments, got 128