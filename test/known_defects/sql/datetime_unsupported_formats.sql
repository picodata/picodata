-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- Datetime inputs that PostgreSQL handles differently.
--
-- * PostgreSQL accepts infinity, -infinity and years beyond 9999, but a tarantool
--   datetime has no value for them.
-- * PostgreSQL reads today, now, tomorrow and yesterday as the current time, which
--   is not supported.

-- TEST: datetime-misc-3
-- SQL:
SELECT 'infinity'::datetime > '9999-12-31T23:59:59Z'::datetime;
-- ERROR:
Type mismatch: can not convert string\('infinity'\) to datetime

-- TEST: datetime-misc-3-param
-- SQL:
SELECT $1::datetime > '9999-12-31T23:59:59Z'::datetime;
-- PARAMS:
'infinity'
-- ERROR:
can not convert string\('infinity'\) to datetime|'infinity' is not a valid timestamptz

-- TEST: datetime-misc-4
-- SQL:
SELECT '-infinity'::datetime < '0001-01-01T00:00:00Z'::datetime;
-- ERROR:
Type mismatch: can not convert string\('-infinity'\) to datetime

-- TEST: datetime-misc-4-param
-- SQL:
SELECT $1::datetime < '0001-01-01T00:00:00Z'::datetime;
-- PARAMS:
'-infinity'
-- ERROR:
can not convert string\('-infinity'\) to datetime|'-infinity' is not a valid timestamptz

-- TEST: datetime-misc-5
-- SQL:
SELECT 'today'::datetime IS NOT NULL;
-- ERROR:
Type mismatch: can not convert string\('today'\) to datetime

-- TEST: datetime-misc-5-param
-- SQL:
SELECT $1::datetime IS NOT NULL;
-- PARAMS:
'today'
-- ERROR:
can not convert string\('today'\) to datetime|'today' is not a valid timestamptz

-- TEST: datetime-year-1-param
-- SKIP_FOR: iproto
-- SQL:
SELECT $1::datetime > '9999-12-31T23:59:59Z'::datetime;
-- PARAMS:
'10000-01-01T00:00:00Z'
-- ERROR:
failed to bind parameter \$1: decoding error: '10000-01-01T00:00:00Z' is not a valid timestamptz

-- TEST: datetime-year-1-result
-- SKIP_FOR: iproto
-- SQL:
SELECT '10000-01-01'::datetime;
-- ERROR:
Type mismatch: can not convert string\('10000-01-01'\) to datetime
