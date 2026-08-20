-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- Datetime text is parsed the way PostgreSQL does it, so fractional seconds are
-- rounded to microseconds, although a tarantool datetime keeps nanoseconds and
-- earlier versions of Picodata parsed them.

-- TEST: datetime-nsec-1
-- SQL:
SELECT '2026-04-29T00:00:00.123456789Z'::datetime = '2026-04-29T00:00:00.123457Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-nsec-1-text-param
-- SQL:
SELECT $1::text::datetime = '2026-04-29T00:00:00.123457Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00.123456789Z'
-- EXPECTED:
true

-- TEST: datetime-nsec-1-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00.123457Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00.123456789Z'
-- EXPECTED:
true

-- TEST: datetime-nsec-2
-- SQL:
SELECT '2026-04-29T00:00:00.000000001Z'::datetime > '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
false

-- TEST: datetime-nsec-2-text-param
-- SQL:
SELECT $1::text::datetime > '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00.000000001Z'
-- EXPECTED:
false

-- TEST: datetime-nsec-2-param
-- SQL:
SELECT $1::datetime > '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00.000000001Z'
-- EXPECTED:
false

-- TEST: datetime-nsec-3
-- SQL:
SELECT '2026-04-29T23:59:59.9999999Z'::datetime = '2026-04-30T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-nsec-3-text-param
-- SQL:
SELECT $1::text::datetime = '2026-04-30T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T23:59:59.9999999Z'
-- EXPECTED:
true

-- TEST: datetime-nsec-3-param
-- SQL:
SELECT $1::datetime = '2026-04-30T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T23:59:59.9999999Z'
-- EXPECTED:
true

-- TEST: datetime-nsec-4
-- SQL:
SELECT '2026-04-29T00:00:00.123456789Z'::datetime::text;
-- EXPECTED:
'2026-04-29T00:00:00.123457Z'

-- TEST: datetime-nsec-4-text-param
-- SQL:
SELECT $1::text::datetime::text;
-- PARAMS:
'2026-04-29T00:00:00.123456789Z'
-- EXPECTED:
'2026-04-29T00:00:00.123457Z'

-- TEST: datetime-nsec-4-param
-- SQL:
SELECT $1::datetime::text;
-- PARAMS:
'2026-04-29T00:00:00.123456789Z'
-- EXPECTED:
'2026-04-29T00:00:00.123457Z'
