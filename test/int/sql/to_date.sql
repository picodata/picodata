-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- Expected values were recorded with the Lua implementation of to_date
-- (datetime.parse() and datetime:set()).

-- TEST: to-date-iso-date
-- SQL:
SELECT to_date('2026-01-13', '')::text, to_char(to_date('2026-01-13', ''), '%Z');
-- EXPECTED:
'2026-01-13T00:00:00Z', ''

-- TEST: to-date-iso-utc
-- SQL:
SELECT to_date('2026-01-13T15:04:05Z', '')::text, to_char(to_date('2026-01-13T15:04:05Z', ''), '%Z');
-- EXPECTED:
'2026-01-13T00:00:00Z', ''

-- TEST: to-date-iso-offset-east
-- SQL:
SELECT to_date('2026-01-13T23:30:00+05:45', '')::text, to_char(to_date('2026-01-13T23:30:00+05:45', ''), '%Z');
-- EXPECTED:
'2026-01-13T00:00:00+0545', ''

-- TEST: to-date-iso-offset-west
-- SQL:
SELECT to_date('2026-01-13T00:30:00-03:30', '')::text, to_char(to_date('2026-01-13T00:30:00-03:30', ''), '%Z');
-- EXPECTED:
'2026-01-13T00:00:00-0330', ''

-- TEST: to-date-iso-offset-max
-- SQL:
SELECT to_date('2026-01-13T23:59:59.999999999+14:00', '')::text, to_char(to_date('2026-01-13T23:59:59.999999999+14:00', ''), '%Z');
-- EXPECTED:
'2026-01-14T00:00:00+1400', ''

-- TEST: to-date-iso-named-zone
-- SQL:
SELECT to_date('2026-07-01T23:30:00 Europe/Moscow', '')::text, to_char(to_date('2026-07-01T23:30:00 Europe/Moscow', ''), '%Z');
-- EXPECTED:
'2026-07-01T00:00:00+0300', ''

-- TEST: to-date-iso-before-epoch
-- SQL:
SELECT to_date('1969-12-31T23:59:59-01:00', '')::text, to_char(to_date('1969-12-31T23:59:59-01:00', ''), '%Z');
-- EXPECTED:
'1969-12-31T00:00:00-0100', ''

-- TEST: to-date-iso-min
-- SQL:
SELECT to_date('4714-11-24 00:00:00+00 BC', '')::text, to_char(to_date('4714-11-24 00:00:00+00 BC', ''), '%Z');
-- EXPECTED:
'-4713-11-24T00:00:00Z', ''

-- TEST: to-date-iso-max
-- SQL:
SELECT to_date('9999-12-31T23:59:59Z', '')::text, to_char(to_date('9999-12-31T23:59:59Z', ''), '%Z');
-- EXPECTED:
'9999-12-31T00:00:00Z', ''

-- TEST: to-date-free-text
-- SQL:
SELECT to_date('Oct 18, 2025 2:59 PM', '')::text, to_char(to_date('Oct 18, 2025 2:59 PM', ''), '%Z');
-- EXPECTED:
'2025-10-18T00:00:00Z', ''

-- TEST: to-date-format-iso8601
-- SQL:
SELECT to_date('2026-01-13T15:04:05+03:00', 'iso8601')::text, to_char(to_date('2026-01-13T15:04:05+03:00', 'iso8601'), '%Z');
-- EXPECTED:
'2026-01-13T00:00:00+0300', ''

-- TEST: to-date-format-rfc3339
-- SQL:
SELECT to_date('2026-01-13T15:04:05+03:00', 'rfc3339')::text, to_char(to_date('2026-01-13T15:04:05+03:00', 'rfc3339'), '%Z');
-- EXPECTED:
'2026-01-13T00:00:00+0300', ''

-- TEST: to-date-strptime-dmy
-- SQL:
SELECT to_date('18.10.2025', '%d.%m.%Y')::text, to_char(to_date('18.10.2025', '%d.%m.%Y'), '%Z');
-- EXPECTED:
'2025-10-18T00:00:00Z', ''

-- TEST: to-date-strptime-offset
-- SQL:
SELECT to_date('2026-01-13 23:30:00 +0530', '%Y-%m-%d %H:%M:%S %z')::text, to_char(to_date('2026-01-13 23:30:00 +0530', '%Y-%m-%d %H:%M:%S %z'), '%Z');
-- EXPECTED:
'2026-01-13T00:00:00+0530', ''

-- TEST: to-date-strptime-trailing-text
-- SQL:
SELECT to_date('2026-01-13xyz', '%Y-%m-%d')::text, to_char(to_date('2026-01-13xyz', '%Y-%m-%d'), '%Z');
-- EXPECTED:
'2026-01-13T00:00:00Z', ''

-- TEST: to-date-strptime-day-of-year
-- SQL:
SELECT to_date('2026 045', '%Y %j')::text, to_char(to_date('2026 045', '%Y %j'), '%Z');
-- EXPECTED:
'2026-02-14T00:00:00Z', ''

-- TEST: to-date-strptime-fraction
-- SQL:
SELECT to_date('2026-01-13 15:04:05.123', '%Y-%m-%d %H:%M:%S.%f')::text, to_char(to_date('2026-01-13 15:04:05.123', '%Y-%m-%d %H:%M:%S.%f'), '%Z');
-- EXPECTED:
'2026-01-13T00:00:00Z', ''

-- TEST: to-date-strptime-epoch
-- SQL:
SELECT to_date('1768316645', '%s')::text, to_char(to_date('1768316645', '%s'), '%Z');
-- EXPECTED:
'1970-01-01T00:00:00Z', ''

-- TEST: to-date-strptime-leap-day
-- SQL:
SELECT to_date('29/02/2000', '%d/%m/%Y')::text, to_char(to_date('29/02/2000', '%d/%m/%Y'), '%Z');
-- EXPECTED:
'2000-02-29T00:00:00Z', ''

-- TEST: to-date-error-garbage
-- SQL:
SELECT to_date('garbage', '')::text, to_char(to_date('garbage', ''), '%Z');
-- ERROR:
could not parse 'garbage'

-- TEST: to-date-error-unknown-zone
-- SQL:
SELECT to_date('2020-01-01T10:00 Foo/Bar', '')::text, to_char(to_date('2020-01-01T10:00 Foo/Bar', ''), '%Z');
-- ERROR:
could not parse '2020\-01\-01T10:00 Foo/Bar'

-- TEST: to-date-error-before-min
-- SQL:
SELECT to_date('4714-11-23 23:59:59+00 BC', '')::text, to_char(to_date('4714-11-23 23:59:59+00 BC', ''), '%Z');
-- ERROR:
could not parse '4714\-11\-23 23:59:59\+00 BC'

-- TEST: to-date-error-after-max
-- SQL:
SELECT to_date('10000-01-01T00:00:00Z', '')::text, to_char(to_date('10000-01-01T00:00:00Z', ''), '%Z');
-- ERROR:
could not parse '10000\-01\-01T00:00:00Z'

-- TEST: to-date-error-strptime
-- SQL:
SELECT to_date('2020/20/20', '%Y/%d/%m')::text, to_char(to_date('2020/20/20', '%Y/%d/%m'), '%Z');
-- ERROR:
could not parse '2020/20/20' using '%Y/%d/%m' format

-- TEST: to-date-error-strptime-empty
-- SQL:
SELECT to_date('', '%Y')::text, to_char(to_date('', '%Y'), '%Z');
-- ERROR:
could not parse '' using '%Y' format

-- TEST: to-date-error-long
-- SQL:
SELECT to_date('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', '%Y');
-- ERROR:
could not parse 'a{32}\.\.\.' using '%Y' format

-- TEST: to-date-null-string
-- SQL:
SELECT to_date(NULL, '%Y');
-- EXPECTED:
NULL

-- TEST: to-date-null-format
-- SQL:
SELECT to_date('2026', NULL);
-- EXPECTED:
NULL
