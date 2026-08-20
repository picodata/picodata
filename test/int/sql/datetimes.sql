-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- TEST: datetime-1.1
-- SQL:
select '2026-01-13' = '2026-01-13'::datetime;
-- EXPECTED:
true

-- TEST: datetime-1.2
-- SQL:
select '2026-01-13'::datetime = '2026-01-13';
-- EXPECTED:
true

-- TEST: datetime-1.3
-- SQL:
select '2026-01-13' = '2026-01-13T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-1.4
-- SQL:
select '2026-01-13'::datetime = '2026-01-13T00:00:00Z';
-- EXPECTED:
true

-- TEST: datetime-1.5
-- SQL:
select '2026-01-13'::datetime = '2026-01-13T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-2.1
-- SQL:
select '2026-01-13'::datetime < '2026-02-01'::datetime;
-- EXPECTED:
true

-- TEST: datetime-2.2
-- SQL:
select '2026-01-13' < '2026-02-01'::datetime;
-- EXPECTED:
true

-- TEST: datetime-3.1
-- SQL:
select '2026-01-13' between '2026-01-01' and '2026-01-20'::datetime;
-- EXPECTED:
true

-- TEST: datetime-3.2
-- SQL:
select '2026-01-13' between '2026-01-01'::datetime and '2026-01-20';
-- EXPECTED:
true

-- TEST: datetime-3.3
-- SQL:
select '2026-01-13' between '2026-01-01'::datetime and '2026-01-20'::datetime;
-- EXPECTED:
true

-- TEST: datetime-3.4
-- SQL:
select '2026-01-13'::datetime between '2026-01-01'::datetime and '2026-01-20'::datetime;
-- EXPECTED:
true

-- TEST: datetime-date-1
-- SQL:
SELECT '2026-04-29'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-date-1-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29'
-- EXPECTED:
true

-- TEST: datetime-date-2
-- SQL:
SELECT '20260429'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-date-2-text-param
-- SQL:
SELECT $1::text::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'20260429'
-- EXPECTED:
true

-- TEST: datetime-date-2-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'20260429'
-- EXPECTED:
true

-- TEST: datetime-date-3
-- SQL:
SELECT '2026-119'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-date-3-text-param
-- SQL:
SELECT $1::text::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-119'
-- EXPECTED:
true

-- TEST: datetime-date-3-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-119'
-- EXPECTED:
true

-- TEST: datetime-naive-1
-- SQL:
SELECT '2026-04-29T00:00:00'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-naive-1-text-param
-- SQL:
SELECT $1::text::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00'
-- EXPECTED:
true

-- TEST: datetime-naive-1-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00'
-- EXPECTED:
true

-- TEST: datetime-naive-2
-- SQL:
SELECT '2026-04-29 00:00:00'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-naive-2-text-param
-- SQL:
SELECT $1::text::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29 00:00:00'
-- EXPECTED:
true

-- TEST: datetime-naive-2-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29 00:00:00'
-- EXPECTED:
true

-- TEST: datetime-naive-3
-- SQL:
SELECT '2026-04-29t00:00:00'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-naive-3-text-param
-- SQL:
SELECT $1::text::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29t00:00:00'
-- EXPECTED:
true

-- TEST: datetime-naive-3-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29t00:00:00'
-- EXPECTED:
true

-- TEST: datetime-naive-4
-- SQL:
SELECT '2026-04-29T00:00'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-naive-4-text-param
-- SQL:
SELECT $1::text::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00'
-- EXPECTED:
true

-- TEST: datetime-naive-4-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00'
-- EXPECTED:
true

-- TEST: datetime-naive-5
-- SQL:
SELECT '20260429T000000'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-naive-5-text-param
-- SQL:
SELECT $1::text::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'20260429T000000'
-- EXPECTED:
true

-- TEST: datetime-naive-5-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'20260429T000000'
-- EXPECTED:
true

-- TEST: datetime-zulu-1
-- SQL:
SELECT '2026-04-29T00:00:00Z'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-zulu-1-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00Z'
-- EXPECTED:
true

-- TEST: datetime-zulu-2
-- SQL:
SELECT '2026-04-29 00:00:00Z'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-zulu-2-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29 00:00:00Z'
-- EXPECTED:
true

-- TEST: datetime-zulu-3
-- SQL:
SELECT '2026-04-29t00:00:00z'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-zulu-3-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29t00:00:00z'
-- EXPECTED:
true

-- TEST: datetime-zulu-4
-- SQL:
SELECT '2026-04-29T00:00Z'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-zulu-4-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00Z'
-- EXPECTED:
true

-- TEST: datetime-offset-1
-- SQL:
SELECT '2026-04-29T00:00:00+03:00'::datetime = '2026-04-28T21:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-offset-1-param
-- SQL:
SELECT $1::datetime = '2026-04-28T21:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00+03:00'
-- EXPECTED:
true

-- TEST: datetime-offset-2
-- SQL:
SELECT '2026-04-29T00:00:00+03'::datetime = '2026-04-28T21:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-offset-2-param
-- SQL:
SELECT $1::datetime = '2026-04-28T21:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00+03'
-- EXPECTED:
true

-- TEST: datetime-offset-3
-- SQL:
SELECT '2026-04-29T00:00:00+0300'::datetime = '2026-04-28T21:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-offset-3-param
-- SQL:
SELECT $1::datetime = '2026-04-28T21:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00+0300'
-- EXPECTED:
true

-- TEST: datetime-offset-4
-- SQL:
SELECT '2026-04-29T00:00:00+3'::datetime = '2026-04-28T21:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-offset-4-text-param
-- SQL:
SELECT $1::text::datetime = '2026-04-28T21:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00+3'
-- EXPECTED:
true

-- TEST: datetime-offset-4-param
-- SQL:
SELECT $1::datetime = '2026-04-28T21:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00+3'
-- EXPECTED:
true

-- TEST: datetime-offset-5
-- SQL:
SELECT '2026-04-29T00:00:00+3:00'::datetime = '2026-04-28T21:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-offset-5-text-param
-- SQL:
SELECT $1::text::datetime = '2026-04-28T21:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00+3:00'
-- EXPECTED:
true

-- TEST: datetime-offset-5-param
-- SQL:
SELECT $1::datetime = '2026-04-28T21:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00+3:00'
-- EXPECTED:
true

-- TEST: datetime-offset-6
-- SQL:
SELECT '2026-04-29 00:00:00+03:00'::datetime = '2026-04-28T21:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-offset-6-param
-- SQL:
SELECT $1::datetime = '2026-04-28T21:00:00Z'::datetime;
-- PARAMS:
'2026-04-29 00:00:00+03:00'
-- EXPECTED:
true

-- TEST: datetime-offset-7
-- SQL:
SELECT '2026-04-29 00:00:00+03'::datetime = '2026-04-28T21:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-offset-7-param
-- SQL:
SELECT $1::datetime = '2026-04-28T21:00:00Z'::datetime;
-- PARAMS:
'2026-04-29 00:00:00+03'
-- EXPECTED:
true

-- TEST: datetime-offset-8
-- SQL:
SELECT '2026-04-29 00:00:00 +03:00'::datetime = '2026-04-28T21:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-offset-8-text-param
-- SQL:
SELECT $1::text::datetime = '2026-04-28T21:00:00Z'::datetime;
-- PARAMS:
'2026-04-29 00:00:00 +03:00'
-- EXPECTED:
true

-- TEST: datetime-offset-8-param
-- SQL:
SELECT $1::datetime = '2026-04-28T21:00:00Z'::datetime;
-- PARAMS:
'2026-04-29 00:00:00 +03:00'
-- EXPECTED:
true

-- TEST: datetime-offset-9
-- SQL:
SELECT '2026-04-29 00:00:00 +03:00:00'::datetime = '2026-04-28T21:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-offset-9-text-param
-- SQL:
SELECT $1::text::datetime = '2026-04-28T21:00:00Z'::datetime;
-- PARAMS:
'2026-04-29 00:00:00 +03:00:00'
-- EXPECTED:
true

-- TEST: datetime-offset-9-param
-- SQL:
SELECT $1::datetime = '2026-04-28T21:00:00Z'::datetime;
-- PARAMS:
'2026-04-29 00:00:00 +03:00:00'
-- EXPECTED:
true

-- TEST: datetime-offset-10
-- SQL:
SELECT '2026-04-29 00:00:00+00'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-offset-10-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29 00:00:00+00'
-- EXPECTED:
true

-- TEST: datetime-tzname-1
-- SQL:
SELECT '2026-04-29T00:00:00 UTC'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-tzname-1-text-param
-- SQL:
SELECT $1::text::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00 UTC'
-- EXPECTED:
true

-- TEST: datetime-tzname-1-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00 UTC'
-- EXPECTED:
true

-- TEST: datetime-tzname-2
-- SQL:
SELECT '2026-04-29T00:00:00 GMT'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-tzname-2-text-param
-- SQL:
SELECT $1::text::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00 GMT'
-- EXPECTED:
true

-- TEST: datetime-tzname-2-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00 GMT'
-- EXPECTED:
true

-- TEST: datetime-tzname-3
-- SQL:
SELECT '2026-04-29T00:00:00 MSK'::datetime = '2026-04-28T21:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-tzname-3-text-param
-- SQL:
SELECT $1::text::datetime = '2026-04-28T21:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00 MSK'
-- EXPECTED:
true

-- TEST: datetime-tzname-3-param
-- SQL:
SELECT $1::datetime = '2026-04-28T21:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00 MSK'
-- EXPECTED:
true

-- TEST: datetime-tzname-4
-- SQL:
SELECT '2026-04-29T00:00:00MSK'::datetime = '2026-04-28T21:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-tzname-4-text-param
-- SQL:
SELECT $1::text::datetime = '2026-04-28T21:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00MSK'
-- EXPECTED:
true

-- TEST: datetime-tzname-4-param
-- SQL:
SELECT $1::datetime = '2026-04-28T21:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00MSK'
-- EXPECTED:
true

-- TEST: datetime-tzname-5
-- SQL:
SELECT '2026-04-29T00:00:00 Europe/Moscow'::datetime = '2026-04-28T21:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-tzname-5-text-param
-- SQL:
SELECT $1::text::datetime = '2026-04-28T21:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00 Europe/Moscow'
-- EXPECTED:
true

-- TEST: datetime-tzname-5-param
-- SQL:
SELECT $1::datetime = '2026-04-28T21:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00 Europe/Moscow'
-- EXPECTED:
true

-- TEST: datetime-frac-1
-- SQL:
SELECT '2026-04-29T00:00:00.123'::datetime = '2026-04-29T00:00:00.123Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-frac-1-text-param
-- SQL:
SELECT $1::text::datetime = '2026-04-29T00:00:00.123Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00.123'
-- EXPECTED:
true

-- TEST: datetime-frac-1-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00.123Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00.123'
-- EXPECTED:
true

-- TEST: datetime-frac-2
-- SQL:
SELECT '2026-04-29T00:00:00.123456'::datetime = '2026-04-29T00:00:00.123456Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-frac-2-text-param
-- SQL:
SELECT $1::text::datetime = '2026-04-29T00:00:00.123456Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00.123456'
-- EXPECTED:
true

-- TEST: datetime-frac-2-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00.123456Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00.123456'
-- EXPECTED:
true

-- TEST: datetime-frac-3
-- SQL:
SELECT '2026-04-29T00:00:00.123456Z'::datetime = '2026-04-29T00:00:00.123456Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-frac-3-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00.123456Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00.123456Z'
-- EXPECTED:
true

-- TEST: datetime-frac-4
-- SQL:
SELECT '2026-04-29 00:00:00.123+03:00'::datetime = '2026-04-28T21:00:00.123Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-frac-4-param
-- SQL:
SELECT $1::datetime = '2026-04-28T21:00:00.123Z'::datetime;
-- PARAMS:
'2026-04-29 00:00:00.123+03:00'
-- EXPECTED:
true

-- TEST: datetime-frac-5
-- SQL:
SELECT '2026-04-29T00:00:00.123456789'::datetime = '2026-04-29T00:00:00.123456789Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-frac-5-text-param
-- SQL:
SELECT $1::text::datetime = '2026-04-29T00:00:00.123456789Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00.123456789'
-- EXPECTED:
true

-- TEST: datetime-frac-5-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00.123456789Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00.123456789'
-- EXPECTED:
true

-- TEST: datetime-textual-4
-- SQL:
SELECT 'Wed, 29 Apr 2026 00:00:00 +0000'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-misc-1
-- SQL:
SELECT '2026-04-28 24:00:00'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-misc-1-text-param
-- SQL:
SELECT $1::text::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-28 24:00:00'
-- EXPECTED:
true

-- TEST: datetime-misc-1-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-28 24:00:00'
-- EXPECTED:
true

-- TEST: datetime-offset-11
-- SQL:
SELECT '2026-04-29 0:00:00.0 +00:00:00'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-offset-11-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29 0:00:00.0 +00:00:00'
-- EXPECTED:
true

-- TEST: datetime-posix-tz-1
-- SQL:
SELECT '2026-04-29T00:00:00 GMT+3'::datetime = '2026-04-29T03:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-posix-tz-1-param
-- SQL:
SELECT $1::datetime = '2026-04-29T03:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00 GMT+3'
-- EXPECTED:
true

-- TEST: datetime-posix-tz-2
-- SQL:
SELECT '2026-04-29T00:00:00 UTC+03:00'::datetime = '2026-04-29T03:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-posix-tz-2-param
-- SQL:
SELECT $1::datetime = '2026-04-29T03:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00 UTC+03:00'
-- EXPECTED:
true

-- TEST: datetime-textual-1
-- SQL:
SELECT '04/29/2026'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-textual-1-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'04/29/2026'
-- EXPECTED:
true

-- TEST: datetime-textual-2
-- SQL:
SELECT '29 Apr 2026'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-textual-2-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'29 Apr 2026'
-- EXPECTED:
true

-- TEST: datetime-textual-3
-- SQL:
SELECT 'April 29, 2026'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-misc-2
-- SQL:
SELECT 'epoch'::datetime = '1970-01-01T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-misc-2-param
-- SQL:
SELECT $1::datetime = '1970-01-01T00:00:00Z'::datetime;
-- PARAMS:
'epoch'
-- EXPECTED:
true

-- TEST: datetime-tzabbr-1
-- SQL:
SELECT '2026-04-29T00:00:00 EST'::datetime = '2026-04-29T05:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-tzabbr-1-param
-- SQL:
SELECT $1::datetime = '2026-04-29T05:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00 EST'
-- EXPECTED:
true

-- TEST: datetime-tzabbr-2
-- SQL:
SELECT '2026-04-29T00:00:00 EDT'::datetime = '2026-04-29T04:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-tzabbr-2-param
-- SQL:
SELECT $1::datetime = '2026-04-29T04:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00 EDT'
-- EXPECTED:
true

-- TEST: datetime-tzabbr-3
-- SQL:
SELECT '2026-04-29T00:00:00 PST'::datetime = '2026-04-29T08:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-tzabbr-3-param
-- SQL:
SELECT $1::datetime = '2026-04-29T08:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00 PST'
-- EXPECTED:
true

-- TEST: datetime-date-tzname-1
-- SQL:
SELECT '2026-04-29 Europe/Moscow'::datetime = '2026-04-28T21:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-date-tzname-1-param
-- SQL:
SELECT $1::datetime = '2026-04-28T21:00:00Z'::datetime;
-- PARAMS:
'2026-04-29 Europe/Moscow'
-- EXPECTED:
true

-- TEST: datetime-whitespace-1
-- SQL:
SELECT ' 2026-04-29'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-whitespace-1-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
' 2026-04-29'
-- EXPECTED:
true

-- TEST: datetime-whitespace-2
-- SQL:
SELECT '2026-04-29T00:00:00Z '::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-whitespace-2-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00Z '
-- EXPECTED:
true

-- TEST: datetime-leap-second-1
-- SQL:
SELECT '2026-04-29T23:59:60'::datetime = '2026-04-30T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-leap-second-1-param
-- SQL:
SELECT $1::datetime = '2026-04-30T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T23:59:60'
-- EXPECTED:
true

-- TEST: datetime-offset-seconds-1
-- SQL:
SELECT '2026-04-29T00:00:00+03:30:15'::datetime = '2026-04-28T20:29:45Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-offset-seconds-1-param
-- SQL:
SELECT $1::datetime = '2026-04-28T20:29:45Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00+03:30:15'
-- EXPECTED:
true

-- TEST: datetime-textual-5
-- SQL:
SELECT '2026/04/29'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-textual-5-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026/04/29'
-- EXPECTED:
true

-- TEST: datetime-textual-6
-- SQL:
SELECT '2026.04.29'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-textual-6-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026.04.29'
-- EXPECTED:
true

-- TEST: datetime-textual-7
-- SQL:
SELECT 'Apr 29 2026'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-textual-7-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'Apr 29 2026'
-- EXPECTED:
true

-- TEST: datetime-textual-8
-- SQL:
SELECT '2026-Apr-29'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-textual-8-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-Apr-29'
-- EXPECTED:
true

-- TEST: datetime-ampm-1
-- SQL:
SELECT '2026-04-29 01:00:00 PM'::datetime = '2026-04-29T13:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-ampm-1-param
-- SQL:
SELECT $1::datetime = '2026-04-29T13:00:00Z'::datetime;
-- PARAMS:
'2026-04-29 01:00:00 PM'
-- EXPECTED:
true

-- TEST: datetime-ampm-2
-- SQL:
SELECT '2026-04-29 12:00:00 AM'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-ampm-2-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29 12:00:00 AM'
-- EXPECTED:
true

-- TEST: datetime-julian-1
-- SQL:
SELECT 'J2461160'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-julian-1-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'J2461160'
-- EXPECTED:
true

-- TEST: datetime-date-4
-- SQL:
SELECT '2026119'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- ERROR:
Type mismatch: can not convert string\('2026119'\) to datetime

-- TEST: datetime-date-4-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026119'
-- ERROR:
can not convert string\('2026119'\) to datetime|'2026119' is not a valid timestamptz

-- TEST: datetime-date-5
-- SQL:
SELECT '2026-W18-3'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- ERROR:
Type mismatch: can not convert string\('2026-W18-3'\) to datetime

-- TEST: datetime-date-5-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-W18-3'
-- ERROR:
can not convert string\('2026-W18-3'\) to datetime|'2026-W18-3' is not a valid timestamptz

-- TEST: datetime-date-6
-- SQL:
SELECT '2026W183'::datetime = '2026-04-29T00:00:00Z'::datetime;
-- ERROR:
Type mismatch: can not convert string\('2026W183'\) to datetime

-- TEST: datetime-date-6-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026W183'
-- ERROR:
can not convert string\('2026W183'\) to datetime|'2026W183' is not a valid timestamptz

-- TEST: datetime-date-7-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29 '
-- EXPECTED:
true

-- TEST: datetime-naive-6
-- SQL:
SELECT '2026-04-29T12'::datetime = '2026-04-29T12:00:00Z'::datetime;
-- ERROR:
Type mismatch: can not convert string\('2026-04-29T12'\) to datetime

-- TEST: datetime-naive-6-param
-- SQL:
SELECT $1::datetime = '2026-04-29T12:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T12'
-- ERROR:
can not convert string\('2026-04-29T12'\) to datetime|'2026-04-29T12' is not a valid timestamptz

-- TEST: datetime-naive-7-param
-- SQL:
SELECT $1::datetime = '2026-04-29T12:34:00Z'::datetime;
-- PARAMS:
'2026-04-29T1234'
-- EXPECTED:
true

-- TEST: datetime-naive-8-param
-- SQL:
SELECT $1::datetime = '2026-04-29T12:34:56Z'::datetime;
-- PARAMS:
'2026-04-29T123456'
-- EXPECTED:
true

-- TEST: datetime-naive-9-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29TT00:00:00'
-- EXPECTED:
true

-- TEST: datetime-zulu-5-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29 00:00:00 Z'
-- EXPECTED:
true

-- TEST: datetime-zulu-6-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29 00:00:00 z'
-- EXPECTED:
true

-- TEST: datetime-offset-12-param
-- SQL:
SELECT $1::datetime = '2026-04-29T03:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00-3'
-- EXPECTED:
true

-- TEST: datetime-offset-13-param
-- SQL:
SELECT $1::datetime = '2026-04-29T03:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00-3:00'
-- EXPECTED:
true

-- TEST: datetime-offset-14-param
-- SQL:
SELECT $1::datetime = '2026-04-29T03:00:00Z'::datetime;
-- PARAMS:
'2026-04-29 00:00:00 -03'
-- EXPECTED:
true

-- TEST: datetime-offset-15-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00 +00:00'
-- EXPECTED:
true

-- TEST: datetime-offset-16-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00+00:00:00'
-- EXPECTED:
true

-- TEST: datetime-tzname-6-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00 utc'
-- EXPECTED:
true

-- TEST: datetime-tzname-7-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00 Etc/UTC'
-- EXPECTED:
true

-- TEST: datetime-tzname-8-param
-- SQL:
SELECT $1::datetime = '2026-04-28T23:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00 CET'
-- EXPECTED:
true

-- TEST: datetime-tzname-9-param
-- SQL:
SELECT $1::datetime = '2026-04-28T22:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00 CEST'
-- EXPECTED:
true

-- TEST: datetime-tzname-10-param
-- SQL:
SELECT $1::datetime = '2026-04-28T15:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00 JST'
-- EXPECTED:
true

-- TEST: datetime-tzname-11-param
-- SQL:
SELECT $1::datetime = '2026-04-29T04:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00 America/New_York'
-- EXPECTED:
true

-- TEST: datetime-tzname-12-param
-- SQL:
SELECT $1::datetime = '2026-04-28T18:30:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00 Asia/Kolkata'
-- EXPECTED:
true

-- TEST: datetime-tzname-13-param
-- SQL:
SELECT $1::datetime = '2026-04-28T10:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00 Pacific/Kiritimati'
-- EXPECTED:
true

-- TEST: datetime-tzname-14-param
-- SQL:
SELECT $1::datetime = '2026-04-29T10:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00 Pacific/Honolulu'
-- EXPECTED:
true

-- TEST: datetime-frac-6-param
-- SQL:
SELECT $1::datetime = '2026-04-29T00:00:00.1Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00.1'
-- EXPECTED:
true

-- TEST: datetime-frac-7-param
-- SQL:
SELECT $1::datetime = '2026-04-29T23:59:59.999999Z'::datetime;
-- PARAMS:
'2026-04-29T23:59:59.999999'
-- EXPECTED:
true

-- TEST: datetime-untyped-param
-- SKIP_FOR: iproto
-- SQL:
SELECT $1 = '2026-04-29T00:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00'
-- EXPECTED:
true

-- TEST: datetime-offset-range-1
-- SQL:
SELECT '2026-04-29T00:00:00+14:01'::datetime = '2026-04-28T09:59:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-offset-range-2
-- SQL:
SELECT '2026-04-29T00:00:00-12:01'::datetime = '2026-04-29T12:01:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-offset-range-3
-- SQL:
SELECT '2026-04-29T00:00:00+15:00'::datetime = '2026-04-28T09:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-offset-range-4
-- SQL:
SELECT '2026-04-29T00:00:00-13:00'::datetime = '2026-04-29T13:00:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-offset-range-5
-- SQL:
SELECT '2026-04-29T00:00:00+15:59'::datetime = '2026-04-28T08:01:00Z'::datetime;
-- EXPECTED:
true

-- TEST: datetime-offset-range-6
-- SQL:
SELECT '2026-04-29T00:00:00+23:59'::datetime IS NULL;
-- ERROR:
Type mismatch: can not convert string\('2026-04-29T00:00:00\+23:59'\) to datetime

-- TEST: datetime-offset-range-3-param
-- SQL:
SELECT $1::datetime = '2026-04-28T09:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00+15:00'
-- EXPECTED:
true

-- TEST: datetime-offset-range-3-text-param
-- SQL:
SELECT $1::text::datetime = '2026-04-28T09:00:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00+15:00'
-- EXPECTED:
true

-- TEST: datetime-offset-range-7-param
-- SQL:
SELECT $1::datetime = '2026-04-29T12:30:00Z'::datetime;
-- PARAMS:
'2026-04-29T00:00:00-12:30'
-- EXPECTED:
true

-- TEST: datetime-offset-kept-1
-- SQL:
SELECT to_char('2026-04-29T00:00:00+03:00'::datetime, '%Y-%m-%dT%H:%M:%S%z');
-- EXPECTED:
'2026-04-29T00:00:00+0300'

-- TEST: datetime-offset-kept-1-param
-- SQL:
SELECT to_char($1::datetime, '%Y-%m-%dT%H:%M:%S%z');
-- PARAMS:
'2026-04-29T00:00:00+03:00'
-- EXPECTED:
'2026-04-29T00:00:00+0300'

-- TEST: datetime-offset-kept-2
-- SQL:
SELECT to_char('2026-04-29T00:00:00 Europe/Moscow'::datetime, '%Y-%m-%dT%H:%M:%S%z');
-- EXPECTED:
'2026-04-29T00:00:00+0300'

-- TEST: datetime-offset-kept-3
-- SQL:
SELECT to_char('2026-04-29T00:00:00'::datetime, '%Y-%m-%dT%H:%M:%S%z');
-- EXPECTED:
'2026-04-29T00:00:00+0000'

-- TEST: datetime-offset-range-utc-1
-- SQL:
SELECT to_char('2026-04-29T00:00:00+15:00'::datetime, '%Y-%m-%dT%H:%M:%S%z');
-- EXPECTED:
'2026-04-28T09:00:00+0000'

-- TEST: datetime-offset-range-utc-1-param
-- SQL:
SELECT to_char($1::datetime, '%Y-%m-%dT%H:%M:%S%z');
-- PARAMS:
'2026-04-29T00:00:00+15:00'
-- EXPECTED:
'2026-04-28T09:00:00+0000'

-- TEST: datetime-offset-range-utc-2
-- SQL:
SELECT to_char('2026-04-29T00:00:00+03:30:15'::datetime, '%Y-%m-%dT%H:%M:%S%z');
-- EXPECTED:
'2026-04-28T20:29:45+0000'

-- TEST: datetime-offset-range-table-create
-- SQL:
CREATE TABLE datetime_offsets (id INT PRIMARY KEY, v DATETIME);

-- TEST: datetime-offset-range-table-literal
-- SQL:
INSERT INTO datetime_offsets VALUES (1, '2026-04-29T00:00:00+15:00'::datetime);

-- TEST: datetime-offset-range-table-param
-- SQL:
INSERT INTO datetime_offsets VALUES ($1, $2::datetime);
-- PARAMS:
2, '2026-04-29T00:00:00-12:30'

-- TEST: datetime-offset-range-table-text-param
-- SQL:
INSERT INTO datetime_offsets VALUES ($1, $2::text::datetime);
-- PARAMS:
3, '2026-04-29T00:00:00+15:00'

-- TEST: datetime-offset-range-table-insert-select
-- SQL:
INSERT INTO datetime_offsets SELECT $1, $2::datetime;
-- PARAMS:
4, '2026-04-29T00:00:00+15:00'

-- TEST: datetime-offset-range-table-read
-- SQL:
SELECT id, to_char(v, '%Y-%m-%dT%H:%M:%S%z') FROM datetime_offsets ORDER BY id;
-- EXPECTED:
1, '2026-04-28T09:00:00+0000',
2, '2026-04-29T12:30:00+0000',
3, '2026-04-28T09:00:00+0000',
4, '2026-04-28T09:00:00+0000'

-- TEST: datetime-offset-range-table-untyped-param
-- SKIP_FOR: iproto
-- SQL:
INSERT INTO datetime_offsets VALUES ($1, $2);
-- PARAMS:
5, '2026-04-29T00:00:00+15:00'

-- TEST: datetime-offset-range-table-untyped-param.2
-- SKIP_FOR: iproto
-- SQL:
SELECT to_char(v, '%Y-%m-%dT%H:%M:%S%z') FROM datetime_offsets WHERE id = 5;
-- EXPECTED:
'2026-04-28T09:00:00+0000'

-- TEST: datetime-offset-range-table-drop
-- SQL:
DROP TABLE datetime_offsets;

-- TEST: datetime-table-param-create
-- SKIP_FOR: iproto
-- SQL:
CREATE TABLE datetime_params (id INT PRIMARY KEY, v DATETIME);
INSERT INTO datetime_params VALUES (1, '2026-04-29T00:00:00Z'::datetime);

-- TEST: datetime-table-param-insert
-- SKIP_FOR: iproto
-- SQL:
INSERT INTO datetime_params VALUES ($1, $2);
-- PARAMS:
2, '2026-04-30 00:00:00'

-- TEST: datetime-table-param-where
-- SKIP_FOR: iproto
-- SQL:
SELECT id FROM datetime_params WHERE v = $1;
-- PARAMS:
'2026-04-29T00:00:00'
-- EXPECTED:
1

-- TEST: datetime-table-param-between
-- SKIP_FOR: iproto
-- SQL:
SELECT id FROM datetime_params WHERE v BETWEEN $1 AND $2 ORDER BY id;
-- PARAMS:
'2026-04-28T00:00:00Z', '2026-04-30 00:00:00'
-- EXPECTED:
1,
2

-- TEST: datetime-table-param-in
-- SKIP_FOR: iproto
-- SQL:
SELECT id FROM datetime_params WHERE v IN ($1, $2);
-- PARAMS:
'2026-04-28T00:00:00Z', '20260429'
-- EXPECTED:
1

-- TEST: datetime-table-param-update
-- SKIP_FOR: iproto
-- SQL:
UPDATE datetime_params SET v = $1 WHERE id = 1;
-- PARAMS:
'2026-05-01T00:00:00 UTC'

-- TEST: datetime-table-param-delete
-- SKIP_FOR: iproto
-- SQL:
DELETE FROM datetime_params WHERE v = $1;
-- PARAMS:
'2026-04-30T00:00:00 Z'

-- TEST: datetime-table-param-result
-- SKIP_FOR: iproto
-- SQL:
SELECT id, v = '2026-05-01T00:00:00Z'::datetime FROM datetime_params;
-- EXPECTED:
1, true

-- TEST: datetime-table-param-drop
-- SKIP_FOR: iproto
-- SQL:
DROP TABLE datetime_params;

-- TEST: datetime-runtime-cast-1
-- SQL:
SELECT CAST("COLUMN_1" AS datetime) = '2026-04-29T00:00:00Z'::datetime FROM (VALUES ('April 29, 2026'));
-- EXPECTED:
true

-- TEST: datetime-runtime-cast-2
-- SQL:
SELECT CAST("COLUMN_1" AS datetime) FROM (VALUES ('2026-W18-3'));
-- ERROR:
Type mismatch: can not convert string\('2026-W18-3'\) to datetime

-- TEST: datetime-runtime-cast-3
-- SQL:
SELECT to_char(CAST("COLUMN_1" AS datetime), '%Y-%m-%dT%H:%M:%S%z') FROM (VALUES ('2026-04-29T00:00:00 Europe/Moscow'));
-- EXPECTED:
'2026-04-29T00:00:00+0300'

-- TEST: datetime-runtime-cast-4
-- SQL:
SELECT to_char(CAST("COLUMN_1" AS datetime), '%Y-%m-%dT%H:%M:%S%z') FROM (VALUES ('2026-04-29T00:00:00+15:00'));
-- EXPECTED:
'2026-04-28T09:00:00+0000'

-- TEST: datetime-to-date-1
-- SQL:
SELECT to_date('April 29, 2026 12:34:56', '') = '2026-04-29T00:00:00Z'::datetime;
-- EXPECTED:
true
