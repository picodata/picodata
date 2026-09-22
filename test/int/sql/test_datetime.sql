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

-- TEST: datetime-4.1
-- SQL:
select '2026-01-13'::datetime;
-- EXPECTED:
Datetime('2026-01-13T00:00:00Z')

-- TEST: datetime-4.2
-- SQL:
select '2026-01-13T10:20:30+03:00'::datetime;
-- EXPECTED:
Datetime('2026-01-13T10:20:30+03:00')

-- TEST: datetime-4.3
-- SQL:
select '2026-01-13T10:20:30.123456Z'::datetime;
-- EXPECTED:
Datetime('2026-01-13T10:20:30.123456Z')

-- TEST: datetime-4.4
-- SQL:
select '1969-07-20T20:17:40Z'::datetime;
-- EXPECTED:
Datetime('1969-07-20T20:17:40Z')

-- TEST: datetime-4.5
-- SQL:
CREATE TABLE dt (id int primary key, v datetime);
INSERT INTO dt VALUES (1, '2026-01-13'), (2, '1969-07-20T20:17:40Z'), (3, NULL);

-- TEST: datetime-4.6
-- SQL:
select v from dt;
-- UNORDERED:
Datetime('2026-01-13T00:00:00Z'), Datetime('1969-07-20T20:17:40Z'), NULL
