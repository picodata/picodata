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
