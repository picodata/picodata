-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- The expected values are the results of pg_size_pretty of PostgreSQL 18.

-- TEST: init
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t (a INT PRIMARY KEY, n DECIMAL);
INSERT INTO t VALUES (1, 10240.5), (2, 1.5), (3, NULL);

-- TEST: size-pretty-bytes
-- SQL:
SELECT pico_size_pretty(0), pico_size_pretty(1), pico_size_pretty(-1), pico_size_pretty(10239);
-- EXPECTED:
'0 bytes', '1 bytes', '-1 bytes', '10239 bytes'

-- TEST: size-pretty-unit-limits
-- SQL:
SELECT pico_size_pretty(10240), pico_size_pretty(10485247), pico_size_pretty(10485248),
       pico_size_pretty(1073741824), pico_size_pretty(1099511627776),
       pico_size_pretty(1125899906842624), pico_size_pretty(1152921504606846976);
-- EXPECTED:
'10 kB', '10239 kB', '10 MB', '1024 MB', '1024 GB', '1024 TB', '1024 PB'

-- TEST: size-pretty-rounding
-- SQL:
SELECT pico_size_pretty(10751), pico_size_pretty(10752), pico_size_pretty(-10752);
-- EXPECTED:
'10 kB', '11 kB', '-11 kB'

-- TEST: size-pretty-integer-range
-- SQL:
SELECT pico_size_pretty(9223372036854775807), pico_size_pretty(-9223372036854775807 - 1);
-- EXPECTED:
'8192 PB', '-8192 PB'

-- TEST: size-pretty-decimal
-- SQL:
SELECT pico_size_pretty(1.5), pico_size_pretty(-1.5), pico_size_pretty(10240.5),
       pico_size_pretty('123456789012345678901234567890'::numeric);
-- EXPECTED:
'1.5 bytes', '-1.5 bytes', '10 kB', '109651655766237 PB'

-- TEST: size-pretty-decimal-truncation
-- SQL:
SELECT pico_size_pretty(10751.5), pico_size_pretty(-10751.5), pico_size_pretty(-10752.5);
-- EXPECTED:
'10 kB', '-10 kB', '-11 kB'

-- TEST: size-pretty-null
-- SQL:
SELECT pico_size_pretty(NULL);
-- EXPECTED:
None

-- TEST: size-pretty-columns
-- SQL:
SELECT a, pico_size_pretty(n) FROM t ORDER BY a;
-- EXPECTED:
1, '10 kB',
2, '1.5 bytes',
3, None

-- TEST: size-pretty-aggregate
-- SQL:
SELECT pico_size_pretty(sum(a) * 1000000) FROM t;
-- EXPECTED:
'5859 kB'

-- TEST: size-pretty-double-is-decimal
-- SQL:
SELECT pico_size_pretty(1.5::double), pico_size_pretty(1e30::double), pico_size_pretty(-10751.5::double);
-- EXPECTED:
'1.5 bytes', '888178419700125 PB', '-10 kB'

-- TEST: size-pretty-text
-- SQL:
SELECT pico_size_pretty('10'::text);
-- ERROR:
could not resolve function overload
