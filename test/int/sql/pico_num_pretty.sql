-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- TEST: init
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t (a INT PRIMARY KEY, n DECIMAL);
INSERT INTO t VALUES (1, 12345.6), (2, 1.5), (3, NULL);

-- TEST: num-pretty-small
-- SQL:
SELECT pico_num_pretty(0), pico_num_pretty(-1), pico_num_pretty(999), pico_num_pretty(-999);
-- EXPECTED:
'0', '-1', '999', '-999'

-- TEST: num-pretty-suffixes
-- SQL:
SELECT pico_num_pretty(1000), pico_num_pretty(12345), pico_num_pretty(123456),
       pico_num_pretty(1234567890), pico_num_pretty(9223372036854775807);
-- EXPECTED:
'1.00k', '12.3k', '123k', '1.23G', '9.22E'

-- TEST: num-pretty-truncation
-- SQL:
SELECT pico_num_pretty(9999), pico_num_pretty(999999), pico_num_pretty(-12399);
-- EXPECTED:
'9.99k', '999k', '-12.3k'

-- TEST: num-pretty-decimal
-- SQL:
SELECT pico_num_pretty(1.25), pico_num_pretty('10000000000000000000000000000'::numeric),
       pico_num_pretty(1999.5), pico_num_pretty(-1999.5);
-- EXPECTED:
'1.25', '10000Y', '1.99k', '-1.99k'

-- TEST: num-pretty-null
-- SQL:
SELECT pico_num_pretty(NULL);
-- EXPECTED:
None

-- TEST: num-pretty-columns
-- SQL:
SELECT a, pico_num_pretty(n) FROM t ORDER BY a;
-- EXPECTED:
1, '12.3k',
2, '1.5',
3, None

-- TEST: num-pretty-aggregate
-- SQL:
SELECT pico_num_pretty(count(*) * 1000) FROM t;
-- EXPECTED:
'3.00k'

-- TEST: num-pretty-double-is-decimal
-- SQL:
SELECT pico_num_pretty(1.5::double), pico_num_pretty(1e9::double), pico_num_pretty(-1999.5::double);
-- EXPECTED:
'1.5', '1.00G', '-1.99k'
