-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- TEST: substring1
-- SQL:
SELECT SUBSTRING('abcdefg', 1, 2);
-- EXPECTED:
'ab'

-- TEST: substring2
-- SQL:
SELECT SUBSTRING('abcdefg', 'a_c', '#');
-- EXPECTED:
