-- TEST: substring1
-- SQL:
SELECT SUBSTRING('abcdefg', 1, 2)
-- EXPECTED:
'ab'

-- TEST: substring2
-- SQL:
SELECT SUBSTRING('abcdefg', 'a_c', '#')
-- EXPECTED:
