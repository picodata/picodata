-- TEST: trim1
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t(id INT PRIMARY KEY, a INT);
INSERT INTO t VALUES(112211, 2211);

-- TEST: test_trim-1.1
-- SQL:
SELECT trim('  aabb  ') as "a" from "t"
-- EXPECTED:
'aabb'

-- TEST: test_trim-1.2
-- SQL:
SELECT trim(trim('  aabb  ')) from "t"
-- EXPECTED:
'aabb'

-- TEST: test_trim-1.3
-- SQL:
SELECT trim('a' from trim('  aabb  ')) from "t"
-- EXPECTED:
'bb'

-- TEST: test_trim-1.4
-- SQL:
SELECT trim(trim(' aabb ') from trim('  aabb  ')) from "t"
-- EXPECTED:
''

-- TEST: test_trim-1.5
-- SQL:
SELECT trim(leading 'a' from trim('aabb  ')) from "t"
-- EXPECTED:
'bb'

-- TEST: test_trim-1.6
-- SQL:
SELECT trim(trailing 'b' from trim('aabb')) from "t"
-- EXPECTED:
'aa'

-- TEST: test_trim-1.7
-- SQL:
SELECT trim(both 'ab' from 'aabb') from "t"
-- EXPECTED:
''

-- TEST: test_trim-1.8
-- SQL:
SELECT trim(  'a' from trim(  '  aabb  ')) from "t"
-- EXPECTED:
'bb'

-- TEST: test_trim-1.9
-- SQL:
SELECT trim(  trim(  ' aabb ') from trim(  '  aabb  ')) from "t"
-- EXPECTED:
''

-- TEST: test_trim-1.10
-- SQL:
SELECT trim(  leading 'a' from trim(  'aabb  ')) from "t"
-- EXPECTED:
'bb'

-- TEST: test_trim-1.11
-- SQL:
SELECT trim(  trailing 'b' from trim(  'aabb')) from "t"
-- EXPECTED:
'aa'

-- TEST: test_trim-1.12
-- SQL:
SELECT trim(  both 'ab' from 'aabb') from "t"
-- EXPECTED:
''
