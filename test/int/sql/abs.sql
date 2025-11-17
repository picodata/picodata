-- TEST: init
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t (a INT PRIMARY KEY);
INSERT INTO t VALUES (10), (-10), (-1);

-- TEST: abs-1
-- SQL:
SELECT ABS(a) from t;
-- EXPECTED:
10,
1,
10

-- TEST: abs-2
-- SQL:
SELECT ABS(-1);
-- EXPECTED:
1

-- TEST: abs-3
-- SQL:
SELECT ABS(10);
-- EXPECTED:
10

-- TEST: abs-4
-- SQL:
SELECT ABS(-1.2);
-- EXPECTED:
Decimal('1.2')

-- TEST: abs-5
-- SQL:
SELECT ABS(1.2);
-- EXPECTED:
Decimal('1.2')
