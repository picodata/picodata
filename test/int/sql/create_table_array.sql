-- TEST: bracket-unsized
-- SQL:
CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b INT[]);

-- TEST: bracket-multidim-unsized
-- SQL:
CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b INT[][]);

-- TEST: keyword-unsized
-- SQL:
CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b INT ARRAY);

-- TEST: keyword-sized
-- SQL:
CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b INT ARRAY[3]);

-- TEST: keyword-multidim-rejected
-- SQL:
CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b INT ARRAY[2][3]);
-- ERROR:
rule parsing error

-- TEST: keyword-multidim-leading-empty-rejected
-- SQL:
CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b INT ARRAY[][3]);
-- ERROR:
rule parsing error

-- TEST: keyword-multidim-trailing-empty-rejected
-- SQL:
CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b INT ARRAY[3][]);
-- ERROR:
rule parsing error

-- TEST: keyword-empty-bound-rejected
-- SQL:
CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b INT ARRAY[]);
-- ERROR:
rule parsing error

-- TEST: bracket-multidim-mixed-sized-leading
-- SQL:
CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b INT[3][]);

-- TEST: bracket-multidim-mixed-sized-trailing
-- SQL:
CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b INT[][3]);

-- TEST: sized-1d
-- SQL:
CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b INT[3]);

-- TEST: text
-- SQL:
CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b TEXT[]);

-- TEST: bool
-- SQL:
CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b BOOL[][]);

-- TEST: uuid-array-keyword
-- SQL:
CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b UUID ARRAY);

-- TEST: double-array-sized
-- SQL:
CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b DOUBLE ARRAY[5]);

-- TEST: standalone-rejected
-- SQL:
CREATE TABLE IF NOT EXISTS t (a INT PRIMARY KEY, b ARRAY);
-- ERROR:
rule parsing error

-- TEST: alter-add-bracket
-- SQL:
ALTER TABLE t ADD COLUMN c1 INT[];

-- TEST: alter-add-bracket-multidim
-- SQL:
ALTER TABLE t ADD COLUMN c2 TEXT[][];

-- TEST: alter-add-array-keyword
-- SQL:
ALTER TABLE t ADD COLUMN c3 INT ARRAY;

-- TEST: alter-add-array-sized
-- SQL:
ALTER TABLE t ADD COLUMN c4 INT ARRAY[3];

-- TEST: alter-add-array-multi-rejected
-- SQL:
ALTER TABLE t ADD COLUMN c5 INT ARRAY[2][3];
-- ERROR:
rule parsing error
