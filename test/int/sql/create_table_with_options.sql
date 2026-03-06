-- TEST: initialization
-- SQL:
DROP TABLE IF EXISTS t;

-- TEST: invalid_option_name
-- SQL:
CREATE TABLE t(a INT PRIMARY KEY) WITH (invalid_opt = 1) DISTRIBUTED GLOBALLY;
-- ERROR:
rule parsing error

-- TEST: missing_equals_sign
-- SQL:
CREATE TABLE t(a INT PRIMARY KEY) WITH (page_size 1024) DISTRIBUTED GLOBALLY;
-- ERROR:
rule parsing error

-- TEST: missing_value
-- SQL:
CREATE TABLE t(a INT PRIMARY KEY) WITH (page_size =) DISTRIBUTED GLOBALLY;
-- ERROR:
rule parsing error

-- TEST: empty_with_clause
-- SQL:
CREATE TABLE t(a INT PRIMARY KEY) WITH () DISTRIBUTED GLOBALLY;
-- ERROR:
rule parsing error

-- TEST: wrong_type_string_for_numeric
-- SQL:
CREATE TABLE t(a INT PRIMARY KEY) WITH (page_size = 'hello') DISTRIBUTED GLOBALLY;
-- ERROR:
rule parsing error

-- TEST: negative_value_for_unsigned
-- SQL:
CREATE TABLE t(a INT PRIMARY KEY) WITH (page_size = -1) DISTRIBUTED GLOBALLY;
-- ERROR:
rule parsing error
