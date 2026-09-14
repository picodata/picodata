-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- TEST: typed-literal-setup
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t (id INT PRIMARY KEY, b TEXT) DISTRIBUTED BY (id);
INSERT INTO t VALUES (int '1', text 'a'), (2, 'b');

-- TEST: typed-literal-values
-- SQL:
SELECT bool 't', int '42', text 'x', double '1.5', decimal '1.5';
-- EXPECTED:
true, 42, 'x', 1.5, Decimal('1.5')

-- TEST: typed-literal-is-a-cast
-- SQL:
EXPLAIN SELECT bool 't';
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (true::bool -> col_1)
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = any

-- TEST: typed-literal-matches-cast-for-every-type
-- SQL:
SELECT
    bool 't' = cast('t' AS bool),
    boolean 'false' = cast('false' AS boolean),
    int '1' = cast('1' AS int),
    int8 '1' = cast('1' AS int8),
    bigint '1' = cast('1' AS bigint),
    smallint '1' = cast('1' AS smallint),
    integer '1' = cast('1' AS integer),
    decimal '1.5' = cast('1.5' AS decimal),
    numeric(5, 2) '1.5' = cast('1.5' AS numeric(5, 2)),
    number '1.5' = cast('1.5' AS number),
    double '1.5' = cast('1.5' AS double),
    text 'x' = cast('x' AS text),
    string 'x' = cast('x' AS string),
    varchar 'x' = cast('x' AS varchar),
    varchar(3) 'x' = cast('x' AS varchar(3)),
    uuid 'e4166fc5-e113-46c5-8ae9-970882ca8842' = cast('e4166fc5-e113-46c5-8ae9-970882ca8842' AS uuid),
    datetime '2003-07-08T19:13:00+03:00' = cast('2003-07-08T19:13:00+03:00' AS datetime);
-- EXPECTED:
true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true

-- TEST: typed-literal-composes
-- SQL:
SELECT int '1'::text, int '1' + 1, bool 't' IS TRUE, NOT bool 'f', bool't', int '1' x;
-- EXPECTED:
'1', 2, true, true, true, 1

-- TEST: typed-literal-in-values
-- SQL:
SELECT * FROM (VALUES (int '1'));
-- EXPECTED:
1

-- TEST: typed-literal-in-filter
-- SQL:
SELECT b FROM t WHERE id = int '1';
-- EXPECTED:
'a'

-- TEST: typed-literal-routes-like-plain-constant
-- SQL:
EXPLAIN (buckets) SELECT b FROM t WHERE id = int '1';
-- EXPECTED:
buckets = [1934]

-- TEST: plain-constant-routing
-- SQL:
EXPLAIN (buckets) SELECT b FROM t WHERE id = 1;
-- EXPECTED:
buckets = [1934]

-- TEST: type-names-stay-identifiers
-- SQL:
SELECT bool text FROM (SELECT 1 AS bool);
-- EXPECTED:
1

-- TEST: typed-literal-requires-string
-- SQL:
SELECT int 1;
-- ERROR:
rule parsing error

-- TEST: typed-literal-rejects-parameter
-- SQL:
SELECT int $1;
-- ERROR:
could not resolve reference "int"

-- TEST: typed-literal-rejects-array-brackets
-- SQL:
SELECT int[] '{1}';
-- ERROR:
rule parsing error

-- TEST: typed-literal-rejects-array-keyword
-- SQL:
SELECT int array '{1}';
-- ERROR:
rule parsing error

-- TEST: typed-literal-rejects-string-continuation
-- SQL:
SELECT int 'a' 'b';
-- ERROR:
rule parsing error

-- TEST: typed-literal-invalid-value
-- SQL:
SELECT int 'abc';
-- ERROR:
Type mismatch: can not convert string\('abc'\) to integer

-- TEST: cast-invalid-value
-- SQL:
SELECT 'abc'::int;
-- ERROR:
Type mismatch: can not convert string\('abc'\) to integer
