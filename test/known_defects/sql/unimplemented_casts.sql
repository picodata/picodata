-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- TEST: cast-array-of-int-to-array-of-text
-- SQL:
select array[1]::text[];
-- ERROR:
invalid value: Failed to cast 1 to string

-- TEST: cast-array-of-bool-to-array-of-text
-- SQL:
select array[true,false]::text[];
-- ERROR:
invalid value: Failed to cast true to string

-- TEST: cast-array-of-decimal-to-array-of-text
-- SQL:
select array[1.0]::text[];
-- ERROR:
invalid value: Failed to cast 1.0 to string

-- TEST: cast-array-of-double-to-array-of-text
-- SQL:
select array[1.0::double]::text[];
-- ERROR:
invalid value: Failed to cast 1 to string

-- TEST: cast-array-of-uuid-to-array-of-text
-- SQL:
select array['556ac82a-0099-4a99-bb5c-afa0017f9692'::uuid]::text[];
-- ERROR:
invalid value: Failed to cast 556ac82a-0099-4a99-bb5c-afa0017f9692 to string

-- TEST: cast-magic-array-to-array-of-text
-- SQL:
select format::text[] from _pico_table where name = '_pico_table';
-- ERROR:
msgpack decode error

-- TEST: parse-array-of-int-1
-- SQL:
select '{}'::int[];
-- ERROR:
Failed to cast '\{\}' to int\[\]

-- TEST: parse-array-of-int-2
-- SQL:
select '{1}'::int[];
-- ERROR:
Failed to cast '\{1\}' to int\[\]

-- TEST: parse-array-of-int-3
-- SQL:
select '{1,2}'::int[];
-- ERROR:
Failed to cast '\{1,2\}' to int\[\]

-- TEST: parse-array-of-text-1
-- SQL:
select '{}'::text[];
-- ERROR:
Failed to cast '\{}\' to string\[\]
