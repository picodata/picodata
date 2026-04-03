-- TEST: fmt-few-columns
-- SQL:
explain (fmt) select id, name from _pico_table;
-- EXPECTED:
projection (_pico_table.id::int -> id, _pico_table.name::string -> name)
  scan _pico_table
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
buckets = any

-- TEST: fmt-many-columns
-- SQL:
explain (fmt) select * from _pico_table;
-- EXPECTED:
projection (
  _pico_table.id::int -> id,
  _pico_table.name::string -> name,
  _pico_table.distribution::map -> distribution,
  _pico_table.format::array -> format,
  _pico_table.schema_version::int -> schema_version,
  _pico_table.operable::bool -> operable,
  _pico_table.engine::string -> engine,
  _pico_table.owner::int -> owner,
  _pico_table.description::string -> description,
  _pico_table.opts::array -> opts
)
  scan _pico_table
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
buckets = any

-- TEST: fmt-one-long-column
-- SQL:
explain (fmt) select id * 2 + id * 3 + id * 4 from _pico_table;
-- EXPECTED:
projection (_pico_table.id::int * 2::int + _pico_table.id::int * 3::int + _pico_table.id::int * 4::int -> col_1)
  scan _pico_table
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
buckets = any

-- TEST: fmt-various-columns
-- SQL:
explain (fmt)
select
  10,
  id * 2 + id * 3 + id * 4,
  id * 2 + id * 3 + id * 4,
  not (id > 10 and id < 10000000),
  'hello'
from _pico_table;
-- EXPECTED:
projection (
  10::int -> col_1,
  _pico_table.id::int * 2::int + _pico_table.id::int * 3::int + _pico_table.id::int * 4::int -> col_2,
  _pico_table.id::int * 2::int + _pico_table.id::int * 3::int + _pico_table.id::int * 4::int -> col_3,
  not (_pico_table.id::int > 10::int and _pico_table.id::int < 10000000::int) -> col_4,
  'hello'::string -> col_5
)
  scan _pico_table
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
buckets = any

-- TEST: fmt-many-simple-columns
-- SQL:
explain (fmt) select 1, 2, 3, 4, 5, 6, 7;
-- EXPECTED:
projection (
  1::int -> col_1,
  2::int -> col_2,
  3::int -> col_3,
  4::int -> col_4,
  5::int -> col_5,
  6::int -> col_6,
  7::int -> col_7
)
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
buckets = any

-- TEST: fmt-complex-column-expr
-- SQL:
explain (fmt)
select not ((id > 1000 and id < 2000 and id % 10 = 5) or
            (id > 3000 and id < 4000 and id % 4 = 2))
from _pico_table;
-- EXPECTED:
projection (not (_pico_table.id::int > 1000::int and _pico_table.id::int < 2000::int and _pico_table.id::int % 10::int = 5::int or _pico_table.id::int > 3000::int and _pico_table.id::int < 4000::int and _pico_table.id::int % 4::int = 2::int) -> col_1)
  scan _pico_table
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
buckets = any

-- TEST: fmt-where-long-in
-- SQL:
explain (fmt)
select count(*) from _pico_table where id in (1,2,3,4,5,6,7,8,9,10);
-- EXPECTED:
projection (count(*)::int -> col_1)
  selection _pico_table.id::int in ROW(1::int, 2::int, 3::int, 4::int, 5::int, 6::int, 7::int, 8::int, 9::int, 10::int)
    scan _pico_table
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
buckets = any

-- TEST: fmt-where-many-ored-conditions
-- SQL:
explain (fmt)
select count(*) from _pico_table
where id = 1 or id = 2 or id = 2 or id = 4 or id = 5;
-- EXPECTED:
projection (count(*)::int -> col_1)
  selection _pico_table.id::int = 1::int or _pico_table.id::int = 2::int or _pico_table.id::int = 2::int or _pico_table.id::int = 4::int or _pico_table.id::int = 5::int
    scan _pico_table
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
buckets = any

-- TEST: fmt-where-many-anded-conditions
-- SQL:
explain (fmt)
select count(*) from _pico_table
where id = 23456
      and name = 'foobar'
      and operable = true
      and description is not null
      and name != (select min(name) from _pico_table);
-- EXPECTED:
projection (count(*)::int -> col_1)
  selection _pico_table.id::int = 23456::int and _pico_table.name::string = 'foobar'::string and _pico_table.operable::bool = true::bool and not _pico_table.description::string is null and _pico_table.name::string <> ROW($0)
    scan _pico_table
subquery $0:
  scan
    projection (min(_pico_table.name::string::string)::string -> col_1)
      scan _pico_table
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
buckets = any

-- TEST: fmt-long-case-when
-- SQL:
explain (fmt)
select 'hello', case 1 when 1 then 1 when 2 then 2 when 3 then 3 end * 2 + 2000;
-- EXPECTED:
projection ('hello'::string -> col_1, case 1::int when 1::int then 1::int when 2::int then 2::int when 3::int then 3::int end * 2::int + 2000::int -> col_2)
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
buckets = any

-- TEST: fmt-long-cte
-- SQL:
explain (fmt)
with q as (
  values (1, 'foobar', NULL, NULL, 3.1415),
         (2, 'foobar', NULL, NULL, 3.1415),
         (3, 'foobar', NULL, NULL, 3.1415),
         (4, '', 0, 0, 0)
) select count(*) from q;
-- EXPECTED:
projection (count(*)::int -> col_1)
  scan cte q($0)
subquery $0:
  motion [policy: full, program: ReshardIfNeeded]
    values
      value ROW(1::int, 'foobar'::string, NULL::unknown, NULL::unknown, 3.1415::decimal)
      value ROW(2::int, 'foobar'::string, NULL::unknown, NULL::unknown, 3.1415::decimal)
      value ROW(3::int, 'foobar'::string, NULL::unknown, NULL::unknown, 3.1415::decimal)
      value ROW(4::int, ''::string, 0::int, 0::int, 0::int)
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
buckets = any
