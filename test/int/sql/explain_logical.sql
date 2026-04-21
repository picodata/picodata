-- TEST: explain-setup
-- SQL:
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS tt;
DROP TABLE IF EXISTS g;
CREATE TABLE t (a INT, b DOUBLE, c TEXT, PRIMARY KEY (c, a));
CREATE TABLE tt (d INT PRIMARY KEY);
CREATE TABLE g (a INT PRIMARY KEY, b DOUBLE, c TEXT) DISTRIBUTED GLOBALLY;

-- TEST: simple-select
-- SQL:
explain (logical) select * from _pico_table;
-- EXPECTED:
projection (_pico_table.id::int -> id, _pico_table.name::string -> name, _pico_table.distribution::map -> distribution, _pico_table.format::array -> format, _pico_table.schema_version::int -> schema_version, _pico_table.operable::bool -> operable, _pico_table.engine::string -> engine, _pico_table.owner::int -> owner, _pico_table.description::string -> description, _pico_table.opts::array -> opts)
  scan _pico_table
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000

-- TEST: many-rel-operators
-- SQL:
explain (logical) select * from t join t tt on true where t.b = 3 group by 1, 2, 3, 4, 5, 6 order by 4 limit 2;
-- EXPECTED:
limit 2
  projection (a::int, b::double, c::string, a::int, b::double, c::string)
    order by (4)
      scan
        projection (gr_expr_1::int -> a, gr_expr_2::double -> b, gr_expr_3::string -> c, gr_expr_4::int -> a, gr_expr_5::double -> b, gr_expr_6::string -> c)
          group by (gr_expr_1::int, gr_expr_2::double, gr_expr_3::string, gr_expr_4::int, gr_expr_5::double, gr_expr_6::string) output (gr_expr_1::int, gr_expr_2::double, gr_expr_3::string, gr_expr_4::int, gr_expr_5::double, gr_expr_6::string)
            motion [policy: full, program: ReshardIfNeeded]
              limit 2
                projection (gr_expr_1::int, gr_expr_2::double, gr_expr_3::string, gr_expr_4::int, gr_expr_5::double, gr_expr_6::string)
                  order by (4)
                    scan
                      projection (t.a::int -> gr_expr_1, t.b::double -> gr_expr_2, t.c::string -> gr_expr_3, tt.a::int -> gr_expr_4, tt.b::double -> gr_expr_5, tt.c::string -> gr_expr_6)
                        group by (t.a::int, t.b::double, t.c::string, tt.a::int, tt.b::double, tt.c::string) output (t.a::int -> a, t.b::double -> b, t.c::string -> c, t.bucket_id::int -> bucket_id, tt.a::int -> a, tt.b::double -> b, tt.c::string -> c, tt.bucket_id::int -> bucket_id)
                          selection (t.b::double = 3::int)
                            join on (true::bool)
                              scan t
                              motion [policy: full, program: ReshardIfNeeded]
                                projection (tt.a::int -> a, tt.b::double -> b, tt.c::string -> c, tt.bucket_id::int -> bucket_id)
                                  scan t -> tt
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000

-- TEST: many-rel-operators-fmt
-- SQL:
explain (logical, fmt) select * from t join t tt on true where t.b = 3 group by 1, 2, 3, 4, 5, 6 order by 4 limit 2;
-- EXPECTED:
limit 2
  projection (
    a::int,
    b::double,
    c::string,
    a::int,
    b::double,
    c::string
  )
    order by (4)
      scan
        projection (
          gr_expr_1::int -> a,
          gr_expr_2::double -> b,
          gr_expr_3::string -> c,
          gr_expr_4::int -> a,
          gr_expr_5::double -> b,
          gr_expr_6::string -> c
        )
          group by (
            gr_expr_1::int,
            gr_expr_2::double,
            gr_expr_3::string,
            gr_expr_4::int,
            gr_expr_5::double,
            gr_expr_6::string
          ) output (
            gr_expr_1::int,
            gr_expr_2::double,
            gr_expr_3::string,
            gr_expr_4::int,
            gr_expr_5::double,
            gr_expr_6::string
          )
            motion [policy: full, program: ReshardIfNeeded]
              limit 2
                projection (
                  gr_expr_1::int,
                  gr_expr_2::double,
                  gr_expr_3::string,
                  gr_expr_4::int,
                  gr_expr_5::double,
                  gr_expr_6::string
                )
                  order by (4)
                    scan
                      projection (
                        t.a::int -> gr_expr_1,
                        t.b::double -> gr_expr_2,
                        t.c::string -> gr_expr_3,
                        tt.a::int -> gr_expr_4,
                        tt.b::double -> gr_expr_5,
                        tt.c::string -> gr_expr_6
                      )
                        group by (
                          t.a::int,
                          t.b::double,
                          t.c::string,
                          tt.a::int,
                          tt.b::double,
                          tt.c::string
                        ) output (
                          t.a::int -> a,
                          t.b::double -> b,
                          t.c::string -> c,
                          t.bucket_id::int -> bucket_id,
                          tt.a::int -> a,
                          tt.b::double -> b,
                          tt.c::string -> c,
                          tt.bucket_id::int -> bucket_id
                        )
                          selection (t.b::double = 3::int)
                            join on (true::bool)
                              scan t
                              motion [policy: full, program: ReshardIfNeeded]
                                projection (
                                  tt.a::int -> a,
                                  tt.b::double -> b,
                                  tt.c::string -> c,
                                  tt.bucket_id::int -> bucket_id
                                )
                                  scan t -> tt
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000

-- TEST: insert
-- SQL:
explain (logical) insert into t values (1, 1, '1');
-- EXPECTED:
insert into t on conflict: fail
  motion [policy: segment([ref("COLUMN_3"), ref("COLUMN_1")]), program: ReshardIfNeeded]
    values
      value ROW(1::int, 1::int, '1'::string)
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000

-- TEST: delete
-- SQL:
explain (logical) delete from t where a = 1 and c = '1';
-- EXPECTED:
delete from t
  motion [policy: local, program: [PrimaryKey(0, 1), ReshardIfNeeded]]
    projection (t.c::string -> pk_col_0, t.a::int -> pk_col_1)
      selection ((t.a::int = 1::int and t.c::string = '1'::string))
        scan t
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000

-- TEST: update
-- SQL:
explain (logical) update t set b = b + 1 where a = 1;
-- EXPECTED:
update t (b = col_0)
  motion [policy: local, program: ReshardIfNeeded]
    projection (t.b::double + 1::int -> col_0, t.c::string -> col_1, t.a::int -> col_2)
      selection (t.a::int = 1::int)
        scan t
''
execution options:
  sql_vdbe_opcode_max = 45000
  sql_motion_row_max = 5000
