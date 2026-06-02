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

-- TEST: insert
-- SQL:
explain (logical) insert into t values (1, 1, '1');
-- EXPECTED:
insert into t on conflict: fail
  motion [policy: segment([ref("COLUMN_3"), ref("COLUMN_1")]), program: ReshardIfNeeded]
    values
      value ROW(1::int, 1::int, '1'::string)

-- TEST: delete
-- SQL:
explain (logical) delete from t where a = 1 and c = '1';
-- EXPECTED:
delete from t
  motion [policy: local, program: [PrimaryKey(0, 1), ReshardIfNeeded]]
    projection (t.c::string -> pk_col_0, t.a::int -> pk_col_1)
      selection ((t.a::int = 1::int and t.c::string = '1'::string))
        scan t

-- TEST: update
-- SQL:
explain (logical) update t set b = b + 1 where a = 1;
-- EXPECTED:
update t (b = col_0)
  motion [policy: local, program: ReshardIfNeeded]
    projection (t.b::double + 1::int -> col_0, t.c::string -> col_1, t.a::int -> col_2)
      selection (t.a::int = 1::int)
        scan t

-- TEST: block-let-elete-with-return-query
-- SQL:
EXPLAIN (logical)
DO $$ BEGIN
    LET a = (SELECT d FROM tt WHERE d = 42);
    RETURN QUERY SELECT a;
    DELETE FROM tt WHERE d = 42;
END $$;
-- EXPECTED:
╭───────────────────────────────╮
│ 1. Let "a" (FILTERED STORAGE) │
╰───────────────────────────────╯
''
SELECT "tt"."d" FROM "tt" WHERE "tt"."d" = CAST(42 AS int)
''
projection (tt.d::int -> d)
  selection (tt.d::int = 42::int)
    scan tt
''
╭────────────────────────────────────╮
│ 2. Return query (FILTERED STORAGE) │
╰────────────────────────────────────╯
''
SELECT CAST(:a AS int) as "col_1"
''
projection (:a::int -> col_1)
''
╭─────────────────────────────╮
│ 3. Query (FILTERED STORAGE) │
╰─────────────────────────────╯
''
DELETE FROM "tt" WHERE "tt"."d" = CAST(42 AS int)
''
delete from tt
  projection (tt.d::int -> pk_col_0)
    selection (tt.d::int = 42::int)
      scan tt

-- TEST: block-if-let-delete-insert
-- SQL:
EXPLAIN (logical)
DO $$ BEGIN
    LET a = (SELECT d FROM tt WHERE d = 2 ORDER BY 1 LIMIT 1);

    IF a = 5 THEN
        INSERT INTO tt VALUES (2);
    END IF;

    DELETE FROM tt WHERE d = 2;
END $$;
-- EXPECTED:
╭───────────────────────────────╮
│ 1. Let "a" (FILTERED STORAGE) │
╰───────────────────────────────╯
''
SELECT "d" FROM ( SELECT "tt"."d" FROM "tt" WHERE "tt"."d" = CAST(2 AS int) ) ORDER BY 1 LIMIT 1
''
limit 1
  projection (d::int)
    order by (1)
      scan
        projection (tt.d::int -> d)
          selection (tt.d::int = 2::int)
            scan tt
''
╭───────────────────────────────╮
│ 2. If cond (FILTERED STORAGE) │
╰───────────────────────────────╯
''
SELECT CAST(:a AS int) = CAST(5 AS int) as "cond"
''
projection (:a::int = 5::int -> cond)
''
╭───────────────────────────────╮
│ 3. If body (FILTERED STORAGE) │
╰───────────────────────────────╯
''
INSERT INTO "tt" ("d", "bucket_id") VALUES (CAST(2 AS int), 1410)
''
insert into tt on conflict: fail
  values
    value ROW(2::int)
''
╭─────────────────────────────╮
│ 4. Query (FILTERED STORAGE) │
╰─────────────────────────────╯
''
DELETE FROM "tt" WHERE "tt"."d" = CAST(2 AS int)
''
delete from tt
  projection (tt.d::int -> pk_col_0)
    selection (tt.d::int = 2::int)
      scan tt

-- TEST: buckets-block-multiple-return-query
-- SQL:
EXPLAIN (logical)
DO $$ BEGIN
    RETURN QUERY SELECT a FROM g JOIN (SELECT d FROM tt WHERE d = 42) ttt ON true;
    RETURN QUERY SELECT * FROM tt WHERE d = 42;
    RETURN QUERY SELECT a FROM g;
END $$;
-- EXPECTED:
╭────────────────────────────────────╮
│ 1. Return query (FILTERED STORAGE) │
╰────────────────────────────────────╯
''
SELECT "g"."a" FROM "g" INNER JOIN ( SELECT "tt"."d" FROM "tt" WHERE "tt"."d" = CAST(42 AS int) ) as "ttt" ON CAST(true AS bool)
''
projection (g.a::int -> a)
  join on (true::bool)
    scan g
    scan ttt
      projection (tt.d::int -> d)
        selection (tt.d::int = 42::int)
          scan tt
''
╭────────────────────────────────────╮
│ 2. Return query (FILTERED STORAGE) │
╰────────────────────────────────────╯
''
SELECT "tt"."d" FROM "tt" WHERE "tt"."d" = CAST(42 AS int)
''
projection (tt.d::int -> d)
  selection (tt.d::int = 42::int)
    scan tt
''
╭────────────────────────────────────╮
│ 3. Return query (FILTERED STORAGE) │
╰────────────────────────────────────╯
''
SELECT "g"."a" FROM "g"
''
projection (g.a::int -> a)
  scan g

-- TEST: block-unused-let
-- SQL:
EXPLAIN (logical)
DO $$ BEGIN
    LET a = (SELECT d FROM tt WHERE d = 2 ORDER BY 1 LIMIT 1);
    LET a = (SELECT 1);

    IF a = 5 THEN
        INSERT INTO tt VALUES (2);
    END IF;
END $$;
-- EXPECTED:
╭──────────────────────────────────────────╮
│ 1. **Unused** let "a" (FILTERED STORAGE) │
╰──────────────────────────────────────────╯
''
SELECT "d" FROM ( SELECT "tt"."d" FROM "tt" WHERE "tt"."d" = CAST(2 AS int) ) ORDER BY 1 LIMIT 1
''
limit 1
  projection (d::int)
    order by (1)
      scan
        projection (tt.d::int -> d)
          selection (tt.d::int = 2::int)
            scan tt
''
╭───────────────────────────────╮
│ 2. Let "a" (FILTERED STORAGE) │
╰───────────────────────────────╯
''
SELECT CAST(1 AS int) as "col_1"
''
projection (1::int -> col_1)
''
╭───────────────────────────────╮
│ 3. If cond (FILTERED STORAGE) │
╰───────────────────────────────╯
''
SELECT CAST(:a AS int) = CAST(5 AS int) as "cond"
''
projection (:a::int = 5::int -> cond)
''
╭───────────────────────────────╮
│ 4. If body (FILTERED STORAGE) │
╰───────────────────────────────╯
''
INSERT INTO "tt" ("d", "bucket_id") VALUES (CAST(2 AS int), 1410)
''
insert into tt on conflict: fail
  values
    value ROW(2::int)

-- TEST: block-unused-let-return
-- SQL:
EXPLAIN (logical)
DO $$ BEGIN
    LET a = (SELECT d FROM tt WHERE d = 2 ORDER BY 1 LIMIT 1);
    LET a = (SELECT 1);
    RETURN QUERY SELECT a;
END $$;
-- EXPECTED:
╭──────────────────────────────────────────╮
│ 1. **Unused** let "a" (FILTERED STORAGE) │
╰──────────────────────────────────────────╯
''
SELECT "d" FROM ( SELECT "tt"."d" FROM "tt" WHERE "tt"."d" = CAST(2 AS int) ) ORDER BY 1 LIMIT 1
''
limit 1
  projection (d::int)
    order by (1)
      scan
        projection (tt.d::int -> d)
          selection (tt.d::int = 2::int)
            scan tt
''
╭───────────────────────────────╮
│ 2. Let "a" (FILTERED STORAGE) │
╰───────────────────────────────╯
''
SELECT CAST(1 AS int) as "col_1"
''
projection (1::int -> col_1)
''
╭────────────────────────────────────╮
│ 3. Return query (FILTERED STORAGE) │
╰────────────────────────────────────╯
''
SELECT CAST(:a AS int) as "col_1"
''
projection (:a::int -> col_1)
