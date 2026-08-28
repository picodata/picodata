## fix/sql

- `CREATE TABLE` with an `ARRAY` column in the primary key is now rejected by
  the planner with `ARRAY columns are not indexable`. Previously the statement
  reached DDL and failed there with the raw tarantool message
  `field type 'array' is not supported`.
