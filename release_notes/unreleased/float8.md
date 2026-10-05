## feat/sql

- Support the PostgreSQL type name `FLOAT8` as an alias for `DOUBLE`, e.g.
  `CREATE TABLE t (id INT PRIMARY KEY, a FLOAT8)` or `CAST('1.5' AS FLOAT8)`.
