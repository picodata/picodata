## feat/sql

- [picodata#1596] Added support for `ARRAY` columns in `CREATE TABLE` and
  `ALTER TABLE ADD COLUMN`. Supported syntax: `T[]`, `T[N]`, `T[N][M]`,
  `T[][]`, `T ARRAY`, `T ARRAY[N]`. Declared type and sizes are documentation
  only and do not affect the internal implementation for now.
