## breaking/sql

- An SQL query that takes too many parser steps fails with `query is too
  complex to parse`; before, it was parsed however long it took. By
  default this rejects correct queries longer than about 170 KB of a dense
  list like `select x, x, ...` or about 700 KB of ordinary SQL: raise
  `sql_parse_step_max` or set it to `0` for them. A query nested deeper
  than 90 levels may fail whatever the setting: split it ([!3967]).
