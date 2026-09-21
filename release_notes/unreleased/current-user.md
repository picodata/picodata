## feat/sql

- Support the `CURRENT_USER` function. As in PostgreSQL, it is written without
  parentheses and returns the name of the user whose privileges the query is
  executed with, e.g. `SELECT * FROM notes WHERE owner = CURRENT_USER`. It can
  be used in projections, filters, `GROUP BY`, `VALUES`, DML and procedure
  bodies.
