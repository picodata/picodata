## feat/sql

- Support PostgreSQL typed literal syntax sugar: a type name before a string 
  constant, e.g. `SELECT bool 't', int '42'`. It works the same as `CAST('42' AS 
  int)` and `'42'::int`. As in PostgreSQL, array types and parameters are not 
  supported.
