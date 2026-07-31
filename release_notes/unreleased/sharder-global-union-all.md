## fix/sql

- Queries using `UNION ALL`/`EXCEPT` that involve a sharded table and a global 
  table now return the correct result.
