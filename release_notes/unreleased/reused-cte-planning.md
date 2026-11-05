## fix/sql

- The time and memory to plan and run a query no longer grow exponentially with the depth of CTE
  reuse or of subqueries nested in a `SELECT` list. A short query could block the thread of an
  instance for seconds and take gigabytes of memory, part of which stayed in the plan cache. 22
  levels of CTEs that read the previous one twice took about 9 s and 350 MB, now the whole query
  takes about 10 ms. A chain of CTEs on one bucket still repeats its SQL text at each reference.
