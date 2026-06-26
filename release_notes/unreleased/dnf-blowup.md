## fix/sql

- Queries of the form `(a OR b) AND (c OR d) AND (e OR f)...` could grow 
  exponentially after DNF conversion. This led to a stack overflow. A limit of 
  512 disjunctions has been added; if this limit is exceeded, the DNF is not 
  constructed. You may notice this in the `EXPLAIN` output:
  `Query (WHOLE STORAGE)`. It is recommended to rewrite such queries
  [picodata!3353].
