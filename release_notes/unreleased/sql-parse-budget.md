## fix/sql

- A query of a few dozen nested calls, such as `coalesce()` or
  `substring()`, no longer keeps the instance busy for seconds while it is
  parsed. The parsers no longer parse a nested part of a query again for
  every alternative they try, and a query that still takes too many parser
  steps fails with `query is too complex to parse`: a query gets 350 steps
  per byte plus 900 000 steps, at most `sql_parse_step_max` ([!3967]).
- A syntax error shows at most 100 characters of the query line on each
  side of the error position, not the whole line ([!3967]).
