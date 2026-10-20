## perf/sql

- A subquery used both in the select list and in `GROUP BY` now moves its data
  between nodes once, not twice, when the grouping runs on a single node: over
  a global table, with window functions, or with a filter on the sharding key.
