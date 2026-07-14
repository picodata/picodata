## fix/sql

- Fixed a SQL planner panic caused by stale type metadata after clone-based
  rewrites such as `BETWEEN` normalization and `GROUP BY` alias expansion.
