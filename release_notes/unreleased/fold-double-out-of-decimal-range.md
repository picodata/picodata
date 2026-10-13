## fix/sql

- Fixed a panic in the planner when a query compares a `DOUBLE` value outside
  the `DECIMAL` range, e.g. `1.7976931348623157E308 > 0.5`.
