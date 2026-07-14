## fix/sql

- Fixed the `query for request_id with plan_id not found` error that occurred
  on queries with `UNION` of `CTE` on a cluster consisting of multiple instances.
