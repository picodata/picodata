## feat/sql

- Added the `DISCARD CACHE` statement, which flushes node-local SQL caches on
  the instance executing it. The call is not broadcast cluster-wide, requires
  superuser privileges and returns the number of evicted entries:

  ```sql
  DISCARD CACHE SQL_STMT;   -- storage statement cache
  DISCARD CACHE SQL_ROUTER; -- router logical plan cache
  DISCARD CACHE SQL_BLOCK;  -- anonymous block pattern cache
  DISCARD CACHE;            -- all of the above
  ```
