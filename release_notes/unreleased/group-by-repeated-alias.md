## fix/sql

- A `GROUP BY` that names one select-list alias more than once
  (`GROUP BY c1, c1`, `GROUP BY x + x`) or uses an alias in `BETWEEN`
  (`GROUP BY x BETWEEN 0 AND 2`) no longer crashes the instance.
