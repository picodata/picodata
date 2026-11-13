## fix/sql

- The time to plan a query no longer grows exponentially with the depth of grouped derived
  tables (`GROUP BY ... HAVING` inside `FROM (...)`) in a subquery of a `GROUP BY` expression.
  30 levels took more than 10 minutes and blocked the thread of the instance, now they take
  milliseconds.
