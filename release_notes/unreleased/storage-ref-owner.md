## fix/sql

- A distributed DML query, or a query that reads one replicaset, that fails
  or times out on the router no longer releases the storage reference of a
  query part still executing on another storage. Before, bucket rebalancing
  could start on that storage while the query part was still writing its data.
  A query that reads several replicasets at once still has this problem.
