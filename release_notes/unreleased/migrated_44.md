## fix/sql

- Fixed the `NO_ROUTE_TO_BUCKET` error that occurred when sending a request to
  the single node of a replicaset during its restart. This error is now retried
  by the router.
