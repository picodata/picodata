## feat/sql

- Implement the `forward` option for DQL/DML queries that controls how queries
  are routed with respect to bucket ownership:
  - `on`: scatter-gather across replica set leaders;
  - `ro_to_rw`: all buckets on one node, forwarding allowed;
  - `off`: true locality, error if client is not on the correct node.
