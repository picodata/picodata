## feat/cli

- Added `picodata export`, which writes a logical dump of the cluster
  schema (tables and indexes) as plain SQL. The interface is modeled after
  `pg_dump`, and the dump is restored by passing it to `psql`:

  ```
  picodata export -s 'host=192.168.0.1 port=4327 user=admin' -f schema.sql
  psql 'host=192.168.0.2 port=4327 user=admin' -f schema.sql
  ```

  Only the schema can be exported for now, so `-s/--schema-only` is
  required. Connecting over TLS is not supported yet.
