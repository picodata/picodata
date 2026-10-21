### Benchmarking the PostgreSQL protocol

These benchmarks measure the overhead of pgproto, the PostgreSQL protocol
implementation.

With `--protocol prepared`, each pgbench client keeps a named prepared
statement for each command of the script.

- `select1.sql` has one `SELECT 1;`. It measures the protocol overhead
  for one of the simplest queries.
- `selectCx500.sql` has 500 selects of a constant, from `SELECT 1;` to
  `SELECT 500;`. It measures the same overhead when the node holds many
  statements. 100 clients hold 50000 statements. Each select is as simple
  as `SELECT 1`. All clients together can hold at most `pg_statement_max`
  statements (100000 by default). For more clients, increase the limit, for
  example `ALTER SYSTEM SET pg_statement_max = 200000;`.
- `selectCx500-pipeline.sql` has the same 500 selects in one pipeline.
  pgbench sends all of them and then one Sync, so the client does not wait
  for a reply after each query, and the server flushes less often.
  Pipelines need `--protocol extended` or `--protocol prepared`.

#### Running

Start a release build and set the admin password:

```bash
cargo build --release
./target/release/picodata run -i
```

```sql
ALTER USER "admin" WITH PASSWORD 'Passw0rd';
```

Run each script with the same options:

```bash
pgbench \
    "postgres://admin:Passw0rd@127.0.0.1:4327?sslmode=disable" \
    --file benchmark/pgproto/selectCx500.sql \
    --protocol prepared \
    --client 100 \
    --jobs 4 \
    --time 30 \
    --no-vacuum
```

To compare the scripts, use statements per second: tps multiplied by the
number of selects in the script (1 or 500).
