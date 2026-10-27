# Catalog Size Benchmark

A pgbench script `select.sql` with a point read from one table while the
catalog holds many other tables. The query reads only `t0`, so its speed must
not depend on the number of tables in `_pico_table`.

The query sets `read_preference = any`: with any value other than `leader` the
router checks whether the plan reads unlogged tables, and that check used to
scan every row of `_pico_table`.

## Init

First create the benchmark user (from the picodata admin console):

```sql
CREATE USER postgres WITH PASSWORD 'Passw0rd';
GRANT CREATE TABLE TO postgres;
```

Then create the tables (`--tables` is the total number of tables, `t0`
included):

`uv run python init.py "postgres://postgres:Passw0rd@127.0.0.1:4327?sslmode=disable" --tables 100`

- `t0` — one row, the table the query reads.
- `filler_N` — empty tables that only grow the catalog.

Existing tables are kept, so run `init.py` again with a larger `--tables` to
grow the catalog of the same instance. Each `CREATE TABLE` takes about 0.3 s
on a single instance, so 1000 tables take about 5 minutes.

## Running

```bash
pgbench "postgres://postgres:Passw0rd@127.0.0.1:4327?sslmode=disable" \
  --file select.sql --time 10 --client 1 \
  --protocol prepared --jobs 1 --no-vacuum
```

Compare the TPS at 100 and 1000 tables.
