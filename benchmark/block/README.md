# Block Benchmarks

pgbench scripts with benchmarks for transactional blocks `bN.sql`. To compare
against the same statements run individually use `qN.sql`. A single `init.py`
sets up every benchmark.

## Benchmarks

- `b1` / `q1` — **wallet transfer**: move `amount` from a user's checking to
  savings, only if funded. Money is conserved and no account overdraws — an
  invariant the atomic block protects and individual statements can't. Both
  tables are sharded by `user_id`, so `checking[u]`/`savings[u]` co-locate on
  one bucket and the block stays single-bucket.
- `b2` / `q2` — point read of `checking`.
- `b3` / `q3` — append to `ledger`.
- `b4` — hot-row upsert on a single seeded `counter` row.

## Init

First create the benchmark user (from the picodata admin console):

```sql
CREATE USER postgres WITH PASSWORD 'Passw0rd';
GRANT CREATE TABLE TO postgres;
```

Then initialize the tables (`S` = `--scale`):

`uv run python init.py postgres://postgres:Passw0rd@127.0.0.1:4327?sslmode=disable --scale 1`

- `checking` / `savings` — `100000 * S` rows each, sharded by `user_id`. Every
  `checking` row is funded at `1000000` (well above the `[1, 100]` transfer
  amount, so the funds guard always fires); `savings` starts at `0`.
- `ledger` — starts empty; grows unbounded as the append benchmark inserts.
- `counter` — a single row (fixed), the one hot row every upsert contends on.

## Running

```bash
pgbench "postgres://postgres:Passw0rd@127.0.0.1:4327?sslmode=disable" \
  --file b1.sql --scale 1 --time 30 --progress 1 --client 200 \
  --protocol prepared --jobs 1 --no-vacuum
```
