## Steps how to run the benchmark

1. Install `tpchgen-cli` via `cargo install tpchgen-cli`
2. Generate data: `cd benchmark/tpch/data && tpchgen-cli --format csv`. For more options see `tpchgen-cli --help`
3. Launch picodata:
   1.  Do not forget to bump memtx.memory, 2G is enough for scale 1 (the default in tpchgen)
   2.  `ALTER USER "admin" PASSWORD 'Admin1234' USING md5;`
4. Populate tables: `poetry run python benchmark/tpch/tpch.py init "postgres://admin:Admin1234@127.0.0.1:4327"`
5. For convenience increase vdbe opcode limit: `ALTER SYSTEM SET sql_vdbe_opcode_max = 999999999;`
6. See tpch queries here: https://github.com/apache/datafusion/blob/main/benchmarks/queries/q1.sql Note that some may need adjustments due to type system and general differences in dialect.

```bash
pgbench \
    "postgres://admin:Admin1234@127.0.0.1:4327" \
    --file benchmark/tpch/q1.sql \
    --time 20 \
    --jobs 1 \
    --progress 10 \
    --no-vacuum
```