### Benchmarking Picodata with pgbench

pgbench is a PostgreSQL benchmarking tool that can also be used with picodata,
as it supports the PostgreSQL wire protocol. However, due to picodata's SQL
limitations, you'll need to customize the initialization and queries.
The following scenario is based on TPC-B used by default in pgbench.

#### 1. **Build Picodata**

First, build picodata in release mode:

```bash
cargo build --release
```

Run picodata with the `--pg-listen` option to start the pgproto server
and `-i` for interactive mode:

```bash
./target/release/picodata --pg-listen 127.0.0.1:5433 -i
```

#### 2. **Create a Test User**

Next, create a test user for the benchmark from interactive console by running
the following SQL commands:

```sql
CREATE USER postgres WITH PASSWORD 'Passw0rd';
GRANT CREATE TABLE TO postgres;
GRANT READ TABLE TO postgres;
GRANT WRITE TABLE TO postgres;
```

This will create a user named `postgres` with necessary permissions for benchmarking.

#### 3. **Initialize the Database**

Initialize the database with using the `init.py` script. Scale is used as a multiplier for table sizes (1, 10 and 100000).

```bash
poetry run python init.py \
    "postgres://postgres:Passw0rd@localhost:5433?sslmode=disable" \
    --scale 10
```

This sets up the benchmark tables.

#### 4. **Run the Benchmark**

Once the database is initialized, run the benchmark using `pgbench` with a custom SQL script.

```bash
pgbench \
    "postgres://postgres:Passw0rd@127.0.0.1:5433?sslmode=disable" \
    --file script.sql \
    --scale 10 \
    --time 30 \
    --client 200 \
    --protocol prepared \
    --jobs 1 --progress 1 --no-vacuum
```

**Option Notes:**
- **Custom SQL Script (`--file script.sql`)**: Required to work around picodata's SQL limitations.
- **Scaling Factor (`--scale 10`)**: Use the same value as for initialization.
- **Duration (`--time 30`)**: Benchmark duration that can be increased to smooth out deviations.
- **Number of clients (`--client 200`)**: Use 200 concurrent clients. 200-500 clients are usually enough for complete utilization. Use `htop` to verify that.
- **Prepared Statements (`--protocol prepared`)**: Execute prepared statements which is our target scenario.

#### 5. **Collect Performance Info**

During the benchmark performance info can be collected for future analyzes. It can be done
using [cargo flamegraph](https://github.com/flamegraph-rs/flamegraph) tool based on `perf`.

First, start recording during the benchmark using the following command:

```bash
flamegraph --pid=$(pidof picodata) -c "record --call-graph=dwarf,65528 -F99 -g"
```

*Note that with `--call-graph=dwarf,65528` 65528 bytes will be captured from the stack frames, which helps to generate better flamegraphs.
However, fiber stacks can be larger, so truncated traces will be captured, which will result in artifacts on the generated flamegraph.
See https://git.picodata.io/picodata/picodata/picodata/-/issues/1033#note_107273 for more details.*

Once the benchmark is finished, stop the recording by pressing Ctrl + C.
After a delay, a flamegraph.svg file will be generated. This file can be
analyzed to understand the performance characteristics of picodata during
the benchmark.
