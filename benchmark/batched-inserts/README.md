## Benchmarking Batched Inserts with `pgbench`

### 1. Create the test table and user

```sql
CREATE TABLE t (a INT PRIMARY KEY, b DATETIME, c TEXT);

CREATE USER postgres WITH PASSWORD 'Passw0rd';
GRANT WRITE TABLE TO postgres;
```

### 2. Run the benchmark

Ensure:

- `batched_insert.sql` is located in the current working directory
- Pgproto is running and accessible at 127.0.0.1:4327
- Release build is being used

```bash
pgbench \
    "postgres://postgres:Passw0rd@127.0.0.1:4327?sslmode=disable" \
    --file batched_insert.sql \
    --scale 1 \
    --time 30 \
    --client 20 \
    --protocol prepared \
    --jobs 1 \
    --progress 1 \
    --no-vacuum \
    --define "filler='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'" \
    --define "date=2025-11-27T12:21:32.750190+0300"
```

Notes:

- String parameters (filler, date) are used to simulate workloads from
  some of our clients that perform inserts with complex schema containing
  many strings and timestamps.

- The benchmark simulates batched inserts similar to how
  Apache Spark performs batch writes through the JDBC driver.

- String parameters can only be passed through the command line using --define;
  they cannot be defined inside the SQL script file.

- Feel free to vary filler length to send and encode more or less data.
