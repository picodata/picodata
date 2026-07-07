# Block Benchmarks

## Overview

This benchmark suite contains a set of pgbench scripts designed to evaluate block query performance under different scenarios.

Scripts that use block queries include the letter b in their filename.
Most block scripts have a corresponding q script where the same queries are executed individually.
This allows comparison between block execution and standard query execution.

## Running the Benchmark

1. **Initialize the database** 
   Run the initialization steps from the TPC-B benchmark (create the user, tables, and populate them with data).

   To run the IOCDU block benchmark script (`b4.sql`), also create
   and seed a single-row conflict counter table:

```sql
CREATE TABLE pgbench_block_conflict_counter (
    id INT NOT NULL,
    counter INT NOT NULL,
    PRIMARY KEY (bucket_id, id)
) USING MEMTX
DISTRIBUTED BY (id);

INSERT INTO pgbench_block_conflict_counter VALUES (1, 0);
```

2. **Run the benchmark**  
   Execute pgbench with the `--file` option pointing to the script you want to test.

```bash
pgbench \
  "postgres://postgres:Passw0rd@127.0.0.1:4327?sslmode=disable" \
  --file b1.sql \
  --scale 10 \
  --time 30 \
  --client 200 \
  --protocol prepared \
  --jobs 1 \
  --progress 1 \
  --no-vacuum
```
