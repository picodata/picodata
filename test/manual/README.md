# Manual tests

CI does not run anything here: `test/manual` is outside `pytest.ini`'s
`testpaths`. Name the file to run it.

## Transactional block fuzz

`test_txn_block_fuzz.py` is a Jepsen-style consistency test for transactional
blocks (`DO $$ ... $$`). Client threads run blocks against every instance of a
synchronous tier at once, while an optional nemesis kills, pauses and
restarts instances underneath them. Only sharded tables are covered,
because DML on a global table inside a block is not supported.

```
CI=1 uv run pytest test/manual/test_txn_block_fuzz.py -s
```

`CI=1` reuses the binary already in `target/`; drop it to rebuild first.
`-s` shows the report as it is printed -- without it pytest captures the
output and shows it only when the run fails. Deploying the cluster and the
final checks add a few seconds on top of `FUZZ_DURATION`.

Every knob is an environment variable, listed with its default in the
module docstring. The ones worth knowing: `FUZZ_DURATION`, `FUZZ_WORKLOAD`,
`FUZZ_NEMESIS` and `FUZZ_SEED`, which is printed on every run so a finding
can be replayed.

### Workloads

`FUZZ_WORKLOAD` takes a comma separated list; the default is `bank`.

- **bank** transfers money between accounts. Every read block checks the
  invariants against the rows it saw: money is conserved, no account is
  negative, none is missing. That catches a half-applied block and nothing
  that leaves the total intact.

### Faults

`FUZZ_NEMESIS` is `none` by default. It takes `all`, or a comma separated
list of `kill`, `pause`, `restart`, `leader` and `compact`, one applied
every `FUZZ_NEMESIS_PERIOD` seconds. A long run under every fault:

```
FUZZ_NEMESIS=all FUZZ_DURATION=900 \
    CI=1 uv run pytest test/manual/test_txn_block_fuzz.py -s
```

A fault run can end as `skipped`: the faults left an instance Offline and
nothing brought it back, so there was no healthy cluster left to check. That is
a known liveness gap rather than a finding of this harness. A consistency
violation still fails the run, and without a nemesis the same state fails too.

### Reading a failure

The report ends with `valid? False` when a consistency anomaly or an
unexpected error was recorded, and each one is printed with the block that
produced it and the instance it ran against. The run directory, printed at
the start, keeps the faults the nemesis applied with their timestamps, one JSON object per line.

The workload stops at the first anomaly, so the report shows the finding
rather than the errors it would go on to cause. An anomaly is still not
automatically a database bug -- a crash can come from the machine, the OOM
killer or a full disk, and an error can be misclassified -- so place it
against the fault log and replay it with the same `FUZZ_SEED`.
