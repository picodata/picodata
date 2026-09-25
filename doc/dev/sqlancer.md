## Fuzzing SQL with SQLancer

[SQLancer](https://github.com/sqlancer/sqlancer) generates random schemas, data and queries and
checks what the DBMS answers: crashes and unexpected errors, which picodata survives today, and
wrong results, which it does not. Our driver lives in a fork,
[core/sqlancer](https://git.picodata.io/core/sqlancer), on branch `picodata`; `docs/Picodata.md`
there documents the driver itself.

`tools/sqlancer.py` is the entry point on this side: it deploys a single instance with the pytest
framework (`test/conftest.py`), creates the user the driver expects, builds and runs the fuzzer,
and collects everything a finding needs into one directory.

### Requirements

- the pytest virtualenv (`uv sync`, see [CONTRIBUTING.md](../../CONTRIBUTING.md)) — the tool
  imports `Cluster` from `test/conftest.py`;
- a JDK and Maven (the fork builds with `source`/`target` 11);
- a picodata binary — `target/release/picodata` by default, built with `make build-release` if it
  is missing; `--target debug` and `--target fast-release` pick another profile, `--picodata` one
  built elsewhere;
- ssh access to `core/sqlancer`, unless you point `--src` at a checkout you already have.

### Running

```shell
# clone core/sqlancer@picodata, build it, fuzz for five minutes
uv run tools/sqlancer.py

# fuzz a local checkout as it stands, uncommitted changes included
uv run tools/sqlancer.py --src ~/p/sqlancer --duration 60

# the other oracles, several at once
uv run tools/sqlancer.py --oracle "FUZZER EXPLAIN"
```

Options for SQLancer itself go in `--sqlancer-args`, options for the picodata driver in
`--driver-args`. They cannot share one passthrough, because SQLancer's CLI takes the driver's
options after the `picodata` command word and its own before it:

```shell
uv run tools/sqlancer.py --sqlancer-args "--num-queries 200" --driver-args "--test-global-tables false"
```

`--help` lists the rest. Every option also reads an environment variable (`SQLANCER_DURATION`,
`SQLANCER_ORACLE`, `SQLANCER_REF`, `SQLANCER_ERRORS`, ...), which is how CI will pass them.

The console shows the fuzzer and nothing else. The instance's log and audit events go to the
artifact tree, and the test framework's own deploy steps are silenced; `--verbose` puts all three
back on the console, which is what you want when the instance itself misbehaves — it prints
hundreds of thousands of lines a minute, so pipe it somewhere.

A run fuzzes with one worker, which is what makes it stop at the first finding with the instance
still in the state that produced it, and leaves a single `-cur.log` whose last statement is the
culprit of a crash.

### Oracles

| Oracle | Finds |
| --- | --- |
| `FUZZER` (default) | Errors and crashes over the whole supported SELECT surface |
| `EXPLAIN` | The same queries behind `EXPLAIN`, exercising the planner and the plan printer |
| `WHERE`, `NOREC` | Wrong results, by partitioning a query on a predicate |

`FUZZER` and `EXPLAIN` are expected to pass: a finding from either is news. `WHERE` and `NOREC`
are not, they stop within seconds on wrong results we already know about, so run them by hand and
read the finding rather than the verdict.

### Reading the result

A run ends with a verdict, the numbers behind it, and the files it wrote:

```
PASSED: no findings in 61s
  queries:       44729 (738/s)
  success rate:  93% of statements
```

`success rate` is the share of statements the instance accepted, taken from SQLancer's last
progress line. It matters as much as the verdict: a rate that drops towards zero means the
generator is emitting SQL picodata rejects, so the run passes while testing almost nothing. Above
90% is normal — the remainder is generated queries that legitimately fail, such as an overflow or a
type mismatch. A run shorter than five seconds reports no numbers, because that is SQLancer's
progress interval.

A failure names what happened instead:

```
FAILED: SQLancer reported a finding after 2s
FAILED: picodata died after 47s
BROKEN: SQLancer itself failed after 0s; this is not a picodata finding
```

`BROKEN` means the fuzzer never tested anything — a bad option, a missing class, an OOM — and it
exits 2 rather than 1, so CI can tell a broken job from a bug.

### Exit codes

| Code | Meaning |
| --- | --- |
| 0 | Nothing found |
| 1 | A finding, a crash or a hang |
| 2 | The run never tested anything: no binary, the build failed, the instance did not come up, SQLancer rejected its own arguments or died of a JVM error |

### What it leaves behind

Every run ends with a list of the files it wrote and, after a finding, the command that
replays it, so the paths below are printed rather than remembered.

```
target/sqlancer/artifacts/
  run-info.txt                     picodata version and commit, sqlancer SHA, command line, exit code
  sqlancer.out                     the fuzzer's output, including the stack trace of a finding
  logs/picodata/database0.log      the statements that built the schema, then the failing query
  logs/picodata/database0-cur.log  every statement as executed; the last one killed a crashed instance
  cluster/<port>/                  instance dir: picodata.log, *.snap, *.xlog, backtraces
```

`databaseN.log` is the repro: SQLancer writes its stack trace as `--` comments, so psql skips it
and runs the statements below it. Replay it on an instance that does not already hold the run's
tables — the log creates them but never drops them first, so on a dirty instance the CREATE TABLE
statements fail and the inserts land in the old schema.

```shell
PGPASSWORD='Passw0rd!' psql -h 127.0.0.1 -p 4327 -U sqlancer -d sqlancer \
    -f target/sqlancer/artifacts/logs/picodata/database0.log
```

A crash replays the same way and kills the instance again, so restart it between attempts. The
reducers, though, cannot shrink one: they re-run each candidate to see whether it still reproduces,
and a dead instance answers nothing.

### The error catalogue

`tools/sqlancer-errors.yaml` lists what a run may see without failing: `errors:` a statement may
fail with, anything else is a finding, and `suppress:` switches that keep a pattern out of the
generated statements, which is what a crashing bug needs. Each entry carries a repro, which
SQLancer runs at startup, warning about the ones whose error no longer appears.

**When you fix a bug, delete its entry in the same merge request.** From that commit on the error
is a finding again, so an incomplete fix fails the run. A new known bug is a new entry.
