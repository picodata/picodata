# Real-world SQL queries corpus

## Formatter

Use [sql-formatter](https://github.com/sql-formatter-org/sql-formatter). Formatter
output changes between releases, so the version is pinned in the root `Makefile`
(`SQL_FORMATTER_VERSION`); CI's image installs that same version
(`docker-build-base/Dockerfile`), and `make lint-sql` refuses to run against any
other one. `make check-sql-formatter` verifies your local install and prints the
exact `npm install` command when it is missing or mismatched.

Config is in [sql_format](./sql_format.json).

Command (or just `make fmt`):

```bash
sql-formatter -c sql_format.json --fix queries.sql
```

`make lint` (target `lint-sql`) enforces that `queries.sql` matches this
formatter's output.
