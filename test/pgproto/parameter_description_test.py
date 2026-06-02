import asyncio

import asyncpg  # type: ignore
from conftest import Postgres

# Regression test for https://git.picodata.io/core/picodata/-/issues/2943
#
# pgwire < 0.38.2 encoded the `ParameterDescription` backend message under a
# 30_000 byte limit (`SMALL_BACKEND_PACKET_SIZE_LIMIT`). Preparing a statement
# (Parse + Describe) with many parameters produced a `ParameterDescription`
# larger than that limit, so the server failed to encode it and dropped the
# connection with:
#
#   IO error: Invalid message length, expected max 30006, actual: 30000
#   connection closed
#
# The `ParameterDescription` size is `4 + 2 + N_params * 4` bytes, so the limit
# is hit once `N_params > 7498`. With 25 columns and 300 rows we get exactly
# 7500 params => 30006 bytes, which used to fail. asyncpg always prepares
# statements, so it triggers the Describe that produces `ParameterDescription`.

TABLE = "repro_params"

# 25 columns x 300 rows = 7500 params => ParameterDescription is 30006 bytes,
# which exceeded the old 30_000 limit. 299 rows stays just under it.
N_COLS = 25
BATCH_ERROR = 300
BATCH_OK = 299

COL_NAMES = ["id"] + [f"field_{i:02d}" for i in range(1, N_COLS)]


def _pd_size(n_params: int) -> int:
    return 4 + 2 + n_params * 4


def make_insert(n_rows: int) -> tuple[str, list[int]]:
    ncols = len(COL_NAMES)
    rows_ph = ",\n    ".join(
        "(" + ", ".join(f"${i * ncols + j + 1}" for j in range(ncols)) + ")" for i in range(n_rows)
    )
    sql = f"INSERT INTO {TABLE} ({', '.join(COL_NAMES)})\nVALUES\n    {rows_ph}"
    # use unique values so id-columns stay unique between batches
    values = list(range(1, n_rows * ncols + 1))
    return sql, values


async def _run(host: str, port: int, user: str, password: str) -> None:
    conn = await asyncpg.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        ssl=False,
    )
    try:
        create_sql = (
            f"CREATE TABLE {TABLE} (\n"
            + "".join(f"    {c} INTEGER NOT NULL,\n" for c in COL_NAMES)
            + "    PRIMARY KEY (id)\n"
            ") USING MEMTX DISTRIBUTED BY (id)"
        )
        await conn.execute(create_sql)

        # Sanity baseline: just under the old limit, always worked.
        assert _pd_size(N_COLS * BATCH_OK) < 30_000
        sql, values = make_insert(BATCH_OK)
        await conn.execute(sql, *values)
        await conn.execute(f"TRUNCATE TABLE {TABLE}")

        # Regression: over the old 30_000 limit. Fails on pgwire 0.37.3 with a
        # dropped connection, succeeds after upgrading to pgwire >= 0.38.2.
        assert _pd_size(N_COLS * BATCH_ERROR) > 30_000
        sql, values = make_insert(BATCH_ERROR)
        await conn.execute(sql, *values)

        count = await conn.fetchval(f"SELECT COUNT(*) FROM {TABLE}")
        assert count == BATCH_ERROR
    finally:
        await conn.close()


def test_large_parameter_description(postgres: Postgres):
    user = "postgres"
    password = "P@ssw0rd"

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}' USING md5")
    postgres.instance.sql(f'GRANT CREATE TABLE TO "{user}"', sudo=True)

    asyncio.run(_run(postgres.host, postgres.port, user, password))
