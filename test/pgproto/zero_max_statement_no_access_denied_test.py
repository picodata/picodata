import re

import pytest
import pg8000.native as pg  # type: ignore
from conftest import Postgres
from pg8000.exceptions import DatabaseError  # type: ignore


# a test case for https://git.picodata.io/core/picodata/-/issues/1829
def test_zero_max_statement_no_access_denied(postgres: Postgres):
    user = "Rem"
    password = "P@ssw0rd"

    postgres.instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    conn = pg.Connection(user, password=password, host=postgres.host, port=postgres.port)

    conn.run("SELECT 1")  # this runs fine
    postgres.instance.sql("ALTER SYSTEM SET pg_statement_max = 0", sudo=True)
    # the error should be related to statement storage being full, not the read access to `_pico_db_config` being denied
    with pytest.raises(
        DatabaseError,
        match=re.escape(
            r'Statement storage is full. Current size limit: 0. Please, increase storage limit using: ALTER SYSTEM SET "pg_statement_max" TO <new-limit>'
        ),
    ):
        conn.run("SELECT 1")
