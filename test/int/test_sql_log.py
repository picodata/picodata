import os

import pg8000.dbapi as pg  # type: ignore
import pytest
from conftest import (
    Instance,
    log_crawler,
)


@pytest.mark.parametrize(
    "protocol_type",
    [("pgproto"), ("iproto")],
)
def test_sql_log(instance: Instance, protocol_type: str):
    def set_sql_log(val: bool, fn):
        # Run ALTER SYSTEM twice to wait for parameter application.
        # See https://git.picodata.io/core/picodata/-/issues/2667
        fn(f"ALTER SYSTEM SET sql_log = {val};")
        fn(f"ALTER SYSTEM SET sql_log = {val};")

    instance.start()

    user = "pico_service"

    if protocol_type == "pgproto":
        password = "P@ssw0rd"
        instance.sql(f"ALTER USER \"{user}\" WITH PASSWORD '{password}'")

        os.environ["PGSSLMODE"] = "disable"
        conn = pg.Connection(user, password=password, host=instance.pg_host, port=instance.pg_port)
        conn.autocommit = True
        cur = conn.cursor()
        execute_func = cur.execute
    else:
        execute_func = instance.sql

    set_sql_log(True, execute_func)

    sql_list = [
        "CREATE TABLE test (id UNSIGNED NOT NULL PRIMARY KEY, value TEXT)",
        "SELECT * FROM test",
        "INSERT INTO test VALUES (42, 'my_value')",
        "UPDATE test SET value = 'my_value2' WHERE id = 42",
        "CREATE PROCEDURE test_proc() language sql AS $$ UPDATE test SET value = 'my_value3' WHERE id = 42 $$",
        "CALL test_proc()",
        "DELETE FROM test WHERE id = 42",
        "ALTER SYSTEM SET sql_log = false",
    ]

    for sql in sql_list:
        lc = log_crawler(instance, f"sql-log: {sql}")
        execute_func(sql)
        lc.wait_matched()

    # Verify that with sql_log=false, statements are NOT logged.
    # To avoid waiting for a negative (which requires an arbitrary timeout),
    # execute the statement, then re-enable sql_log and wait for a known
    # log entry. If the earlier statement didn't produce a log line by the
    # time the later one does, it never will.
    set_sql_log(False, execute_func)
    sql = "INSERT INTO test VALUES (43, 'my_value')"
    lc_must_not_appear = log_crawler(instance, f"sql-log: {sql}")
    execute_func(sql)

    set_sql_log(True, execute_func)
    # Execute a statement that WILL be logged, as a barrier.
    barrier_sql = "SELECT 'sql_log_disabled_barrier'"
    lc_barrier = log_crawler(instance, f"sql-log: {barrier_sql}")
    execute_func(barrier_sql)
    lc_barrier.wait_matched()
    assert not lc_must_not_appear.matched, f"sql-log should not have been emitted for: {sql}"

    # Verify that ACL statements are not logged even with sql_log=true.
    sql = "ALTER USER admin WITH PASSWORD 'P@ssw0rd'"
    lc_must_not_appear = log_crawler(instance, f"sql-log: {sql}")
    execute_func(sql)
    # Use another logged statement as a barrier.
    barrier_sql = "SELECT 'acl_not_logged_barrier'"
    lc_barrier = log_crawler(instance, f"sql-log: {barrier_sql}")
    execute_func(barrier_sql)
    lc_barrier.wait_matched()
    assert not lc_must_not_appear.matched, f"sql-log should not have been emitted for ACL: {sql}"
