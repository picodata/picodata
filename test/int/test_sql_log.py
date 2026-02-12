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

    execute_func("ALTER SYSTEM SET sql_log = true;")

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

    # logging is disabled
    sql = "INSERT INTO test VALUES (43, 'my_value')"
    lc = log_crawler(instance, f"sql-log: {sql}")
    execute_func(sql)
    with pytest.raises(AssertionError):
        lc.wait_matched(timeout=2)

    execute_func("ALTER SYSTEM SET sql_log = true;")
    # do not log ACL
    sql = "ALTER USER admin WITH PASSWORD 'P@ssw0rd'"
    lc = log_crawler(instance, f"sql-log: {sql}")
    execute_func(sql)
    with pytest.raises(AssertionError):
        lc.wait_matched(timeout=2)
