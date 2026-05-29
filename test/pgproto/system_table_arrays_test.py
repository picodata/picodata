import psycopg
import pytest
from conftest import Postgres

# Regression test that verifies pgproto encoding/decoding of arrays from system columns.
# The main test is `test/int/sql/test_system_table_arrays.py`.

_PLUGIN = "testplug"
_PLUGIN_WM = "testplug_w_migration"
_PLUGIN_VERSION = "0.1.0"
_TIER = "default"
_PASSWORD = "P@ssw0rd"


@pytest.fixture(params=[False, True], ids=["text", "binary"])
def binary(request):
    return request.param


def _admin_conn(postgres: Postgres):
    postgres.instance.sql(f"ALTER USER \"admin\" WITH PASSWORD '{_PASSWORD}'")
    conn = psycopg.connect(f"user=admin password={_PASSWORD} host={postgres.host} port={postgres.port} sslmode=disable")
    conn.autocommit = True
    return conn


def _setup(postgres: Postgres):
    i1 = postgres.instance
    i1.sql("CREATE TABLE t1 (a INT PRIMARY KEY, b TEXT) DISTRIBUTED GLOBALLY")
    i1.sql("CREATE PROCEDURE proc(int, text) AS $$INSERT INTO t1 VALUES($1::int, $2::text)$$")
    i1.sql(f"CREATE PLUGIN {_PLUGIN} {_PLUGIN_VERSION}")
    i1.sql(f"CREATE PLUGIN {_PLUGIN_WM} {_PLUGIN_VERSION}")
    i1.sql(f"ALTER PLUGIN {_PLUGIN} {_PLUGIN_VERSION} ADD SERVICE testservice_1 TO TIER {_TIER}")
    return _admin_conn(postgres)


def _fetchall(conn, sql, binary, params=None):
    return conn.execute(sql, params, binary=binary).fetchall()


def test_wire_whole_text_array(postgres: Postgres, binary):
    conn = _setup(postgres)
    ph = "%b" if binary else "%t"

    assert _fetchall(conn, f"SELECT services::text[] FROM _pico_plugin WHERE name = {ph}", binary, [_PLUGIN]) == [
        (["testservice_1", "testservice_2"],)
    ]
    assert _fetchall(
        conn, f"SELECT migration_list::text[] FROM _pico_plugin WHERE name = {ph}", binary, [_PLUGIN_WM]
    ) == [(["author.db", "book.db"],)]
    assert _fetchall(
        conn,
        f"SELECT tiers::text[] FROM _pico_service WHERE plugin_name = {ph} AND name = {ph}",
        binary,
        [_PLUGIN, "testservice_1"],
    ) == [(["default"],)]

    assert _fetchall(conn, f"SELECT services::text[][1] FROM _pico_plugin WHERE name = {ph}", binary, [_PLUGIN]) == [
        ("testservice_1",)
    ]


def test_wire_empty_array(postgres: Postgres, binary):
    conn = _setup(postgres)
    ph = "%b" if binary else "%t"

    assert _fetchall(conn, f"SELECT opts::text[] FROM _pico_table WHERE name = {ph}", binary, ["t1"]) == [([],)]
    assert _fetchall(conn, f"SELECT returns::text[] FROM _pico_routine WHERE name = {ph}", binary, ["proc"]) == [([],)]


def test_wire_uncast_any_element(postgres: Postgres, binary):
    conn = _setup(postgres)
    ph = "%b" if binary else "%t"
    index_where = f"table_id = (SELECT id FROM _pico_table WHERE name = {ph})"

    assert _fetchall(conn, "SELECT current_state[1] FROM _pico_instance", binary) == [("Online",)]
    assert _fetchall(conn, f"SELECT format[1]['name'] FROM _pico_table WHERE name = {ph}", binary, ["t1"]) == [("a",)]
    assert _fetchall(conn, f"SELECT parts[1][2] FROM _pico_index WHERE {index_where}", binary, ["t1"]) == [("integer",)]

    assert _fetchall(conn, "SELECT current_state[0] FROM _pico_instance", binary) == [(None,)]
    assert _fetchall(conn, f"SELECT format[1]['nonexistent'] FROM _pico_table WHERE name = {ph}", binary, ["t1"]) == [
        (None,)
    ]


def test_wire_array_param_roundtrip(postgres: Postgres, binary):
    conn = _setup(postgres)
    ph = "%b" if binary else "%t"

    assert _fetchall(conn, f"SELECT {ph}::text[]", binary, [["alpha", "beta"]]) == [(["alpha", "beta"],)]
    assert _fetchall(conn, f"SELECT {ph}::int[]", binary, [[1, 2, 3]]) == [([1, 2, 3],)]
