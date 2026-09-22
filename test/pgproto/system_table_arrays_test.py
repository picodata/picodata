from decimal import Decimal

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


def test_wire_vinyl_index_opts(postgres: Postgres, binary):
    # `bloom_fpr` is stored as a msgpack DECIMAL extension nested in the
    # `array(any)` column `_pico_index.opts`. It has no json counterpart, so it
    # must come out as a string rather than the raw `[tag, [bytes]]` form.
    conn = _setup(postgres)
    i1 = postgres.instance
    i1.sql(
        "CREATE TABLE t_vinyl (a INT PRIMARY KEY, b INT) USING vinyl "
        "WITH (bloom_fpr = 0.001, run_size_ratio = 3.5, page_size = 4096) "
        "DISTRIBUTED BY (a)"
    )
    i1.sql("CREATE INDEX i_vinyl ON t_vinyl (b) WITH (bloom_fpr = 0.023)")

    index_where = "table_id = (SELECT id FROM _pico_table WHERE name = 't_vinyl')"

    # Note: the primary index is named after the table id, so select it by id.
    assert _fetchall(conn, f"SELECT opts FROM _pico_index WHERE {index_where} AND id = 0", binary) == [
        (
            [
                {"unique": True},
                {"bloom_fpr": "0.001"},
                {"page_size": 4096},
                {"run_size_ratio": "3.5"},
            ],
        )
    ]
    assert _fetchall(conn, "SELECT opts FROM _pico_index WHERE name = 'i_vinyl'", binary) == [
        ([{"unique": False}, {"bloom_fpr": "0.023"}],)
    ]

    # A single element of an untyped array is of type any, which is encoded as
    # json just the same.
    assert _fetchall(conn, "SELECT opts[2]['bloom_fpr'] FROM _pico_index WHERE name = 'i_vinyl'", binary) == [
        ("0.023",)
    ]

    # An explicit cast still gives the value's own type.
    assert _fetchall(
        conn, "SELECT CAST(opts[2]['bloom_fpr'] AS DECIMAL) FROM _pico_index WHERE name = 'i_vinyl'", binary
    ) == [(Decimal("0.023"),)]


def test_wire_array_param_roundtrip(postgres: Postgres, binary):
    conn = _setup(postgres)
    ph = "%b" if binary else "%t"

    assert _fetchall(conn, f"SELECT {ph}::text[]", binary, [["alpha", "beta"]]) == [(["alpha", "beta"],)]
    assert _fetchall(conn, f"SELECT {ph}::int[]", binary, [[1, 2, 3]]) == [([1, 2, 3],)]
