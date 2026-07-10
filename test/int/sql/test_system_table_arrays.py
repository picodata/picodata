from decimal import Decimal
from uuid import UUID

import pytest
from conftest import Cluster, Instance, TarantoolError
from tarantool import Datetime  # type: ignore[attr-defined]

_PLUGIN = "testplug"
_PLUGIN_VERSION = "0.1.0"
_PLUGIN_SERVICES = ["testservice_1", "testservice_2"]
_PLUGIN_WITH_MIGRATION = "testplug_w_migration"
_DEFAULT_TIER = "default"

_INDEX_WHERE = "table_id = (SELECT id FROM _pico_table WHERE name = 't1')"
_SERVICE_WHERE = f"plugin_name = '{_PLUGIN}' AND name = 'testservice_1'"


def _setup(cluster: Cluster) -> Instance:
    (i1,) = cluster.deploy(instance_count=1)
    i1.sql("CREATE TABLE t1 (a INT PRIMARY KEY, b TEXT) DISTRIBUTED GLOBALLY")
    i1.sql("CREATE USER postgres WITH PASSWORD 'Passw0rd' USING chap-sha1")
    i1.sql("CREATE PROCEDURE proc(int, text) AS $$INSERT INTO t1 VALUES($1::int, $2::text)$$")
    i1.sql(f"CREATE PLUGIN {_PLUGIN} {_PLUGIN_VERSION}")
    i1.sql(f"CREATE PLUGIN {_PLUGIN_WITH_MIGRATION} {_PLUGIN_VERSION}")
    i1.sql(f"ALTER PLUGIN {_PLUGIN} {_PLUGIN_VERSION} ADD SERVICE testservice_1 TO TIER {_DEFAULT_TIER}")
    return i1


def test_dql_each_array_column(cluster: Cluster):
    i1 = _setup(cluster)

    [[fmt]] = i1.sql("SELECT format FROM _pico_table WHERE name = 't1'")
    assert fmt == [
        {"name": "a", "field_type": "integer", "is_nullable": False},
        {"name": "b", "field_type": "string", "is_nullable": True},
    ]
    [[opts]] = i1.sql("SELECT opts FROM _pico_table WHERE name = 't1'")
    assert opts == []

    [[parts]] = i1.sql(f"SELECT parts FROM _pico_index WHERE {_INDEX_WHERE}")
    assert parts == [["a", "integer", None, False, None]]
    [[index_opts]] = i1.sql(f"SELECT opts FROM _pico_index WHERE {_INDEX_WHERE}")
    assert index_opts == [{"unique": True}]

    [[current_state, target_state]] = i1.sql("SELECT current_state, target_state FROM _pico_instance")
    assert current_state[0] == "Online" and isinstance(current_state[1], int)
    assert target_state[0] == "Online" and isinstance(target_state[1], int)

    [[auth]] = i1.sql("SELECT auth FROM _pico_user WHERE name = 'postgres'")
    assert len(auth) == 2 and auth[0] == "chap-sha1"
    assert isinstance(auth[1], str) and auth[1]  # hash is non-deterministic

    [[params]] = i1.sql("SELECT params FROM _pico_routine WHERE name = 'proc'")
    assert [p[1] for p in params] == ["integer", "string"]
    [[returns]] = i1.sql("SELECT returns FROM _pico_routine WHERE name = 'proc'")
    assert returns == []

    [[services]] = i1.sql("SELECT services FROM _pico_plugin WHERE name = ?", _PLUGIN)
    assert services == _PLUGIN_SERVICES
    [[migration_list]] = i1.sql("SELECT migration_list FROM _pico_plugin WHERE name = ?", _PLUGIN)
    assert migration_list == []
    [[migrations]] = i1.sql("SELECT migration_list FROM _pico_plugin WHERE name = ?", _PLUGIN_WITH_MIGRATION)
    assert migrations == ["author.db", "book.db"]

    [[tiers]] = i1.sql(f"SELECT tiers FROM _pico_service WHERE {_SERVICE_WHERE}")
    assert tiers == [_DEFAULT_TIER]


def test_msgpack_extensions_over_iproto(cluster: Cluster):
    """Over iproto msgpack extension values nested in an `any` column keep their
    own type - unlike pgproto, which has to render them as json strings.
    See `test/pgproto/types_test.py::test_map_with_msgpack_extensions`."""
    i1 = _setup(cluster)

    i1.sql(
        "CREATE TABLE t_vinyl (a INT PRIMARY KEY, b INT) USING vinyl "
        "WITH (bloom_fpr = 0.001, run_size_ratio = 3.5) DISTRIBUTED BY (a)"
    )
    i1.sql("CREATE INDEX i_vinyl ON t_vinyl (b) WITH (bloom_fpr = 0.023)")

    [[opts]] = i1.sql("SELECT opts FROM _pico_index WHERE name = 'i_vinyl'")
    assert opts == [{"unique": False}, {"bloom_fpr": Decimal("0.023")}]

    [[bloom_fpr]] = i1.sql("SELECT opts[2]['bloom_fpr'] FROM _pico_index WHERE name = 'i_vinyl'")
    assert bloom_fpr == Decimal("0.023")

    # Uuids & datetimes never land in an `any` column on their own, so put them
    # into a map column via lua. The SQL `JSON` type is native json now, which
    # cannot hold them, and SQL has no `map` type, so the table is created
    # through the internal DDL path.
    i1.propose_create_space(
        dict(
            id=902,
            name="t_json",
            format=[
                dict(name="id", type="integer", is_nullable=False),
                dict(name="j", type="map", is_nullable=True),
            ],
            primary_key=[dict(field="id")],
            distribution=dict(kind="global"),
            engine="memtx",
            owner=0,
        ),
    )
    i1.eval(
        """
        box.space.t_json:replace{1, {
            dec = require('decimal').new('0.001'),
            uid = require('uuid').fromstr('00112233-4455-6677-8899-aabbccddeeff'),
            dt = require('datetime').new{
                year=2024, month=1, day=2, hour=3, min=4, sec=5, tzoffset=0
            },
        }}
        """
    )

    [[j]] = i1.sql("SELECT j FROM t_json")
    assert j["dec"] == Decimal("0.001")
    assert j["uid"] == UUID("00112233-4455-6677-8899-aabbccddeeff")
    assert j["dt"] == Datetime(year=2024, month=1, day=2, hour=3, minute=4, sec=5)


def test_element_access(cluster: Cluster):
    i1 = _setup(cluster)

    cases = [
        ("SELECT format[1]['name'] FROM _pico_table WHERE name = 't1'", "a"),
        ("SELECT format[1]['field_type'] FROM _pico_table WHERE name = 't1'", "integer"),
        ("SELECT format[1]['is_nullable'] FROM _pico_table WHERE name = 't1'", False),
        ("SELECT format[2]['is_nullable'] FROM _pico_table WHERE name = 't1'", True),
        (f"SELECT parts[1][1] FROM _pico_index WHERE {_INDEX_WHERE}", "a"),
        (f"SELECT parts[1][2] FROM _pico_index WHERE {_INDEX_WHERE}", "integer"),
        (f"SELECT opts[1]['unique'] FROM _pico_index WHERE {_INDEX_WHERE}", True),
        ("SELECT current_state[1] FROM _pico_instance", "Online"),
        ("SELECT target_state[1] FROM _pico_instance", "Online"),
        ("SELECT auth[1] FROM _pico_user WHERE name = 'postgres'", "chap-sha1"),
        ("SELECT params[1][2] FROM _pico_routine WHERE name = 'proc'", "integer"),
        ("SELECT params[2][2] FROM _pico_routine WHERE name = 'proc'", "string"),
        (f"SELECT services[1] FROM _pico_plugin WHERE name = '{_PLUGIN}'", "testservice_1"),
        (f"SELECT services[2] FROM _pico_plugin WHERE name = '{_PLUGIN}'", "testservice_2"),
        (f"SELECT migration_list[1] FROM _pico_plugin WHERE name = '{_PLUGIN_WITH_MIGRATION}'", "author.db"),
        (f"SELECT tiers[1] FROM _pico_service WHERE {_SERVICE_WHERE}", "default"),
    ]
    for query, expected in cases:
        [[value]] = i1.sql(query)
        assert value == expected, f"{query!r} -> {value!r}, expected {expected!r}"

    [[generation]] = i1.sql("SELECT current_state[2] FROM _pico_instance")
    assert isinstance(generation, int)


def test_out_of_bounds_and_missing_key(cluster: Cluster):
    i1 = _setup(cluster)

    cases = [
        "SELECT opts[1] FROM _pico_table WHERE name = 't1'",
        "SELECT current_state[0] FROM _pico_instance",  # arrays are 1-based
        f"SELECT services[9223372036854775807] FROM _pico_plugin WHERE name = '{_PLUGIN}'",
        f"SELECT parts[1][2][1] FROM _pico_index WHERE {_INDEX_WHERE}",
        "SELECT format[1]['name']['x'] FROM _pico_table WHERE name = 't1'",
        "SELECT format[1]['nonexistent'] FROM _pico_table WHERE name = 't1'",
    ]
    for query in cases:
        [[value]] = i1.sql(query)
        assert value is None, f"{query!r} -> {value!r}, expected None"


def test_whole_array_cast(cluster: Cluster):
    i1 = _setup(cluster)

    [[services]] = i1.sql(f"SELECT services::text[] FROM _pico_plugin WHERE name = '{_PLUGIN}'")
    assert services == _PLUGIN_SERVICES
    [[migration_list]] = i1.sql(
        f"SELECT migration_list::text[] FROM _pico_plugin WHERE name = '{_PLUGIN_WITH_MIGRATION}'"
    )
    assert migration_list == ["author.db", "book.db"]
    [[tiers]] = i1.sql(f"SELECT tiers::text[] FROM _pico_service WHERE {_SERVICE_WHERE}")
    assert tiers == [_DEFAULT_TIER]

    [[auth]] = i1.sql("SELECT auth::text[] FROM _pico_user WHERE name = 'postgres'")
    assert auth[0] == "chap-sha1"
    assert isinstance(auth[1], str) and auth[1]

    [[opts]] = i1.sql("SELECT opts::text[] FROM _pico_table WHERE name = 't1'")
    assert opts == []
    [[returns]] = i1.sql("SELECT returns::text[] FROM _pico_routine WHERE name = 'proc'")
    assert returns == []

    [[first]] = i1.sql(f"SELECT services::text[][1] FROM _pico_plugin WHERE name = '{_PLUGIN}'")
    assert first == "testservice_1"
    [[second]] = i1.sql(f"SELECT (services::text[])[2] FROM _pico_plugin WHERE name = '{_PLUGIN}'")
    assert second == "testservice_2"


def test_whole_array_cast_errors(cluster: Cluster):
    i1 = _setup(cluster)

    cases = [
        (
            f"SELECT services::int[] FROM _pico_plugin WHERE name = '{_PLUGIN}'",
            r"Failed to cast 'testservice_1' to int",
        ),
        ("SELECT current_state::text[] FROM _pico_instance", r"Failed to cast 1 to text"),
        ("SELECT current_state::int[] FROM _pico_instance", r"Failed to cast 'Online' to int"),
        ("SELECT format::text[] FROM _pico_table WHERE name = 't1'", r"msgpack decode error"),
        (f"SELECT parts::text[] FROM _pico_index WHERE {_INDEX_WHERE}", r"Failed to cast"),
    ]
    for query, error in cases:
        with pytest.raises(TarantoolError, match=error):
            i1.sql(query)


def test_element_cast_errors(cluster: Cluster):
    i1 = _setup(cluster)

    with pytest.raises(TarantoolError, match=r"can not convert any\('a'\) to integer"):
        i1.sql("SELECT format[1]['name']::int FROM _pico_table WHERE name = 't1'")


def test_any_element_operator_restrictions(cluster: Cluster):
    i1 = _setup(cluster)

    with pytest.raises(TarantoolError, match=r"could not resolve operator overload for \+\(any, int\)"):
        i1.sql("SELECT current_state[1] + 1 FROM _pico_instance")

    with pytest.raises(TarantoolError, match=r"could not resolve operator overload for \|\|\(any, text\)"):
        i1.sql("SELECT format[1]['name'] || '!' FROM _pico_table WHERE name = 't1'")

    [[greeting]] = i1.sql(f"SELECT services[1]::text || '!' FROM _pico_plugin WHERE name = '{_PLUGIN}'")
    assert greeting == "testservice_1!"


def test_write_untyped_array(cluster: Cluster):
    i1 = _setup(cluster)

    # A column of type `any[]` takes a value of every type: INSERT and
    # UPDATE assign to it outside the type system (`can_assign`), so an
    # untyped array literal needs no explicit cast. Before, every array
    # write into such a column failed with `unexpected expression of
    # type array(any)`, see the release note of any-type-in-migrations.
    columns = "name, enabled, services, version, description, migration_list, webui"
    assert (
        i1.sql(
            f"INSERT INTO _pico_plugin ({columns}) VALUES ('p', false, ARRAY['x'], '0.1.0', 'd', ARRAY[], ARRAY[])",
            sudo=True,
        )["row_count"]
        == 1
    )
    assert i1.sql("SELECT services, migration_list, webui FROM _pico_plugin WHERE name = 'p'", sudo=True) == [
        [["x"], [], []]
    ]
    assert i1.sql("DELETE FROM _pico_plugin WHERE name = 'p'", sudo=True)["row_count"] == 1

    # An UPDATE writes the same way, with the cast or without it.
    assert (
        i1.sql(
            f"UPDATE _pico_plugin SET services = ARRAY['{_PLUGIN_SERVICES[0]}'] WHERE name = '{_PLUGIN}'",
            sudo=True,
        )["row_count"]
        == 1
    )
    assert (
        i1.sql(
            f"UPDATE _pico_plugin SET services = ARRAY['{_PLUGIN_SERVICES[0]}']::text[] WHERE name = '{_PLUGIN}'",
            sudo=True,
        )["row_count"]
        == 1
    )


def test_scalar_element_into_user_column(cluster: Cluster):
    (i1,) = cluster.deploy(instance_count=1)

    i1.sql("CREATE TABLE ttext (a INT PRIMARY KEY, b TEXT) DISTRIBUTED GLOBALLY")
    i1.sql("CREATE TABLE tint (a INT PRIMARY KEY, b INT) DISTRIBUTED GLOBALLY")

    i1.sql("INSERT INTO ttext SELECT raft_id, current_state[1] FROM _pico_instance")
    [[state]] = i1.sql("SELECT b FROM ttext")
    assert state == "Online"

    i1.sql("INSERT INTO tint (a, b) VALUES (1, (SELECT current_state[2] FROM _pico_instance LIMIT 1)::int)")
    [[generation]] = i1.sql("SELECT b FROM tint WHERE a = 1")
    assert isinstance(generation, int)

    with pytest.raises(TarantoolError, match=r"Failed to cast 'Online' to int"):
        i1.sql("INSERT INTO tint SELECT raft_id, current_state[1] FROM _pico_instance")

    with pytest.raises(
        TarantoolError, match=r"unexpected expression of type any, explicit cast to the actual type is required"
    ):
        i1.sql("INSERT INTO tint (a, b) VALUES (2, (SELECT current_state[1] FROM _pico_instance LIMIT 1))")

    with pytest.raises(TarantoolError, match=r"can not convert string\('Online'\) to integer"):
        i1.sql("INSERT INTO tint (a, b) VALUES (3, (SELECT current_state[1] FROM _pico_instance LIMIT 1)::int)")


def test_service_tiers_lifecycle(cluster: Cluster):
    (i1,) = cluster.deploy(instance_count=1)
    i1.sql(f"CREATE PLUGIN {_PLUGIN} {_PLUGIN_VERSION}")

    [[tiers_before]] = i1.sql(f"SELECT tiers FROM _pico_service WHERE {_SERVICE_WHERE}")
    assert tiers_before == []

    i1.sql(f"ALTER PLUGIN {_PLUGIN} {_PLUGIN_VERSION} ADD SERVICE testservice_1 TO TIER {_DEFAULT_TIER}")
    [[tiers_after]] = i1.sql(f"SELECT tiers FROM _pico_service WHERE {_SERVICE_WHERE}")
    assert tiers_after == [_DEFAULT_TIER]


def test_write_untyped_array_needs_admin(cluster: Cluster):
    # `array(any)` columns of the catalog take an array of any content, so
    # only a superuser may write them: table privileges are not sufficient.
    i1 = _setup(cluster)
    i1.sql("CREATE USER alice WITH PASSWORD 'T0psecret' USING chap-sha1", sudo=True)
    i1.sql("GRANT WRITE TABLE TO alice", sudo=True)
    i1.sql("GRANT READ TABLE TO alice", sudo=True)

    for table, query in [
        ("_pico_table", "UPDATE _pico_table SET format = ARRAY[] WHERE name = 't1'"),
        ("_pico_plugin", f"UPDATE _pico_plugin SET services = ARRAY['x'] WHERE name = '{_PLUGIN}'"),
        ("_pico_instance", "UPDATE _pico_instance SET current_state = ARRAY['Online'] WHERE raft_id = 1"),
    ]:
        with pytest.raises(TarantoolError, match=f"Write access to table '{table}' is denied for all users"):
            i1.sql(query, user="alice", password="T0psecret")

    # The tables that DDL owns refuse direct DML from a superuser also.
    with pytest.raises(TarantoolError, match="table _pico_table cannot be modified by DML Raft Operation directly"):
        i1.sql("UPDATE _pico_table SET format = ARRAY[] WHERE name = 't1'", sudo=True)
