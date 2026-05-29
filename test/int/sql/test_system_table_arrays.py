import pytest
from conftest import Cluster, Instance, TarantoolError

_PLUGIN = "testplug"
_PLUGIN_VERSION = "0.1.0"
_PLUGIN_SERVICES = ["testservice_1", "testservice_2"]
_PLUGIN_WITH_MIGRATION = "testplug_w_migration"
_DEFAULT_TIER = "default"

_INDEX_WHERE = "table_id = (SELECT id FROM _pico_table WHERE name = 't1')"
_SERVICE_WHERE = f"plugin_name = '{_PLUGIN}' AND name = 'testservice_1'"

_ARRAY_WRITE_ERROR = r"unexpected expression of type array\(any\), explicit cast to the typed array is required"


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
        ("SELECT current_state::text[] FROM _pico_instance", r"Failed to cast 1 to string"),
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


def test_write_errors(cluster: Cluster):
    i1 = _setup(cluster)

    columns = "name, enabled, services, version, description, migration_list"
    inserts = [
        f"INSERT INTO _pico_plugin ({columns}) VALUES ('p', false, ARRAY['x'], '0.1.0', 'd', ARRAY[])",
        f"INSERT INTO _pico_plugin ({columns}) VALUES ('p', false, ARRAY['x']::text[], '0.1.0', 'd', ARRAY[]::text[])",
    ]
    for query in inserts:
        with pytest.raises(TarantoolError, match=_ARRAY_WRITE_ERROR):
            i1.sql(query)

    updates = [
        "UPDATE _pico_table SET format = ARRAY[] WHERE name = 't1'",
        "UPDATE _pico_table SET format = ARRAY[]::int[] WHERE name = 't1'",
        f"UPDATE _pico_index SET parts = ARRAY[]::text[] WHERE {_INDEX_WHERE}",
        "UPDATE _pico_user SET auth = ARRAY['x', 'y']::text[] WHERE name = 'postgres'",
        "UPDATE _pico_routine SET params = ARRAY[]::text[] WHERE name = 'proc'",
        f"UPDATE _pico_plugin SET services = ARRAY['x']::text[] WHERE name = '{_PLUGIN}'",
        f"UPDATE _pico_service SET tiers = ARRAY['x']::text[] WHERE {_SERVICE_WHERE}",
        "UPDATE _pico_instance SET current_state = ARRAY['Online', 1]::text[] WHERE raft_id = 1",
    ]
    for query in updates:
        with pytest.raises(TarantoolError, match=_ARRAY_WRITE_ERROR):
            i1.sql(query)


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
