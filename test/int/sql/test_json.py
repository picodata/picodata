from typing import Iterator

import psycopg
import pytest
from psycopg.types.json import Json, Jsonb
from psycopg.types.string import TextLoader
from conftest import Cluster, TarantoolError

PG_PASSWORD = "Passw0rd"


def _pg_conn_info(instance) -> str:
    instance.sql("CREATE USER postgres WITH PASSWORD 'Passw0rd'", sudo=True)
    for priv in ("CREATE TABLE", "READ TABLE", "WRITE TABLE", "DROP TABLE"):
        instance.sql(f"GRANT {priv} TO postgres", sudo=True)
    return f"host={instance.pg_host} port={instance.pg_port} user=postgres password={PG_PASSWORD}"


def _json_values(conn, query):
    """Run `query` and return {first column: raw json text} instead of parsed
    json. Two properties are only observable in the wire text: byte-level key
    normalization (a parsed dict is order-insensitive) and a present json null,
    which the default loader collapses to None just like SQL NULL. The read is
    forced to text format so the jsonb document arrives as plain json, without the
    binary jsonb version-byte prefix, and a raw-text loader hands it back verbatim.
    The cursor is opened after registering the loader: a loader registered on the
    connection is only picked up by cursors opened afterwards."""
    conn.adapters.register_loader("jsonb", TextLoader)
    with conn.cursor() as cur:
        cur.execute(query, binary=False)
        return {row[0]: row[1] for row in cur.fetchall()}


def _create_legacy_map_table(instance, name: str = "legacy", space_id: int = 901):
    """Create a table with a `map`-typed column, mirroring a legacy `json`
    column that predates the native json type (persisted as `field_type =
    "map"`). The SQL grammar has no user-facing `map` type, so the table is
    created through the internal DDL path."""
    instance.propose_create_space(
        dict(
            id=space_id,
            name=name,
            format=[
                dict(name="id", type="integer", is_nullable=False),
                dict(name="m", type="map", is_nullable=True),
            ],
            primary_key=[dict(field="id")],
            distribution=dict(kind="global"),
            engine="memtx",
            owner=0,
        ),
    )


class TestJson:
    """Tests whose assertions the `.sql` runner cannot express: they inspect the
    raw json wire text (byte-level key order, a present json null), bind json
    through psycopg adapters, or reach a legacy map column through the internal
    DDL path. The value-level cases live in `json.sql`.

    Every test runs against a single cluster of two replicasets at replication
    factor 2, deployed once for the whole class to avoid paying the startup and
    shutdown cost per test. Tests stay independent by dropping the tables they
    create after each one."""

    @pytest.fixture(scope="class")
    def cluster(self, cluster_factory) -> Iterator[Cluster]:
        cluster = cluster_factory()
        cluster.deploy(instance_count=4, init_replication_factor=2)
        cluster.wait_until_buckets_balanced()
        yield cluster
        cluster.terminate()

    @pytest.fixture(scope="class")
    def conn_info(self, cluster: Cluster) -> str:
        return _pg_conn_info(cluster.leader())

    @pytest.fixture(autouse=True)
    def _drop_tables(self, cluster: Cluster):
        yield
        # Every test shares the table `t`; drop it between tests to keep them
        # independent. The legacy map test uses space `legacy`, which sits at a
        # reserved space id and is undroppable by design.
        cluster.leader().sql("DROP TABLE IF EXISTS t", sudo=True)

    def test_json_null_distinct_from_sql_null(self, conn_info: str):
        with psycopg.connect(conn_info, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSON)")
            cur.execute("INSERT INTO t VALUES (1, 'null'::json)")
            cur.execute("INSERT INTO t VALUES (2, NULL)")
            cur.execute("INSERT INTO t VALUES (3, NULL::json)")

            values = _json_values(conn, "SELECT id, data FROM t ORDER BY id")
            # a present json null surfaces as the literal text `null`, SQL NULL as None
            assert values[1] == "null"
            assert values[2] is None
            assert values[3] is None

    def test_json_normalization_and_duplicate_keys(self, conn_info: str):
        with psycopg.connect(conn_info, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSON)")
            cur.execute("""INSERT INTO t VALUES (1, '{"bb":1,"a":2}'::json)""")
            cur.execute("""INSERT INTO t VALUES (2, '{"a":2,"bb":1}'::json)""")
            cur.execute("""INSERT INTO t VALUES (3, '{"a":1,"a":2}'::json)""")

            values = _json_values(conn, "SELECT id, data FROM t ORDER BY id")
            # Keys are sorted, so the unsorted and canonical inputs store identically.
            assert values[1] == values[2] == '{"a": 2, "bb": 1}'
            # Duplicate key: the last occurrence wins, the first is dropped.
            assert values[3] == '{"a": 2}'

    def test_json_bound_parameter(self, conn_info: str):
        with psycopg.connect(conn_info, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSON)")
            cur.execute("""INSERT INTO t VALUES (1, '{"a":1}'::json)""")
            cur.execute("INSERT INTO t VALUES (%s, %s)", (2, Json({"a": 1})))
            cur.execute("INSERT INTO t VALUES (%s, %s)", (3, Json(None)))
            cur.execute("INSERT INTO t VALUES (%s, %s)", (4, None))
            cur.execute("INSERT INTO t VALUES (%s, %s)", (5, Jsonb({"x": 5})))
            cur.execute("INSERT INTO t VALUES (%s, %s)", (6, Json({"bb": 1, "a": 2})))
            cur.execute("""INSERT INTO t VALUES (7, '{"a":2,"bb":1}'::json)""")

            cur.execute("SELECT id, data IS NULL FROM t ORDER BY id")
            assert cur.fetchall() == [
                (1, False),
                (2, False),
                (3, False),
                (4, True),
                (5, False),
                (6, False),
                (7, False),
            ]

            values = _json_values(conn, "SELECT id, data FROM t ORDER BY id")
            assert values[1] == '{"a": 1}'
            assert values[2] == values[1]  # parameter stores identically to literal
            assert values[6] == values[7] == '{"a": 2, "bb": 1}'  # unsorted parameter normalized like a literal
            assert values[3] == "null"  # JSON null parameter stays a present null
            assert values[5] == '{"x": 5}'  # jsonb OID accepted too

    def test_json_array_parameter_rejected(self, conn_info: str):
        # There is no array of json, so a json[] or jsonb[] parameter is refused.
        with psycopg.connect(conn_info, autocommit=True) as conn, conn.cursor() as cur:
            # `_json` and `_jsonb` are PostgreSQL's internal names for the array types.
            for value, name in [([Json({"a": 1})], "_json"), ([Jsonb({"a": 1})], "_jsonb")]:
                with pytest.raises(psycopg.errors.FeatureNotSupported, match=f"{name} parameters"):
                    cur.execute("SELECT %s", (value,))

    def test_json_extract_path_on_legacy_map_column(self, cluster: Cluster):
        """Acceptance criteria 12 and 16: JSON_EXTRACT_PATH works on a legacy
        map-backed column, which keeps behaving as object-only, and a json literal
        INSERT into it fails with the natural cast-mismatch error."""
        i1 = cluster.leader()
        _create_legacy_map_table(i1)

        i1.eval("""box.space.legacy:insert{1, require('json').decode('{"a":{"b":"c"},"d":{"e":[1,2,3]},"f":42}')}""")
        i1.eval("""box.space.legacy:insert{2, require('json').decode('{"a":2}')}""")

        assert i1.sql("SELECT id, m FROM legacy ORDER BY id") == [
            [1, {"a": {"b": "c"}, "d": {"e": [1, 2, 3]}, "f": 42}],
            [2, {"a": 2}],
        ]

        assert i1.sql("SELECT id, JSON_EXTRACT_PATH(m, 'a') FROM legacy ORDER BY id") == [
            [1, {"b": "c"}],
            [2, 2],
        ]
        assert i1.sql("SELECT id, JSON_EXTRACT_PATH(m, 'a', 'b') FROM legacy ORDER BY id") == [[1, "c"], [2, None]]
        assert i1.sql("SELECT id, JSON_EXTRACT_PATH(m, 'd', 'e') FROM legacy ORDER BY id") == [
            [1, [1, 2, 3]],
            [2, None],
        ]
        assert i1.sql("SELECT id, JSON_EXTRACT_PATH(m, 'missing') FROM legacy ORDER BY id") == [[1, None], [2, None]]
        assert i1.sql("SELECT id, JSON_EXTRACT_PATH(m, 'f')::int FROM legacy ORDER BY id") == [[1, 42], [2, None]]
        assert i1.sql("SELECT JSON_EXTRACT_PATH(m, 'a') FROM legacy WHERE id = 1") == [[{"b": "c"}]]

        # A path that runs past a scalar or a missing key is a miss at any depth.
        assert i1.sql("SELECT id, JSON_EXTRACT_PATH(m, 'a', 'b', 'c') FROM legacy ORDER BY id") == [
            [1, None],
            [2, None],
        ]
        assert i1.sql("SELECT id, JSON_EXTRACT_PATH(m, 'a', 'b', 'c', 'd') FROM legacy ORDER BY id") == [
            [1, None],
            [2, None],
        ]
        assert i1.sql("SELECT id, JSON_EXTRACT_PATH(m, 'b') FROM legacy ORDER BY id") == [[1, None], [2, None]]

        # The result composes like any other expression.
        assert i1.sql("SELECT id, JSON_EXTRACT_PATH(m, 'a', 'b')::text FROM legacy ORDER BY id") == [
            [1, "c"],
            [2, None],
        ]
        assert i1.sql("SELECT id, JSON_EXTRACT_PATH(m, 'a', (SELECT 'b')) FROM legacy ORDER BY id") == [
            [1, "c"],
            [2, None],
        ]
        assert i1.sql("SELECT id, JSON_EXTRACT_PATH(m, 'd', 'e')[1] FROM legacy ORDER BY id") == [[1, 1], [2, None]]
        assert i1.sql("SELECT id FROM legacy WHERE id = 2 AND id = JSON_EXTRACT_PATH(m, 'a')::int") == [[2]]

        # A NULL source or a NULL key gives NULL.
        assert i1.sql("SELECT id, JSON_EXTRACT_PATH(m, 'd', NULL) FROM legacy ORDER BY id") == [[1, None], [2, None]]
        assert i1.sql("SELECT id, JSON_EXTRACT_PATH(m, NULL) FROM legacy ORDER BY id") == [[1, None], [2, None]]
        assert i1.sql("SELECT id, JSON_EXTRACT_PATH(NULL, NULL) FROM legacy ORDER BY id") == [[1, None], [2, None]]

        with pytest.raises(TarantoolError, match="json_extract_path expects at least two arguments"):
            i1.sql("SELECT JSON_EXTRACT_PATH(m) FROM legacy")
        with pytest.raises(TarantoolError, match="json_extract_path expects at least two arguments"):
            i1.sql("SELECT JSON_EXTRACT_PATH() FROM legacy")
        with pytest.raises(TarantoolError, match="cannot index expression of type int"):
            i1.sql("SELECT JSON_EXTRACT_PATH(id, 'a') FROM legacy")
        # JSON_EXTRACT_PATH desugars to the native [] subscript, which accepts both
        # text and integer keys on a map (same as on an array); an integer key that
        # matches no map key is a miss, not a type error.
        assert i1.sql("SELECT id, JSON_EXTRACT_PATH(m, 1) FROM legacy ORDER BY id") == [[1, None], [2, None]]
        assert i1.sql("SELECT id, JSON_EXTRACT_PATH(m, 'a', 1, 'b') FROM legacy ORDER BY id") == [
            [1, None],
            [2, None],
        ]

        with pytest.raises(TarantoolError, match="of type map, but expression is of type json"):
            i1.sql("""INSERT INTO legacy VALUES (3, '{"a":1}'::json)""")


def _owner_name(instances, bucket_id: int) -> str:
    for inst in instances:
        if inst.eval("return box.space._bucket:get(...) ~= nil", bucket_id):
            return inst.name
    raise AssertionError(f"no instance owns bucket {bucket_id}")


def test_json_number_equality_in_distributed_union(cluster: Cluster):
    """A JSON number is one value whatever its MessagePack encoding: integer
    `42` and decimal `42.0` are equal.

    UNION DISTINCT over a sharded table also removes duplicates on the router,
    in `VirtualTable::remove_duplicates`, when rows from different storages
    meet. The router compares json with the comparator storage uses, so the
    two spellings become one row there too.

    The router does this only when the two rows are on different storages, so
    the test compares row 1 with a row that is on the other storage.
    """
    instances = cluster.deploy(instance_count=2, init_replication_factor=1)
    # `deploy` waits for the initial rebalance only when it can, so buckets can
    # still move here. Wait until they stop before the placement check.
    cluster.wait_until_buckets_balanced()
    i1 = instances[0]
    conn_info = _pg_conn_info(i1)

    with psycopg.connect(conn_info, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSON)")
        cur.execute("INSERT INTO t VALUES (1, '42'::json)")
        # Twenty rows make it almost certain that one of them is on the other
        # storage.
        values = ", ".join(f"({i}, '42.0'::json)" for i in range(2, 22))
        cur.execute(f"INSERT INTO t VALUES {values}")

        cur.execute('SELECT id, "bucket_id" FROM t ORDER BY id')
        owners = {row[0]: _owner_name(instances, row[1]) for row in cur.fetchall()}
        other = next(i for i in owners if owners[i] != owners[1])

        cur.execute(f"SELECT data FROM t WHERE id = 1 UNION SELECT data FROM t WHERE id = {other}")
        assert len(cur.fetchall()) == 1
