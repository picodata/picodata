from conftest import Cluster, Instance
from tarantool import Datetime  # type: ignore[attr-defined]

STORED = "2026-04-29T00:00:00.123456789Z"


def test_datetime_nanoseconds_stored_before_upgrade(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1 = cluster.instances[0]

    i1.sql("CREATE TABLE t (id INT PRIMARY KEY, d DATETIME, v INT)")

    dt = Datetime(year=2026, month=4, day=29, nsec=123456789)
    assert i1.sql("INSERT INTO t VALUES (1, ?, 0)", dt)["row_count"] == 1
    assert i1.sql("SELECT d FROM t") == [[dt]]
    assert i1.sql("SELECT d::text FROM t") == [[STORED]]

    assert i1.sql(f"SELECT count(*) FROM t WHERE d = '{STORED}'") == [[0]]
    assert i1.sql("SELECT count(*) FROM t WHERE d = ?::datetime", STORED) == [[0]]
    assert i1.sql("SELECT count(*) FROM t WHERE d = d::text::datetime") == [[0]]

    assert i1.sql(f"SELECT count(*) FROM t WHERE d >= '{STORED}'") == [[0]]

    assert i1.sql(f"UPDATE t SET v = 1 WHERE d = '{STORED}'")["row_count"] == 0
    assert i1.sql(f"DELETE FROM t WHERE d = '{STORED}'")["row_count"] == 0
    assert i1.sql("SELECT v FROM t") == [[0]]


def test_datetime_nanoseconds_lua_parse(instance: Instance):
    nsec = instance.eval(f"return require('datetime').parse('{STORED}').nsec")
    assert nsec == 123457000

    roundtrip = instance.eval(
        """
        local datetime = require('datetime')
        local dt = datetime.new{year = 2026, month = 4, day = 29, nsec = 123456789}
        return datetime.parse(tostring(dt)) == dt
        """
    )
    assert roundtrip is False
