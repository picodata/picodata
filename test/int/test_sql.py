import funcy  # type: ignore
import pytest
import re

from conftest import (
    Cluster,
    Instance,
    ReturnError,
)


@funcy.retry(tries=30, timeout=0.2)
def apply_migration(i: Instance, n: int):
    assert i.call("pico.migrate", n) == n


def test_pico_sql(cluster: Cluster):
    cluster.deploy(instance_count=1)
    i1 = cluster.instances[0]

    usage_msg = re.escape("Usage: sql(query[, params])")
    with pytest.raises(ReturnError, match=usage_msg):
        i1.call(
            "pico.sql",
        )
    with pytest.raises(ReturnError, match=usage_msg):
        i1.call(
            "pico.sql",
            "select * from t",
            {},
            "extra",
        )

    first_arg_msg = re.escape("SQL query must be a string")
    with pytest.raises(ReturnError, match=first_arg_msg):
        i1.call(
            "pico.sql",
            1,
        )

    second_arg_msg = re.escape("SQL params must be a table")
    with pytest.raises(ReturnError, match=second_arg_msg):
        i1.call(
            "pico.sql",
            "select * from t",
            1,
        )

    invalid_meta_msg = re.escape("sbroad: space")
    with pytest.raises(ReturnError, match=invalid_meta_msg):
        i1.call(
            "pico.sql",
            "select * from absent_table",
        )
    with pytest.raises(ReturnError, match=invalid_meta_msg):
        i1.call(
            "pico.sql",
            "select * from absent_table where a = ?",
            (1,),
        )


def test_select(cluster: Cluster):
    cluster.deploy(instance_count=2)
    i1, i2 = cluster.instances

    space_id = 739
    index = i1.ddl_create_space(
        dict(
            id=space_id,
            name="T",
            format=[
                dict(name="A", type="integer", is_nullable=False),
            ],
            primary_key=[dict(field="A")],
            # sharding function is implicitly murmur3
            distribution=dict(kind="sharded_implicitly", sharding_key=["A"]),
        )
    )
    i2.call(".proc_sync_raft", index, [3, 0])

    data = i1.sql("""insert into t values(1);""")
    assert data["row_count"] == 1
    i2.sql("""insert into t values(2);""")
    i2.sql("""insert into t values(?);""", 2000)
    data = i1.sql("""select * from t where a = ?""", 2)
    assert data["rows"] == [[2]]
    data = i1.sql("""select * from t""")
    assert data["rows"] == [[1], [2], [2000]]
    data = i2.sql(
        """select * from t as t1
           join (select a as a2 from t) as t2
           on t1.a = t2.a2 where t1.a = ?""",
        2,
    )
    assert data["rows"] == [[2, 2]]
