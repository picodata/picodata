import uuid_utils.compat as uuid
from conftest import Cluster


def test_insert_uuids(cluster: Cluster):
    cluster.deploy(instance_count=2)
    cluster.wait_until_buckets_balanced()

    i1 = cluster.instances[0]
    i1.sql("create table t (a uuid primary key)")

    namespace = uuid.uuid4()
    name = "lol"

    uuids = [
        uuid.uuid1(),
        uuid.uuid3(namespace, name),
        uuid.uuid4(),
        uuid.uuid5(namespace, name),
        uuid.uuid6(),
        uuid.uuid7(),
    ]

    for u in uuids:
        i1.sql(f"insert into t values ('{u}'::uuid)")

    for u in uuids:
        row = i1.sql(f"select a from t where a = '{u}'::uuid")
        assert row == [[u]]
