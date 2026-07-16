from framework.sqltester import (
    ClusterSingleInstance,
    ClusterTwoInstances,
    sql_test_file,
)
from conftest import Cluster


@sql_test_file("array.sql")
class TestArray(ClusterSingleInstance):
    pass


@sql_test_file("array.sql")
class TestArray2(ClusterTwoInstances):
    pass


# https://git.picodata.io/core/picodata/-/issues/3029
def test_gl_3029(cluster: Cluster):
    cluster.deploy(instance_count=2, init_replication_factor=2)
    cluster.wait_until_buckets_balanced()

    i1 = cluster.instances[0]
    i1.sql("""create user postgres with password 'Passw0rd';""", sudo=True)
    i1.sql("""grant create table to postgres;""", sudo=True)

    pg_conn = i1.connect_via_pgproto(user="postgres", password="Passw0rd")
    pg_conn.execute("""create table foo(id int primary key, arr int[]);""")
    pg_conn.execute("""insert into foo values (1, null);""")
    pg_conn.execute("""insert into foo values (2, array[1,2,3]);""")
    rows = pg_conn.execute("""SELECT * FROM foo ORDER BY id;""").fetchall()
    assert rows == [(1, None), (2, [1, 2, 3])]
