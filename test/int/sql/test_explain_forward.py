import psycopg
from conftest import Cluster
from framework.sqltester import (
    ClusterSingleInstance,
    sql_test_file,
)


@sql_test_file("explain_forward.sql")
class TestExplainForward(ClusterSingleInstance):
    pass


def test_forward_option(cluster: Cluster):
    cluster.deploy(instance_count=2, init_replication_factor=2)
    cluster.wait_until_buckets_balanced()

    i1 = cluster.instances[0]
    i1.sql("""create user postgres with password 'Passw0rd';""", sudo=True)
    i1.sql("""grant create table to postgres;""", sudo=True)

    master = cluster.masters()[0]
    for instance in cluster.instances:
        if instance.name != master.name:
            replica = instance

    master_conn = master.connect_via_pgproto(user="postgres", password="Passw0rd")
    replica_conn = replica.connect_via_pgproto(user="postgres", password="Passw0rd")
    master_conn.execute("""CREATE TABLE t (a INT PRIMARY KEY, b TEXT);""")
    master_conn.execute("""CREATE TABLE b (id INT PRIMARY KEY, val INT);""")
    master_conn.execute("""CREATE TABLE c (id INT PRIMARY KEY, val INT);""")

    master_conn.execute("""INSERT INTO c VALUES (2, 2);""")
    master_conn.execute("""INSERT INTO b VALUES (1, 1);""")
    master_conn.execute("""INSERT INTO b VALUES (2, 2);""")

    # Check that "FORWARD = OFF" is not compatible
    # with READ_PREFERENCE = REPLICA | ANY. Please check ADR
    # https://git.picodata.io/core/picodata/-/blob/master/doc/adr/2026-03-13-forward-option.md?ref_type=heads#%D0%B2%D0%B7%D0%B0%D0%B8%D0%BC%D0%BE%D0%B4%D0%B5%D0%B9%D1%81%D1%82%D0%B2%D0%B8%D0%B5-%D0%BE%D0%BF%D1%86%D0%B8%D0%B8-forward-%D1%81-%D0%BE%D0%BF%D1%86%D0%B8%D0%B5%D0%B9-read_preference
    # for more info.
    try:
        master_conn.execute("""
        SELECT * FROM t WHERE a = 1 OPTION (FORWARD = OFF, READ_PREFERENCE = REPLICA)
""")
    except Exception as e:
        assert (
            str(e) == 'sbroad: invalid OptionSpec: "forward = off" is not compatible with "read_preference = replica"'
        )

    try:
        master_conn.execute("""
        SELECT * FROM t WHERE a = 1 OPTION (FORWARD = OFF, READ_PREFERENCE = ANY)
""")
    except Exception as e:
        assert str(e) == 'sbroad: invalid OptionSpec: "forward = off" is not compatible with "read_preference = any"'

    master_conn.execute("""
    SELECT * FROM t WHERE a = 1 OPTION (FORWARD = OFF, READ_PREFERENCE = LEADER)
""")

    # Check that specifying forward option does not affect
    # explain output, i.e. user can specify option that is not
    # possible and still get explain output.
    rows = master_conn.execute("""EXPLAIN (FORWARD) SELECT * FROM t OPTION (FORWARD = OFF)""").fetchall()
    explain = "\n".join(row[0] for row in rows)
    assert explain == "forward analysis (on > ro_to_rw > off):\n  forward = on"

    rows = master_conn.execute("""EXPLAIN (RAW, FORWARD) SELECT * FROM t OPTION (FORWARD = OFF)""").fetchall()
    explain = "\n".join(row[0] for row in rows)
    assert (
        explain
        == """\
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────

╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯

SELECT "t"."a", "t"."b" FROM "t"

plan:
    [0] SCAN TABLE t (~1048576 rows)

──────────────────────────────────────────────────────────────────────
 # Forward                                                            
──────────────────────────────────────────────────────────────────────

forward analysis (on > ro_to_rw > off):
  forward = on\
"""
    )

    # Check that query can be executed with stricter forward option than specified
    # in explain output.
    rows = replica_conn.execute("""
        EXPLAIN (FORWARD) SELECT * FROM b WHERE b.id IN (SELECT max(val) FROM c where id = 5 order by 1) option (forward = ro_to_rw)
""").fetchall()
    explain = "\n".join(row[0] for row in rows)
    assert explain == "forward analysis (on > ro_to_rw > off):\n  forward = on"
    rows = replica_conn.execute("""
        SELECT * FROM b WHERE b.id IN (SELECT max(val) FROM c where id = 2 order by 1) option (forward = ro_to_rw)
""").fetchall()
    assert rows == [(2, 2)]

    # Check that option specified in connection string is applied to each query
    # executed within that connection.
    master_conn = psycopg.connect(
        conninfo=f"postgres://postgres:Passw0rd@{master.pg_host}:{master.pg_port}?options=forward%3Doff"
    )
    master_conn.execute("""
        SELECT * FROM t WHERE a IN (1, 2)
""").fetchall()
    try:
        master_conn.execute("""
            SELECT * FROM t
    """).fetchall()
    except Exception as e:
        assert str(e) == "".join(
            [
                'sbroad: invalid option: cannot satisfy "forward = off": ',
                'buckets span multiple nodes and are not present on the current node, try using "forward = on" instead',
            ]
        )

    # Check that specifying forward option in query overrides that passed in
    # connection string.
    master_conn.execute("""
        SELECT * FROM t OPTION (FORWARD = ON)
""").fetchall()
