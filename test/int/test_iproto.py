import pytest

from conftest import Instance, Connection, MalformedAPI, TarantoolError, ErrorCode, Cluster, log_crawler


USER_NAME = "kelthuzad"
USER_PASS = "g$$dP4ss"
TABLE_NAME = "warehouse"


def create_connection(instance: Instance, use_call_16: bool = False):
    dcl = instance.sql(
        f"""
        CREATE USER {USER_NAME} WITH PASSWORD '{USER_PASS}' USING chap-sha1
    """
    )
    assert dcl["row_count"] == 1

    conn = Connection(
        instance.host,
        instance.port,
        user=USER_NAME,
        password=USER_PASS,
        connect_now=True,
        reconnect_max_attempts=0,
        call_16=use_call_16,
    )
    assert conn

    return conn


def test_iproto_forbidden(instance: Instance):
    conn = create_connection(instance, use_call_16=True)

    with pytest.raises(TarantoolError) as data:
        conn.call("fake_function")
    assert data.value.args[:2] == (
        "ER_UNSUPPORTED",
        "picodata does not support IPROTO_CALL_16 request type",
    )


def test_iproto_execute(instance: Instance):
    # https://docs.picodata.io/picodata/stable/reference/legend/#create_test_tables
    ddl = instance.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (
            id INTEGER NOT NULL,
            item TEXT NOT NULL,
            type TEXT NOT NULL,
            PRIMARY KEY (id))
        USING memtx DISTRIBUTED BY (id)
        OPTION (TIMEOUT = 3.0)
    """
    )
    assert ddl["row_count"] == 1

    # https://docs.picodata.io/picodata/stable/reference/legend/#populate_test_tables
    data = instance.sql(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
            (1, 'bricks', 'heavy'),
            (2, 'panels', 'light')
    """
    )
    assert data["row_count"] == 2

    conn = create_connection(instance)

    with pytest.raises(TarantoolError) as data:
        conn.execute(f"SELECT * FROM {TABLE_NAME}")
    assert data.value.args[:2] == (
        "ER_ACCESS_DENIED",
        f"Read access to space '{TABLE_NAME}' is denied for user '{USER_NAME}'",
    )

    acl = instance.sql(f"GRANT READ ON TABLE {TABLE_NAME} TO {USER_NAME}")
    assert acl["row_count"] == 1

    with pytest.raises(MalformedAPI) as dql:
        conn.execute(f"SELECT * FROM {TABLE_NAME}")
    assert dql.value.args == ([1, "bricks", "heavy"], [2, "panels", "light"])

    with pytest.raises(TarantoolError) as dql:  # type: ignore
        conn.execute(f"SELECT * FRUM {TABLE_NAME}")
    assert dql.value.args[:2] == (
        ErrorCode.SbroadError,
        f"sbroad: rule parsing error:  --> 1:10\n  |\n1 | SELECT * FRUM {TABLE_NAME}\n  |          ^---\n  |\n  = expected EOI, OrderBy, Limit, UnionOp, ExceptOp, UnionAllOp, or DqlOption",  # noqa: E501
    )

    acl = instance.sql(f"GRANT WRITE ON TABLE {TABLE_NAME} TO {USER_NAME}")
    assert acl["row_count"] == 1

    dml = conn.execute(f"DELETE FROM {TABLE_NAME} WHERE id = 1")
    assert dml["row_count"] == 1


def test_cross_cluster_isolation(cluster: Cluster):
    """
    Test isolation between clusters.

    Scenario:
    1. Create cluster A with 3 nodes
    2. Create test table and data in cluster A
    3. Stop all nodes of cluster A
    4. Clear data from i1 and i2, leave i3 with cluster A data
    5. Create a new test table in cluster B and insert new data
    6. Check that cluster B is functional
    7. Start i3 with cluster A data.
    8. Check that i3 still has its own data from cluster A
    """

    # Step 1: Create cluster A with 3 nodes
    i1, i2, i3 = cluster.deploy(instance_count=3, init_replication_factor=3)
    i1.promote_or_fail()

    first_cluster_uuid = i1.cluster_uuid

    # Step 2: Create test table and data in cluster A
    i1.sql("""
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            data TEXT NOT NULL
        ) DISTRIBUTED BY (id)
    """)
    i1.sql("INSERT INTO test_table VALUES (1, 'cluster_a_data')")

    # Step 3: Stop all nodes of cluster A
    for instance in [i3, i2, i1]:
        instance.terminate()

    # Step 4: Clear data from i1 and i2, leave i3 with cluster A data
    i1.restart(remove_data=True)
    i2.restart(remove_data=True)
    i1.wait_online()
    i2.wait_online()
    i1.promote_or_fail()

    assert i1.cluster_uuid == i2.cluster_uuid
    assert i1.cluster_uuid != first_cluster_uuid

    # Step 5: Create a new test table in cluster B and insert new data
    i1.sql("""
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            data TEXT NOT NULL
        ) DISTRIBUTED GLOBALLY
    """)
    i1.sql("INSERT INTO test_table VALUES (1, 'cluster_b_data')")

    # Step 6: Check that cluster B is functional
    for instance in [i1, i2]:
        result = instance.sql("SELECT * FROM test_table WHERE id = 1")
        assert result[0] == [1, "cluster_b_data"]

    # Step 7: Start i3 with cluster A data.
    # i3 will try to connect to cluster B (using peer addresses),
    # but it has a different cluster_uuid.
    i3.peers = [i1.iproto_listen, i2.iproto_listen]
    i3.start()

    lc_cu = log_crawler(i3, "cluster UUID mismatch")
    lc_cu.wait_matched()
    assert i3.current_state()[0] != "Online"

    assert i1.cluster_uuid != i3.cluster_uuid

    # We expect that i3 fails to route SQL due to cluster UUID mismatch.
    # The node does not initialize tier routers in this state, so the error
    # message must explicitly indicate missing router for the tier. This check
    # verifies cross-cluster isolation: data/routing from cluster A is not used
    # when connecting to cluster B.
    with pytest.raises(TarantoolError, match="no router found for tier"):
        i3.sql("SELECT * FROM test_table WHERE id = 1")
