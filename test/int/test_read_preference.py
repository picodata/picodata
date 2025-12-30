import pytest

from conftest import Cluster, Instance, TarantoolError


def init_table(i: Instance):
    i.sql("create table wonderland (id int primary key, creature string, count int)")
    i.sql("insert into wonderland values (1, 'alice', 1), (2, 'krolik', 1), (3, 'gorilla', 0)")


def init_unlogged_table(i: Instance):
    i.sql("create unlogged table wonderland (id int primary key, creature string, count int)")
    i.sql("insert into wonderland values (1, 'alice', 1), (2, 'krolik', 1), (3, 'gorilla', 0)")


def test_default(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    i2 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    # i1 is master as the first member of the replicaset
    assert i2.replicaset_master_name() == i1.name

    # prepare data for DQL
    init_table(i1)
    dql_query = """
        select pico_instance_name(pico_instance_uuid()), creature
        from wonderland
        order by creature
    """
    dql_expect = [[i1.name, "alice"], [i1.name, "gorilla"], [i1.name, "krolik"]]

    # i1 handles DQL by default
    dql = i1.sql(dql_query)
    assert dql == dql_expect
    dql = i2.sql(dql_query)
    assert dql == dql_expect


def test_leader(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    i2 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    # i1 is leader as the first member of the replicaset
    assert i2.replicaset_master_name() == i1.name

    # prepare data for DQL
    init_table(i1)
    dql_query = """
        select pico_instance_name(pico_instance_uuid()), creature
        from wonderland
        order by creature
        option(read_preference = leader)
    """
    dql_expect = [[i1.name, "alice"], [i1.name, "gorilla"], [i1.name, "krolik"]]

    # i1 handles DQL on read_preference = leader
    dql = i1.sql(dql_query)
    assert dql == dql_expect
    dql = i2.sql(dql_query)
    assert dql == dql_expect


def test_replica(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    i2 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    # i1 is leader as the first member of the replicaset
    assert i2.replicaset_master_name() == i1.name

    # prepare data for DQL
    init_table(i1)
    dql_query = """
        select pico_instance_name(pico_instance_uuid()), creature
        from wonderland
        order by creature
        option(read_preference = replica)
    """
    dql_expect = [[i2.name, "alice"], [i2.name, "gorilla"], [i2.name, "krolik"]]

    # i2 handles DQL on read_preference = replica
    dql = i1.sql(dql_query)
    assert dql == dql_expect
    dql = i2.sql(dql_query)
    assert dql == dql_expect


def test_replica_sql_preemption_error(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    i2 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    assert i2.replicaset_master_name() == i1.name

    i1.sql("alter system set sql_preemption = true")
    i1.sql("alter system set sql_preemption_interval_us = 100")

    init_table(i1)
    dql_query = """
        select pico_instance_name(pico_instance_uuid()), creature
        from wonderland
        order by creature
        option(read_preference = replica)
    """

    # only 'leader' is supported
    with pytest.raises(TarantoolError, match="read_preference must be set to 'leader' when sql_preemption is enabled"):
        i1.sql(dql_query)


def test_replica_error(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=True, replicaset_name="r99")

    # prepare data for DQL
    init_table(i1)
    dql_query = """
        select pico_instance_name(pico_instance_uuid()), creature
        from wonderland
        order by creature
        option(read_preference = replica)
    """

    # i1 is the only node in replicaset (leader)
    with pytest.raises(TimeoutError):
        i1.sql(dql_query)


def test_replica_alter_system(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    i2 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    # i1 is leader as the first member of the replicaset
    assert i2.replicaset_master_name() == i1.name

    # prepare data for DQL
    init_table(i1)
    dql_query = """
        select pico_instance_name(pico_instance_uuid()), creature
        from wonderland
        order by creature
    """
    dql_expect = [[i2.name, "alice"], [i2.name, "gorilla"], [i2.name, "krolik"]]

    i1.sql("alter system set read_preference = 'replica'")
    dql = i1.sql(dql_query)
    assert dql == dql_expect


def test_any(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    i2 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    # i1 is leader as the first member of the replicaset
    assert i2.replicaset_master_name() == i1.name

    # prepare data for DQL
    init_table(i1)
    dql_query = """
        select pico_instance_name(pico_instance_uuid()), creature
        from wonderland
        order by creature
        option(read_preference = any)
    """
    dql_expect_1 = ["alice", "gorilla", "krolik"]
    dql_names = set()

    for _ in range(10):
        dql = i1.sql(dql_query)
        names = set(map(lambda x: x[0], dql))
        # sanity check
        assert len(names) == 1
        dql_names |= names
        assert list(map(lambda x: x[1], dql)) == dql_expect_1

    # probability of failure is 1/2^10
    assert i2.name in dql_names


def test_any_one(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=True, replicaset_name="r99")

    # prepare data for DQL
    init_table(i1)
    dql_query = """
        select pico_instance_name(pico_instance_uuid()), creature
        from wonderland
        order by creature
        option(read_preference = any)
    """
    dql_expect = [[i1.name, "alice"], [i1.name, "gorilla"], [i1.name, "krolik"]]

    # dql must not failed
    dql = i1.sql(dql_query)
    assert dql == dql_expect


def test_replica_many(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    repl = [cluster.add_instance(wait_online=True, replicaset_name="r99") for _ in range(5)]
    # i1 is leader as the first member of the replicaset
    for i in repl:
        assert i.replicaset_master_name() == i1.name

    # prepare data for DQL
    init_table(i1)
    dql_query = """
        select pico_instance_name(pico_instance_uuid()), creature
        from wonderland
        order by creature
        option(read_preference = replica)
    """
    dql_expect_0 = ["alice", "gorilla", "krolik"]

    balance = set()
    for i in [i1] + repl:
        for _ in range(3):
            dql = i.sql(dql_query)
            # not i1 handles DQL on read_preference = replica
            assert any(map(lambda i: dql == [[i.name, x] for x in dql_expect_0], repl))
            balance.add(next(map(lambda x: x[0], dql)))
    # balancer works
    assert len(balance) > 1


def test_replica_custom_plan(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    i2 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    # i1 is leader as the first member of the replicaset
    assert i2.replicaset_master_name() == i1.name

    # prepare data for DQL
    # base table and data
    init_table(i1)
    # second table with the same columns but different sharding key to force motion segment
    i1.sql("create table zoo (id int primary key, creature string, count int) distributed by (creature)")
    i1.sql("insert into zoo select * from wonderland")

    dql_query = """
        select pico_instance_name(pico_instance_uuid()), count(*)
        from wonderland
        join zoo on wonderland.id = zoo.id
        option(read_preference = replica)
    """

    dql = i1.sql(dql_query)
    # it's ok for custom plan to return data from the router
    assert dql == [[i1.name, 3]]
    dql = i2.sql(dql_query)
    assert dql == [[i2.name, 3]]


@pytest.mark.parametrize(
    "should_succeed, read_preference",
    [
        (True, "leader"),
        (False, "replica"),
        (False, "any"),
    ],
)
def test_replica_alter_system_unlogged(cluster: Cluster, should_succeed: bool, read_preference: str):
    """
    Test that unlogged tables are forbidden to read from replicas when read_preference is not set to leader system-wise
    """
    i1 = cluster.add_instance(wait_online=True, replicaset_name="unlogged_rs")
    i2 = cluster.add_instance(wait_online=True, replicaset_name="unlogged_rs")
    # i1 is leader as the first member of the replicaset
    assert i2.replicaset_master_name() == i1.name

    # prepare data for DQL
    init_unlogged_table(i1)
    dql_query = """
        select pico_instance_name(pico_instance_uuid()), creature
        from wonderland
        order by creature
    """

    i1.sql(f"alter system set read_preference = '{read_preference}'")
    if should_succeed:
        dql_expect = [[i1.name, "alice"], [i1.name, "gorilla"], [i1.name, "krolik"]]

        dql = i1.sql(dql_query)
        assert dql == dql_expect
        dql = i2.sql(dql_query)
        assert dql == dql_expect
    else:
        error_message = "read_preference must be set to 'leader' when querying unlogged tables"
        with pytest.raises(TarantoolError, match=error_message):
            i1.sql(dql_query)
        with pytest.raises(TarantoolError, match=error_message):
            i2.sql(dql_query)


@pytest.mark.parametrize(
    "should_succeed, read_preference_option",
    [
        (True, ""),
        (True, "option(read_preference = leader)"),
        (False, "option(read_preference = replica)"),
        (False, "option(read_preference = any)"),
    ],
)
def test_unlogged_table_read_preference(cluster: Cluster, should_succeed: bool, read_preference_option: str):
    """
    Test that unlogged tables are forbidden to read from replicas when read_preference is not set to leader as a DQL option
    """
    i1 = cluster.add_instance(wait_online=True, replicaset_name="unlogged_rs")
    i2 = cluster.add_instance(wait_online=True, replicaset_name="unlogged_rs")
    assert i2.replicaset_master_name() == i1.name

    init_unlogged_table(i1)

    dql_query = f"""
        select pico_instance_name(pico_instance_uuid()), creature
        from wonderland
        order by creature
        {read_preference_option}
    """

    if should_succeed:
        dql_expect = [[i1.name, "alice"], [i1.name, "gorilla"], [i1.name, "krolik"]]

        dql = i1.sql(dql_query)
        assert dql == dql_expect
        dql = i2.sql(dql_query)
        assert dql == dql_expect
    else:
        error_message = "read_preference must be set to 'leader' when querying unlogged tables"
        with pytest.raises(TarantoolError, match=error_message):
            i1.sql(dql_query)
        with pytest.raises(TarantoolError, match=error_message):
            i2.sql(dql_query)
