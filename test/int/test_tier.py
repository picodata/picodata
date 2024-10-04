import pytest

from conftest import Cluster, Instance, TarantoolError, Retriable


def test_single_tier_query(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    cluster_name: test
    tier:
        default:
        router:
        empty_tier:
"""
    )

    cluster.deploy(instance_count=1)
    i1 = cluster.add_instance(tier="default")
    _ = cluster.add_instance(tier="router")

    def get_tier_from_distribution_field_from_pico_table(table_name):
        data = i1.sql(
            f"""
            SELECT "distribution" FROM "_pico_table" WHERE "name" = '{table_name}'
            """,
            sudo=True,
        )

        distribution_field = data[0][0]
        distribution_parameters = list(distribution_field.values())
        assert len(distribution_parameters) == 1
        tier = distribution_parameters[0][2]
        return tier

    # Create sharded table in unexistent tier failed.
    with pytest.raises(
        TarantoolError, match="specified tier 'unexistent_tier' doesn't exist"
    ):
        i1.sql(
            """
            CREATE TABLE "table_in_unexistent_tier" (a INT NOT NULL, b INT, PRIMARY KEY (a))
            DISTRIBUTED BY (b)
            IN TIER "unexistent_tier"
            OPTION (TIMEOUT = 3)
            """
        )

    # Create sharded table in empty tier failed. Tier `storage` is empty, because single instance
    # belongs to default tier.
    with pytest.raises(
        TarantoolError,
        match="specified tier 'empty_tier' doesn't contain at least one instance",
    ):
        i1.sql(
            """
            CREATE TABLE "table_in_empty_tier" (a INT NOT NULL, b INT, PRIMARY KEY (a))
            DISTRIBUTED BY (b)
            IN TIER "empty_tier"
            OPTION (TIMEOUT = 3)
            """
        )

    # [in tier] part with `DISTRIBUTED BY GLOBALLY` should fail.
    with pytest.raises(TarantoolError, match="rule parsing error"):
        i1.sql(
            """
            CREATE TABLE "global_table_in_tier" (a INT NOT NULL, b INT, PRIMARY KEY (a))
            DISTRIBUTED GLOBALLY
            IN TIER "storage"
            OPTION (TIMEOUT = 3)
            """
        )

    # Empty [in tier] part should fail.
    with pytest.raises(TarantoolError, match="expected Identifier"):
        i1.sql(
            """
            CREATE TABLE "global_table_in_tier" (a INT NOT NULL, b INT, PRIMARY KEY (a))
            DISTRIBUTED BY (a)
            IN TIER
            OPTION (TIMEOUT = 3)
            """
        )

    # Empty tier name doesn't exist, so should fail.
    with pytest.raises(TarantoolError, match="specified tier '' doesn't exist"):
        i1.sql(
            """
            CREATE TABLE "sharded_table_in_empty_tier_name"
            (a INT NOT NULL, b INT, PRIMARY KEY (a))
            DISTRIBUTED BY (a)
            IN TIER ""
            OPTION (TIMEOUT = 3)
            """
        )

    # Tier name with parentheses should fail.
    with pytest.raises(TarantoolError, match="rule parsing error"):
        i1.sql(
            """
            CREATE TABLE "sharded_table_in_tier_default" (a INT NOT NULL, b INT, PRIMARY KEY (a))
            DISTRIBUTED BY (a)
            IN TIER("default")
            OPTION (TIMEOUT = 3)
            """
        )

    # Create sharded table without [in tier] part. In that case tier value in distribution field
    # in `_pico_table` should equals "default".
    ddl = i1.sql(
        """
        CREATE TABLE "sharded_table_in_default"
        (a INT NOT NULL, b INT, PRIMARY KEY (a))
        DISTRIBUTED BY (b)
        OPTION (TIMEOUT = 3)
        """
    )
    assert ddl["row_count"] == 1
    assert (
        get_tier_from_distribution_field_from_pico_table("sharded_table_in_default")
        == "default"
    )

    # Create sharded table in existing tier.
    ddl = i1.sql(
        """
        CREATE TABLE "sharded_table_in_router" (a INT NOT NULL, b INT, PRIMARY KEY (a))
        DISTRIBUTED BY (b)
        IN TIER "router"
        OPTION (TIMEOUT = 3)
        """
    )
    assert ddl["row_count"] == 1
    assert (
        get_tier_from_distribution_field_from_pico_table("sharded_table_in_router")
        == "router"
    )

    # It's ok to use global tables with sharded in single tier.
    dql = i1.retriable_sql(
        """
            SELECT a FROM "sharded_table_in_router"
            UNION ALL
            SELECT "id" FROM "_pico_table"
            """
    )

    assert len(dql) > 0

    # Query with tables from different tier is aborted.
    with pytest.raises(
        TarantoolError, match="Query cannot use tables from different tiers"
    ):
        i1.sql(
            """
            SELECT * FROM "sharded_table_in_router"
            UNION ALL
            SELECT * FROM "sharded_table_in_default"
            """
        )

    # Query with tables from different tier is aborted.
    with pytest.raises(
        TarantoolError, match="Query cannot use tables from different tiers"
    ):
        i1.sql(
            """
            SELECT a FROM "sharded_table_in_router"
            UNION ALL
            select a FROM "sharded_table_in_default"
            UNION ALL
            SELECT "id" FROM "_pico_table"
            """
        )

    # Query with tables from different tier is aborted.
    with pytest.raises(
        TarantoolError, match="Query cannot use tables from different tiers"
    ):
        i1.sql(
            """
            INSERT INTO "sharded_table_in_router" SELECT * FROM "sharded_table_in_default"
            """
        )


def table_size(instance: Instance, table_name: str):
    table_size = instance.eval(f"return box.space.{table_name}:count()")

    return table_size


def test_executing_query_on_instances_from_different_tiers(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    cluster_name: test
    tier:
        storage:
            replication_factor: 1
            can_vote: true
        router:
            replication_factor: 1
            can_vote: true
"""
    )

    storage_instance = cluster.add_instance(tier="storage")
    router_instance = cluster.add_instance(tier="router")

    ddl = storage_instance.sql(
        """
        CREATE TABLE "table_in_storage" ( "id" INTEGER NOT NULL, PRIMARY KEY ("id") )
        DISTRIBUTED BY ("id")
        IN TIER "storage"
        """
    )

    assert ddl["row_count"] == 1

    ddl = storage_instance.sql(
        """
        CREATE TABLE "table_in_router" ( "id" INTEGER NOT NULL, PRIMARY KEY ("id") )
        DISTRIBUTED BY ("id")
        IN TIER "router"
        """
    )

    assert ddl["row_count"] == 1

    # ensure that all tables in form of box spaces present on
    # all instances of all tiers
    for instance in [storage_instance, router_instance]:
        for table in ["table_in_storage", "table_in_router"]:
            assert table_size(instance, table) == 0

    # dml to tables from corresponding tiers
    data = storage_instance.sql("""INSERT INTO "table_in_storage" VALUES(1) """)
    assert data["row_count"] == 1

    data = router_instance.sql("""INSERT INTO "table_in_router" VALUES(2) """)
    assert data["row_count"] == 1

    # ensure that on each instance of tier that tables
    # from other tiers still empty
    assert table_size(storage_instance, "table_in_router") == 0
    assert table_size(router_instance, "table_in_storage") == 0

    # ensure that on each instance of tier that tables
    # from other tiers still empty
    assert table_size(storage_instance, "table_in_storage") == 1
    assert table_size(router_instance, "table_in_router") == 1

    # check from i2 (router), that data on i2(router) is present
    data = router_instance.sql("""SELECT * from "table_in_router" """)
    assert data == [[2]]

    # check from i1 (storage), that data on i2(router) is present
    data = storage_instance.sql("""SELECT * from "table_in_router" """)
    assert data == [[2]]

    # check from i2 (router), that data on i1(storage) is present
    data = router_instance.sql("""SELECT * from "table_in_storage" """)
    assert data == [[1]]

    # check from i1 (storage), that data on i1(storage) is present
    data = storage_instance.sql("""SELECT * from "table_in_storage" """)
    assert data == [[1]]

    # dml to tables from not their tiers also ok
    data = router_instance.sql("""INSERT INTO "table_in_storage" VALUES(3) """)
    assert data["row_count"] == 1

    data = storage_instance.sql("""INSERT INTO "table_in_router" VALUES(4) """)
    assert data["row_count"] == 1

    assert table_size(storage_instance, "table_in_storage") == 2
    assert table_size(router_instance, "table_in_router") == 2


def get_vshards_opinion_about_replicaset_masters(i: Instance, tier_name: str):
    return i.eval(
        f"""
            local res = {{}}
            local router = pico.router["{tier_name}"]
            for _, r in pairs(router.replicasets) do
                res[r.uuid] = r.master.name
            end
            return res
        """
    )


def test_vshard_configuration_on_different_tiers(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    cluster_name: test
    tier:
        storage:
            replication_factor: 2
            can_vote: true
        router:
            replication_factor: 1
            can_vote: true
"""
    )

    router_instance = cluster.add_instance(tier="router")
    storage_instance_1 = cluster.add_instance(tier="storage")

    # create table sharded in `uninitialized` tier is ok
    ddl = storage_instance_1.sql(
        """
        CREATE TABLE "table_in_storage" ( "id" INTEGER NOT NULL, PRIMARY KEY ("id") )
        DISTRIBUTED BY ("id")
        IN TIER "storage"
        """
    )

    assert ddl["row_count"] == 1

    # create table sharded in `uninitialized` tier is ok, even via instance from other tier
    ddl = router_instance.sql(
        """
        CREATE TABLE "table_in_storage_from_router" ( "id" INTEGER NOT NULL, PRIMARY KEY ("id") )
        DISTRIBUTED BY ("id")
        IN TIER "storage"
        """
    )

    assert ddl["row_count"] == 1

    index_nullable_router_error = "no router found for tier \\\\'storage\\\\'"

    # insert and select from table sharded in `uninitialized` tier is failed
    # because it uses vshard
    with pytest.raises(TarantoolError, match=index_nullable_router_error):
        storage_instance_1.sql("""INSERT INTO "table_in_storage" VALUES(1) """)

    with pytest.raises(TarantoolError, match=index_nullable_router_error):
        router_instance.sql("""INSERT INTO "table_in_storage" VALUES(1) """)

    # since now `storage` tier contains full replicaset, so it's tables are operable
    storage_instance_2 = cluster.add_instance(tier="storage")

    # ensure that table created
    assert table_size(storage_instance_2, "table_in_storage") == 0

    data = storage_instance_1.sql("""INSERT INTO "table_in_storage" VALUES(1) """)
    assert data["row_count"] == 1

    data = storage_instance_2.sql("""INSERT INTO "table_in_storage" VALUES(2) """)
    assert data["row_count"] == 1

    data = router_instance.sql("""INSERT INTO "table_in_storage" VALUES(3) """)
    assert data["row_count"] == 1

    # ensure that `table_in_storage` filled up on right instances
    assert table_size(storage_instance_1, "table_in_storage") == 3
    assert table_size(storage_instance_2, "table_in_storage") == 3
    assert table_size(router_instance, "table_in_storage") == 0

    r2_uuid = router_instance.eval(
        "return box.space._pico_replicaset:get('r2').replicaset_uuid"
    )

    # if we kill master of replicaset data will be unavailiable temporarily, until
    # governor make it right: master switchower + deliver changed vshard configuration to routers
    storage_instance_1.kill()

    def wait_until_governor_deliver_vshard_configuration():

        replicaset_masters = get_vshards_opinion_about_replicaset_masters(
            router_instance, "storage"
        )
        assert replicaset_masters[r2_uuid] == storage_instance_2.name

    Retriable(timeout=10, rps=5).call(wait_until_governor_deliver_vshard_configuration)

    # ensure that after `storage_istance_1` killed, `storage` tier tables still operable

    # still can insert and select
    data = router_instance.sql("""INSERT INTO "table_in_storage" VALUES(4) """)
    assert data["row_count"] == 1

    data = router_instance.sql("""SELECT * from "table_in_storage" """)
    assert data == [[1], [2], [3], [4]]


def test_multiple_tier_and_global_tables(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    cluster_name: test
    tier:
        storage:
            replication_factor: 1
            can_vote: true
        router:
            replication_factor: 1
            can_vote: true
"""
    )

    router_instance_1 = cluster.add_instance(tier="router")
    router_instance_2 = cluster.add_instance(tier="router")
    storage_instance_1 = cluster.add_instance(tier="storage")
    storage_instance_2 = cluster.add_instance(tier="storage")

    ddl = storage_instance_1.sql(
        """
        CREATE TABLE "global_table_via_storage" ( "id" INTEGER NOT NULL, PRIMARY KEY ("id") )
        DISTRIBUTED GLOBALLY
        """
    )

    assert ddl["row_count"] == 1

    ddl = router_instance_1.sql(
        """
        CREATE TABLE "global_table_via_router" ( "id" INTEGER NOT NULL, PRIMARY KEY ("id") )
        DISTRIBUTED GLOBALLY
        """
    )

    assert ddl["row_count"] == 1

    # ensure that tables is created
    for instance in [
        storage_instance_1,
        storage_instance_2,
        router_instance_1,
        router_instance_2,
    ]:
        for table_name in ["global_table_via_storage", "global_table_via_router"]:
            assert table_size(instance, table_name) == 0

    # insert to global tables from different tiers
    data = storage_instance_1.sql(
        """INSERT INTO "global_table_via_storage" VALUES(1) """
    )
    assert data["row_count"] == 1
    data = storage_instance_1.sql(
        """INSERT INTO "global_table_via_router" VALUES(1) """
    )
    assert data["row_count"] == 1

    data = router_instance_1.sql(
        """INSERT INTO "global_table_via_storage" VALUES(2) """
    )
    assert data["row_count"] == 1
    data = router_instance_1.sql("""INSERT INTO "global_table_via_router" VALUES(2) """)
    assert data["row_count"] == 1

    # ensure that tables is filled up
    for instance in [
        storage_instance_1,
        storage_instance_2,
        router_instance_1,
        router_instance_2,
    ]:
        for table_name in ["global_table_via_storage", "global_table_via_router"]:
            assert table_size(instance, table_name) == 2


@pytest.mark.xfail(reason=("very flaky, will be fixed ASAP"))
def test_tier_with_several_replicasets(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    cluster_name: test
    tier:
        router:
            replication_factor: 1
            can_vote: true
        storage:
            replication_factor: 2
            can_vote: true
"""
    )

    router_instance = cluster.add_instance(tier="router")
    replicaset_1_i1 = cluster.add_instance(tier="storage")
    replicaset_1_i2 = cluster.add_instance(tier="storage")
    replicaset_2_i1 = cluster.add_instance(tier="storage")
    replicaset_2_i2 = cluster.add_instance(tier="storage")

    cluster.wait_until_instance_has_this_many_active_buckets(replicaset_1_i1, 1500)
    cluster.wait_until_instance_has_this_many_active_buckets(replicaset_2_i1, 1500)

    # create table sharded in `uninitialized` tier is ok
    ddl = router_instance.sql(
        """
        CREATE TABLE "sharded_table" ( "id" INTEGER NOT NULL, PRIMARY KEY ("id") )
        DISTRIBUTED BY ("id")
        IN TIER "storage"
        """
    )

    assert ddl["row_count"] == 1

    # ensure that `sharded_table` is created
    assert table_size(replicaset_1_i1, "sharded_table") == 0
    assert table_size(replicaset_2_i1, "sharded_table") == 0
    assert table_size(router_instance, "sharded_table") == 0

    data = router_instance.sql("""INSERT INTO "sharded_table" VALUES(1) """)
    assert data["row_count"] == 1

    data = router_instance.sql("""INSERT INTO "sharded_table" VALUES(5) """)
    assert data["row_count"] == 1

    # contains record with sharding key '1'
    assert table_size(replicaset_1_i1, "sharded_table") == 1
    content = replicaset_1_i1.eval("return box.space.sharded_table:select()")
    # id and bucket id
    assert content == [[1, 1934]]

    # contains record with sharding key '2'
    assert table_size(replicaset_2_i1, "sharded_table") == 1
    content = replicaset_2_i1.eval("return box.space.sharded_table:select()")
    # id and bucket id
    assert content == [[5, 219]]

    # router contains no record for `sharded_table`
    assert table_size(router_instance, "sharded_table") == 0

    # but via SQL api
    data = router_instance.sql("""SELECT * from "sharded_table" """)
    assert sorted(data) == sorted([[1], [5]])

    r2_uuid = router_instance.eval(
        "return box.space._pico_replicaset:get('r2').replicaset_uuid"
    )
    r3_uuid = router_instance.eval(
        "return box.space._pico_replicaset:get('r3').replicaset_uuid"
    )

    # after fail of one replica from replicaset, `sharded_table` still operable
    replicaset_1_i1.kill()
    replicaset_2_i1.kill()

    def wait_until_governor_deliver_vshard_configuration(instance: Instance, r_uuid):
        replicaset_masters = get_vshards_opinion_about_replicaset_masters(
            router_instance, "storage"
        )
        assert replicaset_masters[r_uuid] == instance.name

    Retriable(timeout=10, rps=5).call(
        wait_until_governor_deliver_vshard_configuration, replicaset_1_i2, r2_uuid
    )
    Retriable(timeout=10, rps=5).call(
        wait_until_governor_deliver_vshard_configuration, replicaset_2_i2, r3_uuid
    )

    # to second replicaset
    data = router_instance.sql("""INSERT INTO "sharded_table" VALUES(2) """)
    assert data["row_count"] == 1

    # to first replicaset
    data = router_instance.sql("""INSERT INTO "sharded_table" VALUES(10) """)
    assert data["row_count"] == 1

    # contains record with sharding key '1', '2'
    assert table_size(replicaset_1_i2, "sharded_table") == 2
    content = replicaset_1_i2.eval("return box.space.sharded_table:select()")
    # id and bucket id
    assert content == [[1, 1934], [2, 1410]]

    # contains record with sharding key '5', '10'
    assert table_size(replicaset_2_i2, "sharded_table") == 2
    content = replicaset_2_i2.eval("return box.space.sharded_table:select()")
    # id and bucket id
    assert content == [[5, 219], [10, 626]]

    # router contains no record for `sharded_table`
    assert table_size(router_instance, "sharded_table") == 0

    # but via SQL api
    data = router_instance.sql("""SELECT * from "sharded_table" """)
    assert sorted(data) == sorted([[1], [2], [5], [10]])
