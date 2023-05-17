from conftest import Cluster


def test_global_space_dml_catchup_by_log(cluster: Cluster):
    # Leader
    i1 = cluster.add_instance(wait_online=True, replicaset_id="r1")
    # For quorum
    i2 = cluster.add_instance(wait_online=True, replicaset_id="r1")
    # For quorum
    i3 = cluster.add_instance(wait_online=True, replicaset_id="r1")
    # Catcher-upper replicaset master
    i4 = cluster.add_instance(wait_online=True, replicaset_id="r2")
    # Catcher-upper replicaset follower
    i5 = cluster.add_instance(wait_online=True, replicaset_id="r2")

    index = i1.ddl_create_space(
        dict(
            id=812,
            name="candy",
            format=[
                dict(name="id", type="unsigned", is_nullable=False),
                dict(name="kind", type="string", is_nullable=False),
                dict(name="kilos", type="number", is_nullable=False),
            ],
            primary_key=[dict(field="id")],
            distribution=dict(kind="global"),
        ),
    )
    i2.raft_wait_index(index, 3)

    # Some dml
    index = i1.cas("insert", "candy", [1, "marshmallow", 2.7])
    i1.raft_wait_index(index, 3)
    index = i1.cas("insert", "candy", [2, "milk chocolate", 6.9])
    i1.raft_wait_index(index, 3)
    i2.raft_wait_index(index, 3)
    i3.raft_wait_index(index, 3)
    i4.raft_wait_index(index, 3)
    i5.raft_wait_index(index, 3)

    # Dml applied ok
    expected_tuples = [
        [1, "marshmallow", 2.7],
        [2, "milk chocolate", 6.9],
    ]
    assert i1.call("box.space.candy:select") == expected_tuples
    assert i2.call("box.space.candy:select") == expected_tuples
    assert i3.call("box.space.candy:select") == expected_tuples
    assert i4.call("box.space.candy:select") == expected_tuples
    assert i5.call("box.space.candy:select") == expected_tuples

    # These will be catching up
    i4.terminate()
    i5.terminate()

    # More DML
    index = i1.cas("replace", "candy", [2, "dark chocolate", 13.37])
    i1.raft_wait_index(index, 3)
    index = i1.cas("delete", "candy", [1])
    i1.raft_wait_index(index, 3)
    index = i1.cas("insert", "candy", [3, "ice cream", 0.3])
    i1.raft_wait_index(index, 3)
    i2.raft_wait_index(index, 3)
    i3.raft_wait_index(index, 3)

    # Dml applied ok again
    expected_tuples = [
        [2, "dark chocolate", 13.37],
        [3, "ice cream", 0.3],
    ]
    assert i1.call("box.space.candy:select") == expected_tuples
    assert i2.call("box.space.candy:select") == expected_tuples
    assert i3.call("box.space.candy:select") == expected_tuples

    # Master catch up by log
    i4.start()
    i4.wait_online()
    assert i4.call("box.space.candy:select") == expected_tuples

    # Follower catch up by log
    i5.start()
    i5.wait_online()
    assert i5.call("box.space.candy:select") == expected_tuples

    # Master boot by log
    i6 = cluster.add_instance(wait_online=True, replicaset_id="r3")
    assert i6.call("box.space.candy:select") == expected_tuples

    # Follower boot by log
    i7 = cluster.add_instance(wait_online=True, replicaset_id="r3")
    assert i7.call("box.space.candy:select") == expected_tuples


def test_global_space_dml_catchup_by_snapshot(cluster: Cluster):
    # Leader
    i1 = cluster.add_instance(wait_online=True, replicaset_id="r1")
    # For quorum
    i2 = cluster.add_instance(wait_online=True, replicaset_id="r1")
    # For quorum
    i3 = cluster.add_instance(wait_online=True, replicaset_id="r1")
    # Catcher-upper replicaset master
    i4 = cluster.add_instance(wait_online=True, replicaset_id="r2")
    # Catcher-upper replicaset follower
    i5 = cluster.add_instance(wait_online=True, replicaset_id="r2")

    index = i1.ddl_create_space(
        dict(
            id=812,
            name="candy",
            format=[
                dict(name="id", type="unsigned", is_nullable=False),
                dict(name="kind", type="string", is_nullable=False),
                dict(name="kilos", type="number", is_nullable=False),
            ],
            primary_key=[dict(field="id")],
            distribution=dict(kind="global"),
        ),
    )
    i2.raft_wait_index(index, 3)

    # Some dml
    index = i1.cas("insert", "candy", [1, "marshmallow", 2.7])
    i1.raft_wait_index(index, 3)
    index = i1.cas("insert", "candy", [2, "milk chocolate", 6.9])
    i1.raft_wait_index(index, 3)
    i2.raft_wait_index(index, 3)
    i3.raft_wait_index(index, 3)
    i4.raft_wait_index(index, 3)
    i5.raft_wait_index(index, 3)

    # Dml applied ok
    expected_tuples = [
        [1, "marshmallow", 2.7],
        [2, "milk chocolate", 6.9],
    ]
    assert i1.call("box.space.candy:select") == expected_tuples
    assert i2.call("box.space.candy:select") == expected_tuples
    assert i3.call("box.space.candy:select") == expected_tuples
    assert i4.call("box.space.candy:select") == expected_tuples
    assert i5.call("box.space.candy:select") == expected_tuples

    # These will be catching up
    i4.terminate()
    i5.terminate()

    # More DML
    index = i1.cas("replace", "candy", [2, "dark chocolate", 13.37])
    i1.raft_wait_index(index, 3)
    index = i1.cas("delete", "candy", [1])
    i1.raft_wait_index(index, 3)
    index = i1.cas("insert", "candy", [3, "ice cream", 0.3])
    i1.raft_wait_index(index, 3)
    i2.raft_wait_index(index, 3)
    i3.raft_wait_index(index, 3)

    # Dml applied ok again
    expected_tuples = [
        [2, "dark chocolate", 13.37],
        [3, "ice cream", 0.3],
    ]
    assert i1.call("box.space.candy:select") == expected_tuples
    assert i2.call("box.space.candy:select") == expected_tuples
    assert i3.call("box.space.candy:select") == expected_tuples

    # Compact raft log to trigger snapshot generation
    i1.raft_compact_log()
    i2.raft_compact_log()
    i3.raft_compact_log()

    # Master catch up by snapshot
    i4.start()
    i4.wait_online()
    assert i4.call("box.space.candy:select") == expected_tuples

    # Follower catch up by snapshot
    i5.start()
    i5.wait_online()
    assert i5.call("box.space.candy:select") == expected_tuples

    # Master boot by snapshot
    i6 = cluster.add_instance(wait_online=True, replicaset_id="r3")
    assert i6.call("box.space.candy:select") == expected_tuples

    # Follower boot by snapshot
    i7 = cluster.add_instance(wait_online=True, replicaset_id="r3")
    assert i7.call("box.space.candy:select") == expected_tuples
