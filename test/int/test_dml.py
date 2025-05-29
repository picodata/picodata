from conftest import Cluster
from framework.log import log


def test_global_space_dml_catchup_by_log(cluster: Cluster):
    # Leader
    i1 = cluster.add_instance(wait_online=True, replicaset_name="r1")
    # For quorum
    i2 = cluster.add_instance(wait_online=True, replicaset_name="r1")
    # For quorum
    i3 = cluster.add_instance(wait_online=True, replicaset_name="r1")
    # Catcher-upper replicaset master
    i4 = cluster.add_instance(wait_online=True, replicaset_name="r2")
    # Catcher-upper replicaset follower
    i5 = cluster.add_instance(wait_online=True, replicaset_name="r2")

    cluster.create_table(
        dict(
            id=812,
            name="candy",
            format=[
                dict(name="id", type="unsigned", is_nullable=False),
                dict(name="kind", type="string", is_nullable=False),
                dict(name="kilos", type="double", is_nullable=False),
            ],
            primary_key=["id"],
            distribution="global",
        ),
    )

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
    index = i1.cas("delete", "candy", key=[1])
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
    i6 = cluster.add_instance(wait_online=True, replicaset_name="r3")
    assert i6.call("box.space.candy:select") == expected_tuples

    # Follower boot by log
    i7 = cluster.add_instance(wait_online=True, replicaset_name="r3")
    assert i7.call("box.space.candy:select") == expected_tuples


def test_global_space_dml_catchup_by_snapshot(cluster: Cluster):
    # Leader
    i1 = cluster.add_instance(wait_online=True, replicaset_name="r1")
    # For quorum
    i2 = cluster.add_instance(wait_online=True, replicaset_name="r1")
    # For quorum
    i3 = cluster.add_instance(wait_online=True, replicaset_name="r1")
    # Catcher-upper replicaset master
    i4 = cluster.add_instance(wait_online=True, replicaset_name="r2")
    # Catcher-upper replicaset follower
    i5 = cluster.add_instance(wait_online=True, replicaset_name="r2")

    cluster.create_table(
        dict(
            id=812,
            name="candy",
            format=[
                dict(name="id", type="unsigned", is_nullable=False),
                dict(name="kind", type="string", is_nullable=False),
                dict(name="kilos", type="double", is_nullable=False),
            ],
            primary_key=["id"],
            distribution="global",
        )
    )

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
    index = i1.cas("delete", "candy", key=[1])
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
    i6 = cluster.add_instance(wait_online=True, replicaset_name="r3")
    assert i6.call("box.space.candy:select") == expected_tuples

    # Follower boot by snapshot
    i7 = cluster.add_instance(wait_online=True, replicaset_name="r3")
    assert i7.call("box.space.candy:select") == expected_tuples


def test_global_dml_benchmark(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=True)
    i1.sql("""
        CREATE TABLE test (id TEXT PRIMARY KEY, flag BOOLEAN NOT NULL)
        USING MEMTX
        DISTRIBUTED GLOBALLY
    """)

    # Run performance critical code inside lua, to avoid spending time in RPC
    # and overall python slowness
    N, elapsed = i1.eval(
        """
        local uuid = require 'uuid'
        local fiber = require 'fiber'
        local math = require 'math'
        local log = require 'log'

        local data_set = {}

        log.info('preparing data')

        local N = 20 * 1000
        for i = 1, N do
            local key = tostring(uuid.new())
            local value = (math.random() < 0.5)
            table.insert(data_set, {key, value})
        end

        log.info('done preparing data: %d rows', N)

        local t0 = fiber.time()
        local tN = t0

        for i, row in ipairs(data_set) do
            local t = fiber.time()
            if t - tN > 1 then
                local elapsed_so_far = t - t0
                log.info('\x1b[32minserted so far: %d in %f seconds (RPS ~%f)\x1b[0m', i, elapsed_so_far, i / elapsed_so_far)
                tN = t
            end
            pico.sql('INSERT INTO test VALUES (?, ?)', row)
        end

        local elapsed = fiber.time() - t0
        log.info('done inserting data: %d rows in %f seconds (RPS ~%f)', N, elapsed, N / elapsed)

        return {N, elapsed}

        """,
        timeout=10 * 60,
    )

    rps = N / elapsed
    log.info(f"{N} insertions into a global table took {elapsed} seconds (RPS ~{rps})")

    # Nothing less is acceptable!
    assert rps == 100500
