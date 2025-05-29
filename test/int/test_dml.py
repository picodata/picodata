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


def do_test_global_dml_benchmark(cluster: Cluster, instance_count, total_row_count, batch_size):
    assert total_row_count % batch_size == 0

    cluster.deploy(instance_count=instance_count)

    leader = cluster.leader()
    leader.sql("""
        CREATE TABLE test (id TEXT PRIMARY KEY, flag BOOLEAN NOT NULL)
        USING MEMTX
        DISTRIBUTED GLOBALLY
    """)

    # Run performance critical code inside lua, to avoid spending time in RPC
    # and overall python slowness
    benchmark_code = """
        local uuid = require 'uuid'
        local fiber = require 'fiber'
        local math = require 'math'
        local log = require 'log'
        local json = require 'json'

        local total_row_count, batch_size = ...
        assert(total_row_count % batch_size == 0)
        local batch_count = total_row_count / batch_size

        log.info('preparing data')

        local data_set = {}

        for _ = 1, batch_count do
            local batch = {}
            for _ = 1, batch_size do
                local key = tostring(uuid.new())
                local value = (math.random() < 0.5)
                table.insert(batch, key)
                table.insert(batch, value)
            end
            table.insert(data_set, batch)
        end

        local sql_query = 'INSERT INTO test VALUES (?, ?)'
        for _ = 1, batch_size - 1 do
            sql_query = sql_query .. ', (?, ?)'
        end
        log.info('sql_query: "%s"', sql_query)

        log.info('done preparing data: %d rows (%d batches %d rows each)', total_row_count, #data_set, #data_set[1])

        local t0 = fiber.time()
        local tN = t0
        local row_count = 0

        for _, batch in ipairs(data_set) do
            local t = fiber.time()
            if t - tN > 1 then
                local elapsed_so_far = t - t0
                log.info('\x1b[32minserted so far: %d in %f seconds (RPS ~%f)\x1b[0m', row_count, elapsed_so_far, row_count / elapsed_so_far)
                tN = t
            end

            ok, err = pico.sql(sql_query, batch)
            if err ~= box.NULL then
                error(err)
            end

            row_count = row_count + batch_size
        end

        local elapsed = fiber.time() - t0
        local rps = total_row_count / elapsed
        log.info('done inserting data: %d rows in %f seconds (RPS ~%f)', total_row_count, elapsed, rps)

        assert(row_count == total_row_count)

        return elapsed
    """

    rows = leader.sql("SELECT * FROM test")
    log.info(f"{rows=}")

    elapsed = leader.eval(benchmark_code, total_row_count, batch_size, timeout=10 * 60)

    # Hide the large text constant from pytest output in case of test failure
    del benchmark_code

    rows_per_second = total_row_count / elapsed
    log.info(
        f"{total_row_count} insertions into a global table took {elapsed} seconds (RPS ~{rows_per_second}) (batch size: {batch_size})"
    )

    return rows_per_second


def test_global_dml_benchmark_single_instance_no_batching(cluster: Cluster):
    rows_per_second = do_test_global_dml_benchmark(cluster, instance_count=1, total_row_count=20000, batch_size=1)
    # I'm observing ~1000 on my machine on debug build,
    # but we don't want this test to be flaky due to CI machine being overloaded
    assert rows_per_second > 200


def test_global_dml_benchmark_5_instances_no_batching(cluster: Cluster):
    # I'm observing ~500 on my machine on debug build,
    # but we don't want this test to be flaky due to CI machine being overloaded
    rows_per_second = do_test_global_dml_benchmark(cluster, instance_count=5, total_row_count=20000, batch_size=1)
    assert rows_per_second > 100


def test_global_dml_benchmark_single_instance_batching(cluster: Cluster):
    # I'm observing ~9000 on my machine on debug build,
    # but we don't want this test to be flaky due to CI machine being overloaded
    rows_per_second = do_test_global_dml_benchmark(cluster, instance_count=1, total_row_count=20000, batch_size=100)
    assert rows_per_second > 500


def test_global_dml_benchmark_5_instances_batching(cluster: Cluster):
    # I'm observing ~8000 on my machine on debug build,
    # but we don't want this test to be flaky due to CI machine being overloaded
    rows_per_second = do_test_global_dml_benchmark(cluster, instance_count=5, total_row_count=20000, batch_size=100)
    assert rows_per_second > 500
