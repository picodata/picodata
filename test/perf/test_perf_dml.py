from conftest import Cluster
from framework.log import log


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
