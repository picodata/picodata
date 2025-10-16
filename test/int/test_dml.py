from conftest import Cluster, Instance
from framework.log import log
from framework.thread import spawn_thread
import time


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
    index, res_row_count = i1.cas("insert", "candy", [1, "marshmallow", 2.7])
    assert res_row_count == 1
    i1.raft_wait_index(index, 3)
    index, res_row_count = i1.cas("insert", "candy", [2, "milk chocolate", 6.9])
    assert res_row_count == 1
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
    index, res_row_count = i1.cas("replace", "candy", [2, "dark chocolate", 13.37])
    assert res_row_count == 1
    i1.raft_wait_index(index, 3)
    index, _ = i1.cas("delete", "candy", key=[1])
    i1.raft_wait_index(index, 3)
    index, res_row_count = i1.cas("insert", "candy", [3, "ice cream", 0.3])
    assert res_row_count == 1
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
    index, res_row_count = i1.cas("insert", "candy", [1, "marshmallow", 2.7])
    assert res_row_count == 1
    i1.raft_wait_index(index, 3)
    index, res_row_count = i1.cas("insert", "candy", [2, "milk chocolate", 6.9])
    assert res_row_count == 1
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
    index, res_row_count = i1.cas("replace", "candy", [2, "dark chocolate", 13.37])
    assert res_row_count == 1
    i1.raft_wait_index(index, 3)
    index, res_row_count = i1.cas("delete", "candy", key=[1])
    i1.raft_wait_index(index, 3)
    index, res_row_count = i1.cas("insert", "candy", [3, "ice cream", 0.3])
    assert res_row_count == 1
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


def test_vinyl_tmp_table(cluster: Cluster):
    # Check insertion into temporary table for vinyl engine
    i1 = cluster.add_instance(wait_online=True, replicaset_name="r1")
    i2 = cluster.add_instance(wait_online=True, replicaset_name="r1")

    ddl = i1.sql(
        """
        create table v1 (a int primary key, b int) using vinyl distributed by (a);
"""
    )
    assert ddl["row_count"] == 1

    ddl = i1.sql(
        """
        create table v2 (a int primary key, b int) using vinyl distributed by (a);
"""
    )
    assert ddl["row_count"] == 1

    dml = i1.sql(
        """
        insert into v1 values (1, 1), (3, 3), (5, 5), (7, 7), (9, 9);
"""
    )
    assert dml["row_count"] == 5

    dml = i1.sql(
        """
        insert into v2 values (2, 2), (4, 4), (6, 6), (8, 8), (10, 10);
"""
    )
    assert dml["row_count"] == 5

    ro_replica = i2
    if not ro_replica.eval("return box.cfg.read_only"):
        ro_replica = i1
        # i1 became rw replica between i2.eval('return box.cfg.read_only') and i1.eval('return box.cfg.read_only')
        assert ro_replica.eval("return box.cfg.read_only")

    dml = ro_replica.sql(
        """
        insert into v1 (a, b) select * from v2;
"""
    )

    assert dml["row_count"] == 5

    def check_table_row_count(table_name, expected_count):
        dql = ro_replica.sql(
            f"""
            select * from {table_name};
"""
        )
        assert len(dql) == expected_count

    check_table_row_count("v1", 10)

    dml = ro_replica.sql(
        """
        insert into v1 select a + 10, b + 10 from v1;
"""
    )

    check_table_row_count("v1", 20)


def do_test_global_dml_contention_load(
    cluster: Cluster, instance_count, worker_count, update_count, raft_wal_count_max
):
    leader = cluster.add_instance(wait_online=False, enable_http=True)

    followers = []
    for _ in range(instance_count):
        follower = cluster.add_instance(wait_online=False, enable_http=True)
        followers.append(follower)

    cluster.wait_online()
    leader.promote_or_fail()

    leader.sql("""
        CREATE TABLE test (id INT PRIMARY KEY, counter INT NOT NULL)
        USING MEMTX
        DISTRIBUTED GLOBALLY
    """)
    leader.sql("INSERT INTO test VALUES (1, 0)")

    leader.sql(f"ALTER SYSTEM SET raft_wal_count_max = {raft_wal_count_max}")

    # Run performance critical code inside lua, to avoid spending time in RPC
    # and overall python slowness
    test_code = """
        local fiber = require 'fiber'
        local log = require 'log'

        local worker_count, update_count = ...

        local t0 = fiber.time()
        local tN = t0

        local fibers = {}

        log.info('starting %d worker fibers...', worker_count)

        for worker_index = 1, worker_count do
            local f = fiber.new(function()
                local instance_name = pico.instance_info().name
                fiber.name(string.format('%s:worker_%d', instance_name, worker_index))
                local start = fiber.time()

                for i = 1, update_count do
                    local t = fiber.time()
                    if t - tN > 1 then
                        local elapsed_so_far = t - t0
                        log.info('\x1b[32mdone so far: %d in %f seconds (RPS ~%f)\x1b[0m', i, elapsed_so_far, i / elapsed_so_far)
                        tN = t
                    end

                    ok, err = pico.sql('UPDATE test SET counter = counter + 1 WHERE id = 1')
                    if err ~= box.NULL then
                        error(err)
                    end
                end

                return fiber.time() - start
            end)
            f:set_joinable(true)

            table.insert(fibers, f)
        end

        log.info('waiting for %d workers to finish', worker_count)

        local durations = {}
        for _, f in ipairs(fibers) do
            local fiber_name = f:name()
            local ok, res = f:join()
            if not ok then
                error(res)
            end
            table.insert(durations, res)
            log.info('fiber %s finished in %d seconds', fiber_name, res)
        end


        local elapsed = fiber.time() - t0
        log.info('all done, elapsed %f seconds', elapsed)

        assert(row_count == total_row_count)

        return durations
    """

    rows = leader.sql("SELECT * FROM test")
    log.info(f"{rows=}")

    start = time.time()

    threads = []
    for follower in followers:
        t = spawn_thread(
            Instance.eval,
            (follower, test_code, worker_count, update_count),
            kwargs=dict(timeout=10 * 60),
            name=follower.name,
        )
        threads.append(t)

    # Hide the large text constant from pytest output in case of test failure
    del test_code

    stats = []

    results = dict()
    t0 = time.time()
    while threads:
        now = time.time()
        if now - t0 > 0.1:
            t0 = now
            [[counter]] = leader.sql("SELECT counter FROM test WHERE id = 1")
            per_instance = []
            for follower in followers:
                metrics = follower.get_metrics()
                [total, retries] = get_global_dml_metrics(metrics)
                per_instance.append((total, retries))
            stats.append((counter, per_instance))

        try:
            res = threads[0].join(timeout=0.3)
            t = threads.pop(0)
            results[t.args[0].name] = res
        except TimeoutError:
            continue

    total_elapsed = time.time() - start

    [[counter]] = leader.sql("SELECT counter FROM test WHERE id = 1")
    assert counter == instance_count * worker_count * update_count

    per_instance = []
    for follower in followers:
        metrics = follower.get_metrics()
        [total, retries] = get_global_dml_metrics(metrics)
        per_instance.append((total, retries))
    stats.append((counter, per_instance))

    cluster.kill()

    for stat in stats:
        log.info(f"{stat=}")

    instances = [follower.name for follower in followers]
    log.info(f"{instances=}")

    per_worker = []
    for k, v in results.items():
        per_worker.extend(v)
        timings = str.join(", ", [f"{t:.1f}" for t in v])
        log.info(f"{k}: {timings}")

    m, sd = mean_and_standard_deviation(per_worker)
    log.info(
        f"{instance_count=} {worker_count=} {update_count=} {raft_wal_count_max=} elapsed={total_elapsed:.1f} (mean {m:.1f}, sd {sd:.1f})"
    )

    return total_elapsed, m, sd


def mean_and_standard_deviation(samples):
    count = len(samples)
    mean = sum(samples) / count
    variance = sum(((s - mean) ** 2 for s in samples)) / count
    sd = variance**0.5
    return mean, sd


def get_global_dml_metrics(metrics):
    metric = metrics["pico_sql_global_dml_query"]
    [total] = [sample.value for sample in metric.samples]

    metric = metrics["pico_sql_global_dml_query_retries"]
    [retries] = [sample.value for sample in metric.samples]

    return total, retries


def test_global_dml_contention_load(cluster: Cluster):
    # (instance_count, worker_count, update_count, raft_wal_count_max)
    parameters = [
        (1, 1, 6000, 16),
        (4, 4, 375, 16),
    ]
    results = []
    for params in parameters:
        cluster.reset()
        res = do_test_global_dml_contention_load(cluster, *params)
        results.append((params, res))

    for params, result in results:
        instance_count, worker_count, update_count, raft_wal_count_max = params
        t, m, sd = result
        log.info(
            f"{instance_count=} {worker_count=} {update_count=} {raft_wal_count_max=} elapsed={t:.1f} (mean {m:.1f}, sd {sd:.1f})"
        )
