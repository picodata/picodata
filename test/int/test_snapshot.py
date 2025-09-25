import time

import pytest

from conftest import Cluster, Retriable, log_crawler


_3_SEC = 3


def test_bootstrap_from_snapshot(cluster: Cluster):
    [i1] = cluster.deploy(instance_count=1)

    ret, _ = i1.cas("insert", "_pico_property", ["animal", "horse"])
    i1.raft_wait_index(ret, _3_SEC)
    assert i1.raft_read_index(_3_SEC) == ret

    # Compact the log up to insert
    i1.raft_compact_log(ret + 1)

    # Ensure i2 bootstraps from a snapshot
    i2 = cluster.add_instance(wait_online=True)
    # The snapshot was receieved and applied.
    # Several entries related to joining an instance have already been committed
    # on i1 after the compaction, so i2 first index from snapshot will be higher.
    assert i2.raft_first_index() > i1.raft_first_index()

    # Ensure new instance replicates the property
    assert i2.call("box.space._pico_property:get", "animal") == ["animal", "horse"]


def test_revoke_default_privileges_then_bootstrap_from_raft_snapshot(cluster: Cluster):
    user_name = "leeroy"
    impossible_error_on_select = "_pico_privilege cannot be empty"
    select_user_privileges = f"""
    SELECT
        p.*
    FROM
        _pico_privilege p
    JOIN
        _pico_user gr ON p.grantee_id = gr.id
    WHERE
        gr.name = '{user_name}';
    """

    i1, i2 = cluster.deploy(instance_count=2)
    i1.promote_or_fail()
    i1.assert_raft_status("Leader")

    i1.sql(f"CREATE USER {user_name} WITH PASSWORD 'J333333nkins'")
    i1.sql(f"REVOKE ALTER ON USER {user_name} FROM {user_name}")

    before = i1.sql(select_user_privileges)
    assert before, impossible_error_on_select

    i1.raft_compact_log()
    i2.raft_compact_log()

    i3 = cluster.add_instance(wait_online=True)

    after = i3.sql(select_user_privileges)
    assert after, impossible_error_on_select

    error = "reset to default privileges occurred which should not have happened"
    assert before == after, error


@pytest.mark.flaky(reruns=3)
def test_catchup_by_snapshot(cluster: Cluster):
    """
    flaky: https://git.picodata.io/core/picodata/-/issues/1029
    """
    i1, i2, i3 = cluster.deploy(instance_count=3)
    i1.assert_raft_status("Leader")
    ret, _ = i1.cas("insert", "_pico_property", ["animal", "tiger"])

    i3.raft_wait_index(ret, _3_SEC)
    assert i3.call("box.space._pico_property:get", "animal") == ["animal", "tiger"]
    assert i3.raft_first_index() == 1
    i3.terminate()

    i1.cas("delete", "_pico_property", key=["animal"])
    ret, _ = i1.cas("insert", "_pico_property", ["tree", "birch"])

    for i in [i1, i2]:
        i.raft_wait_index(ret, _3_SEC)
        assert i.raft_compact_log() == ret + 1

    # Ensure i3 is able to sync raft using a snapshot
    i3.start()
    i3.wait_online()

    assert i3.call("box.space._pico_property:get", "animal") is None
    assert i3.call("box.space._pico_property:get", "tree") == ["tree", "birch"]

    # Since there were no cas requests since log compaction, the indexes
    # should be equal.
    assert i3.raft_first_index() == ret + 1

    # There used to be a problem: catching snapshot used to cause
    # inconsistent raft state so the second restart used to panic.
    i3.terminate()
    i3.start()
    i3.wait_online()


def assert_eq(lhs, rhs):
    assert lhs == rhs


def test_large_snapshot(cluster: Cluster):
    i1, i2, i3, i4 = cluster.deploy(instance_count=4)

    # TODO: rewrite using clusterwide settings when we implement those
    script_path = f"{cluster.data_dir}/postjoin.lua"
    with open(script_path, "w") as f:
        f.write(
            """\
            box.cfg { memtx_memory = 4294967296 }
            """
        )

    param = dict(
        # Number of tuples to insert
        # N=4 * 1024 * 1024,
        N=4 * 1024 * 1024 / 32,
        # Average size of each tuple (approximate, doesn't include key and meta)
        T=512,
        # Max deviation from the average tuple size
        D=480,
        # Number of unique tuple sizes (resolution of sample space)
        R=32,
    )

    def def_prepare_samples(i):
        i.eval(
            """\
            local digest = require 'digest'
            local param = ...

            function prepare_samples()
                local values = {}
                for i = 0, param.R-1 do
                    local l = param.T + (i * 2 / (param.R - 1) - 1) * param.D;
                    local c = string.char(string.byte('A') + i)
                    values[i] = string.rep(c, l)
                end

                local samples = {}
                samples.values = values
                samples.param = param
                samples.get = function(self, i)
                    local R = self.param.R
                    local idx = digest.xxhash32(tostring(i)) % R
                    return self.values[idx]
                end

                return samples
            end
            """,
            param,
        )

    def start_preparing_data(i):
        # NOTE: we're inserting into the global space directly and not via CaS
        # because we don't want this test to run for days.
        i.eval(
            """\
            local fiber = require 'fiber'
            fiber.new(function()
                local log = require 'log'
                local math = require 'math'
                local json = require 'json'

                box.cfg { memtx_memory = 4294967296 }

                local samples = prepare_samples()
                log.info(("inserting tuples into space 'BIG' (param: %s)..."):format(json.encode(samples.param)))
                local N = samples.param.N
                local K = math.floor(N / 20);
                local t0 = fiber.clock()
                local S = 0
                local SDt = 0

                box.begin()
                for i = 0, N-1 do
                    if math.fmod(i+1, K) == 0 then
                        box.commit()
                        log.info(('insertion progress: %d%%'):format( 100 * i / (N-1) ))
                        fiber.yield()
                        box.begin()
                    end
                    local value = samples:get(i)
                    local t = box.space.BIG:put{ i, value }

                    local s = t:bsize()
                    S = S + s
                    SDt = SDt + s * s / N
                end
                box.commit()

                local E = S / N
                SD = math.sqrt(SDt - E * E)

                log.info(("done inserting data into space 'BIG', (%f secs, S: %.02fMB, E: %.02fB, SD: %.02fB)")
                    :format(fiber.clock() - t0, S / 1024 / 1024, E, SD))
            end)
        """  # noqa: E501
        )

    def wait_data_prepared(i, timeout=20):
        def inner():
            assert i.call("box.space.BIG:count") == param["N"]

        Retriable(timeout, 5).call(inner)

    cluster.cas("replace", "_pico_property", ["snapshot_chunk_max_size", 1024 * 1024])
    cluster.create_table(
        dict(
            name="BIG",
            format=[
                dict(name="id", type="unsigned", is_nullable=False),
                dict(name="value", type="string", is_nullable=False),
            ],
            primary_key=["id"],
            distribution="global",
        )
    )

    # This one will be receiving a snapshot
    i4.terminate()

    for i in [i1, i2, i3]:
        def_prepare_samples(i)
        start_preparing_data(i)

    for i in [i1, i2, i3]:
        wait_data_prepared(i)

    index, _ = cluster.cas("insert", "_pico_property", ["big", "data"])
    cluster.raft_wait_index(index)

    i1.raft_compact_log()
    i2.raft_compact_log()
    i3.raft_compact_log()

    # First i1 is leader and i4 starts reading snapshot from it.
    i1.promote_or_fail()

    t_i4 = time.time()
    # Restart the instance triggering the chunky snapshot application.
    i4.env["PICODATA_SCRIPT"] = script_path
    i4.start()

    # In the middle of snapshot application propose a new entry
    index, _ = cluster.cas("insert", "_pico_property", ["pokemon", "snap"])
    for i in [i1, i2, i3]:
        i.raft_wait_index(index)

    # Add a new instance so that it starts reading the same snapshot
    t_i5 = time.time()
    i5 = cluster.add_instance(wait_online=False)
    i5.env["PICODATA_SCRIPT"] = script_path
    i5.start()

    i1.raft_compact_log()
    i2.raft_compact_log()
    i3.raft_compact_log()

    # At some point i2 becomes leader but i4 keeps reading snapshot from i1.
    i2.promote_or_fail()

    i4.wait_online(timeout=30 - (time.time() - t_i4))
    print(f"i4 catching up by snapshot took: {time.time() - t_i4}s")

    i5.wait_online(timeout=20 - (time.time() - t_i5))
    print(f"i5 booting up by snapshot took: {time.time() - t_i5}s")

    #
    # Check snapshot was applied correctly.
    #
    assert i4.call("box.space._pico_property:get", "big") == ["big", "data"]

    expected_count = i1.call("box.space._pico_property:count")
    assert isinstance(expected_count, int)
    assert i4.call("box.space._pico_property:count") == expected_count

    def_prepare_samples(i4)
    i4.eval(
        """\
        local log = require 'log'
        local math = require 'math'
        local fiber = require 'fiber'
        local json = require 'json'

        local samples = prepare_samples()
        log.info(("checking space 'BIG' contents (param: %s)"):format(json.encode(param)))
        local N = samples.param.N
        local K = math.floor(N / 20);
        local t0 = fiber.clock()
        local S = 0
        local SDt = 0

        for i = 0, N-1 do
            if math.fmod(i+1, K) == 0 then
                log.info(('checking progress: %d%%'):format( 100 * i / N-1 ))
                fiber.yield()
            end
            local t = box.space.BIG:get{i}
            local expected = samples:get(i)
            if t == nil then
                box.error(1, ("key %s not found in space 'BIG'"):format(i))
            elseif t[2] ~= expected then
                box.error(1, ("unexpected value for key '%s' got '%s' '%s' expected"):format(t[1], t[2], expected))
            end

            local s = t:bsize()
            S = S + s
            SDt = SDt + s * s / N
        end

        local E = S / N
        SD = math.sqrt(SDt - E * E)

        log.info(("done checking space 'BIG' contents, (%f secs, S: %.02fMB, E: %.02fB, SD: %.02fB)")
            :format(fiber.clock() - t0, S / 1024 / 1024, E, SD))
    """,  # noqa: E501
        timeout=10,
    )


def test_repeated_snapshot_after_repeated_compaction(cluster: Cluster):
    [i1] = cluster.deploy(instance_count=1)

    index, _ = i1.cas("insert", "_pico_property", ["googoo", "gaga"])
    i1.raft_wait_index(index)

    # Compact raft log to trigger creation of snapshot
    i1.raft_compact_log()
    i2 = cluster.add_instance(wait_online=True)

    index, _ = cluster.cas("insert", "_pico_property", ["booboo", "baba"])
    i1.raft_wait_index(index)
    i2.raft_wait_index(index)

    # Compact raft log again
    i1.raft_compact_log()
    i2.raft_compact_log()

    # A new snapshot is generated after the latest compaction
    i3 = cluster.add_instance(wait_online=True)

    # Compact raft log yet again
    i1.raft_compact_log()
    i2.raft_compact_log()
    i3.raft_compact_log()

    # Everything is still ok
    _ = cluster.add_instance(wait_online=True)


def test_snapshot_after_conf_change(cluster: Cluster):
    [i1, i2, i3] = cluster.deploy(instance_count=3)

    i3.terminate()

    index, _ = i1.cas("insert", "_pico_property", ["yoyo", "yaya"])
    i1.raft_wait_index(index)
    i2.raft_wait_index(index)

    # Compact raft log to trigger creation of snapshot
    i1.raft_compact_log()
    i2.raft_compact_log()

    i3.start()
    i3.wait_online()

    # The generated snapshot contains the uptodate conf state, otherwise
    # instance wouldn't've become online.
    _ = cluster.add_instance(wait_online=True)


def test_crash_before_applying_raft_snapshot(cluster: Cluster):
    [i1, i2, i3] = cluster.deploy(instance_count=3)

    i3.terminate()

    index, _ = i1.cas("insert", "_pico_property", ["yoyo", "yaya"])
    i1.raft_wait_index(index)
    i2.raft_wait_index(index)

    # Compact raft log to trigger creation of snapshot
    i1.raft_compact_log()
    i2.raft_compact_log()

    injected_error = "EXIT_BEFORE_APPLYING_RAFT_SNAPSHOT"
    lc = log_crawler(i3, injected_error)
    i3.env[f"PICODATA_ERROR_INJECTION_{injected_error}"] = "1"
    # Instance crashes after receiving a raft snapshot before applying it.
    i3.fail_to_start()
    lc.wait_matched()

    # After restart the Instance receives the snapshot again and applies it successfully.
    del i3.env[f"PICODATA_ERROR_INJECTION_{injected_error}"]
    i3.start()
    i3.wait_online()

    # Another instance also successfully joins (just checking)
    _ = cluster.add_instance(wait_online=True)


def test_snapshot_with_stale_schema_version(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        default:
        storage:
            can_vote: false
            replication_factor: 2
"""
    )
    leader = cluster.add_instance(tier="default", wait_online=False)
    storage_1_1 = cluster.add_instance(tier="storage", wait_online=False)
    storage_1_2 = cluster.add_instance(tier="storage", wait_online=False)
    cluster.wait_online()

    # Make sure storage_1_1 is master
    [[master_name]] = leader.sql("SELECT current_master_name FROM _pico_replicaset WHERE tier = 'storage'")
    if storage_1_1.name != master_name:
        storage_1_1, storage_1_2 = storage_1_2, storage_1_1

    # Increase the threshold so that log compaction isn't triggered before we need it
    leader.sql("ALTER SYSTEM SET raft_wal_count_max = 1024")

    # Make it so `storage_1_2` stops handling raft messages (log and snapshot updates).
    injected_error_1 = "IGNORE_ALL_RAFT_MESSAGES"
    storage_1_2.call("pico._inject_error", injected_error_1, True)

    injected_error_2 = "BLOCK_BEFORE_APPLYING_RAFT_SNAPSHOT"
    lc = log_crawler(storage_1_2, injected_error_2)
    storage_1_2.call("pico._inject_error", injected_error_2, True)

    # This is a pretty tricky situation to reproduce as you may notice
    injected_error_3 = "IGNORE_NEWER_SNAPSHOT"
    storage_1_2.call("pico._inject_error", injected_error_3, True)

    # Trigger log compaction now
    leader.sql("ALTER SYSTEM SET raft_wal_count_max = 1")

    # Fix raft message handling, now it will receive a raft snapshot instead of any raft log entries
    storage_1_2.call("pico._inject_error", injected_error_1, False)

    # Make sure the snapshot is received
    lc.wait_matched()

    # Make a schema change. WAIT APPLIED LOCALLY is needed because `storage_1_2`
    # is blocked by the injection
    leader.sql("CREATE TABLE life_is_good (i_like_life INTEGER PRIMARY KEY) DISTRIBUTED GLOBALLY WAIT APPLIED LOCALLY")

    # Wait until storage_1_2 synchronizes via tarantool replication stream.
    # This will make it so `_schema.local_schema_version` > `snapshot.schema_version` on it.
    master_vclock = storage_1_1.call(".proc_get_vclock")
    storage_1_2.call(".proc_wait_vclock", master_vclock, 10)

    # Disable the error and make sure the instance successfully catches up eventually
    storage_1_2.call("pico._inject_error", injected_error_2, False)

    applied = leader.call(".proc_get_index")
    storage_1_2.call(".proc_wait_index", applied, 10)

    leader.sql("INSERT INTO life_is_good VALUES (1), (2), (3)")
    rows = storage_1_2.sql("SELECT * FROM life_is_good")
    assert rows == [[1], [2], [3]]


# TODO: test case which would show if we sent snapshot conf state inconsistent
# with snapshot data. The situation should involve a conf change after the
# snapshot data was generated which would be incompatible with conf state at the
# moment the snapshot was requested.
