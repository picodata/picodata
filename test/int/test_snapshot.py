from conftest import Cluster
from conftest import Retriable
import time

_3_SEC = 3


def test_bootstrap_from_snapshot(cluster: Cluster):
    [i1] = cluster.deploy(instance_count=1)

    ret = i1.cas("insert", "_pico_property", ["animal", "horse"])
    i1.raft_wait_index(ret, _3_SEC)
    assert i1.raft_read_index(_3_SEC) == ret

    # Compact the whole log
    assert i1.raft_compact_log() == ret + 1

    # Ensure i2 bootstraps from a snapshot
    i2 = cluster.add_instance(wait_online=True)
    # Whenever a snapshot is applied, all preceeding raft log is
    # implicitly compacted. Adding an instance implies appending 3
    # entries to the raft log. i2 catches them via a snapshot.
    assert i2.raft_first_index() == i1.raft_first_index() + 3

    # Ensure new instance replicates the property
    assert i2.call("box.space._pico_property:get", "animal") == ["animal", "horse"]


def test_catchup_by_snapshot(cluster: Cluster):
    i1, i2, i3 = cluster.deploy(instance_count=3)
    i1.assert_raft_status("Leader")
    ret = i1.cas("insert", "_pico_property", ["animal", "tiger"])

    i3.raft_wait_index(ret, _3_SEC)
    assert i3.call("box.space._pico_property:get", "animal") == ["animal", "tiger"]
    assert i3.raft_first_index() == 1
    i3.terminate()

    i1.cas("delete", "_pico_property", ["animal"])
    ret = i1.cas("insert", "_pico_property", ["tree", "birch"])

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
    i1, i2, i3 = cluster.deploy(instance_count=3)

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
        N=4 * 1024 * 1024 / 8,
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
    i3.terminate()

    for i in [i1, i2]:
        def_prepare_samples(i)
        start_preparing_data(i)

    for i in [i1, i2]:
        wait_data_prepared(i)

    index = cluster.cas("insert", "_pico_property", ["big", "data"])
    cluster.raft_wait_index(index)

    i1.raft_compact_log()
    i2.raft_compact_log()

    # First i1 is leader and i3 starts reading snapshot from it.
    i1.promote_or_fail()

    t_i3 = time.time()
    # Restart the instance triggering the chunky snapshot application.
    i3.env["PICODATA_SCRIPT"] = script_path
    i3.start()

    # Wait for i3 to start receiving the snapshot
    Retriable(10, 60).call(
        lambda: assert_eq(i3._raft_status().main_loop_status, "receiving snapshot")
    )

    # In the middle of snapshot application propose a new entry
    index = cluster.cas("insert", "_pico_property", ["pokemon", "snap"])
    for i in [i1, i2]:
        i.raft_wait_index(index)

    # Add a new instance so that it starts reading the same snapshot
    t_i4 = time.time()
    i4 = cluster.add_instance(wait_online=False)
    i4.env["PICODATA_SCRIPT"] = script_path
    i4.start()

    # Wait for i4 to start receiving the snapshot
    Retriable(10, 60).call(
        lambda: assert_eq(i3._raft_status().main_loop_status, "receiving snapshot")
    )

    i1.raft_compact_log()
    i2.raft_compact_log()

    # At some point i2 becomes leader but i3 keeps reading snapshot from i1.
    i2.promote_or_fail()

    i3.wait_online(timeout=30 - (time.time() - t_i3))
    print(f"i3 catching up by snapshot took: {time.time() - t_i3}s")

    i4.wait_online(timeout=20 - (time.time() - t_i4))
    print(f"i4 booting up by snapshot took: {time.time() - t_i4}s")

    #
    # Check snapshot was applied correctly.
    #
    assert i3.call("box.space._pico_property:get", "big") == ["big", "data"]

    expected_count = i1.call("box.space._pico_property:count")
    assert isinstance(expected_count, int)
    assert i3.call("box.space._pico_property:count") == expected_count

    def_prepare_samples(i3)
    i3.eval(
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
