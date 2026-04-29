import time

from conftest import Cluster, Retriable


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

    def wait_data_prepared(i):
        def inner():
            assert i.call("box.space.BIG:count") == param["N"]

        Retriable().call(inner)

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
    cluster.wait_leader_elected()

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

    # Transfer leadership to i2 while i4 keeps reading snapshot from i1.
    i1.raft_transfer_leadership(i2.raft_id)

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
