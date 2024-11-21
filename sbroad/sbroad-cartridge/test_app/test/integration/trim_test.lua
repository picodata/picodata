
local t = require('luatest')
local g = t.group('integration_api.trim')

local helper = require('test.helper.cluster_no_replication')

g.before_all(
        function()
            helper.start_test_cluster(helper.cluster_config)
            local api = helper.cluster:server("api-1").net_box

            local r, err = api:call("sbroad.execute",
                    { [[ INSERT INTO "t"("id", "a") VALUES (112211, 2211) ]], }
            )
            t.assert_equals(err, nil)
            t.assert_equals(r, {row_count = 1})
        end
)

g.after_all(function()
    helper.stop_test_cluster()
end)

g.test_trim = function ()
    local api = helper.cluster:server("api-1").net_box

    -- basic trim test
    local r, err = api:call("sbroad.execute",
            { [[SELECT trim('  aabb  ') as "a" from "t"]], }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = { {name = "a", type = "string"}, },
        rows = { { 'aabb' } },
    })

    -- trim inside trim
    r, err = api:call("sbroad.execute",
            { [[SELECT trim(trim('  aabb  ')) from "t"]], }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r["rows"], { { 'aabb' } })

    -- trim with literal in pattern
    r, err = api:call("sbroad.execute",
            { [[SELECT trim('a' from trim('  aabb  ')) from "t"]], }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r["rows"], { { 'bb' } })

    -- trim with expression in pattern
    r, err = api:call("sbroad.execute",
            { [[SELECT trim(trim(' aabb ') from trim('  aabb  ')) from "t"]], }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r["rows"], { { '' } })

    -- trim with leading modifier
    r, err = api:call("sbroad.execute",
            { [[SELECT trim(leading 'a' from trim('aabb  ')) from "t"]], }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r["rows"], { { 'bb' } })

    -- trim with trailing modifier
    r, err = api:call("sbroad.execute",
            { [[SELECT trim(trailing 'b' from trim('aabb')) from "t"]], }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r["rows"], { { 'aa' } })

    -- trim with both modifier
    r, err = api:call("sbroad.execute",
            { [[SELECT trim(both 'ab' from 'aabb') from "t"]], }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r["rows"], { { '' } })
end
