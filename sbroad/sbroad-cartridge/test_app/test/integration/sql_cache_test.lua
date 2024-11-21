local t = require('luatest')
local g = t.group('configuration.sql_cache')

local helper = require('test.helper.cluster_sync_replication')
local cluster = nil

g.before_all(
    function()
        helper.start_test_cluster(helper.cluster_config)
        cluster = helper.cluster

        local storage1 = cluster:server("storage-1-1").net_box
        local storage2 = cluster:server("storage-2-1").net_box

        storage1:call("box.space.space_for_breake_cache:alter", { {is_sync = true}})
        storage1:call("box.execute", { [[truncate table "space_for_breake_cache"]] })
        storage2:call("box.execute", { [[truncate table "space_for_breake_cache"]] })
    end
)

g.after_all(
    function()
        local storage1 = cluster:server("storage-1-1").net_box
        storage1:call("box.execute", { [[truncate table "space_for_breake_cache"]] })

        local storage2 = cluster:server("storage-2-1").net_box
        storage2:call("box.execute", { [[truncate table "space_for_breake_cache"]] })
        helper.stop_test_cluster()
    end
)

g.test_change_cache_by_config_replica = function()
    local api = cluster:server("api-1").net_box
    local storage11 = cluster:server("storage-1-1").net_box
    local storage12 = cluster:server("storage-1-2").net_box
    local storage2 = cluster:server("storage-2-1").net_box

    -- config will be applied by sbroad after the first query is executed
    local _, err = api:call("sbroad.execute", { [[SELECT * FROM "space_for_breake_cache"]], {} })
    t.assert_equals(err, nil)

    local c = cluster:download_config()
    local cache_before_config = c["storage_cache_size_bytes"]
    t.assert_equals(20480000, cache_before_config)

    -- here we check that storage-1-1 is master
    t.assert_equals(false, storage11:eval("return box.info.ro"))

    -- config was applied on the master because the first query was 'select *' and it was run on the master
    t.assert_equals(cache_before_config, storage11:eval("return box.cfg.sql_cache_size"))
    -- but on the replica cache params won't be updated
    t.assert_not_equals(cache_before_config, storage12:eval("return box.cfg.sql_cache_size"))
    -- storage2 is master so the query was executed there and the cache was updated
    t.assert_equals(cache_before_config, storage2:eval("return box.cfg.sql_cache_size"))

    local cache_after_config = 4239361
    c["storage_cache_size_bytes"] = cache_after_config
    cluster:upload_config(c)

    -- config will be applied by sbroad on the replica after the first query is executed
    _, err = api:call("sbroad.execute", { [[SELECT * FROM "space_for_breake_cache"]], {} })
    t.assert_equals(err, nil)

    t.assert_equals(false, storage11:eval("return box.info.ro"))

    -- on the master config was applied, because `select *` was executed on master
    t.assert_equals(cache_after_config, storage11:eval("return box.cfg.sql_cache_size"))
    -- but on the replica cache params were not updated
    t.assert_not_equals(cache_after_config, storage12:eval("return box.cfg.sql_cache_size"))
    -- storage2 is master so the query was executed there and the cache was updated
    t.assert_equals(cache_after_config, storage2:eval("return box.cfg.sql_cache_size"))
end
