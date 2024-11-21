local t = require('luatest')
local g = t.group('sbroad_with_acl')

local helper = require('test.helper.cluster_no_replication')
local cluster = nil

g.before_all(
        function()
            helper.start_test_cluster(helper.cluster_config)
            cluster = helper.cluster
        end
)

g.after_all(
        function()
            helper.stop_test_cluster()
        end
)

g.test_drop_user = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call(
            "sbroad.execute",
            { [[ DROP USER user_name ]], {} }
    )
    t.assert_equals(
            string.format("%s", err),
            [[Sbroad Error: ACL queries are not supported]]
    )
end

g.test_drop_role = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call(
            "sbroad.execute",
            { [[ DROP ROLE role ]], {} }
    )
    t.assert_equals(
            string.format("%s", err),
            [[Sbroad Error: ACL queries are not supported]]
    )
end

g.test_create_role = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call(
            "sbroad.execute",
            { [[ CREATE ROLE role ]], {} }
    )
    t.assert_equals(
            string.format("%s", err),
            [[Sbroad Error: ACL queries are not supported]]
    )
end

g.test_create_user = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call(
            "sbroad.execute",
            { [[ CREATE USER "user" WITH PASSWORD '123' USING MD5 ]], {} }
    )
    t.assert_equals(
            string.format("%s", err),
            [[Sbroad Error: ACL queries are not supported]]
    )
end

g.test_alter_user = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call(
            "sbroad.execute",
            { [[ ALTER USER "user" WITH PASSWORD '123' USING MD5 ]], {} }
    )
    t.assert_equals(
            string.format("%s", err),
            [[Sbroad Error: ACL queries are not supported]]
    )
end

g.test_grant_privilege = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call(
            "sbroad.execute",
            { [[ GRANT READ ON TABLE "t" TO "role" ]], {} }
    )
    t.assert_equals(
            string.format("%s", err),
            [[Sbroad Error: ACL queries are not supported]]
    )
end

g.test_revoke_privilege = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call(
            "sbroad.execute",
            { [[ REVOKE READ ON TABLE "t" FROM "role" ]], {} }
    )
    t.assert_equals(
            string.format("%s", err),
            [[Sbroad Error: ACL queries are not supported]]
    )
end
