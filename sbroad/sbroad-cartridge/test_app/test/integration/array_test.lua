local t = require('luatest')
local g = t.group('array_field')
local helper = require('test.helper.cluster_no_replication')

g.before_all(
  function()
    --helper.stop_test_cluster()

    local cfg = {
      schema = {
        spaces = {
          t = {
            format = {
              {
                name = "id",
                type = "integer",
                is_nullable = false,
              },
              {
                name = "a",
                type = "array",
                is_nullable = false,
              },
              {
                name = "bucket_id",
                type = "unsigned",
                is_nullable = true,
              },
            },
            temporary = false,
            engine = "memtx",
            indexes = {
              {
                unique = true,
                parts = {
                  {
                    path = "id",
                    type = "integer",
                    is_nullable = false,
                  },
                },
                type = "TREE",
                name = "id",
              },
              {
                unique = false,
                parts = {
                  {
                    path = "bucket_id",
                    type = "unsigned",
                    is_nullable = true,
                  },
                },
                type = "TREE",
                name = "bucket_id",
              },
            },
            is_local = false,
            sharding_key = { "id" },
            sharding_func = "crud_sharding_func"
          },
        }
      }
    }

    helper.start_test_cluster(cfg)

    local api = helper.cluster:server("api-1").net_box
    local _, err = api:call('crud.insert', {'t', {1, { 1, 2, 'a' }, box.NULL}})
    t.assert_equals(err, nil)
  end
)

g.after_all(function()
  helper.stop_test_cluster()
end)

-- TODO: https://git.picodata.io/picodata/picodata/sbroad/-/issues/260
-- This test check that we can read from system tables in picodata,
-- but still we don't support arrays in our IR completely.
g.test_array_read = function ()
  local api = helper.cluster:server("api-1").net_box

  local r, err = api:call("sbroad.execute", { [[select "id", "a" from "t"]], {}})
  t.assert_equals(err, nil)
  t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "a", type = "array"},
        },
        rows = {
          { 1, { 1, 2, 'a' }}
        },
    })
end
