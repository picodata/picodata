local t = require('luatest')
local g = t.group('data_query_test')
local helper = require('test.helper.cluster_no_replication')
local data = require('test.data.test_data')

g.before_all(
    function()
      local cfg = {
        schema = {
          spaces = {
            VSA_PROXY = {
              format = {
                {
                  name = 'bucket_id',
                  type = 'unsigned',
                  is_nullable = false,
                },
                {
                  name = 'fid',
                  type = 'integer',
                  is_nullable = false,
                },
                {
                  name = 'date_start',
                  type = 'integer',
                  is_nullable = false,
                },
                {
                  name = 'date_end',
                  type = 'integer',
                  is_nullable = false,
                },
                {
                  name = 'common_id',
                  type = 'string',
                  is_nullable = false,
                },
                {
                  name = 'exclude_id',
                  type = 'string',
                  is_nullable = true
                },
                {
                  name = 'common_text',
                  type = 'string',
                  is_nullable = false,
                },
                {
                  name = 'common_detail',
                  type = 'string',
                  is_nullable = false,
                },
                {
                  name = 'typology_type',
                  type = 'integer',
                  is_nullable = true
                },
                {
                  name = 'typology_id',
                  type = 'string',
                  is_nullable = true
                }
              },
              temporary = false,
              engine = "memtx",
              indexes = {
                {
                  unique = true,
                  parts = {
                    {
                      path = "fid",
                      type = "integer",
                      is_nullable = false,
                    },
                    {
                      path = "common_id",
                      type = "string",
                      is_nullable = false,
                    },
                    {
                      path = "date_start",
                      type = "integer",
                      is_nullable = false,
                    },
                  },
                  type = "TREE",
                  name = "primary",
                },
                {
                  unique = false,
                  parts = {
                    {
                      path = "bucket_id",
                      type = "unsigned",
                      is_nullable = false,
                    },
                  },
                  type = "TREE",
                  name = "bucket_id",
                },
              },
              is_local = false,
              sharding_key = { "common_id" },
            }
          }
        }
      }

      helper.start_test_cluster(cfg)

      local api = helper.cluster:server("api-1").net_box

      for _, rec in pairs(data.vsa_proxy_records) do
        local _, err = api:call(
          "sbroad.execute",
          {
            [[INSERT INTO "VSA_PROXY" (
              "fid", "date_start", "date_end",
              "common_id", "exclude_id",
              "common_text", "common_detail",
              "typology_type", "typology_id"
             )
             VALUES (?,?,?,?,?,?,?,?,?)]],
            rec
          }
        )
        t.assert_equals(err, nil, rec)
      end

    end
)

g.after_all(function()
  helper.stop_test_cluster()
end)

g.test_schema_invalid = function ()
  local api = helper.cluster:server("api-1").net_box

  local _, err = api:call("sbroad.execute", { [[SELECT "a"."fid" AS "fid1", "b1"."fid2",
"a"."common_text" AS "common_text1", "b1"."common_text2", "a"."date_start" AS "date_sort1",
"a"."date_end" AS "date_end1", "b1"."date_sort2", "b1"."date_end2", "a"."common_detail" AS "common_detail1",
"b1"."common_detail2", "a"."typology_type", "a"."typology_id"
    FROM "VSA_PROXY" as "a"
      INNER JOIN (SELECT "b"."common_id" AS "common_id2", "b"."fid" AS "fid2",
                         "b"."exclude_id" AS "exclude_id2", "b"."common_text" AS "common_text2",
                         "b"."date_start" AS "date_sort2", "b"."date_end" AS "date_end2",
                         "b"."common_detail" AS "common_detail2"
                  FROM "VSA_PROXY" as "b") as "b1" ON "a"."common_id" = "b1"."common_id2"
    WHERE "a"."fid" IN (6659253, -21, 5933116, 8257405, 3676468, 6580234, 9557717)
      AND "b1"."fid2" IN (6659253, -21, 5933116, 8257405, 3676468, 6580234, 9557717)
      AND "a"."fid" < "b1"."fid2"
      AND ("a"."exclude_id" <> "b1"."exclude_id2" OR "a"."exclude_id" IS NULL)]], {}})
  t.assert_equals(err, nil)
end
