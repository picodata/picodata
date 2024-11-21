#!/usr/bin/env tarantool

local nb = require("net.box")
local fiber = require("fiber")
local yaml = require('yaml')
local clock = require("clock")

local params = { ... }

if #params ~= 1 then
    print("The amount of records to generate is required")
    os.exit(1)
end

local function fail_if_error(err, msg)
    if err ~= nil then
        error(msg .. ": " .. err)
        os.exit(1)
    end
end

local start_time = clock.time();

local record_count = params[1]

local api = nb.connect("admin:app-cluster-cookie@localhost:3301")

local _, err = api:eval("function set_schema(s) local cartridge = require('cartridge'); cartridge.set_schema(s) end")
fail_if_error(err, "Failed to create set_schema function")

local config = {
    spaces = {
        t = {
            format = {
                {
                    name = "a0",
                    type = "integer",
                    is_nullable = false,
                },
                {
                    name = "a1",
                    type = "integer",
                    is_nullable = false,
                },
                {
                    name = "a2",
                    type = "integer",
                    is_nullable = false,
                },
                {
                    name = "a3",
                    type = "integer",
                    is_nullable = false,
                },
                {
                    name = "a4",
                    type = "integer",
                    is_nullable = false,
                },
                {
                    name = "a5",
                    type = "integer",
                    is_nullable = false,
                },
                {
                    name = "a6",
                    type = "integer",
                    is_nullable = false,
                },
                {
                    name = "a7",
                    type = "integer",
                    is_nullable = false,
                },
                {
                    name = "a8",
                    type = "integer",
                    is_nullable = false,
                },
                {
                    name = "a9",
                    type = "integer",
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
                            path = "a0",
                            type = "integer",
                            is_nullable = false,
                        },
                    },
                    type = "TREE",
                    name = "a0",
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
            sharding_key = { "a0" },
        },
    }
}

_, err = api:call("set_schema", { yaml.encode(config) })
fail_if_error(err, "Failed to set schema")
print("Initialized the schema")

fiber.sleep(1)
print("Start data loading")

local storage1 = nb.connect("admin:app-cluster-cookie@localhost:3302")
local storage2 = nb.connect("admin:app-cluster-cookie@localhost:3304")

_, err = storage1:eval("return box.space.t:truncate();")
fail_if_error(err, "Failed to truncate t on storage1")
_, err = storage2:eval("return box.space.t:truncate();")
fail_if_error(err, "Failed to truncate t on storage2")

for i = 1, record_count, 1 do
    _, err = api:call("sbroad.execute", {
        [[insert into "t" ("a0","a1","a2","a3","a4","a5","a6","a7","a8","a9")
        values (?,?,?,?,?,?,?,?,?,?)]],
        {i,i % 1,i % 2,i % 3,i % 4,i % 5,i % 6,i % 7,i % 8,i % 9}
    })
    fail_if_error(err, "Failed to insert record into t")
end
api:close()

print("Finish data loading")
fiber.sleep(1)

print(
        string.format(
                "Storage 1 has %d records",
                storage1:eval("return box.space.t:count();")
        )
)

print(
        string.format(
                "Storage 2 has %d records",
                storage2:eval("return box.space.t:count();")
        )
)

print(

)

storage1:close()
storage2:close()

print(string.format("Execution time: %f s", (clock.time() - start_time) ))
