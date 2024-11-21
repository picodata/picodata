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
        vehicle_actual = {
            format = {
                {
                    name = "id",
                    type = "integer",
                    is_nullable = false,
                },
                {
                    name = "gov_number",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "sys_op",
                    type = "number",
                    is_nullable = false,
                },
                {
                    name = "bucket_id",
                    type = "unsigned",
                    is_nullable = true,
                },
            },
            temporary = false,
            engine = "vinyl",
            indexes = {
                {
                    unique = true,
                    parts = {
                        {
                            path = "id",
                            type = "integer",
                            is_nullable = false,
                        },
                        {
                            path = "sys_op",
                            type = "number",
                            is_nullable = false,
                        }
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
        },
        vehicle_history = {
            format = {
                {
                    name = "id",
                    type = "integer",
                    is_nullable = false,
                },
                {
                    name = "gov_number",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "sys_op",
                    type = "number",
                    is_nullable = false,
                },
                {
                    name = "bucket_id",
                    type = "unsigned",
                    is_nullable = true,
                },
            },
            temporary = false,
            engine = "vinyl",
            indexes = {
                {
                    unique = true,
                    parts = {
                        {
                            path = "id",
                            type = "integer",
                            is_nullable = false,
                        },
                        {
                            path = "sys_op",
                            type = "number",
                            is_nullable = false,
                        }
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

_, err = storage1:eval("return box.space.vehicle_actual:truncate();")
fail_if_error(err, "Failed to truncate vehicle_actual on storage1")
_, err = storage1:eval("return box.space.vehicle_history:truncate();")
fail_if_error(err, "Failed to truncate vehicle_history on storage1")
_, err = storage2:eval("return box.space.vehicle_actual:truncate();")
fail_if_error(err, "Failed to truncate vehicle_actual on storage2")
_, err = storage2:eval("return box.space.vehicle_history:truncate();")
fail_if_error(err, "Failed to truncate vehicle_history on storage2")

for i = 1, record_count, 1 do
    _, err = api:call("sbroad.execute", {
        [[insert into "vehicle_actual" ("id", "gov_number", "sys_op") values (?, ?, ?)]],
        {i, "a777a750", 0}
    })
    fail_if_error(err, "Failed to insert record into vehicle_actual")

    _, err = api:call("sbroad.execute", {
        [[insert into "vehicle_history" ("id", "gov_number", "sys_op") values (?, ?, ?)]],
        {i, "a777a750", 0}
    })
    fail_if_error(err, "Failed to insert record into vehicle_history")
end
api:close()

print("Finish data loading")
fiber.sleep(1)

print(
        string.format(
                "Storage 1 has %d actual and %d history records",
                storage1:eval("return box.space.vehicle_actual:count();"),
                storage1:eval("return box.space.vehicle_history:count();")
        )
)

print(
        string.format(
                "Storage 2 has %d actual and %d history records",
                storage2:eval("return box.space.vehicle_actual:count();"),
                storage2:eval("return box.space.vehicle_history:count();")
        )
)

print(

)

storage1:close()
storage2:close()

print(string.format("Execution time: %f s", (clock.time() - start_time) ))
