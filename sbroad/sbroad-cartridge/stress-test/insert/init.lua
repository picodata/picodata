#!/usr/bin/env tarantool

local nb = require("net.box")
local fiber = require("fiber")
local yaml = require('yaml')
local clock = require("clock")

local start_time = clock.time();

local api = nb.connect("admin:app-cluster-cookie@localhost:3301")

local function fail_if_error(err, msg)
    if err ~= nil then
        error(msg .. ": " .. err)
        os.exit(1)
    end
end

local _, err = api:eval("function set_schema(s) local cartridge = require('cartridge'); cartridge.set_schema(s) end")
fail_if_error(err, "Failed to set schema")

local config = {
    spaces = {
        t = {
            format = {
                {
                    name = "id",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "name",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "product_units",
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
                            path = "id",
                            type = "string",
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
        },
    }
}

_, err = api:call("set_schema", { yaml.encode(config) })
fail_if_error(err, "Failed to set schema")
print("Initialized the schema")

fiber.sleep(1)

local storage1 = nb.connect("admin:app-cluster-cookie@localhost:3302")
local storage2 = nb.connect("admin:app-cluster-cookie@localhost:3304")

print("Start space truncation on the storages")
_, err = storage1:eval("return box.space.t:truncate();")
fail_if_error(err, "Failed to truncate space on storage1")
_, err = storage2:eval("return box.space.t:truncate();")
fail_if_error(err, "Failed to truncate space on storage2")
print("Finish truncation")

api:close()
storage1:close()
storage2:close()

fiber.sleep(3)

print(string.format("Execution time: %f s", (clock.time() - start_time) ))
