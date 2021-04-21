#!/usr/bin/env tarantool

fiber = require('fiber')

box.cfg{
    listen = 3301,
}

-- Init test database
box.once('bootstrap_tests', function()
    box.schema.user.create('test_user', { password = 'password' })
    box.schema.user.grant('test_user', 'read,write,execute,create,drop', 'universe')

    box.schema.sequence.create('test_seq')

    box.schema.func.create('test_stored_proc')
    box.schema.func.create('test_schema_update')
    box.schema.func.create('test_schema_cleanup')
end)

function test_stored_proc(a, b)
    return a + b
end

function test_timeout()
    fiber.sleep(1.5)
end

function test_schema_update()
    box.schema.space.create('test_s_tmp')
end

function test_schema_cleanup()
    box.space.test_s_tmp:drop()
end

-- Add test runner library location to lua search path
package.cpath = 'target/debug/?.so;' .. package.cpath

-- Run tests
local test_main = require('libtarantool_module_test_runner')
local exit_code = test_main()
os.exit(exit_code)
