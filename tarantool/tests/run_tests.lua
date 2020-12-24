#!/usr/bin/env tarantool

fiber = require('fiber')

box.cfg{
    listen = 3301,
}

-- Init test database
box.once('bootstrap_tests', function()
    box.schema.user.create('test_user', { password = 'password' })
    box.schema.user.grant('test_user', 'read,write,execute,create,drop', 'universe')

    local test_s1 = box.schema.space.create('test_s1')
    test_s1:format{
        {name = 'id', type = 'unsigned'},
        {name = 'text', type = 'string' }
    }
    test_s1:create_index('primary', {type = 'TREE', parts = {{ field = 1, type = 'unsigned' }}})

    local test_s2 = box.schema.space.create('test_s2')
    test_s2:format{
        {name = 'id', type = 'unsigned'},
        {name = 'key', type = 'string'},
        {name = 'value', type = 'string'},
        {name = 'a', type = 'integer'},
        {name = 'b', type = 'integer'},
    }
    test_s2:create_index('primary', {type = 'TREE', parts = {{ field = 1, type = 'unsigned' }}})
    test_s2:create_index('idx_1', {type = 'HASH', parts = {{ field = 2, type = 'string' }}})
    test_s2:create_index('idx_2', {
        type = 'TREE',
        parts = {
            { field = 1, type = 'unsigned' },
            { field = 4, type = 'integer' },
            { field = 5, type = 'integer' },
        }
    })
    test_s2:create_index('idx_3', {type = 'TREE', unique=false, parts = {{ field = 4, type = 'integer' }}})
    for i = 1, 20 do
        test_s2:insert{i, 'key_' .. i, 'value_' .. i, i % 5, math.floor(i / 5)}
    end

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
