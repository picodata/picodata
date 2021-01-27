#!/usr/bin/env tarantool

box.cfg{
    listen = 3301,
    memtx_memory = 48 * 1024 * 1024 * 1024,
    net_msg_max = 500000,
    readahead = 1024 * 1024,
    wal_mode = 'none'
}

-- Init test database
box.once('bootstrap_bench', function()
    box.schema.user.create('bench_user', { password = 'password' })
    box.schema.user.grant('bench_user', 'read,write,execute,create,drop', 'universe')

    local bench_s1 = box.schema.space.create('bench_s1')
    bench_s1:format{
        {name = 'id', type = 'unsigned'},
        {name = 'text', type = 'string' }
    }
    bench_s1:create_index('primary', {type = 'TREE', parts = {{ field = 1, type = 'unsigned' }}})
end)

-- Add test runner library location to lua search path
package.cpath = 'target/debug/?.so;' .. package.cpath

-- Prepare config
json = require('json')
cfg = json.encode({bench = true})

-- Run tests
local test_main = require('libtarantool_module_test_runner')
local exit_code = test_main(cfg)
os.exit(exit_code)
