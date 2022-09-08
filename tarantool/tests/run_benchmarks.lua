#!/usr/bin/env tarantool
json = require('json')

port = 3302
box.cfg{
    listen = port,
    wal_mode = 'none',
    memtx_memory = 1024 * 1024 * 1024,
}

-- Add test runner library location to lua search path
package.cpath = 'target/debug/?.so;' .. package.cpath

-- Init test database
box.once('bootstrap_benchs', function()
    box.schema.user.create('bench_user', { password = 'password' })
    box.schema.user.grant('bench_user', 'read,write,execute,create,drop', 'universe')

    box.schema.space.create('bench_s1'):create_index('pk')

    box.schema.func.create('_cleanup')
end)

function _cleanup()
    box.space.bench_s1:truncate()
end

-- Prepare config
cfg = json.encode {
    bench = true,
    listen = port,
}

-- Run tests
local test_main = require('libtarantool_module_test_runner')
local exit_code = test_main(cfg)
os.exit(exit_code)
