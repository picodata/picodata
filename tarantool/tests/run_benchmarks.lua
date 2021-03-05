#!/usr/bin/env tarantool

box.cfg{
    listen = 3302,
    wal_mode = 'none'
}

-- Add test runner library location to lua search path
package.cpath = 'target/debug/?.so;' .. package.cpath

-- Prepare config
json = require('json')
cfg = json.encode({bench = true})

-- Run tests
local test_main = require('libtarantool_module_test_runner')
local exit_code = test_main(cfg)
os.exit(exit_code)
