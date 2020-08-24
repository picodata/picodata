#!/usr/bin/env tarantool

-- Add test runner library location to lua search path
package.cpath = 'target/debug/?.so;' .. package.cpath

-- Run tests
local exit_code = require('libtarantool_module_test_runner')
os.exit(exit_code)
