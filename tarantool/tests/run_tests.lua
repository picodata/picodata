#!/usr/bin/env tarantool
json = require('json')

local fio = require('fio')
local fiber = require('fiber')
local log = require('log')

local tmpdir = fio.tempdir()

-- Change current working directory into the one this file is in
local current_file = debug.getinfo(1).source
while current_file:sub(1, 1) == '@' do
    current_file = current_file:sub(2)
end
local current_dir = fio.dirname(current_file)
fio.chdir(current_dir)

box.cfg{
    log_level = 'verbose',
    listen = 'localhost:0',
    wal_mode = 'none',
    memtx_dir = tmpdir,
    wal_dir = tmpdir,
}

log.info("version: " .. box.info.version)

fio.rmtree(tmpdir)

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

function target_dir()
    if rawget(_G, '_target_dir') == nil then
        local data = io.popen('cargo metadata --format-version 1'):read('*l')
        rawset(_G, '_target_dir', json.decode(data).target_directory)
    end
    return _target_dir
end

function build_mode()
    local build_mode_env = os.getenv('TARANTOOL_MODULE_BUILD_MODE')
    if not build_mode_env then
        build_mode_env = 'debug'
    end
    return build_mode_env
end

-- Add test runner library location to lua search path
package.cpath = string.format(
    '%s/%s/?.so;%s/%s/?.dylib;%s',
    target_dir(), build_mode(),
    target_dir(), build_mode(),
    package.cpath
)

box.schema.func.create('libtarantool_module_test_runner.entry', { language = 'C' })

local cfg = { filter = arg[1] or "" }
box.func['libtarantool_module_test_runner.entry']:call{cfg}

os.exit(0)
