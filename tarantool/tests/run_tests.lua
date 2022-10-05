#!/usr/bin/env tarantool
json = require('json')

local fio = require('fio')
local fiber = require('fiber')

local tmpdir = fio.tempdir()

function free_port()
    local socket = require 'socket'
    local port = 3301
    for i = 1,64,1 do
        local sock, err = socket.bind('localhost', port)
        if sock then
            sock:close()
            return port
        elseif err ~= 'Address already in use' then
            io.stderr:write(string.format('Error: %s\n', err))
            os.exit(1)
        end
        port = math.random(49152, 65535)
    end

    io.stderr:write("Couldn't pick an available port to listen on")
    os.exit(1)
end

local port = free_port()

box.cfg{
    listen = port,
    wal_mode = 'none',
    memtx_dir = tmpdir,
}

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

-- Prepare config
cfg = json.encode {
    filter = arg[1] or "",
    listen = port,
}

-- Run tests
local test_main = require('libtarantool_module_test_runner')
local exit_code = test_main(cfg)
os.exit(exit_code)
