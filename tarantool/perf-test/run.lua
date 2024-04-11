local fio = require('fio')
local json = require('json')
local log = require('log')

local tmpdir = fio.tempdir()

box.cfg{
    log_level = 'verbose',
    listen = '127.0.0.1:0',
    wal_mode = 'none',
    memtx_dir = tmpdir,
}

fio.rmtree(tmpdir)

-- Init test database
box.once('bootstrap_tests', function()
    box.schema.user.grant('guest', 'read,write,execute,create,drop', 'universe')

    box.schema.func.create('test_stored_proc')
end)

function test_stored_proc(a, b)
    return a + b
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

box.schema.func.create('libperf_test.bench_netbox', {language = 'C'})
box.schema.func.create('libperf_test.bench_network_client', {language = 'C'})
box.schema.func.create('libperf_test.bench_custom_encode', {language = 'C'})
box.schema.func.create('libperf_test.bench_custom_decode', {language = 'C'})
box.schema.func.create('libperf_test.bench_serde_encode', {language = 'C'})
box.schema.func.create('libperf_test.bench_serde_decode', {language = 'C'})
box.schema.func.create('libperf_test.l_print_stats', {language = 'C'})
box.schema.func.create('libperf_test.l_n_iters', {language = 'C'})

function bench_lua_netbox()
    local clock = require('clock')
    local net_box = require("net.box")

    local connect_deadline = clock.monotonic() + 3 -- seconds
    local conn
    repeat
        conn = net_box:connect(box.info.listen)
        local ok = conn:wait_connected(clock.monotonic() - connect_deadline)
        if clock.monotonic() > connect_deadline then
            error(string.format('Failed to establish a connection to port %s', box.info.listen))
        end
    until ok or clock.monotonic() > connect_deadline

    local samples = {}
    local n = box.func['libperf_test.l_n_iters']:call()

    -- benchmarking loop
    for i = 1, n do
        local start = clock.monotonic64()
        local res = conn:call('test_stored_proc', {1, 2})
        samples[i] = clock.monotonic64() - start
    end

    conn:close()
    box.func['libperf_test.l_print_stats']:call{"lua_netbox", samples}
end

print("================ iproto_clients =================")
bench_lua_netbox()
box.func['libperf_test.bench_netbox']:call()
box.func['libperf_test.bench_network_client']:call()
print()
print("============= msgpack_serialization =============")
box.func['libperf_test.bench_custom_encode']:call()
box.func['libperf_test.bench_serde_encode']:call()
box.func['libperf_test.bench_custom_decode']:call()
box.func['libperf_test.bench_serde_decode']:call()
os.exit(0)
