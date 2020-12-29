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

------------------------------------------------------------------------------

clock = require('clock')
fiber = require('fiber')
net_box = require('net.box')

local test_size = 1000;
local num_fibers = 503;
local num_rows = 1000;
local num_passes = 301;

local text = string.rep('X', test_size)
local pass_times = {}

------------------------------------------------------------------------------

for i = 1, num_passes do
    local begin_time = tonumber(clock.monotonic64())
    local local_space = box.space.bench_s1
    local_space:truncate()

    local fiber_pool = {}
    for i = 1, num_fibers do
        local fiber = fiber.create(
            function(id_base)
                local conn = net_box.connect('bench_user:password@127.0.0.1:3301')
                local remote_space = conn.space.bench_s1
                for id = 1, num_rows  do
                    remote_space:insert{id + id_base, text}
                end
                conn:close()
            end,
            i * num_rows
        )
        fiber:set_joinable(true)
        table.insert(fiber_pool, fiber)
    end

    for i = 1, num_fibers do
        local fiber = fiber_pool[i]
        if fiber:status() ~= 'dead' then
            local result = fiber:join()
        end
    end

    local end_time = tonumber(clock.monotonic64())
    table.insert(pass_times, end_time - begin_time)
end

------------------------------------------------------------------------------

local avg = 0
for i = 1, num_passes do
    avg = avg + pass_times[i]
end
avg = avg / num_passes

local std_div = 0
for i = 1, num_passes do
    local t = pass_times[i]
    local dt = t - avg
    std_div = std_div + (dt * dt)
end
std_div = math.sqrt(std_div / num_passes)

print(math.floor(avg))
print(math.floor(std_div))

os.exit(0)
