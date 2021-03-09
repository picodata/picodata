#!/usr/bin/env tarantool

box.cfg{
    listen = 3302,
    wal_mode = 'none'
}

------------------------------------------------------------------------------

clock = require('clock')
fiber = require('fiber')
log = require('log')
net_box = require('net.box')

local test_size = 64;
local num_fibers = 256;
local num_rows = 1000;
local num_passes = 30;

local text = string.rep('X', test_size)
local pass_times = {}

------------------------------------------------------------------------------

local conn = net_box.connect('bench_user:password@127.0.0.1:3301')
conn:call('_cleanup')

local id_base = 1
for pass_id = 1, num_passes do
    local begin_time = tonumber(clock.monotonic64())
    local local_space = box.space.bench_s1

    local fiber_pool = {}
    for fiber_id = 1, num_fibers do
        local fiber = fiber.create(
            function(id_base)
                local remote_space = conn.space.bench_s1
                for id = id_base, (id_base + num_rows - 1)  do
                    local res = pcall(remote_space.insert, remote_space, {id + id_base, text})
                    if not res then
                        io.write('x')
                    end
                end
            end,
            id_base
        )
        fiber:set_joinable(true)
        table.insert(fiber_pool, fiber)
        id_base = id_base + num_rows
    end

    for i = 1, num_fibers do
        local fiber = fiber_pool[i]
        if fiber:status() ~= 'dead' then
            fiber:join()
        end
    end

    local end_time = tonumber(clock.monotonic64())
    table.insert(pass_times, end_time - begin_time)
    io.write('.')
    io.flush()
end

conn:close()

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
