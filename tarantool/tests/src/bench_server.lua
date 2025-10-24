box.cfg{
    listen = '127.0.0.1:3301',
    memtx_memory = 1024 * 1024 * 1024,
    net_msg_max = 500000,
    readahead = 1024 * 1024,
    wal_mode = 'none'
}

box.once('bootstrap_bench', function()
    box.schema.user.create('bench_user', { password = 'password' })
    box.schema.user.grant('bench_user', 'read,write,execute,create,drop', 'universe')

    local bench_s1 = box.schema.space.create('bench_s1')
    bench_s1:format{
        {name = 'id', type = 'unsigned'},
        {name = 'text', type = 'string' }
    }
    bench_s1:create_index('primary', {type = 'TREE', parts = {{ field = 1, type = 'unsigned' }}})

    box.schema.func.create('_cleanup')
end)

function _cleanup()
    box.space.bench_s1:truncate()
end
