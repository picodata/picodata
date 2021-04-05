#!/usr/bin/env tarantool

package.cpath = 'target/debug/?.so;' .. package.cpath

box.cfg{
    listen = 3301,
}

box.once('bootstrap_libcluster_node', function()
    box.schema.func.create('libcluster_node.run_node', {language = 'C'})
    box.schema.func.create('libcluster_node.rpc', {language = 'C'})
    box.schema.user.grant('guest', 'execute', 'function', 'libcluster_node.rpc')
end)

box.func['libcluster_node.run_node']:call()
os.exit(0)
