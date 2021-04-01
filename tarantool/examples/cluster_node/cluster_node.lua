#!/usr/bin/env tarantool

package.cpath = 'target/debug/?.so;' .. package.cpath

box.cfg{
    listen = 3301,
}

require('libcluster_node')
os.exit(0)
