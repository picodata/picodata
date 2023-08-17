#!/usr/bin/env -S picodata run --script

package.cpath = 'target/debug/?.so;target/release/?.so;target/debug/?.dylib;target/release/?.dylib'

local log = require('log')

local fiber = require 'fiber'
fiber.create(function()
    -- TODO: find a better way
    fiber.sleep(3.14159265)

    box.once('1fe8bbdb-3426-4dd3-860b-17006c31c885', function()
        log.info('creating the necessary stored functions')
        box.schema.func.create('libpgproto.server_start', { language = 'C' })
        box.schema.user.grant('guest', 'execute', 'function', 'libpgproto.server_start')

        log.info('creating user "postgres" with password "password"')
        box.schema.user.create('postgres', { auth_type = 'md5', password = 'password' })

        log.info('creating exemplary tables')
        pico.sql [[
            create table foo (val int, primary key(val)) distributed by (val);
        ]]
        pico.sql [[
            insert into foo values (1), (2), (3);
        ]]
    end)

    box.func['libpgproto.server_start']:call { 'localhost', '5432' }
end)
