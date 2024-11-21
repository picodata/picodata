-- This file is required automatically by luatest.
-- Add common configuration here.

local fio = require('fio')
local t = require('luatest')
local cartridge_helpers = require('cartridge.test-helpers')
local errno = require('errno')
local config_handler = require('test.helper.config_handler')

local helper = {
    cluster = nil
}

-- luacheck: ignore package
helper.root = fio.dirname(fio.abspath(package.search('init')))
helper.datadir = fio.pathjoin(helper.root, 'tmp', 'db_test')
helper.server_command = fio.pathjoin(helper.root, 'init.lua')

helper.grep_log = function(what, bytes)
  local filename = fio.pathjoin(helper.root, 'tmp', 'tarantool.log')
  local file = fio.open(filename, {'O_RDONLY', 'O_NONBLOCK'})

  local function fail(msg)
      local err = errno.strerror()
      if file ~= nil then
          file:close()
      end
      error(string.format("%s: %s: %s", msg, filename, err))
  end

  if file == nil then
      fail("Failed to open log file")
  end
  io.flush() -- attempt to flush stdout == log fd
  local filesize = file:seek(0, 'SEEK_END')
  if filesize == nil then
      fail("Failed to get log file size")
  end
  local bytes = bytes or 65536 -- don't read whole log - it can be huge
  bytes = bytes > filesize and filesize or bytes
  if file:seek(-bytes, 'SEEK_END') == nil then
      fail("Failed to seek log file")
  end
  local found, buf
  repeat -- read file in chunks
      local s = file:read(2048)
      if s == nil then
          fail("Failed to read log file")
      end
      local pos = 1
      repeat -- split read string in lines
          local endpos = string.find(s, '\n', pos)
          endpos = endpos and endpos - 1 -- strip terminating \n
          local line = string.sub(s, pos, endpos)
          if endpos == nil and s ~= '' then
              -- line doesn't end with \n or eof, append it to buffer
              -- to be checked on next iteration
              buf = buf or {}
              table.insert(buf, line)
          else
              if buf ~= nil then -- prepend line with buffered data
                  table.insert(buf, line)
                  line = table.concat(buf)
                  buf = nil
              end
              if string.match(line, "Starting instance") then
                  found = nil -- server was restarted, reset search
              else
                  found = string.match(line, what) or found
              end
          end
          pos = endpos and endpos + 2 -- jump to char after \n
      until pos == nil
  until s == ''
  file:close()
  return found
end

local config, err = config_handler.get_init_config(helper.root)
if err ~= nil then
  t.fail(err)
end

helper.cluster_config = config

helper.start_test_cluster = function (cfg)
    fio.rmtree(helper.datadir)
    fio.mktree(helper.datadir)

    helper.cluster = cartridge_helpers.Cluster:new({
            server_command = helper.server_command,
            datadir = helper.datadir,
            use_vshard = true,
            cookie='123',
            replicasets = {
                {
                    alias = "api",
                    uuid = cartridge_helpers.uuid('a'),
                    roles = {'app.roles.api'},
                    servers = 1,
                },
                {
                    alias = "storage-1",
                    uuid = cartridge_helpers.uuid("b"),
                    roles = { "app.roles.storage" },
                    servers = 1,
                },
                {
                    alias = "storage-2",
                    uuid = cartridge_helpers.uuid("c"),
                    roles = { "app.roles.storage" },
                    servers = 1,
                }
            }
    })

    helper.cluster:start()
    helper.cluster:upload_config(cfg)
end

helper.stop_test_cluster = function ()
    helper.cluster:stop()
end

return helper
