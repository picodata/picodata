local checks = require('checks')
local log = require('log')
local fun = require('fun')
local netbox = require('net.box')

local luatest = require('luatest')
local Process = require('luatest.process')

-- Defaults.
local Picodata = {
    workdir = nil,
    name = 'default',
    listen = '127.0.0.1:13301',
    peer = {'127.0.0.1:13301'},
    args = {'run'},
    env = {},

    command = 'target/debug/picodata',
    process = nil,
    __type = 'Picodata',
}

function Picodata:inherit(object)
    setmetatable(object, self)
    self.__index = self
    return object
end

--- Build picodata node.
-- @param object
-- @string object.name Human-readable node name.
-- @string object.data_dir Path to the data directory.
-- @string object.listen Socket bind address.
-- @table object.peer URL of other peers in cluster.
-- @tab[opt] object.env Environment variables passed to the process.
-- @tab[opt] object.args Command-line arguments passed to the process.
-- @return object
function Picodata:new(object)
    checks('table', {
        name = '?string',
        data_dir = 'string',
        listen = '?string',
        peer = '?table',
        args = '?table',
        env = '?table',
    })
    self:inherit(object)
    object:initialize()
    return object
end

function Picodata:initialize()
    checks('Picodata')

    self.env = fun.chain({
        PICODATA_DATA_DIR = self.data_dir,
        PICODATA_LISTEN = self.listen,
        PICODATA_PEER = table.concat(self.peer, ','),
    }, self.env):tomap()
end

--- Start the node.
function Picodata:start()
    checks('Picodata')

    local env = table.copy(os.environ())
    local log_cmd = {}
    for k, v in pairs(self.env) do
        table.insert(log_cmd, string.format('%s=%q', k, v))
        env[k] = v
    end
    table.insert(log_cmd, self.command)
    for _, v in ipairs(self.args) do
        table.insert(log_cmd, string.format('%q', v))
    end
    log.info(table.concat(log_cmd, ' '))

    self.process = Process:start(self.command, self.args, env, {
        output_prefix = self.name,
    })
    log.debug('Started server PID: ' .. self.process.pid)

    luatest.helpers.retrying({}, function()
        self:connect()
    end)

end

--- Connect to the node.
--
-- Connection is established synchronously. The result is cached.
--
-- @function connect
-- @return netbox connection
function Picodata:connect()
    checks('Picodata')

    if self.conn ~= nil and self.conn:is_connected() then
        return self.conn
    end

    local conn = netbox.connect(self.listen)
    if conn.error then
        error(conn.error)
    end

    self.conn = conn
    return conn
end

--- Stop the node.
function Picodata:stop()
    local process = self.process
    if process == nil then
        return
    end
    self.process:kill()
    luatest.helpers.retrying({}, function()
        luatest.assert_not(process:is_alive(),
            '%s is still running', self.name
        )
    end)
    log.warn('%s killed', self.name)
    self.process = nil
end

function Picodata:interact(opts)
    checks('Picodata', {
        -- See picolib/traft/row/message.rs
        msg_type = "string",
        to = "number",
        from = "number",
        term = "?number",
        log_term = "?number",
        index = "?number",
        entries = "?table",
        commit = "?number",
        commit_term = "?number",
        reject = "?boolean",
        reject_hint = "?number",
        priority = "?number",
    })
    return self:connect():call('.raft_interact', {
        opts.msg_type,
        opts.to,
        opts.from,
        opts.term or 0,
        opts.log_term or 0,
        opts.index or 0,
        opts.entries or {},
        opts.commit or 0,
        opts.commit_term or 0,
        opts.reject or false,
        opts.reject_hint or 0,
        opts.priority or 0,
    })
end

return Picodata
