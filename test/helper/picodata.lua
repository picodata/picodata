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
    id = -1,
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
        PICODATA_INSTANCE_ID = self.name,
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
end

function Picodata:wait_started()
    checks('Picodata')

    luatest.helpers.retrying({}, function()
        self:connect()
        local raft_status = self:raft_status()
        luatest.assert(raft_status)
        luatest.assert_ge(raft_status.leader_id, 1)
        self.id = raft_status.id
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

--- Get the status of raft node.
-- @function
-- @return {id = number, leader_id = number, state = string}
--   State can be one of "Follower", "Candidate", "Leader", "PreCandidate".
function Picodata:raft_status()
    checks('Picodata')
    return self:connect():call('picolib.raft_status')
end

--- Assert raft status matches expectations.
-- @function
-- @tparam string raft_state
-- @tparam[opt] number leader_id
function Picodata:assert_raft_status(raft_state, leader_id)
    checks('Picodata', 'string', '?number')
    return luatest.assert_covers(
        self:raft_status(),
        {
            leader_id = leader_id,
            raft_state = raft_state,
        }
    )
end

--- Propose Lua code evaluation on every node in cluster.
-- @tparam number timeout
-- @tparam string code
-- @treturn boolean whether proposal was committed on the current node.
function Picodata:raft_propose_eval(timeout, code)
    checks('Picodata', 'number', 'string')
    return self:connect():call(
        'picolib.raft_propose_eval',
        {timeout, code}
    )
end

--- Forcing leader election as if previous leader was dead.
-- Wait for the node becoming a leader.
-- Raise an exception if promotion fails.
function Picodata:promote_or_fail()
    checks('Picodata')
    return luatest.helpers.retrying({}, function()
        self:connect():call('picolib.raft_timeout_now')
        self:assert_raft_status("Leader", self.id)
    end)
end

return Picodata
