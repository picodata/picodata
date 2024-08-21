local pico = {}
local help = {}
setmetatable(pico, help)

local fiber = require 'fiber'
local log = require 'log'
local utils = require('internal.utils')

local check_param = utils.check_param
local check_param_table = utils.check_param_table

local intro = [[
pico.help([topic])
==================

Show built-in Picodata reference for the given topic.

Full Picodata documentation:

    https://docs.picodata.io/picodata/

Params:

    1. topic (optional string)

Returns:

    (string)
    or
    (nil) if topic not found

Example:

    picodata> pico.help("help")
    -- Shows this message

Topics:

]]

function pico.help(topic)
    if topic == nil or topic == "help" then
        local topics = {"    - help"}
        for k, _ in pairs(help) do
            table.insert(topics, "    - " .. k)
        end
        table.sort(topics)
        return intro .. table.concat(topics, "\n") .. "\n"
    else
        return help[topic]
    end
end

local TIMEOUT_INFINITY = 100 * 365 * 24 * 60 * 60

local function mandatory_param(value, name)
    if value == nil then
        box.error(box.error.ILLEGAL_PARAMS, name .. ' is mandatory')
    end
end

-- Get next id unoccupied by a user or a role. Tarantool stores both
-- users and roles in the same space, so they share the same set of ids.
local function get_next_grantee_id()
    -- TODO: if id overflows start filling the holes
    local max_user = box.space._pico_user.index[0]:max()
    if max_user then
        return max_user.id + 1
    end

    -- There are always builtin tarantool users
    local tt_user = box.space._user.index[0]:max()
    return tt_user.id + 1
end

local function next_schema_version()
    local t = box.space._pico_property:get("next_schema_version")
    -- This shouldn't be necessary if this code is ran after raft node is
    -- initialized, but currently we can call this from the code passed via
    -- `--script` option which is called before node initialization
    if t ~= nil then
        return t.value
    end
    return 1
end

local function has_pending_schema_change()
    return box.space._pico_property:get("pending_schema_change") ~= nil
end

local function is_retriable_error(error)
    if type(error) ~= 'string' then
        -- TODO
        return false
    end

    return pico._is_retriable_error_message(error)
end

-- Performs a reenterable schema change CaS request. On success returns an index
-- of the proposed raft entry.
--
-- Params:
--
--     1. deadline (number), time by which the request should complete,
--          or an error is returned.
--
--     2. make_op_if_needed (function), callback which should check if the
--          corresponding schema entity is already in the desired state or else
--          return a raft operation for CaS request. Should return `nil` if no
--          action is needed, a table representing a raft operation if request
--          should proceed. May throw an error if conflict is detected.
--          It is called after raft_read_index and after any pending schema
--          change has been finalized.
--
-- Returns:
--
--      (number) raft index
--      or
--      (nil, error) in case of an error
--
local function reenterable_schema_change_request(deadline, make_op_if_needed)
    while true do
        ::retry::

        if fiber.clock() >= deadline then
            return nil, box.error.new(box.error.TIMEOUT)
        end

        local index, err = pico.raft_read_index(deadline - fiber.clock())
        if index == nil then
            return nil, err
        end

        if has_pending_schema_change() then
            -- Wait until applied index changes (or timeout) and then retry.
            pico.raft_wait_index(index + 1, deadline - fiber.clock())
            goto retry
        end

        local ok, op = pcall(make_op_if_needed)
        if not ok then
            local err = op
            return nil, err
        elseif op == nil then
            -- Request is satisfied at current index
            return index
        end

        local index, term = pico._schema_change_cas_request(op, index, deadline - fiber.clock())
        if index == nil then
            local err = term
            if is_retriable_error(err) then
                goto retry
            else
                return nil, err
            end
        end

        local res = pico.raft_wait_index(index, deadline - fiber.clock())
        if res == nil then
            return nil, box.error.new(box.error.TIMEOUT)
        end

        if pico.raft_term(index) ~= term then
            -- Leader has changed and the entry got rolled back, retry.
            goto retry
        end

        return index
    end
end

local function check_password_min_length(password, auth_type)
    if string.lower(auth_type) == 'ldap' then
        -- LDAP doesn't need password for authentication
        return
    end

    local password_min_length = box.space._pico_property:get("password_min_length")
    if password_min_length == nil then
        password_min_length = 0
    else
        password_min_length = password_min_length[2]
    end

    local password_len = string.len(password)
    if password_len < password_min_length then
        box.error(box.error.ILLEGAL_PARAMS,
                    "password is too short: expected at least " .. password_min_length .. ", got " .. password_len)
    end
end

-- A lookup map
local supported_priveleges = {
    read = true,
    write = true,
    execute = true,

    -- temporal hack, as lua API is deprecated
    login = true,
    session = true,
    usage = true,

    create = true,
    drop = true,
    alter = true,
    insert = true,
    update = true,
    delete = true,
    grant = true,
    revoke = true,
}

-- Implementation is based on function privilege_check
-- from tarantool-sys/src/box/lua/schema.lua
local function privilege_check(privilege, object_type, entrypoint)
    if type(privilege) ~= 'string' then
        box.error(box.error.ILLEGAL_PARAMS, 'privilege must be a string')
    end

    if supported_priveleges[privilege] == nil then
        box.error(box.error.ILLEGAL_PARAMS, string.format(
            "unsupported privilege '%s', see pico.help('%s') for details",
            privilege, entrypoint
        ))
    end

    if type(object_type) ~= 'string' then
        box.error(box.error.ILLEGAL_PARAMS, 'object_type must be a string')
    end

    if object_type == 'universe' then
        return
    end

    if object_type == 'table' then
        local black_list = {
            session = true,
            revoke = true,
            grant = true,
            execute = true,
        }
        if black_list[privilege] then
            box.error(box.error.UNSUPPORTED_PRIV, object_type, privilege)
        else
            return
        end
    end

    local white_lists = {
        ['sequence'] = {
            read = true,
            write = true,
            usage = true,
            create = true,
            drop = true,
        },
        ['function'] = {
            execute = true,
            usage = true,
            create = true,
            drop = true,
        },
        ['role'] = {
            execute = true,
            usage = true,
            create = true,
            drop = true,
        },
        ['user'] = {
            create = true,
            drop = true,
            alter = true,
        },
    }

    local white_list = white_lists[object_type]
    if white_list == nil then
        box.error(box.error.UNKNOWN_SCHEMA_OBJECT, object_type)
    end

    if not white_list[privilege] then
        box.error(box.error.UNSUPPORTED_PRIV, object_type, privilege)
    end
end

-- Copy-pasted from tarantool-sys/src/box/lua/schema.lua
-- TODO: patch tarantool-sys to export this function and use it here directly
local function object_resolve(object_type, object_name)
    if object_name ~= nil and type(object_name) ~= 'string'
            and type(object_name) ~= 'number' then
        box.error(box.error.ILLEGAL_PARAMS, "wrong object name type")
    end
    if object_type == 'universe' then
        return 0
    end
    if object_type == 'table' then
        if object_name == '' then
            return ''
        end
        local space = box.space[object_name]
        if  space == nil then
            box.error(box.error.NO_SUCH_SPACE, object_name)
        end
        return space.id
    end
    if object_type == 'function' then
        if object_name == '' then
            return ''
        end
        local _vfunc = box.space[box.schema.VFUNC_ID]
        local func
        if type(object_name) == 'string' then
            func = _vfunc.index.name:get{object_name}
        else
            func = _vfunc:get{object_name}
        end
        if func then
            return func.id
        else
            box.error(box.error.NO_SUCH_FUNCTION, object_name)
        end
    end
    if object_type == 'sequence' then
        if object_name == '' then
            return ''
        end
        local seq = sequence_resolve(object_name)
        if seq == nil then
            box.error(box.error.NO_SUCH_SEQUENCE, object_name)
        end
        return seq
    end
    if object_type == 'role' or object_type == 'user' then
        if object_name == '' then
            return ''
        end
        local _vuser = box.space[box.schema.VUSER_ID]
        local role_or_user
        if type(object_name) == 'string' then
            role_or_user = _vuser.index.name:get{object_name}
        else
            role_or_user = _vuser:get{object_name}
        end
        if role_or_user and role_or_user.type == object_type then
            return role_or_user.id
        elseif object_type == 'role' then
            box.error(box.error.NO_SUCH_ROLE, object_name)
        else
            box.error(box.error.NO_SUCH_USER, object_name)
        end
    end

    box.error(box.error.UNKNOWN_SCHEMA_OBJECT, object_type)
end

help.update_plugin_config = [[
pico.update_plugin_config(plugin_name, plugin_version, service_name, new_config, [opts])
======================

Update a plugin service configuration.

Params:
        1. plugin_name - plugin name

        2. plugin_version - plugin version

        3. service_name - service name

        4. new_config - new configuration

        5. opts (optional table)
            - timeout (optional number), in seconds, default: 10
]]
function pico.update_plugin_config(plugin_name, plugin_version, service_name, new_config, opts)
    local raw_new_config = require'msgpack'.encode(new_config)
    return pico._update_plugin_config(plugin_name, plugin_version, service_name, raw_new_config, opts)
end

function pico._replicaset_priority_list(replicaset_uuid)
    local replicaset = vshard.router.internal.static_router.replicasets[replicaset_uuid]
    if replicaset == nil then
        error(vshard.error.vshard(vshard.error.code.NO_SUCH_REPLICASET, replicaset_uuid))
    end
    local closest_replica = replicaset.replica
    local priority_list = replicaset.priority_list
    local result = {}
    -- Make sure the closest replica which vshard uses for most communication is
    -- at the top of the priority list (this may not be the case if there were
    -- communication failure to the replica with top priority and one with a
    -- lower priority became the closest replica)
    if closest_replica ~= nil then
        table.insert(result, closest_replica.name)
    end
    for _, replica in ipairs(priority_list) do
        if replica ~= closest_replica then
            table.insert(result, replica.name)
        end
    end
    return result
end

function pico._replicaset_uuid_by_bucket_id(bucket_id)
    local replicaset, err = vshard.router.internal.static_router:route(bucket_id)
    if replicaset == nil then
        error(err)
    end
    return replicaset.uuid
end

_G.pico = pico
package.loaded.pico = pico
