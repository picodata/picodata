local pico = {}
local help = {}
setmetatable(pico, help)

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

local function get_next_user_id()
    -- TODO: if user id overflows start filling the holes
    local user = box.space._pico_user.index[0]:max()
    if user ~= nil then
        return user.id + 1
    else
        local tt_user = box.space._user.index[0]:max()
        return tt_user.id + 1
    end
end

help.create_user = [[
pico.create_user(user, password, [opts])
========================================

Creates a user on each instance of the cluster.

Proposes a raft entry which when applied on an instance creates a user on it.
On success returns a raft index at which the user should exist.

Params:

    1. user (string), username
    2. password (string)
    3. opts (table)
        - if_not_exists (boolean), if true, do nothing if user with given name already exists
        - timeout (number), wait for this many seconds for the proposed entry
            to be applied locally, default: 3 seconds

Returns:

    (number) raft index
    or
    (nil, error) in case of an error
]]
function pico.create_user(user, password, opts)
    box.internal.check_param_table(opts, { if_not_exists = 'boolean', timeout = 'number' })
    opts = opts or {}

    local grantee_def = box.space._user.index.name:get(user)
    if grantee_def ~= nil then
        if grantee_def.type == "role" then
            return nil, box.error.new(box.error.ROLE_EXISTS, user)
        elseif opts.if_not_exists == true then
            -- User exists at current index.
            return pico.raft_get_index()
        else
            return nil, box.error.new(box.error.USER_EXISTS, user)
        end
    end

    if type(password) ~= "string" then
        return nil, box.error.new(box.error.ILLEGAL_PARAMS, "password should be a string")
    end

    -- TODO: check password requirements.

    local op = {
        kind = 'acl',
        op_kind = 'create_user',
        user_def = {
            id = get_next_user_id(),
            name = user,
            schema_version = box.space._pico_property:get("next_schema_version").value,
            auth = {
                method = "chap-sha1",
                data = box.internal.prepare_auth("chap-sha1", password),
            }
        }
    }

    return pico._prepare_schema_change(op, opts.timeout or 3)
end

help.change_password = [[
pico.change_password(user, password, [opts])
========================================

Change the user's password on each instance of the cluster.

Proposes a raft entry which when applied on an instance changes the user's password on it.
On success returns an index of the corresponding raft entry.

Params:

    1. user (string), username
    2. password (string)
    3. opts (table)
        - timeout (number), wait for this many seconds for the proposed entry
            to be applied locally, default: 3 seconds

Returns:

    (number) raft index
    or
    (nil, error) in case of an error
]]
function pico.change_password(user, password, opts)
    box.internal.check_param_table(opts, { timeout = 'number' })
    opts = opts or {}

    -- TODO: allow `user` to be a user id instead of name
    local user_def = box.space._pico_user.index.name:get(user)
    if user_def == nil then
        return nil, box.error.new(box.error.NO_SUCH_USER, user)
    end

    if type(password) ~= "string" then
        return nil, box.error.new(box.error.ILLEGAL_PARAMS, "password should be a string")
    end

    -- TODO: check password requirements.

    local op = {
        kind = 'acl',
        op_kind = 'change_auth',
        user_id = user_def.id,
        schema_version = box.space._pico_property:get("next_schema_version").value,
        auth = {
            method = "chap-sha1",
            data = box.internal.prepare_auth("chap-sha1", password),
        }
    }

    return pico._prepare_schema_change(op, opts.timeout or 3)
end

help.drop_user = [[
pico.drop_user(user, [opts])
========================================

Drop the user and any entities owned by them on each instance of the cluster.

Proposes a raft entry which when applied on an instance drops the user on it.
On success returns a raft index at which the user should no longer exist.

Params:

    1. user (string), username
    2. opts (table)
        - if_exists (boolean), if true do nothing if user with given name doesn't exist
        - timeout (number), wait for this many seconds for the proposed entry
            to be applied locally, default: 3 seconds

Returns:

    (number) raft index
    or
    (nil, error) in case of an error
]]
function pico.drop_user(user, password, opts)
    box.internal.check_param_table(opts, { if_exists = 'boolean', timeout = 'number' })
    opts = opts or {}

    local user_def = box.space._pico_user.index.name:get(user)
    if user_def == nil then
        if opts.if_exists then
            -- User doesn't exist at current index
            return pico.raft_get_index()
        else
            return nil, box.error.new(box.error.NO_SUCH_USER, user)
        end
    end

    local op = {
        kind = 'acl',
        op_kind = 'drop_user',
        user_id = user_def.id,
        schema_version = box.space._pico_property:get("next_schema_version").value,
    }

    return pico._prepare_schema_change(op, opts.timeout or 3)
end

_G.pico = pico
package.loaded.pico = pico
