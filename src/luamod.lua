local pico = {}
local help = {}
setmetatable(pico, help)

local fiber = require 'fiber'
local log = require 'log'

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

-- Get next id unoccupied by a user or a role. Tarantool stores both users and
-- roles in the same space, so they share the same set of ids.
local function get_next_grantee_id()
    -- TODO: if id overflows start filling the holes
    local max_user = box.space._pico_user.index[0]:max()
    local max_role = box.space._pico_role.index[0]:max()
    local new_id = 0
    if max_user then
        new_id = max_user.id + 1
    end
    if max_role and new_id <= max_role.id then
        new_id = max_role.id + 1
    end
    if new_id ~= 0 then
        return new_id
    else
        -- There are always builtin tarantool users
        local tt_user = box.space._user.index[0]:max()
        return tt_user.id + 1
    end
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

    if error:find('operation request from different term')
        or error:find('not a leader')
        or error:find('log unavailable')
    then
        return true
    end

    if error:find('compare-and-swap') then
        return error:find('Compacted') or error:find('ConflictFound')
    end

    return false
end

-- Performs a reenterable schema change CaS request.
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

help.create_user = [[
pico.create_user(user, password, [opts])
========================================

Creates a user on each instance of the cluster.

Proposes a raft entry which when applied on an instance creates a user on it.
Waits for opts.timeout seconds for the entry to be applied locally.
On success returns a raft index at which the user should exist.
Skips the request if the user already exists.

NOTE: If this function returns a timeout error, the user may have been locally
created and in the future it's creation can either be committed or rolled back.

Params:

    1. user (string), username
    2. password (string)
    3. opts (table)
        - timeout (number), seconds
        - auth_type (string)

Returns:

    (number) raft index
    or
    (nil, error) in case of an error
]]
function pico.create_user(user, password, opts)
    local ok, err = pcall(function()
        box.internal.check_param(user, 'user', 'string')
        box.internal.check_param(password, 'password', 'string')
        -- TODO: check password requirements.
        box.internal.check_param_table(opts, {
            timeout = 'number',
            auth_type = 'string',
        })
        opts = opts or {}
        if not opts.timeout then
            box.error(box.error.ILLEGAL_PARAMS, 'opts.timeout is mandatory')
        end
    end)
    if not ok then
        return nil, err
    end

    local deadline = fiber.clock() + opts.timeout

    -- XXX: we construct this closure every time the function is called,
    -- which is bad for performance/jit. Refactor if problems are discovered.
    local auth_type = opts.auth_type or box.cfg.auth_type
    local auth_data = box.internal.prepare_auth(auth_type, password, user)
    local function make_op_if_needed()
        local grantee_def = box.space._user.index.name:get(user)
        if grantee_def ~= nil then
            if grantee_def.type == "role" then
                box.error(box.error.ROLE_EXISTS, user)
            else
                -- TODO: check auth is the same
                -- User already exists, request is satisfied, no op needed
                return nil
            end
        end

        -- Doesn't exist yet, need to send request
        return {
            kind = 'acl',
            op_kind = 'create_user',
            user_def = {
                id = get_next_grantee_id(),
                name = user,
                schema_version = next_schema_version(),
                auth = {
                    method = auth_type,
                    data = auth_data,
                }
            }
        }
    end

    return reenterable_schema_change_request(deadline, make_op_if_needed)
end

help.change_password = [[
pico.change_password(user, password, [opts])
========================================

Change the user's password on each instance of the cluster.

Proposes a raft entry which when applied on an instance changes the user's password on it.
Waits for opts.timeout seconds for the entry to be applied locally.
On success returns an index of the corresponding raft entry.
Skips the request if the password matches the current one.

NOTE: If this function returns a timeout error, the change may have been locally
applied and in the future it can either be committed or rolled back.

Params:

    1. user (string), username
    2. password (string)
    3. opts (table)
        - timeout (number), seconds
        - auth_type (string)

Returns:

    (number) raft index
    or
    (nil, error) in case of an error
]]
function pico.change_password(user, password, opts)
    local ok, err = pcall(function()
        box.internal.check_param(user, 'user', 'string')
        box.internal.check_param(password, 'password', 'string')
        box.internal.check_param_table(opts, {
            timeout = 'number',
            auth_type = 'string',
        })
        opts = opts or {}
        if not opts.timeout then
            box.error(box.error.ILLEGAL_PARAMS, 'opts.timeout is mandatory')
        end
    end)
    if not ok then
        return nil, err
    end

    -- TODO: check password requirements.

    local deadline = fiber.clock() + opts.timeout

    -- XXX: we construct this closure every time the function is called,
    -- which is bad for performance/jit. Refactor if problems are discovered.
    local auth_type = opts.auth_type or box.cfg.auth_type
    local auth_data = box.internal.prepare_auth(auth_type, password, user)
    local function make_op_if_needed()
        -- TODO: allow `user` to be a user id instead of name
        local user_def = box.space._pico_user.index.name:get(user)
        if user_def == nil then
            box.error(box.error.NO_SUCH_USER, user)
        end

        if table.equals(user_def.auth, { [auth_type] = auth_data }) then
            -- Password is already the one given, no op needed
            return nil
        end

        return {
            kind = 'acl',
            op_kind = 'change_auth',
            user_id = user_def.id,
            schema_version = next_schema_version(),
            auth = {
                method = auth_type,
                data = auth_data,
            }
        }
    end

    return reenterable_schema_change_request(deadline, make_op_if_needed)
end

help.drop_user = [[
pico.drop_user(user, [opts])
========================================

Drop the user and any entities owned by them on each instance of the cluster.

Proposes a raft entry which when applied on an instance drops the user on it.
Waits for opts.timeout seconds for the entry to be applied locally.
On success returns a raft index at which the user should no longer exist.
Skips the request if the user doesn't exist.

NOTE: If this function returns a timeout error, the user may have been locally
dropped and in the future the change can either be committed or rolled back.

Params:

    1. user (string), username
    2. opts (table)
        - timeout (number), seconds

Returns:

    (number) raft index
    or
    (nil, error) in case of an error
]]
function pico.drop_user(user, opts)
    local ok, err = pcall(function()
        box.internal.check_param(user, 'user', 'string')
        box.internal.check_param_table(opts, { timeout = 'number' })
        opts = opts or {}
        if not opts.timeout then
            box.error(box.error.ILLEGAL_PARAMS, 'opts.timeout is mandatory')
        end
    end)
    if not ok then
        return nil, err
    end

    local deadline = fiber.clock() + opts.timeout

    -- XXX: we construct this closure every time the function is called,
    -- which is bad for performance/jit. Refactor if problems are discovered.
    local function make_op_if_needed()
        local user_def = box.space._pico_user.index.name:get(user)
        if user_def == nil then
            -- User doesn't exists, request is satisfied, no op needed
            return nil
        end

        -- Does still exist, need to send request
        return {
            kind = 'acl',
            op_kind = 'drop_user',
            user_id = user_def.id,
            schema_version = next_schema_version(),
        }
    end

    return reenterable_schema_change_request(deadline, make_op_if_needed)
end

help.create_role = [[
pico.create_role(name, [opts])
========================================

Creates a role on each instance of the cluster.

Proposes a raft entry which when applied on an instance creates a role on it.
Waits for opts.timeout seconds for the entry to be applied locally.
On success returns an index of the corresponding raft entry.
Skips the request if the role already exists.

NOTE: If this function returns a timeout error, the role may have been locally
created and in the future it's creation can either be committed or rolled back.

Params:

    1. name (string), role name
    2. opts (table)
        - timeout (number), seconds

Returns:

    (number) raft index
    or
    (nil, error) in case of an error
]]
function pico.create_role(role, opts)
    local ok, err = pcall(function()
        box.internal.check_param(role, 'role', 'string')
        box.internal.check_param_table(opts, { timeout = 'number' })
        opts = opts or {}
        if not opts.timeout then
            box.error(box.error.ILLEGAL_PARAMS, 'opts.timeout is mandatory')
        end
    end)
    if not ok then
        return nil, err
    end

    local deadline = fiber.clock() + opts.timeout

    -- XXX: we construct this closure every time the function is called,
    -- which is bad for performance/jit. Refactor if problems are discovered.
    local function make_op_if_needed()
        local grantee_def = box.space._user.index.name:get(role)
        if grantee_def ~= nil then
            if grantee_def.type == "user" then
                box.error(box.error.USER_EXISTS, role)
            else
                -- Role exists at current index, no op needed
                return nil
            end
        end

        return {
            kind = 'acl',
            op_kind = 'create_role',
            role_def = {
                id = get_next_grantee_id(),
                name = role,
                schema_version = next_schema_version(),
            }
        }
    end

    return reenterable_schema_change_request(deadline, make_op_if_needed)
end

help.drop_role = [[
pico.drop_role(role, [opts])
========================================

Drop the role and any entities owned by them on each instance of the cluster.

Proposes a raft entry which when applied on an instance drops the role on it.
Waits for opts.timeout seconds for the entry to be applied locally.
On success returns a raft index at which the role should no longer exist.
Skips the request if the role doesn't exist.

NOTE: If this function returns a timeout error, the role may have been locally
dropped and in the future the change can either be committed or rolled back.

Params:

    1. role (string), role name
    2. opts (table)
        - timeout (number), seconds

Returns:

    (number) raft index
    or
    (nil, error) in case of an error
]]
function pico.drop_role(role, opts)
    local ok, err = pcall(function()
        box.internal.check_param(role, 'role', 'string')
        box.internal.check_param_table(opts, { timeout = 'number' })
        opts = opts or {}
        if not opts.timeout then
            box.error(box.error.ILLEGAL_PARAMS, 'opts.timeout is mandatory')
        end
    end)
    if not ok then
        return nil, err
    end

    local deadline = fiber.clock() + opts.timeout

    -- XXX: we construct this closure every time the function is called,
    -- which is bad for performance/jit. Refactor if problems are discovered.
    local function make_op_if_needed()
        local role_def = box.space._pico_role.index.name:get(role)
        if role_def == nil then
            -- Role doesn't exists, request is satisfied, no op needed
            return nil
        end

        return {
            kind = 'acl',
            op_kind = 'drop_role',
            role_id = role_def.id,
            schema_version = next_schema_version(),
        }
    end

    return reenterable_schema_change_request(deadline, make_op_if_needed)
end

-- A lookup map
local supported_priveleges = {
    read = true,
    write = true,
    execute = true,
    session = true,
    usage = true,
    create = true,
    drop = true,
    alter = true,
    reference = true,
    trigger = true,
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

    if object_type == 'space' then
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
    if object_type == 'space' then
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

help.grant_privilege = [[
pico.grant_privilege(grantee, privilege, object_type, [object_name], [opts])
========================================

Grant some privilege to a user or role on each instance of the cluster.

Proposes a raft entry which when applied on an instance grants the grantee the
specified privilege on it.
Waits for opts.timeout seconds for the entry to be applied locally.
On success returns an index of the corresponding raft entry.
Skips the request if the privilege is already granted.

NOTE: If this function returns a timeout error, the change may have been locally
applied and in the future it can either be committed or rolled back.

Params:

    1. grantee (string), name of user or role

    2. privilege (string), one of
        'read' | 'write' | 'execute' | 'session' | 'usage' | 'create' | 'drop' |
        'alter' | 'reference' | 'trigger' | 'insert' | 'update' | 'delete'

    3. object_type (string), one of
        'universe' | 'space' | 'sequence' | 'function' | 'role' | 'user'

    4. object_name (optional string), can be omitted when privilege concerns an
        entire class of entities, see examples below.

    5. opts (table)
        - timeout (number), seconds

Returns:

    (number) raft index
    or
    (nil, error) in case of an error

Examples:

    -- Grant read access to space 'Fruit' for user 'Dave'.
    pico.grant_privilege('Dave', 'read', 'space', 'Fruit')

    -- Grant user 'Dave' privilege to execute arbitrary lua code.
    pico.grant_privilege('Dave', 'execute', 'universe')

    -- Grant user 'Dave' privilege to create new users.
    pico.grant_privilege('Dave', 'create', 'user')

    -- Grant write access to space 'Junk' for role 'Maintainer'.
    pico.grant_privilege('Maintainer', 'write', 'space', 'Junk')

    -- Assign role 'Maintainer' to user 'Dave'.
    pico.grant_privilege('Dave', 'execute', 'role', 'Maintainer')
]]
function pico.grant_privilege(grantee, privilege, object_type, object_name, opts)
    local ok, err = pcall(function()
        box.internal.check_param(grantee, 'grantee', 'string')
        box.internal.check_param(privilege, 'privilege', 'string')
        box.internal.check_param(object_type, 'object_type', 'string')
        object_name = object_name ~= nil and object_name or ''
        -- `object_name` is optional, thus it might contain `opts` instead
        if type(object_name) == 'table' and opts == nil then
            opts = object_name
            object_name = ''
        end
        box.internal.check_param(object_name, 'object_name', 'string')
        box.internal.check_param_table(opts, { timeout = 'number' })
        opts = opts or {}
        if not opts.timeout then
            box.error(box.error.ILLEGAL_PARAMS, 'opts.timeout is mandatory')
        end

        privilege_check(privilege, object_type, 'grant_privilege')
    end)
    if not ok then
        return nil, err
    end

    local deadline = fiber.clock() + opts.timeout

    -- XXX: we construct this closure every time the function is called,
    -- which is bad for performance/jit. Refactor if problems are discovered.
    local function make_op_if_needed()
        -- This is being checked after raft_read_index, so enough time has
        -- passed for any required entities to be created
        local grantee_def = box.space._pico_user.index.name:get(grantee)
        if grantee_def == nil then
            grantee_def = box.space._pico_role.index.name:get(grantee)
        end
        if grantee_def == nil then
            box.error(box.error.NO_SUCH_USER, grantee)
        end

        -- Throws error if object doesn't exist
        object_resolve(object_type, object_name)

        if box.space._pico_privilege:get{grantee_def.id, object_type, object_name, privilege} ~= nil then
            -- Privilege is already granted, no op needed
            return nil
        end

        return {
            kind = 'acl',
            op_kind = 'grant_privilege',
            priv_def = {
                grantee_id = grantee_def.id,
                object_type = object_type,
                object_name = object_name,
                privilege = privilege,
                schema_version = next_schema_version(),
            },
        }
    end

    return reenterable_schema_change_request(deadline, make_op_if_needed)
end

help.revoke_privilege = [[
pico.revoke_privilege(grantee, privilege, object_type, [object_name], [opts])
========================================

Revoke some privilege from the user or role on each instance of the cluster.

Proposes a raft entry which when applied on an instance revokes the specified
privilege from the grantee on it.
Waits for opts.timeout seconds for the entry to be applied locally.
On success returns an index of the corresponding raft entry.
Skips the request if the privilege is not yet granted.

NOTE: If this function returns a timeout error, the change may have been locally
applied and in the future it can either be committed or rolled back.

Params:

    1. grantee (string), name of user or role

    2. privilege (string), one of
        'read' | 'write' | 'execute' | 'session' | 'usage' | 'create' | 'drop' |
        'alter' | 'reference' | 'trigger' | 'insert' | 'update' | 'delete'

    3. object_type (string), one of
        'universe' | 'space' | 'sequence' | 'function' | 'role' | 'user'

    4. object_name (optional string), can be omitted when privilege concerns an
        entire class of entities, see pico.help("pico.grant_privilege") for details.

    5. opts (table)
        - timeout (number), seconds

Returns:

    (number) raft index
    or
    (nil, error) in case of an error
]]
function pico.revoke_privilege(grantee, privilege, object_type, object_name, opts)
    local ok, err = pcall(function()
        box.internal.check_param(grantee, 'grantee', 'string')
        box.internal.check_param(privilege, 'privilege', 'string')
        box.internal.check_param(object_type, 'object_type', 'string')
        object_name = object_name ~= nil and object_name or ''
        box.internal.check_param(object_name, 'object_name', 'string')
        box.internal.check_param_table(opts, { timeout = 'number' })
        opts = opts or {}
        if not opts.timeout then
            box.error(box.error.ILLEGAL_PARAMS, 'opts.timeout is mandatory')
        end

        privilege_check(privilege, object_type, 'revoke_privilege')
    end)
    if not ok then
        return nil, err
    end

    local deadline = fiber.clock() + opts.timeout

    -- XXX: we construct this closure every time the function is called,
    -- which is bad for performance/jit. Refactor if problems are discovered.
    local function make_op_if_needed()
        -- This is being checked after raft_read_index, so enough time has
        -- passed for any required entities to be created
        local grantee_def = box.space._pico_user.index.name:get(grantee)
        if grantee_def == nil then
            grantee_def = box.space._pico_role.index.name:get(grantee)
        end
        if grantee_def == nil then
            box.error(box.error.NO_SUCH_USER, grantee)
        end

        -- Throws error if object doesn't exist
        object_resolve(object_type, object_name)

        if box.space._pico_privilege:get{grantee_def.id, object_type, object_name, privilege} == nil then
            -- Privilege is not yet granted, no op needed
            return nil
        end

        return {
            kind = 'acl',
            op_kind = 'revoke_privilege',
            priv_def = {
                grantee_id = grantee_def.id,
                object_type = object_type,
                object_name = object_name,
                privilege = privilege,
                schema_version = next_schema_version(),
            },
        }
    end

    return reenterable_schema_change_request(deadline, make_op_if_needed)
end

help.drop_space = [[
pico.drop_space(space, [opts])
======================

Drops a space on each instance of the cluster.

Waits for the space to be dropped globally or returns an error if the timeout is
reached before that.

Params:

    1. space (number | string), clusterwide space id or name

    2. opts (table)
        - timeout (number), seconds

Returns:

    (number) raft index
    or
    (nil, error) in case of an error
]]
function pico.drop_space(space, opts)
    local ok, err = pcall(function()
        if type(space) ~= 'string' and type(space) ~= 'number' then
            box.error(box.error.ILLEGAL_PARAMS, 'space should be a number or a string')
        end
        box.internal.check_param_table(opts, { timeout = 'number' })
        opts = opts or {}
        if not opts.timeout then
            box.error(box.error.ILLEGAL_PARAMS, 'opts.timeout is mandatory')
        end
    end)
    if not ok then
        return nil, err
    end

    local space_id
    if type(space) == 'string' then
        local space_def = box.space._pico_space.index.name:get(space)
        if space_def == nil then
            return nil, box.error.new(box.error.NO_SUCH_SPACE, space)
        end
        space_id = space_def.id
    elseif type(space) == 'number' then
        space_id = space
        if box.space._pico_space:get(space_id) == nil then
            return nil, box.error.new(box.error.NO_SUCH_SPACE, space)
        end
    end

    local op = {
        kind = 'ddl_prepare',
        schema_version = next_schema_version(),
        ddl = {
            kind = 'drop_space',
            id = space_id,
        }
    }

    local timeout = opts.timeout
    local ok, err = pico._prepare_schema_change(op, timeout)
    if not ok then
        return nil, err
    end
    local index = ok

    local ok, err = pico.wait_ddl_finalize(index, { timeout = timeout })
    if not ok then
        return nil, err
    end

    local fin_index = ok
    return fin_index
end

_G.pico = pico
package.loaded.pico = pico
