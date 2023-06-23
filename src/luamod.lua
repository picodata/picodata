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

help.create_user = [[
pico.create_user(user, password, [opts])
========================================

Creates a user on each instance of the cluster.

Proposes a raft entry which when applied on an instance creates a user on it.
Waits for opts.timeout seconds for the entry to be applied locally.
On success returns a raft index at which the user should exist.

NOTE: If this function returns a timeout error, the user may have been locally
created and in the future it's creation can either be committed or rolled back,
even if the subsequent call to this function returns an "already exists" error.

Params:

    1. user (string), username
    2. password (string)
    3. opts (table)
        - if_not_exists (boolean), if true, do nothing if user with given name already exists
        - timeout (number), seconds

Returns:

    (number) raft index
    or
    (nil, error) in case of an error
]]
function pico.create_user(user, password, opts)
    local ok, err = pcall(function()
        box.internal.check_param(user, 'user', 'string')
        box.internal.check_param(password, 'password', 'string')
        box.internal.check_param_table(opts, { if_not_exists = 'boolean', timeout = 'number' })
        opts = opts or {}
        if not opts.timeout then
            box.error(box.error.ILLEGAL_PARAMS, 'opts.timeout is mandatory')
        end
    end)
    if not ok then
        return nil, err
    end

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

    -- TODO: check password requirements.

    local op = {
        kind = 'acl',
        op_kind = 'create_user',
        user_def = {
            id = get_next_grantee_id(),
            name = user,
            schema_version = next_schema_version(),
            auth = {
                method = "chap-sha1",
                data = box.internal.prepare_auth("chap-sha1", password),
            }
        }
    }

    return pico._prepare_schema_change(op, opts.timeout)
end

help.change_password = [[
pico.change_password(user, password, [opts])
========================================

Change the user's password on each instance of the cluster.

Proposes a raft entry which when applied on an instance changes the user's password on it.
Waits for opts.timeout seconds for the entry to be applied locally.
On success returns an index of the corresponding raft entry.

NOTE: If this function returns a timeout error, the change may have been locally
applied and in the future it can either be committed or rolled back.

Params:

    1. user (string), username
    2. password (string)
    3. opts (table)
        - timeout (number), seconds

Returns:

    (number) raft index
    or
    (nil, error) in case of an error
]]
function pico.change_password(user, password, opts)
    local ok, err = pcall(function()
        box.internal.check_param(user, 'user', 'string')
        box.internal.check_param(password, 'password', 'string')
        box.internal.check_param_table(opts, { timeout = 'number' })
        opts = opts or {}
        if not opts.timeout then
            box.error(box.error.ILLEGAL_PARAMS, 'opts.timeout is mandatory')
        end
    end)
    if not ok then
        return nil, err
    end

    -- TODO: allow `user` to be a user id instead of name
    local user_def = box.space._pico_user.index.name:get(user)
    if user_def == nil then
        return nil, box.error.new(box.error.NO_SUCH_USER, user)
    end

    -- TODO: check password requirements.

    local op = {
        kind = 'acl',
        op_kind = 'change_auth',
        user_id = user_def.id,
        schema_version = next_schema_version(),
        auth = {
            method = "chap-sha1",
            data = box.internal.prepare_auth("chap-sha1", password),
        }
    }

    return pico._prepare_schema_change(op, opts.timeout)
end

help.drop_user = [[
pico.drop_user(user, [opts])
========================================

Drop the user and any entities owned by them on each instance of the cluster.

Proposes a raft entry which when applied on an instance drops the user on it.
Waits for opts.timeout seconds for the entry to be applied locally.
On success returns a raft index at which the user should no longer exist.

NOTE: If this function returns a timeout error, the user may have been locally
dropped and in the future the change can either be committed or rolled back,
even if the subsequent call to this function returns an "does not exist" error.

Params:

    1. user (string), username
    2. opts (table)
        - if_exists (boolean), if true do nothing if user with given name doesn't exist
        - timeout (number), seconds

Returns:

    (number) raft index
    or
    (nil, error) in case of an error
]]
function pico.drop_user(user, opts)
    local ok, err = pcall(function()
        box.internal.check_param(user, 'user', 'string')
        box.internal.check_param_table(opts, { if_exists = 'boolean', timeout = 'number' })
        opts = opts or {}
        if not opts.timeout then
            box.error(box.error.ILLEGAL_PARAMS, 'opts.timeout is mandatory')
        end
    end)
    if not ok then
        return nil, err
    end

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
        schema_version = next_schema_version(),
    }

    return pico._prepare_schema_change(op, opts.timeout)
end

help.create_role = [[
pico.create_role(name, [opts])
========================================

Creates a role on each instance of the cluster.

Proposes a raft entry which when applied on an instance creates a role on it.
Waits for opts.timeout seconds for the entry to be applied locally.
On success returns an index of the corresponding raft entry.

Params:

    1. name (string), role name
    2. opts (table)
        - if_not_exists (boolean), if true, do nothing if role with given name already exists
        - timeout (number), seconds

Returns:

    (number) raft index
    or
    (nil, error) in case of an error
]]
function pico.create_role(role, opts)
    local ok, err = pcall(function()
        box.internal.check_param(role, 'role', 'string')
        box.internal.check_param_table(opts, { if_not_exists = 'boolean', timeout = 'number' })
        opts = opts or {}
        if not opts.timeout then
            box.error(box.error.ILLEGAL_PARAMS, 'opts.timeout is mandatory')
        end
    end)
    if not ok then
        return nil, err
    end

    local grantee_def = box.space._user.index.name:get(role)
    if grantee_def ~= nil then
        if grantee_def.type == "user" then
            return nil, box.error.new(box.error.USER_EXISTS, role)
        elseif opts.if_not_exists == true then
            -- Role exists at current index.
            return pico.raft_get_index()
        else
            return nil, box.error.new(box.error.ROLE_EXISTS, role)
        end
    end

    local op = {
        kind = 'acl',
        op_kind = 'create_role',
        role_def = {
            id = get_next_grantee_id(),
            name = role,
            schema_version = next_schema_version(),
        }
    }

    return pico._prepare_schema_change(op, opts.timeout)
end

help.drop_role = [[
pico.drop_role(role, [opts])
========================================

Drop the role and any entities owned by them on each instance of the cluster.

Proposes a raft entry which when applied on an instance drops the role on it.
Waits for opts.timeout seconds for the entry to be applied locally.
On success returns a raft index at which the role should no longer exist.

Params:

    1. role (string), role name
    2. opts (table)
        - if_exists (boolean), if true do nothing if role with given name doesn't exist
        - timeout (number), seconds

Returns:

    (number) raft index
    or
    (nil, error) in case of an error
]]
function pico.drop_role(role, opts)
    local ok, err = pcall(function()
        box.internal.check_param(role, 'role', 'string')
        box.internal.check_param_table(opts, { if_exists = 'boolean', timeout = 'number' })
        opts = opts or {}
        if not opts.timeout then
            box.error(box.error.ILLEGAL_PARAMS, 'opts.timeout is mandatory')
        end
    end)
    if not ok then
        return nil, err
    end

    local role_def = box.space._pico_role.index.name:get(role)
    if role_def == nil then
        if opts.if_exists then
            -- Role doesn't exist at current index
            return pico.raft_get_index()
        else
            return nil, box.error.new(box.error.NO_SUCH_ROLE, role)
        end
    end

    local op = {
        kind = 'acl',
        op_kind = 'drop_role',
        role_id = role_def.id,
        schema_version = next_schema_version(),
    }

    return pico._prepare_schema_change(op, opts.timeout)
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

NOTE: If this function returns a timeout error, the change may have been locally
applied and in the future it can either be committed or rolled back,
even if the subsequent call to this function returns an "already granted" error.

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
        box.internal.check_param(object_name, 'object_name', 'string')
        box.internal.check_param_table(opts, { timeout = 'number' })
        opts = opts or {}
        if not opts.timeout then
            box.error(box.error.ILLEGAL_PARAMS, 'opts.timeout is mandatory')
        end
    end)
    if not ok then
        return nil, err
    end

    local grantee_def = box.space._pico_user.index.name:get(grantee)
    if grantee_def == nil then
        grantee_def = box.space._pico_role.index.name:get(grantee)
    end
    if grantee_def == nil then
        return nil, box.error.new(box.error.NO_SUCH_USER, grantee)
    end

    local ok, err = pcall(privilege_check, privilege, object_type, 'grant_privilege')
    if not ok then
        return nil, err
    end

    local ok, err = pcall(object_resolve, object_type, object_name)
    if not ok then
        return nil, err
    end

    if box.space._pico_privilege:get{grantee_def.id, object_type, object_name, privilege} ~= nil then
        return nil, box.error.new(box.error.PRIV_GRANTED, grantee, privilege, object_type, (" '%s'"):format(object_name))
    end

    local op = {
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

    return pico._prepare_schema_change(op, opts.timeout)
end

help.revoke_privilege = [[
pico.revoke_privilege(grantee, privilege, object_type, [object_name], [opts])
========================================

Revoke some privilege from the user or role on each instance of the cluster.

Proposes a raft entry which when applied on an instance revokes the specified
privilege from the grantee on it.
Waits for opts.timeout seconds for the entry to be applied locally.
On success returns an index of the corresponding raft entry.

NOTE: If this function returns a timeout error, the change may have been locally
applied and in the future it can either be committed or rolled back,
even if the subsequent call to this function returns an "not granted" error.

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
    end)
    if not ok then
        return nil, err
    end

    local grantee_def = box.space._pico_user.index.name:get(grantee)
    if grantee_def == nil then
        grantee_def = box.space._pico_role.index.name:get(grantee)
    end
    if grantee_def == nil then
        return nil, box.error.new(box.error.NO_SUCH_USER, grantee)
    end

    local ok, err = pcall(privilege_check, privilege, object_type, 'revoke_privilege')
    if not ok then
        return nil, err
    end

    local ok, err = pcall(object_resolve, object_type, object_name)
    if not ok then
        return nil, err
    end

    if box.space._pico_privilege:get{grantee_def.id, object_type, object_name, privilege} == nil then
        return nil, box.error.new(box.error.PRIV_NOT_GRANTED, grantee, privilege, object_type, object_name)
    end

    local op = {
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

    return pico._prepare_schema_change(op, opts.timeout)
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
