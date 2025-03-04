- status: "accepted"
- decision-makers: Egor Ivkov, Konstantin Osipov
- issue: https://git.picodata.io/core/picodata/-/issues/696
- original adr: https://git.picodata.io/core/picodata/-/merge_requests/1632

# SQL access to pico API - box.schema.func.create

The proposed solution is to export API functions with `box.schema.func.create` by specifying the `exports = {'SQL'}` argument. It is the first stage of functions support in SQL.

## Picodata API

Existing API that we need to allow access to.

1. Raft
  - raft_status() - can return a map
  OR
  - raft_term()
  - raft_commit()
  - raft_leader()
  - raft_state() (Follower | Leader | ...)
  - raft_id()
  - raft_applied()
  OR
  - allow access to _raft_state space
2. Other
  - picodata_version()
  - local_config()
  - vshard_config()
  - whoami()
  - local_vclock()

### JSON

In the future we will also want to support JSON functions.
There are twenty-six scalar functions and operators:

- json(json)
- jsonb(json)
- json_array(value1,value2,...)
- jsonb_array(value1,value2,...)
- json_array_length(json)
- json_array_length(json,path)
- json_error_position(json)
- json_extract(json,path,...)
- jsonb_extract(json,path,...)
- json -> path
- json ->> path
- json_insert(json,path,value,...)
- jsonb_insert(json,path,value,...)
- json_object(label1,value1,...)
- jsonb_object(label1,value1,...)
- json_patch(json1,json2)
- jsonb_patch(json1,json2)
- json_pretty(json)
- json_remove(json,path,...)
- jsonb_remove(json,path,...)
- json_replace(json,path,value,...)
- jsonb_replace(json,path,value,...)
- json_set(json,path,value,...)
- jsonb_set(json,path,value,...)
- json_type(json)
- json_type(json,path)
- json_valid(json)
- json_valid(json,flags)
- json_quote(value)

### Support in SQL

Our clusterwide SQL transpiles into local SQL on each instance. So we just need to support calling functions in grammar,
identify that a function is being called, check that it exists, then transpile back to local sql.

### Grammar

It seems we already support functions invocations in expressions:
```pest
IdentifierWithOptionalContinuation = ${ Identifier ~ (ReferenceContinuation | (WO ~ FunctionInvocationContinuation))? }
                ReferenceContinuation          = ${ "." ~ Identifier }
                FunctionInvocationContinuation = !{ "(" ~ (CountAsterisk | FunctionArgs)? ~ ")" }
                    FunctionArgs = ${ (Distinct ~ W)? ~ FunctionArgsExprs? }
                        FunctionArgsExprs = _{ Expr ~ (WO ~ "," ~ WO ~ Expr)* }
                    CountAsterisk = { "*" }
```

### Existence check
There are two options for managing built-in functions in SQL: either storing them in the `_pico_routine` table alongside stored procedures
or matching them by a constant list of function names.

Storing built-in functions in the _pico_routine table offers centralized management and better scalability if we'll want to have user defined functions.
This approach also allows to add function metadata, making it more flexible in the long term.
We'll also need to add a new column `type` for functions which will require alter table migration.

On the other hand, using a constant list of function names is simpler and can offer faster performance.
For better scalability and flexibility, storing functions in the _pico_routine table is generally the more sustainable choice.

Even simpler choice can be skipping the existence check altogether as it can be done by tarantool during local sql execution.

### ACL

It is proposed to use tarantool prvivileges, we already do it for our ACL and it's most straightforward to use for functions.
> **function**
> execute: Allows calling a function.
>
> create: Allows creating a function. This permission requires read and write access to the _func system space.
>
> If a function is created by a user, they can execute it without granting explicit permission.
>
> drop: Allows dropping a function. This permission requires read and write access to the _func system space.
Source: https://www.tarantool.io/en/doc/latest/admin/access_control/#access-control-list-privileges

If tarantool manages privileges, care should be taken to switch the user to the actual before executing resulting SQL on a target instance.

On our side we should allow clusterwide SQL to grant and revoke `execute` on both `universe` and specific functions.
This should be already done for `routine` (our sql stored procedures) so there can be none or minimum amount of edits.

### Adding a functions

Predefined functions should be created in the `init_common` stage of instance initialization, with `if_not_exists = true` in case the instance starts with existing data_dir.
Functions will have
#### Lua example

```lua
box.schema.func.create('_DECODE',
   {language = 'LUA',
    returns = 'string',
    body = [[function (field, key)
             -- If Tarantool version < 2.10.1, replace next line with
             -- return require('msgpack').decode(field)[key]
             return field[key]
             end]],
    is_sandboxed = false,
    -- If Tarantool version < 2.10.1, replace next line with
    -- param_list = {'string', 'string'},
    param_list = {'map', 'string'},
    exports = {'LUA', 'SQL'},
    is_deterministic = true})
```
Source: https://www.tarantool.io/en/doc/2.11/reference/reference_sql/sql_plus_lua/o

In our case language will be `C`, body should be empty and the name should correspond to the exported symbol in the Rust code. Most probably in Rust the exported function
will be marked with `#[tarantool::proc]` macro.

### _raft_log and _raft_state access

For `_raft_log` and `_raft_state` spaces it seems more logical to grant role `public` read access to them
than to define functions for accessing their contents. For this we will also need to add both spaces to `_pico_table`.
They are special raft spaces and are not replicated the same way like all the other _pico_table spaces, nevertheless
it seems that we can still allow them to be in `_pico_table` for convinience.

## Implementation
Tasks:
- Define built-in picodata API functions
- Ensure that we allow calling built-in functions in our sql
- Ensure that we allow granting and revoking `execute` privilege for functions in our clusterwide ACL (use tarantool ACL under the hood)
- Allow read access to `_raft_state` and `_raft_log`
