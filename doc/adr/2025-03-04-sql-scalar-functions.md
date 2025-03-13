- status: "accepted"
- decision-makers: Egor Ivkov, Konstantin Osipov
- issue: https://git.picodata.io/core/picodata/-/issues/696
- original adr: https://git.picodata.io/core/picodata/-/merge_requests/1632

# SQL access to pico API - box.schema.func.create

The proposed solution is to export API functions with `box.schema.func.create` by specifying the `exports = {'SQL'}` argument. It is the first stage of functions support in SQL.

## Picodata API

Existing API that we need to allow access to (some function names were changed according to our plans to rename them).

1. Raft
  - raft_term() -> unsigned
  - raft_commit_index() -> unsigned
  - raft_applied_index() -> unsigned
  - raft_leader_id() -> unsigned
  - raft_leader_uuid() -> uuid
  - raft_state() -> string (Follower | Leader | ...)
  - raft_id() -> unsigned
2. Other
  - picodata_version() -> string
  - local_config() -> map
  - sharding_config() -> map
  - instance_uuid() -> uuid
  - vclock() -> map

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

On our side we should allow clusterwide SQL to grant and revoke `execute` on specific functions.
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

### Misc

#### Existence Check
The existence of a function will be in any case checked locally when the function will be executed by tarantool.
But to provide better errors and UX, we can check it earlier. For example by adding function name list to the grammar.

#### Autocomplete
For dev tools (like cli/ide autocomplete/help) it might be useful to store builtin function definitions in `_pico_routine` table.
This approach also allows to add function metadata.
In this case we'll need to add a new column `type` for functions to differentiate them from stored procedures.

#### _raft_log and _raft_state access

We also considered granting role `public` access to `_raft_log` and `_raft_state` instead of providing functions but decided against it.
Though in general we still plan to support querying local spaces through sql.

## Implementation
Tasks:
- Define built-in picodata API functions
- Ensure that we allow calling built-in functions in our sql
- Ensure that we allow granting and revoking `execute` privilege for functions in our clusterwide ACL (use tarantool ACL under the hood)
- (Optional) check builtin function existence in grammar
- (Optional) store function meta in `_pico_routine`
