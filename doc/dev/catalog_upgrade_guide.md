# How to Add Schema Changes to the Catalog Upgrade Flow

This guide explains how to add new schema changes that are automatically applied
when a cluster upgrades to a new release version.

## Background

When Picodata releases a new version, the system catalog schema may change
(new tables, new columns, new stored procedures, etc.). These changes are applied
automatically by the **governor** — a fiber running on the Raft leader that
orchestrates cluster-wide operations.

The upgrade flow is driven by `_pico_governor_queue` — a system table that holds
migration operations with statuses (`pending` → `done` / `failed`). The governor
picks up pending operations one by one and executes them.

For the full design rationale, see
[ADR: ability to run upgrade DDL from governor](../adr/2025-04-14-ability-to-run-upgrade-ddl-from-governor.md).

## Key Files

| File | Purpose |
|------|---------|
| `src/governor/upgrade_operations.rs` | `CATALOG_UPGRADE_LIST` — the append-only registry of all upgrade operations, plus `InternalScript` enum and execution functions |
| `src/governor/queue.rs` | Governor queue handling: insertion, execution, and status management of upgrade operations |
| `src/storage.rs` | `LATEST_SYSTEM_CATALOG_VERSION` — derived automatically from the last entry in `CATALOG_UPGRADE_LIST` |
| `src/catalog/governor_queue.rs` | `GovernorOperationDef`, `GovernorOpFormat`, `GovernorOpStatus` types |

## Operation Formats

Each upgrade operation has one of three formats:

| Format | Description | Runs on |
|--------|-------------|---------|
| `sql` | Raw SQL statement (DDL or DML) | Leader via `sql_dispatch` |
| `proc_name` | Creates a Tarantool stored procedure | All replicaset masters |
| `exec_script` | Executes an internal Rust script | All instances |

## Step-by-Step: Adding a New Upgrade

### 1. Choose a catalog version

Decide which version entry in `CATALOG_UPGRADE_LIST` your change belongs to.

- If you are working on an **unreleased version** that already has an entry, add
  your operation to that entry.
- If you need a **new version**, append a new entry at the end of the list.

The version string follows the `MAJOR.MINOR.PATCH` format (e.g., `"26.2.1"`). The version must not use `PATCH=0`, always start from `1` due to our release policy.

> **Important:** `CATALOG_UPGRADE_LIST` is append-only. Never modify or reorder
> existing entries for versions that have been released. If a previous migration
> was buggy, add a corrective migration in a new version (see the `25.4.1` →
> `26.1.1` fix for `is_default` as an example).

### 2. Add the operation to `CATALOG_UPGRADE_LIST`

Open `src/governor/upgrade_operations.rs` and add your operation(s).

#### Option A: SQL statement

For DDL/DML that can be expressed as a single SQL statement:

```rust
// In CATALOG_UPGRADE_LIST
(
    "26.2.1",
    &[
        ("sql", "CREATE TABLE IF NOT EXISTS _pico_my_table (id INT NOT NULL, name TEXT NOT NULL, PRIMARY KEY (id)) DISTRIBUTED GLOBALLY"),
    ]
),
```

#### Option B: Stored procedure

For creating a Tarantool stored procedure on all replicaset masters:

```rust
(
    "26.2.1",
    &[
        ("proc_name", "proc_my_new_procedure"),
    ]
),
```

The procedure itself must be registered in the codebase (see how existing
`proc_*` procedures are defined).

#### Option C: Internal script (`exec_script`)

For schema changes that cannot be expressed as SQL (e.g., altering system tables
like `_pico_instance`, `_pico_replicaset`, `_pico_tier` where SQL ALTER would
cause a deadlock with CAS operations):

```rust
(
    "26.2.1",
    &[
        ("exec_script", InternalScript::MyNewScript.as_str()),
    ]
),
```

This requires additional steps — see below.

### 3. (Only for `exec_script`) Add a new `InternalScript` variant

If you chose `exec_script`, you need to:

**a)** Add a variant to the `InternalScript` enum in
`src/governor/upgrade_operations.rs`:

```rust
pub enum InternalScript {
    // ... existing variants ...

    /// Schema upgrade operation equivalent to:
    /// ```ignore
    /// ALTER TABLE _pico_my_table ADD COLUMN new_field INT NULL
    /// ```
    MyNewScript = "my_new_script",
}
```

**b)** Add a match arm in `proc_internal_script` (same file):

```rust
fn proc_internal_script(req: Request) -> traft::Result<Response> {
    // ...
    match script_name {
        // ... existing arms ...
        InternalScript::MyNewScript =>
            execute_my_new_script(),
    }
}
```

**c)** Implement the execution function:

```rust
fn execute_my_new_script() -> traft::Result<Response> {
    // For system table format changes, use the helper:
    actualize_system_table_format::<MySystemTable>()?;
    Ok(Response {})
}
```

The `actualize_system_table_format::<T>()` helper does the following:
1. Checks if the table already has the expected format (idempotency).
2. On masters: updates the space format via `ddl_change_format_on_master()`.
3. On all instances: updates `_pico_table` metadata to reflect the new format.

For non-format changes (e.g., running Lua code), write custom logic. See
`execute_create_lua_procs()` for an example.

### 4. Update the system table definition (if applicable)

If you are adding a column to a system table, make sure the table's Rust struct
and `format()` method include the new field. The `actualize_system_table_format`
function compares the table's current format against the expected format from
`T::format()` — if they match, it skips the migration (idempotency).

### 5. Add a rolling upgrade integration test

Add a test to `test/int/test_rolling.py` that verifies your schema change
survives a real cluster upgrade. The test framework lets you deploy a cluster on
an older version, upgrade it node-by-node, and assert the new schema is in place.

**Test structure:**

```python
@pytest.mark.xdist_group(name="rolling")
@pytest.mark.required_rolling_versions(
    versions=[
        Version(<OLD_VERSION>),
        get_or_make_registry().next_version(Version(<OLD_VERSION>)),
    ]
)
def test_upgrade_check_my_new_schema(cluster: Cluster, registry: Registry):
    from_version = Version(<OLD_VERSION>)
    from_executable = registry.get(from_version)
    assert from_executable is not None

    to_version = registry.next_version(from_version)
    to_executable = registry.get(to_version)
    assert to_executable is not None

    # 1. Deploy cluster on the previous version
    cluster.deploy(
        executable=from_executable,
        instance_count=4,
        init_replication_factor=2,
    )

    # 2. Upgrade all nodes to the current version
    cluster.change_executable(to_executable)

    # 3. Verify cluster is healthy after upgrade
    assert cluster.is_healthy()

    # 4. Assert your schema change is applied
    i = cluster.instances[0]
    res = i.sql("SELECT my_new_column FROM _pico_my_table")
    assert res is not None
```

**Key points:**

- Use the latest release version as `<OLD_VERSION>` (`"MAJOR.MINOR.PATCH"`) — one that does not yet include the schema changes you want to test.
- Use `@pytest.mark.required_rolling_versions` to declare which versions the test needs.
- For more complex scenarios (e.g., blocked upgrades, partial rollbacks, expel
  during upgrade), see existing tests in the same file for patterns.

## How It Works at Runtime

The following is a simplified view of what happens when a cluster upgrades:

1. Instances with the new binary start. The new binary knows
   `LATEST_SYSTEM_CATALOG_VERSION` (the last entry in `CATALOG_UPGRADE_LIST`).

2. After all instances start with the new binary, the governor detects that `LATEST_SYSTEM_CATALOG_VERSION` is newer than the
   stored `system_catalog_version` and sets `pending_catalog_version` in
   `_pico_property`.

3. On each governor loop iteration, `handle_governor_queue()` is called:
   - **First pass:** No operations in the queue yet → `insert_catalog_upgrade_operations()`
     inserts all operations for the version range
     `(current_version, pending_version]` from `CATALOG_UPGRADE_LIST` into
     `_pico_governor_queue` with status `pending`.
   - **Subsequent passes:** Picks the next `pending` operation and executes it
     via the appropriate mechanism (`sql_dispatch`, RPC to masters, or RPC to all
     instances).
   - **After all operations succeed:** `finish_catalog_upgrade()` updates
     `system_catalog_version` and removes `pending_catalog_version`.

4. If any operation fails, its status is set to `failed` and the governor stops
   processing further operations (manual intervention required).

## Cherry-Picks to Release Branches

When a catalog upgrade operation is developed on `master` and needs to be
cherry-picked to a release branch (e.g., `25.5`), keep the following in mind:

### Keep the version number unchanged

When cherry-picking a catalog upgrade operation from `master` to a release branch,
**do not change the version number**. The operation should use the same version
string (e.g., `"26.2.1"`) on both `master` and the release branch. This keeps
the change named consistently and simplifies tracking across branches. In the
future, catalog versions will be fully decoupled from Picodata release versions.

### The append-only rule still applies

When adding an entry to `CATALOG_UPGRADE_LIST` on a release branch, it must go
**after** all existing entries on that branch. If the branch already has entries
up to `"25.5.8"`, your cherry-picked entry must use `"25.5.9"` or later.

### Watch for conflicts in `CATALOG_UPGRADE_LIST`

Because `CATALOG_UPGRADE_LIST` is append-only on every branch, cherry-picks will
almost always cause merge conflicts in this file. Resolve them carefully —
reordering or dropping entries will break upgrades.

## Upgrading from Master Is Not Supported

Running a cluster on a build from `master` and then upgrading to a release build
(or vice versa) is **not supported**. Here's why:

- The upgrade flow compares the stored `system_catalog_version` against
  `LATEST_SYSTEM_CATALOG_VERSION` to determine which operations to apply. If a
  cluster was bootstrapped on a `master` build with version `"26.1.1"`, and then
  the release build also uses `"26.1.1"` but with different operations, the
  upgrade will be skipped (versions match), leaving the catalog in an
  inconsistent state.

**Bottom line:** builds from `master` are for development and testing only. Production
clusters must always run release builds and upgrade between released versions.

There is a [**ticket**](https://git.picodata.io/core/picodata/-/work_items/2823) to
decouple the Picodata version from the system catalog version, which would enable
upgrading from `master` builds in the future.

## Common Patterns and Gotchas

### System table ALTER via SQL causes deadlock

Do **not** use `("sql", "ALTER TABLE _pico_instance ...")` for system tables
that are used as predicates by CAS operations (`_pico_instance`, `_pico_replicaset`,
`_pico_tier`). There is a risk for deadlock.

Use `exec_script` instead, which modifies the format directly via Tarantool API.

### Operations must be idempotent

The governor may retry operations (e.g., after a leader change). Make sure your
operation handles being called twice without error. For `exec_script` operations,
`actualize_system_table_format` has built-in idempotency.

### Ordering within a version matters

Operations within a version entry are executed sequentially in the order listed.
Put dependencies first (e.g., create a table before inserting into it).
