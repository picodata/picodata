Описание соответствует версии Picodata `25.5.0-51-g8de1078c9`.

### _pico_table

Поля:

* `id`: (_unsigned_)
* `name`: (_string_)
* `distribution`: (_map_)
* `format`: (_array_)
* `schema_version`: (_unsigned_)
* `operable`: (_boolean_)
* `engine`: (_string_)
* `owner`: (_unsigned_)
* `description`: (_string_)

Индексы:

* `_pico_table_id` (unique), parts: `[id]`
* `_pico_table_name` (unique), parts: `[name]`
* `_pico_table_owner_id` (non-unique), parts: `[owner]`

### _pico_index

Поля:

* `table_id`: (_unsigned_)
* `id`: (_unsigned_)
* `name`: (_string_)
* `type`: (_string_)
* `opts`: (_array_)
* `parts`: (_array_)
* `operable`: (_boolean_)
* `schema_version`: (_unsigned_)

Индексы:

* `_pico_index_id` (unique), parts: `[table_id, id]`
* `_pico_index_name` (unique), parts: `[name]`

### _pico_routine

Поля:

* `id`: (_unsigned_)
* `name`: (_string_)
* `kind`: (_string_)
* `params`: (_array_)
* `returns`: (_array_)
* `language`: (_string_)
* `body`: (_string_)
* `security`: (_string_)
* `operable`: (_boolean_)
* `schema_version`: (_unsigned_)
* `owner`: (_unsigned_)

Индексы:

* `_pico_routine_id` (unique), parts: `[id]`
* `_pico_routine_name` (unique), parts: `[name]`
* `_pico_routine_owner_id` (non-unique), parts: `[owner]`

### _pico_property

Поля:

* `key`: (_string_)
* `value`: (_any_)

Индексы:

* `_pico_property_key` (unique), parts: `[key]`

### _pico_db_config

Поля:

* `key`: (_string_)
* `scope`: (_string_)
* `value`: (_any_)

Индексы:

* `_pico_db_config_pk` (unique), parts: `[key, scope]`
* `_pico_db_config_key` (non-unique), parts: `[key]`

### _pico_peer_address

Поля:

* `raft_id`: (_unsigned_)
* `address`: (_string_)
* `connection_type`: (_string_)

Индексы:

* `_pico_peer_address_raft_id` (unique), parts: `[raft_id, connection_type]`

### _pico_instance

Поля:

* `name`: (_string_)
* `uuid`: (_string_)
* `raft_id`: (_unsigned_)
* `replicaset_name`: (_string_)
* `replicaset_uuid`: (_string_)
* `current_state`: (_array_)
* `target_state`: (_array_)
* `failure_domain`: (_map_)
* `tier`: (_string_)
* `picodata_version`: (_string_)

Индексы:

* `_pico_instance_name` (unique), parts: `[name]`
* `_pico_instance_uuid` (unique), parts: `[uuid]`
* `_pico_instance_raft_id` (unique), parts: `[raft_id]`
* `_pico_instance_replicaset_name` (non-unique), parts: `[replicaset_name]`

### _pico_replicaset

Поля:

* `name`: (_string_)
* `uuid`: (_string_)
* `current_master_name`: (_string_)
* `target_master_name`: (_string_)
* `tier`: (_string_)
* `weight`: (_double_)
* `weight_origin`: (_string_)
* `state`: (_string_)
* `current_config_version`: (_unsigned_)
* `target_config_version`: (_unsigned_)
* `promotion_vclock`: (_map_)

Индексы:

* `_pico_replicaset_name` (unique), parts: `[name]`
* `_pico_replicaset_uuid` (unique), parts: `[uuid]`

### _pico_tier

Поля:

* `name`: (_string_)
* `replication_factor`: (_unsigned_)
* `can_vote`: (_boolean_)
* `current_vshard_config_version`: (_unsigned_)
* `target_vshard_config_version`: (_unsigned_)
* `vshard_bootstrapped`: (_boolean_)
* `bucket_count`: (_unsigned_)
* `is_default`: (_boolean_)

Индексы:

* `_pico_tier_name` (unique), parts: `[name]`

### _pico_user

Поля:

* `id`: (_unsigned_)
* `name`: (_string_)
* `schema_version`: (_unsigned_)
* `auth`: (_array_)
* `owner`: (_unsigned_)
* `type`: (_string_)

Индексы:

* `_pico_user_id` (unique), parts: `[id]`
* `_pico_user_name` (unique), parts: `[name]`
* `_pico_user_owner_id` (non-unique), parts: `[owner]`

### _pico_privilege

Поля:

* `grantor_id`: (_unsigned_)
* `grantee_id`: (_unsigned_)
* `privilege`: (_string_)
* `object_type`: (_string_)
* `object_id`: (_integer_)
* `schema_version`: (_unsigned_)

Индексы:

* `_pico_privilege_primary` (unique), parts: `[grantee_id, object_type, object_id, privilege]`
* `_pico_privilege_object` (non-unique), parts: `[object_type, object_id]`

### _pico_plugin

Поля:

* `name`: (_string_)
* `enabled`: (_boolean_)
* `services`: (_array_)
* `version`: (_string_)
* `description`: (_string_)
* `migration_list`: (_array_)

Индексы:

* `_pico_plugin_name` (unique), parts: `[name, version]`

### _pico_service

Поля:

* `plugin_name`: (_string_)
* `name`: (_string_)
* `version`: (_string_)
* `tiers`: (_array_)
* `description`: (_string_)

Индексы:

* `_pico_service_name` (unique), parts: `[plugin_name, name, version]`

### _pico_service_route

Поля:

* `plugin_name`: (_string_)
* `plugin_version`: (_string_)
* `service_name`: (_string_)
* `instance_name`: (_string_)
* `poison`: (_boolean_)

Индексы:

* `_pico_service_routing_key` (unique), parts: `[plugin_name, plugin_version, service_name, instance_name]`

### _pico_plugin_migration

Поля:

* `plugin_name`: (_string_)
* `migration_file`: (_string_)
* `hash`: (_string_)

Индексы:

* `_pico_plugin_migration_primary_key` (unique), parts: `[plugin_name, migration_file]`

### _pico_plugin_config

Поля:

* `plugin`: (_string_)
* `version`: (_string_)
* `entity`: (_string_)
* `key`: (_string_)
* `value`: (_any_)

Индексы:

* `_pico_plugin_config_pk` (unique), parts: `[plugin, version, entity, key]`
