
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

Индексы:

* `_pico_table_id` (unique), parts: `[id]`
* `_pico_table_name` (unique), parts: `[name]`

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
* `owner`: (_unsigned_)

Индексы:

* `_pico_index_id` (unique), parts: `[table_id, id]`
* `_pico_index_name` (unique), parts: `[table_id, name]`

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

### _pico_property

Поля:

* `key`: (_string_)
* `value`: (_any_)

Индексы:

* `_pico_property_key` (unique), parts: `[key]`

### _pico_peer_address

Поля:

* `raft_id`: (_unsigned_)
* `address`: (_string_)

Индексы:

* `_pico_peer_address_raft_id` (unique), parts: `[raft_id]`

### _pico_instance

Поля:

* `instance_id`: (_string_)
* `instance_uuid`: (_string_)
* `raft_id`: (_unsigned_)
* `replicaset_id`: (_string_)
* `replicaset_uuid`: (_string_)
* `current_grade`: (_array_)
* `target_grade`: (_array_)
* `failure_domain`: (_map_)
* `tier`: (_string_)

Индексы:

* `_pico_instance_id` (unique), parts: `[instance_id]`
* `_pico_instance_raft_id` (unique), parts: `[raft_id]`
* `_pico_instance_replicaset_id` (non-unique), parts: `[replicaset_id]`

### _pico_replicaset

Поля:

* `replicaset_id`: (_string_)
* `replicaset_uuid`: (_string_)
* `current_master_id`: (_string_)
* `target_master_id`: (_string_)
* `tier`: (_string_)
* `weight`: (_number_)
* `weight_origin`: (_string_)
* `state`: (_string_)

Индексы:

* `_pico_replicaset_id` (unique), parts: `[replicaset_id]`

### _pico_tier

Поля:

* `name`: (_string_)
* `replication_factor`: (_unsigned_)

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

### _pico_privilege

Поля:

* `privilege`: (_string_)
* `object_type`: (_string_)
* `object_id`: (_integer_)
* `grantee_id`: (_unsigned_)
* `grantor_id`: (_unsigned_)
* `schema_version`: (_unsigned_)

Индексы:

* `_pico_privilege_primary` (unique), parts: `[grantee_id, object_type, object_id, privilege]`
* `_pico_privilege_object` (non-unique), parts: `[object_type, object_id]`
