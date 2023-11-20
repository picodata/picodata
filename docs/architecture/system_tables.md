# Описание системных таблиц

Данный раздел описывает [таблицы](../overview/glossary.md#table)
Picodata, используемые для служебных нужд. Перечисленные системные
таблицы являются глобальными.

<!--
Описание соответствует версии
Picodata 23.06.0-232-ge436159d5
-->

## Описание схемы данных {: #schema }

### _pico_table {: #_pico_table }

Содержит информацию о пользовательских
[таблицах](../overview/glossary.md#table) Picodata.

Поля:

* `id`: (_unsigned_)
* `name`: (_string_)
* `distribution`: (_array_)
* `format`: (_array_)
* `schema_version`: (_unsigned_) версия схемы, в которой таблица была
  создана. Используется при восстановлении из снапшота для корректной
  обработки шардированных таблиц
* `operable`: (_boolean_) признак доступности таблицы на запись.
  Используется в процессе создания и удаления таблиц
* `engine` (_string_)

Индексы:

* `id` (unique), parts: `[id]`
* `name` (unique), parts: `[name]`

### _pico_index {: #_pico_index }

Содержит информацию об [индексах](../overview/glossary.md#index) БД.

Поля:

* `table_id`: (_unsigned_)
* `id`: (_unsigned_)
* `name`: (_string_)
* `local`: (_boolean_)
* `parts`: (_array_)
* `schema_version`: (_unsigned_)
* `operable`: (_boolean_)
* `unique`: (_boolean_)

Индексы:

* `id` (unique), parts: `[table_id, id]`
* `name` (unique), parts: `[table_id, name]`

## Описание свойств кластера {: #cluster_properties }

### _pico_property {: #_pico_property }

Содержит свойства кластера в формате «ключ—значение».

Поля:

* `key` (*string*)
* `value` (*any*)

Индексы:

* `key` (unique), parts: `[key]`

## Описание топологии кластера {: #cluster_topology }

### _pico_peer_address {: #_pico_peer_address }

Содержит адреса всех пиров кластера.

Поля:

* `raft_id` (*unsigned*)
* `address` (*string*)

Индексы:

* `raft_id` (unique), parts: `[raft_id]`

### _pico_instance {: #_pico_instance }

Содержит информацию обо всех инстансах кластера.

Поля:

* `instance_id` (*string*)
* `instance_uuid` (*string*)
* `raft_id` (*unsigned*)
* `replicaset_id` (*string*)
* `replicaset_uuid` (*string*)
* `current_grade` (*array*)
* `target_grade` (*array*)
* `failure_domain` (*map*)
* `tier` (*string*)

Индексы:

* `instance_id` (unique), parts: `[instance_id]`
* `raft_id` (unique), parts: `[raft_id]`
* `replicaset_id` (non-unique), parts: `[replicaset_id]`

### _pico_replicaset {: #_pico_replicaset }

Содержит информацию обо всех репликасетах кластера.

Поля:

* `replicaset_id` (*string*)
* `replicaset_uuid` (*string*)
* `master_id` (*string*)
* `tier` (*string*)
* `weight` (*number*)
* `weight_origin` (*string*)
* `weight_state` (*string*)

Индексы:

* `replicaset_id` (unique), parts: `[replicaset_id]`

### _pico_tier {: #_pico_tier }

Содержит информацию обо всех тирах в кластере.

Поля:

* `name` (*string*)
* `replication_factor` (*unsigned*)

Индексы:

* `name` (unique), parts: `[name]`

## Описание управления доступом {: #access_control }

### _pico_user {: #_pico_user }

Содержит информацию обо всех пользователях Picodata.

Поля:

* `id` (*unsigned*)
* `name` (*string*)
* `schema_version` (*unsigned*)
* `auth `(*array*)

Индексы:

* `id` (unique), parts: `[id]`
* `name` (unique), parts: `[name]`

### _pico_privilege {: #_pico_privilege }

Содержит информацию обо всех привилегиях, предоставленных пользователям Picodata.

Поля:

* `grantor_id` (*unsigned*)
* `grantee_id` (*unsigned*)
* `object_type` (*string*)
* `object_name` (*string*)
* `privilege` (*string*)
* `schema_version` (*unsigned*)

Индексы:

* `primary` (unique), parts: `[grantee_id, object_type, object_name, privilege]`
* `object` (unique), parts: `[object_type, object_name]`

### _pico_role {: #_pico_role }

Содержит информацию обо всех ролях Picodata.

Поля:

* `id` (*unsigned*)
* `name` (*string*)
* `schema_version` (*unsigned*)

Индексы:

* `id` (unique), parts: `[id]`
* `name` (unique), parts: `[name]`
