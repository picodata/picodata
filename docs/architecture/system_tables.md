# Описание системных таблиц

Данный раздел описывает [таблицы](../overview/glossary.md#table)
Picodata, используемые для служебных нужд. Перечисленные системные
таблицы являются глобальными.

<!--
Описание соответствует версии
Picodata 23.06.0-287-ga98dc6919
-->

## Описание схемы данных {: #schema }

### _pico_table {: #_pico_table }

Содержит информацию о пользовательских
[таблицах](../overview/glossary.md#table) Picodata.

Глобальные таблицы реплицируются на каждый инстанс в кластере.

В шардированных таблицах весь набор данных разбивается на сегменты —
виртуальные [бакеты](../overview/glossary.md#bucket) (bucket),
пронумерованные `bucket_id`. Каждый репликасет хранит свой набор
бакетов. Данные реплицируются между инстансами, принадлежащими одному
репликасету.

Поля:

* `id`: (_unsigned_)
* `name`: (_string_) название таблицы
* `distribution`: (_array_) определяет распределение данных в кластере.
  Возможны следующие варианты:
    - `["global"]` — глобальная таблица
    - `["sharded_implicitly", sharding_key, sharding_fn]` —
      шардированная таблица, `bucket_id` вычисляется автоматически
      как `sharding_fn(sharding_key)`
        - `sharding_fn`: (_string_) функция шардирования, на сегодняшний
          день поддерживается только `"crc32"`
        - `sharding_key`: (_array_) ключ шардирования — массив полей
          `[field,...]`, по которым вычисляется `bucket_id`
    - `["sharded_by_field", field]` — шардированная таблица, в качестве
      `bucket_id` используется значение поля `field`
* `format`: (_array_, `[[field_name, field_type, is_nullable]]`) массив с описанием
  формата полей таблицы:
    - `field_name`: (_string_) название поля
    - `field_type`: (_string_, `"any" | "unsigned" | "string" | "number" |
      "double" | "integer" | "boolean" | "varbinary" | "scalar" |
      "decimal" | "uuid" | "datetime" | "interval" | "array" |
      "map"`) тип хранимого значения
    - `is_nullable`: (_boolean_) возможность хранить значение `NULL`
* `schema_version`: (_unsigned_) версия схемы, в которой таблица была
  создана. Используется при восстановлении из снапшота для корректной
  обработки шардированных таблиц
* `operable`: (_boolean_) признак доступности таблицы на запись.
  Используется в процессе создания и удаления таблиц
* `engine`: (_string_, `"memtx" | "vinyl"`) [движок хранения](../overview/glossary.md#db_engine)
* `owner`: (_unsigned_) создатель таблицы

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
* `current_master_id` (*string*)
* `target_master_id` (*string*)
* `tier` (*string*)
* `weight` (*number*)
* `weight_origin` (*string*)
* `state` (*string*, `"ready" | "not-ready"`)

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
* `owner` (*unsigned*)
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
* `object_id` (*integer*)
* `privilege` (*string*)
* `schema_version` (*unsigned*)

Индексы:

* `primary` (unique), parts: `[grantee_id, object_type, object_id, privilege]`
* `object` (unique), parts: `[object_type, object_id]`

### _pico_role {: #_pico_role }

Содержит информацию обо всех ролях Picodata.

Поля:

* `id` (*unsigned*)
* `name` (*string*)
* `owner` (*unsigned*)
* `schema_version` (*unsigned*)

Индексы:

* `id` (unique), parts: `[id]`
* `name` (unique), parts: `[name]`
