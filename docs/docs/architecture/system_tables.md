# Описание системных таблиц

Данный раздел описывает [таблицы][t] Picodata, используемые для служебных
нужд. Перечисленные системные таблицы являются глобальными.

По умолчанию [доступ к системным таблицам][a] запрещен всем [пользователям
СУБД][u], кроме `admin` и `pico_service`.

Описание соответствует версии Picodata `25.2.0-23-ge0b4eb122`.

[t]: ../overview/glossary.md#table
[a]: ../admin/access_control.md#tables_access
[u]: ../admin/access_control.md#users

## Описание схемы данных {: #schema }

### _pico_table

Содержит информацию о пользовательских [таблицах][t] Picodata.

Глобальные таблицы реплицируются на каждый инстанс в кластере.

В шардированных таблицах весь набор данных разбивается на
[бакеты](../overview/glossary.md#bucket), пронумерованные
`bucket_id`. Каждый репликасет хранит свой набор бакетов. Данные
реплицируются между инстансами, принадлежащими одному репликасету.

Поля:

* `id`: (_unsigned_)
* `name`: (_string_) название таблицы
* `distribution`: (_map_) определяет распределение данных в кластере.
  Возможны следующие варианты:
    - `{"Global": null}` — глобальная таблица
    - `{"ShardedImplicitly": [sharding_key, sharding_fn, tier]}` —
      шардированная таблица, `bucket_id` вычисляется автоматически
      как `sharding_fn(sharding_key)`
        - `sharding_fn`: (_string_) функция шардирования, на сегодняшний
          день поддерживается только `"murmur3"`
        - `sharding_key`: (_array_) ключ шардирования — массив полей
          `[field, ...]`, по которым вычисляется `bucket_id`
        - `tier`:  имя тира, на инстансах которого хранятся данные этой таблицы.
        Тир указывается при создании таблицы (`CREATE TABLE ... ON TIER ...`)
    - `{"ShardedByField": [field, tier]}` — шардированная таблица, в качестве
      `bucket_id` используется значение поля `field` (ровно одного).
      Так же, указывается имя тира, на инстансах которого хранятся данные
* `format`: (_array_, `[{"name": ..., "field_type": ..., "is_nullable": ...}]`)
  массив словарей с описанием формата полей таблицы:
    - `name`: (_string_) название поля
    - `field_type`: (_string_, `"any" | "unsigned" | "string" |
      "double" | "integer" | "boolean" | "varbinary" |
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
* `description`: (_string_) описание таблицы

Индексы:

* `_pico_table_id` (unique), parts: `[id]`
* `_pico_table_name` (unique), parts: `[name]`
* `_pico_table_owner_id` (non-unique), parts: `[owner]`

### _pico_index

Содержит информацию об [индексах](../overview/glossary.md#index) БД.

Поля:

* `table_id`: (_unsigned_) идентификатор таблицы, к которой относится
  индекс
* `id`: (_unsigned_) идентификатор индекса, уникальный в пределах таблицы
* `name`: (_string_) название индекса
* `type`: (_string_) тип индекса, определяющий способ хранения и поиска
  данных в индексе — см. [CREATE INDEX ~ Типы индексов]
* `opts`: (_array_) массив параметров ключ-значение, определяющих поведение
  индекса:
    - `{"unique": true}` — индекс должен быть уникальным
    - `{"unique": false}` — допускается наличие дублирующихся значений в
      полях
* `parts`: (_array_) массив частей индекса, где каждая часть представляет
  собой поле или набор полей таблицы, по которым строится индекс
* `operable`: (_boolean_) признак доступности индекса для использования.
  Возможные значения:
    - `true` — индекс доступен
    - `false` — индекс находится в процессе создания или удаления
* `schema_version`: (_unsigned_) версия схемы, в которой индекс был создан

[CREATE INDEX ~ Типы индексов]: ../reference/sql/create_index.md#params

Индексы:

* `_pico_index_id` (unique), parts: `[table_id, id]`
* `_pico_index_name` (unique), parts: `[name]`

### _pico_routine

Содержит информацию о процедурах Picodata.

Поля:

* `id`: (_unsigned_) — идентификатор (тип `u32`, первичный ключ)
* `name`: (_string_) — имя (уникальный индекс)
* `kind`: (_string_) — тип хранимого объекта: функция или процедура
* `params`: (_array_) — таблица с типами параметров объекта, в виде `[
  {type: 'int', mode: 'in', default: 42}, {type: 'text'} ]`.
* `returns`: (_array_) — тип возвращаемого результата. Для процедур это
  пустой массив `[]`, для функций — массив типов в возвращаемом кортеже
* `language`: (_string_) — язык тела процедуры (например, `SQL`)
* `body`: (_string_) — тело основной части хранимой процедуры
* `security`: (_string_) — режим безопасности, определяющий, от чьего
  имени будет исполнена процедура (`invoker` — от имени вызывающего,
  `definer` — от имени стороннего пользователя)
* `operable`: (_boolean_) — признак доступности процедуры (для
  `prepare` — _false_, для `commit` — _true_)
* `schema_version`: (_unsigned_) — версия схемы данных в Raft на момент
  изменения хранимой процедуры
* `owner`: (_unsigned_) — идентификатор владельца (создателя) хранимой
  процедуры

Индексы:

* `_pico_routine_id` (unique), parts: `[id]`
* `_pico_routine_name` (unique), parts: `[name]`
* `_pico_routine_owner_id` (non-unique), parts: `[owner]`

## Описание свойств кластера {: #cluster_properties }

### _pico_property

Содержит свойства кластера в формате «ключ—значение».

Поля:

* `key`: (_string_)
* `value`: (_any_)

Индексы:

* `_pico_property_key` (unique), parts: `[key]`

### _pico_db_config

Хранит параметры (и только их), которые можно менять с помощью [ALTER
SYSTEM].

Поля:

* `key`: (_string_)
* `scope`: (_string_)
* `value`: (_any_)

Индексы:

* `_pico_db_config_pk` (unique), parts: `[key, scope]`
* `_pico_db_config_key` (non-unique), parts: `[key]`

[ALTER SYSTEM]: ../reference/sql/alter_system.md

## Описание топологии кластера {: #cluster_topology }

### _pico_peer_address

Содержит адреса всех пиров кластера.

Поля:

* `raft_id`: (_unsigned_)
* `address`: (_string_)
* `connection_type`: (_string_)

Индексы:

* `_pico_peer_address_raft_id` (unique), parts: `[raft_id, connection_type]`

### _pico_instance

Содержит информацию обо всех инстансах кластера.

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

Содержит информацию обо всех репликасетах кластера.

Поля:

* `name`: (_string_)
* `uuid`: (_string_)
* `current_master_name`: (_string_)
* `target_master_name`: (_string_)
* `tier`: (_string_)
* `weight`: (_double_)
* `weight_origin`: (_string_)
* `state`: (_string_, `"ready" | "not-ready"`)
* `current_config_version`: (_unsigned_)
* `target_config_version`: (_unsigned_)
* `promotion_vclock`: (_map_)

Индексы:

* `_pico_replicaset_name` (unique), parts: `[name]`
* `_pico_replicaset_uuid` (unique), parts: `[uuid]`

### _pico_tier

Содержит информацию обо всех [тирах] в кластере.

Поля:

* `name`: (_string_)
* `replication_factor`: (_unsigned_)
* `can_vote`: (_boolean_)
* `current_vshard_config_version`: (_unsigned_)
* `target_vshard_config_version`: (_unsigned_)
* `vshard_bootstrapped`: (_boolean_)
* `bucket_count`: (_unsigned_)

Индексы:

* `_pico_tier_name` (unique), parts: `[name]`

[тирах]: ../overview/glossary.md#tier

## Описание управления доступом {: #access_control }

### _pico_user

Содержит информацию обо всех пользователях и ролях Picodata.

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

Содержит информацию обо всех привилегиях, предоставленных пользователям Picodata.

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

## Описание плагинов {: #plugins }

Содержит информацию о плагинах Picodata.

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

Содержит информацию о топологии сервисов, включенных в плагины Picodata.

Поля:

* `plugin_name`: (_string_)
* `name`: (_string_)
* `version`: (_string_)
* `tiers`: (_array_)
* `description`: (_string_)

Индексы:

* `_pico_service_name` (unique), parts: `[plugin_name, name, version]`

### _pico_service_route

Содержит информацию о маршрутизации между сервисами Picodata.

Поля:

* `plugin_name`: (_string_)
* `plugin_version`: (_string_)
* `service_name`: (_string_)
* `instance_name`: (_string_)
* `poison`: (_boolean_)

Индексы:

* `_pico_service_routing_key` (unique), parts: `[plugin_name, plugin_version, service_name, instance_name]`

### _pico_plugin_migration

Содержит информацию о миграциях плагинов Picodata.

Поля:

* `plugin_name`: (_string_)
* `migration_file`: (_string_)
* `hash`: (_string_)

Индексы:

* `_pico_plugin_migration_primary_key` (unique), parts: `[plugin_name, migration_file]`

### _pico_plugin_config

Содержит информацию о конфигурациях плагинов Picodata.

Поля:

* `plugin`: (_string_)
* `version`: (_string_)
* `entity`: (_string_)
* `key`: (_string_)
* `value`: (_any_)

Индексы:

* `_pico_plugin_config_pk` (unique), parts: `[plugin, version, entity, key]`
