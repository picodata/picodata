# SQL Execution Protocol

## Context

Мы исполняем запросы DQL в распределенной системе, используя двухфазный протокол:

**Первая фаза (RequiredData)** - отправляем `plan_id` (ключ в кэше) и данные для исполнения подготовленных запросов (
параметры, виртуальные таблицы и другие).

**Вторая фаза (OptionalData)** - происходит только если запрос не найден в кэше. Отправляем все данные из первой фазы
плюс дополнительные данные для подготовки запроса, чтобы в следующий раз выполниться в первой фазе.

Для DML запросов мы используем однофазное исполнение, всегда посылаем полные данные.

## Considered Options

### Version 0 (Current)

**RequiredData (1st phase):**

```rust
pub struct RequiredData {
    pub plan_id: SmolStr,
    pub parameters: Vec<Value>,
    pub query_type: QueryType,
    pub options: Options,
    pub schema_info: SchemaInfo,
    pub vtables: EncodedVTables,
}
```

**OptionalData (2nd phase):**

```rust
pub struct OptionalData {
    pub exec_plan: ExecutionPlan,
    pub ordered: OrderedSyntaxNodes,
    pub vtables_meta: VTablesMeta,
}
```

**Различия между фазами:**

- Первая фаза: `(RequiredData, 'cacheable-1')`
- Вторая фаза: `(RequiredData, OptionalData, 'cacheable-2')`

**Подготовка запроса во второй фазе:**

1. Из параметров `OptionalData` создаем локальный SQL
2. `exec_plan` и `vtables_meta` используются для создания виртуальных таблиц
3. Данные для таблиц находятся в `RequiredData.vtables`
4. За SQL отвечает `OptionalData.ordered`
5. Подготавливаем запрос (добавляем в кэш), заполняем таблицы и выполняем

**Структура RequiredData:**

*Информационные поля:*

- `queryType` - DML, DQL
- `options` - опции исполнения (например, максимальное количество строк для перемещения)
- `schema_info` - версия таблиц для проверки совместимости

*Поля для исполнения:*

- `plan_id` - id запроса в кэше
- `parameters` - параметры для вставки ($1)
- `vtables` - наполнение виртуальных таблиц

**Представление в msgpack:**

## Фаза 1

| Поле           | Тип          | Описание                      |
|----------------|--------------|-------------------------------|
| Array Header   | `[2]`        | Массив из 2 элементов         |
| Metadata Array | `[1]`        | Служебный массив              |
| Marker         | `string[32]` | 32-символьный идентификатор   |
| Length         | `uint`       | Длина полезной нагрузки       |
| Payload        | `bytes`      | Обязательные данные (bincode) |
| Cache Info     | `0`          | 1ая фаза                      |

## Фаза 2

| Поле             | Тип          | Описание                          |
|------------------|--------------|-----------------------------------|
| Array Header     | `[3]`        | Массив из 3 элементов             |
| Metadata Array   | `[1]`        | Служебный массив                  |
| Primary Marker   | `string[32]` | Идентификатор основных данных     |
| Primary Length   | `uint`       | Длина основной полезной нагрузки  |
| Primary Payload  | `bytes`      | Обязательные данные (bincode)     |
| Optional Array   | `[1]`        | Массив для опциональных данных    |
| Optional Marker  | `string[32]` | Идентификатор опциональных данных |
| Optional Length  | `uint`       | Длина опциональной нагрузки       |
| Optional Payload | `bytes`      | Опциональные данные (bincode)     |
| Cache Info       | `1`          | 2ая фаза                          |

**Плюсы:**

- Двухфазная модель минимизирует объем передаваемых данных в большинстве случаев
- vtables представлены в msgpack, позволяя итерироваться без десериализации всей таблицы

**Минусы:**

- Вторая фаза получается большой - `ordered` передается, чтобы построить SQL из `exec_plan`. При этом может быть
  воспроизведен из exec_plan
- Используется bincode, который затем оборачивается в msgpack
- bincode сериализует больше байт чем msgpack

### Version 1 (Proposed)

**Основные изменения:**

- Переход с bincode на msgpack для консистентности и zero-copy десериализации
- Устранение дублирования данных между `exec_plan` и `ordered`
- Разделение логики для DQL и DML запросов

**Структура протокола:**

| Field   | MsgPack Type | Example Value     |
|---------|--------------|-------------------|
| type    | fixint       | 0 (DQL 1st phase) |
| payload | raw bytes    | [1..100]          |

Первый байт определяет тип запроса

| Type          | Value |
|---------------|-------|
| DQL 1st phase | 0     |
| DQL 2nd phase | 1     |
| DML clear     | 2     |
| DML + DQL     | 3     |

#### DQL 1st phase (cache hit)

| Field        | MsgPack Type            | Example Value                     |
|--------------|-------------------------|-----------------------------------|
| shema_info   | map (u64, u64)          | 10, [1..20]                       |
| plan_id      | i64                     | 0                                 |
| spaces' data | map(str, EncodedTuples) | 10, ["TMP_13", EncodedTuples] ... |
| options      | [u64, u64]              | [100, 200]                        |
| params       | array of values         | 3, true, 1.0, 10                  |

EncodedTuples

| Field  | MsgPack Type | Example Value          |
|--------|--------------|------------------------|
| Tuples | array[32bin] | 10, [10, [1..10], ...] |

#### DQL 2nd phase (cache miss)

| Field        | MsgPack Type              | Example Value                     |
|--------------|---------------------------|-----------------------------------|
| shema_info   | map (u64, u64)            | 10, [1..20]                       |
| spaces' meta | array of VirtualTableMeta | [VirtualTableMeta, ...]           |
| spaces' data | map(str, EncodedTuples)   | 10, ["TMP_13", EncodedTuples] ... |
| plan_id      | i64                       | 0                                 |
| sql          | string                    | 10, [1..10]                       |
| options      | [u64, u64]                | [100, 200]                        |
| params       | array of values           | 3, true, 1.0, 10                  |

VirtualTableMeta

| Field   | MsgPack Type    | Example Value |
|---------|-----------------|---------------|
| columns | array of Column | 10, [1..20]   |
| name    | string          | "TMP_10321"   |

Column

| Field | MsgPack Type | Example Value  |
|-------|--------------|----------------|
| name  | string       | "something"    |
| type  | fixint       | 0 (i.e String) |

#### DML

| Field | MsgPack Type | Example Value |
|-------|--------------|---------------|
| type  | fixint       | 1             |

| DML Type | Value |
|----------|-------|
| Insert   | 0     |
| Update   | 1     |
| Delete   | 2     |

#### DML without DQL

-- Delete

| Field                | MsgPack Type          | Example Value           |
|----------------------|-----------------------|-------------------------|
| target table         | u64                   | 1                       |
| target table version | u64                   | 1                       |
| tuples               | Option<EncodedTuples> | 1, [EncodedTuples, ...] |

-- Insert

| Field                | MsgPack Type | Example Value |
|----------------------|--------------|---------------|
| target table         | u64          | 1             |
| target table version | u64          | 1             |
| columns              | array(u64)   | [1, 0, 2]     |
| conflict_policy      | fixint       | 0 (DoNothing) |
| space's data         | EncodedSpace | EncodedSpace  |

| Policy    | Value |
|-----------|-------|
| DoFail    | 0     |
| DoNothing | 1     |
| DoReplace | 2     |

Encoded Space

| Field        | MsgPack Type          | Example Value         |
|--------------|-----------------------|-----------------------|
| column types | array of column types | 10, [String, ...]     |
| buckets data | array of BucketData   | 10, [BucketData, ...] |

BucketData

| Field     | MsgPack Type  | Example Value |
|-----------|---------------|---------------|
| bucket id | u64           | 100           |
| tuples    | EncodedTuples | EncodedTuples |

-- Update

| Field                | MsgPack Type            | Example Value      |
|----------------------|-------------------------|--------------------|
| target table         | u64                     | 1                  |
| target table version | u64                     | 1                  |
| shared update        | Option<SharedUpdate>    | 1, SharedUpdate    |
| local update         | Option<LocalUpdateFull> | 1, LocalUpdateFull |

Shared Update

| Field            | MsgPack Type  | Example Value     |
|------------------|---------------|-------------------|
| delete_tuple_len | usize         | 32                |
| update_columns   | map(u64, u64) | 10, [(0, 0), ...] |
| space's data     | Encoded Space | Encoded Space     |

Local Update Meta

| Field          | MsgPack Type          | Example Value     |
|----------------|-----------------------|-------------------|
| update_columns | map(u64, u64)         | 10, [(0, 0), ...] |
| pk_positions   | array of u64          | 10, [0, ...]      |
| column types   | array of column types | 10, [String, ...] |

Local Update Full

| Field  | MsgPack Type    | Example Value   |
|--------|-----------------|-----------------|
| meta   | LocalUpdateMeta | LocalUpdateMeta |
| tuples | EncodedTuples   | EncodedTuples   |

#### DML with DQL

-- Delete

| Field                | MsgPack Type              | Example Value           |
|----------------------|---------------------------|-------------------------|
| target table         | u64                       | 1                       |
| target table version | u64                       | 1                       |
| shema_info           | map (u64, u64)            | 10, [1..20]             |
| space's meta         | array of VirtualTableMeta | [VirtualTableMeta, ...] |
| space's data         | EncodedTuples             | EncodedTuples           |
| plan_id              | i64                       | 0                       |
| sql                  | string                    | 10, [1..10]             |
| options              | [u64, u64]                | [100, 200]              |
| params               | array of values           | 3, true, 1.0, 10        |

-- Insert

| Field                | MsgPack Type              | Example Value           |
|----------------------|---------------------------|-------------------------|
| target table         | u64                       | 1                       |
| target table version | u64                       | 1                       |
| shema_info           | map (u64, u64)            | 10, [1..20]             |
| columns              | array(u64)                | [1, 0, 2]               |
| conflict_policy      | fixint                    | 0 (DoNothing)           |
| space's meta         | array of VirtualTableMeta | [VirtualTableMeta, ...] |
| space's data         | EncodedTuples             | EncodedTuples           |
| plan_id              | i64                       | 0                       |
| sql                  | string                    | 10, [1..10]             |
| options              | [u64, u64]                | [100, 200]              |
| params               | array of values           | 3, true, 1.0, 10        |

-- Update

[//]: # (case where we need vtable locally)

| Field                | MsgPack Type              | Example Value           |
|----------------------|---------------------------|-------------------------|
| target table         | u64                       | 1                       |
| target table version | u64                       | 1                       |
| shema_info           | map (u64, u64)            | 10, [1..20]             |
| space's meta         | array of VirtualTableMeta | [VirtualTableMeta, ...] |
| local update         | LocalUpdateMeta           | LocalUpdateMeta         |
| plan_id              | i64                       | 0                       |
| sql                  | string                    | 10, [1..10]             |
| options              | [u64, u64]                | [100, 200]              |
| params               | array of values           | 3, true, 1.0, 10        |

**Плюсы:**

- Сохранены преимущества предыдущей версии
- Отсутствует передача лишних данных:
    - DQL получает SQL только один раз
    - DML не получает план, если не нужно материализовать vtable
- Четкое разграничение DQL и DML с учетом их разных потребностей в данных

**Минусы:**

- Более сложный парсинг - на каждом этапе обработки парсим нужные данные (парсинг в разных методах)
- Мы можем исполнить не тот DQL при коллизии plan_id. Можно получить не тот sql, однако по подсчетам,
  такого не должно случиться (https://git.picodata.io/core/tarantool/-/issues/59)

## Decision

Переходим на **Version 1** с breaking changes в совместимости.

**Обоснование:**

- Оптимизация размера передаваемых данных за счет специализации под DQL/DML
- Переход на msgpack обеспечивает консистентность и меньший размер передаваемых данных
- Устранение дублирования данных упрощает поддержку

**Миграционный план:**
Готовим новую версию, покрываем тестами. По готовности протокола, ломаем совместимость и переходим на новый протокол.
Включает в себя также переход plan_id на i64, вместо SmolStr.