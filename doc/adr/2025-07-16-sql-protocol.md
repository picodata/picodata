# SQL Execution Protocol

status:
decision-makers: @ai, @darthunix, @funbringer, @kostja

## Context and Problem

Мы исполняем запросы DQL в распределенной системе, используя двухфазный протокол:

**Первая фаза (RequiredData)** - отправляем `plan_id` (ключ в кэше) и данные для исполнения подготовленных запросов (
параметры, виртуальные таблицы и другие).

**Вторая фаза (OptionalData)** - происходит только если запрос не найден в кэше. Отправляем все данные из первой фазы
плюс дополнительные данные для подготовки запроса, чтобы в следующий раз выполниться в первой фазе.

Для DML запросов мы используем однофазное исполнение, всегда посылаем полные данные.

**RequiredData (1st phase):**

```rust
use std::collections::HashMap;

pub struct RequiredData {
    pub plan_id: SmolStr,
    // values for placeholders
    pub parameters: Vec<Value>,
    // DML/DQL
    pub query_type: QueryType,
    // [sql_motion_row_max, sql_vdbe_opcode_max]
    pub options: Options,
    // table name -> version
    pub schema_info: HashMap<SmolStr, u64>,
    // vtable name -> encoded tuples
    pub vtables: HashMap<SmolStr, EncodedRows>,
}
```

```rust
pub struct EncodedRows {
    /// Lengths of encoded rows.
    pub marking: Vec<usize>,
    /// Encoded rows as msgpack array.
    pub encoded: Binary,
}
```

**OptionalData (2nd phase):**

```rust
pub struct OptionalData {
    // ExecutionPlan have all nodes, relation, vtables, undo transformation 
    pub exec_plan: ExecutionPlan,
    // Lightweight tree that indicates order of nodes
    pub ordered: OrderedSyntaxNodes,
    // Meta of vtables. The meta is used to create vtables 
    pub vtables_meta: VTablesMeta,
}
```

**Различия между фазами:**

- Первая фаза: `(RequiredData, 'cacheable-1')`
- Вторая фаза: `(RequiredData, OptionalData, 'cacheable-2')`

**Представление в msgpack:**

### Фаза 1

| Поле           | Тип          | Описание                      |
|----------------|--------------|-------------------------------|
| Array Header   | `[2]`        | Массив из 2 элементов         |
| Metadata Array | `[1]`        | Служебный массив              |
| Marker         | `string[32]` | 32-символьный идентификатор   |
| Length         | `uint`       | Длина полезной нагрузки       |
| Payload        | `bytes`      | Обязательные данные (bincode) |
| Cache Info     | `0`          | 1ая фаза                      |

### Фаза 2

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

### Motivation

1) Снизить размер передаваемых данных
2) Убрать двойственность в данных
3) Убрать смесь протоколов, чтобы можно было использовать внутри тарантула данные, которые пришли по сети

## Considered Option

**Основные изменения:**

- Переход с bincode на msgpack для консистентности и zero-copy десериализации
- Переход на передачу sql, как более компактной формы плана
- Уменьшение работы кластера за счет формирования финального представления на роутере
- Разделение логики для DQL и DML запросов
- Дозапрос данных при cache miss, для этого добавление связи с router

**Структура протокола:**

| Field      | MsgPack Type | Example Value     |
|------------|--------------|-------------------|
| request_id | string       | 10 [0..10] (uuid) |
| type       | fixint       | 0 (DQL 1st phase) |
| payload    | raw bytes    | [1..100]          |

Сначала мы имеем идентификатор запуска запроса. Он будет один на всех storage и будет сгенерирован на роутере.
Идентификатор предназначен для лучшего логирования и трассировки запросов.
Затем идет тип запроса, типы переведены в таблице.

| Type      | Value |
|-----------|-------|
| DQL       | 0     |
| DML clear | 1     |
| DML + DQL | 2     |

Затем идет payload, который является msgpack.

### DQL

DQL payload выглядит так:

| Field         | MsgPack Type            | Example Value                     |
|---------------|-------------------------|-----------------------------------|
| schema_info   | map (u32, u64)          | 10, [1..20]                       |
| plan_id       | u64                     | 0                                 |
| sender_id     | u64                     | 3 (router raft id)                |
| vtables' data | map(str, EncodedTuples) | 10, ["TMP_13", EncodedTuples] ... |
| options       | [u64, u64]              | [100, 200]                        |
| params        | array of values         | 3, true, 1.0, 10                  |

EncodedTuples

| Field  | MsgPack Type          | Example Value           |
|--------|-----------------------|-------------------------|
| Tuples | array of EncodedTuple | 10, [EncodedTuple, ...] |

EncodedTuple

| Field | MsgPack Type | Example Value |
|-------|--------------|---------------|
| Tuple | 32bin        | 10, [1..10]   |

#### Cache Miss

Если происходит cache miss, тогда производиться дополнительный запрос данных к router'у. Мы обращаемся с помощью
sender_id и
передаем в качестве идентификатора запроса request_id с plan_id.

Пакет с дополнительными данными выглядит так:

| Field         | MsgPack Type                   | Example Value                            |
|---------------|--------------------------------|------------------------------------------|
| schema_info   | map (u32, u64)                 | 10, [1..20]                              |
| vtables' meta | map (string, array of Columns) | 10, ["TMP_10321", 10, [Column, ...]] ... |
| sql           | string                         | 10, "SELECT * FROM t"                    |

Column

| Field | MsgPack Type   | Example Value |
|-------|----------------|---------------|
| name  | string         | "something"   |
| type  | fixint (Types) | 0 (i.e. Map)  |

Types

| Column Type | Value |
|-------------|-------|
| Map         | 0     |
| Boolean     | 1     |
| Datetime    | 2     |
| Decimal     | 3     |
| Double      | 4     |
| Integer     | 5     |
| String      | 6     |
| Uuid        | 7     |
| Any         | 8     |
| Array       | 9     |
| Scalar      | 10    |

### DML

Все DML имеют следующий header

| Field   | MsgPack Type | Example Value |
|---------|--------------|---------------|
| type    | fixint       | 1             |
| payload | raw msgpack  | 5, [...]      |

| DML Type | Value |
|----------|-------|
| Insert   | 0     |
| Update   | 1     |
| Delete   | 2     |

### DML without DQL

Данный тип запросов обычно представляет собой уже материализованную таблицу. Нужно выполнить преобразование на сторадже
с готовыми данными.

**Delete**

Этот пакет обслуживает следующие запросы

Без where

```sql
DELETE
FROM target_table;
```

Без локальной материализации

```sql
DELETE
FROM target_table
WHERE id in (select t_id from t2 where s_id = 2);
```

| Field                | MsgPack Type  | Example Value |
|----------------------|---------------|---------------|
| target table         | u32           | 1             |
| target table version | u64           | 1             |
| tuples               | EncodedTuples | EncodedTuples |

При удалении без условия будет только table_id и table_version. Потому что нам надо различать удаление без условия
и удаление, когда условие дало пустое множество.

**Insert**

Запросы без локальной материализации

```sql
INSERT INTO target_table
VALUES (select a, b, c from t2 where s_id = 2);
```

| Field                | MsgPack Type  | Example Value |
|----------------------|---------------|---------------|
| target table         | u32           | 1             |
| target table version | u64           | 1             |
| conflict_policy      | fixint        | 0 (DoNothing) |
| tuples               | EncodedTuples | EncodedTuples |

| Policy    | Value |
|-----------|-------|
| DoFail    | 0     |
| DoNothing | 1     |
| DoReplace | 2     |

**Update**

Запросы которые обновляют sharding ключи (Shared Update)

```sql
UPDATE target_table
SET key_sharded_by = 1
where id in (select t_id from t2 where s_id = 2);
```

Запросы обновляющие второстепенные поля без локальной материализации (Local Update)

```sql
UPDATE target_table
SET a = 1
where id in (select t_id from t2 where s_id = 2);
```

Здесь мы можем иметь разные имплементации. На текущий момент приходящие tuple относятся к определенному bucket_id и по
Update Node собираются в новые tuple.
У этого подхода есть приемущетсва:

* переиспользование значение из tuple, если колонки требуют одинаковых значений
* не тратим время на роутере

Минусы:

* Пересборка tuple на месте для вставки
* Из пересборки следует и дополнительные данные для пересылки

| Field                | MsgPack Type                  | Example Value                 |
|----------------------|-------------------------------|-------------------------------|
| target table         | u32                           | 1                             |
| target table version | u64                           | 1                             |
| update type          | fixint                        | Update Type                   |
| update data          | Local Update or Shared Update | Local Update or Shared Update |

Update Type

| Type   | Value |
|--------|-------|
| Shared | 0     |
| Local  | 1     |

Shared Update (Обновляются шардирующие ключи)

Мы отправляем готовые данные для space.delete и space.replace. delete_pk это набор ключей для удаления из таблицы.
tuples используется для обновления данных.

| Field | MsgPack Type              | Example Value               |
|-------|---------------------------|-----------------------------|
| data  | array of SharedUpdateData | 10, [SharedUpdateData, ...] |

Shared Update Data

| Field     | MsgPack Type    | Example Value        |
|-----------|-----------------|----------------------|
| delete_pk | array of values | 2, [1, "some", 12.1] |
| tuple     | EncodedTuple    | EncodedTuple         |

Local Update (Не обновляются шардируюшие ключи)

Мы отправляем готовые данные для исполенения space.update. pk - это ключ по которому будет производится
обновление.
values - это пара значений (что установить и на какую позицию). Для values также нужно передавать OpCode равенства, либо
добавлять его на storage прямо перед вызовом space.update.

| Field | MsgPack Type             | Example Value              |
|-------|--------------------------|----------------------------|
| data  | array of LocalUpdateData | 10, [LocalUpdateData, ...] |

Local Update Data

| Field  | MsgPack Type               | Example Value                        |
|--------|----------------------------|--------------------------------------|
| pk     | array of values            | 2, [1, "some", 12.1]                 |
| values | array of [value, position] | 10 [2 [True, 1], 2 ["some", 2], ...] |

### DML with DQL

Такое случается, когда материализация на роутере не имеет смысла (Motion с локальной политикой).
Так как мы будем исполнять DQL локально, тут тоже может возникнуть [cache miss запрос](#cache-miss).

Также стоит учитывать, что если в случае готовой vtable - мы можем послать итоговые tuple.
Но при материализации локально - нам нужно передать больше данных для формирования итоговых tuple.

**Delete**

С локальной материализацией

```sql
DELETE
FROM target_table
WHERE id in (select id from target_table where second_field > 2)
```

| Field                | MsgPack Type    | Example Value      |
|----------------------|-----------------|--------------------|
| target table         | u32             | 1                  |
| target table version | u64             | 1                  |
| schema_info          | map (u32, u64)  | 10, [1..20]        |
| plan_id              | u64             | 0                  |
| sender_id            | u64             | 3 (router raft id) |
| vtable's data        | EncodedTuples   | EncodedTuples      |
| options              | [u64, u64]      | [100, 200]         |
| params               | array of values | 3, true, 1.0, 10   |

**Insert**

C локальной материализацией

```sql
INSERT INTO target_table
VALUES (select * from target_table)
```

columns в каком порядке выстраивать значения
motion key - это значение из LocalSegment, используется для reshard.

| Field                | MsgPack Type    | Example Value      |
|----------------------|-----------------|--------------------|
| target table         | u64             | 1                  |
| target table version | u64             | 1                  |
| columns              | array of u8     | 3, [1, 2, 3]       |
| conflict_policy      | fixint          | 0 (DoNothing)      |
| motion key           | array of Target | 10, [Target, ...]  |
| schema_info          | map (u32, u64)  | 10, [1..20]        |
| plan_id              | u64             | 0                  |
| sender_id            | u64             | 3 (router raft id) |
| vtable's data        | EncodedTuples   | EncodedTuples      |
| options              | [u64, u64]      | [100, 200]         |
| params               | array of values | 3, true, 1.0, 10   |

Target

| Field | MsgPack Type | Example Value                         |
|-------|--------------|---------------------------------------|
| type  | fixint       | Reference (0) or Value (1)            |
| value | msgpack      | Int for reference and Value for value |

```rust
pub enum Target {
    /// A position of the existing column in the tuple.
    Reference(usize),
    /// A value that should be used as a part of the
    /// redistribution key. We need it in a case of
    /// `insert into t1 (b) ...` when
    /// table t1 consists of two columns `a` and `b`.
    /// Column `a` is absent in insertion and should be generated
    /// from the default value.
    Value(Value),
}
```

**Update**

C локальной материализацией

```sql
UPDATE target_table
SET a = 1
WHERE id = (select id from target_table where second_field = 2);
```

mapping columns pos указывает в каком порядке в tuple брать значения

| Field                | MsgPack Type    | Example Value              |
|----------------------|-----------------|----------------------------|
| target table         | u64             | 1                          |
| target table version | u64             | 1                          |
| mapping columns pos  | map (u64, u64)  | 3, [[1, 2], [2, 3], [3,4]] |
| primary keys pos     | aray of u64     | 3 [1, 3, 2]                |
| schema_info          | map (u32, u64)  | 10, [1..20]                |
| plan_id              | u64             | 0                          |
| sender_id            | u64             | 3 (router raft id)         |
| vtable's data        | EncodedTuples   | EncodedTuples              |
| options              | [u64, u64]      | [100, 200]                 |
| params               | array of values | 3, true, 1.0, 10           |

### Итог

**Плюсы:**

- Сохранены преимущества предыдущей версии
- Отсутствует передача лишних данных:
    - DQL получает SQL только один раз
    - DML не получает план, если не нужно материализовать vtable
    - Идет дополнительный запрос отсутствующих данных, вместо дополнительной передачи
- Четкое разграничение DQL и DML с учетом их разных потребностей в данных

**Минусы:**

- Более сложный парсинг - на каждом этапе обработки парсим нужные данные (парсинг в разных методах)
- Мы можем исполнить не тот DQL при коллизии plan_id. Можно получить не тот sql, однако по подсчетам,
  такого не должно случиться (https://git.picodata.io/core/tarantool/-/issues/59)
- Появляется state на стороне router для cache miss

## Decision

Переходим на **Version 1** с breaking changes в совместимости.

**Обоснование:**

- Оптимизация размера передаваемых данных за счет специализации под DQL/DML
- Переход на msgpack обеспечивает консистентность и меньший размер передаваемых данных
- Устранение дублирования данных упрощает поддержку

**Миграционный план:**
Готовим новую версию, покрываем тестами. По готовности протокола, ломаем совместимость и переходим на новый протокол.
Включает в себя также переход plan_id на u64, вместо SmolStr.