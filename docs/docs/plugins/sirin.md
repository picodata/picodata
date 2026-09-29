# Sirin

В данном разделе приведены сведения о Sirin, плагине для СУБД Picodata.

!!! tip "Picodata Enterprise"
    Функциональность плагина доступна только в коммерческой версии Picodata.

## Введение {: #introduction }

Sirin реализует поддержку API Apache Cassandra (CQL и протокол
Cassandra v4) поверх резидентной СУБД Picodata. Sirin позволяет использовать
приложения и драйверы из экосистемы Cassandra без изменений, сохраняя при этом
производительность и отказоустойчивость Picodata, что упрощает миграцию и
интеграцию.

## Совместимость и ограничения {: #compatibility_and_limitations }

### Таблица совместимости {: #compatibility_table }

| Возможность Cassandra                                               | Статус в Sirin      |
|---------------------------------------------------------------------|---------------------|
| CQL (основные операторы)                                            | ✅ Поддерживается    |
| Prepared statements                                                 | ✅ Поддерживается    |
| Named parameter markers                                             | ✅ Поддерживается    |
| Pagination                                                          | ✅ Поддерживается    |
| TTL                                                                 | ✅ Поддерживаются    |
| USING TIMESTAMP                                                     | ✅ Поддерживается    |
| Коллекции `set`, `map`, `list`                                      | ✅ Поддерживаются    |
| Static columns                                                      | ✅ Поддерживаются    |
| Телеметрия                                                          | ✅ Поддерживается    |
| Инструменты (cqlsh, DBeaver, picodata admin)                        | ✅ Поддерживается    |
| BATCH                                                               | ✅ Поддерживается    |
| Управление ролями и правами (CREATE/ALTER/DROP ROLE, GRANT, REVOKE) | ✅ Поддерживается    |
| Lightweight transactions (LWT)                                      | ✅ Поддерживается    |
| User-defined types (UDT)                                            | ❌ Не поддерживаются |
| Материализованные представления (MV)                                | ❌ Не поддерживаются |
| GROUP BY                                                            | ❌ Не поддерживается |

### Ограничения {: #limitations }

- `CREATE KEYSPACE`, `ALTER KEYSPACE` и `DROP KEYSPACE` поддерживаются синтаксически, но параметры
  replication_strategy и replication_factor игнорируются. Настройки берутся из конфигурации Picodata
- репликация управляется средствами Picodata
- [движок хранения] по умолчанию — vinyl. Все настройки vinyl влияют на характеристики хранения
- материализованные представления (MV) отсутствуют
- UDT (User-Defined Type) и вложенные коллекции отсутствуют

[движок хранения]: ../overview/glossary.md#db_engine

## Типы данных {: #data_types }

Sirin поддерживает основные типы данных Cassandra. Ниже приведены поддерживаемые типы,
их допустимый диапазон значений и формат литералов в CQL.

### Числовые типы {: #numeric_types }

| Тип        | Описание                                                            | Диапазон                           |
|------------|---------------------------------------------------------------------|------------------------------------|
| `tinyint`  | 8-битное целое со знаком                                            | от -128 до 127                     |
| `smallint` | 16-битное целое со знаком                                           | от -32 768 до 32 767               |
| `int`      | 32-битное целое со знаком                                           | от -2 147 483 648 до 2 147 483 647 |
| `bigint`   | 64-битное целое со знаком                                           | от -2^63 до 2^63 - 1               |
| `float`    | 32-битное число с плавающей точкой (IEEE 754)                       | ≈ ±3.4 × 10^38                     |
| `double`   | 64-битное число с плавающей точкой (IEEE 754)                       | ≈ ±1.8 × 10^308                    |
| `counter`  | 64-битный атомарный счётчик; поддерживает только операции `+` и `-` | от -2^63 до 2^63 - 1               |

### Строковые и бинарные типы {: #string_types }

| Тип               | Описание                                                                   |
|-------------------|----------------------------------------------------------------------------|
| `text`, `varchar` | UTF-8 строка произвольной длины. `text` и `varchar` являются синонимами.   |
| `ascii`           | Строка, содержащая только символы US-ASCII.                                |
| `blob`            | Произвольные двоичные данные. В CQL записываются в hex-формате: `0x<hex>`. |

### Временны́е типы {: #temporal_types }

| Тип         | Описание                                          | Формат литерала                                           |
|-------------|---------------------------------------------------|-----------------------------------------------------------|
| `timestamp` | Момент времени с точностью до миллисекунды (UTC). | `'2024-01-15 10:30:00+0000'` или `'2024-01-15T10:30:00Z'` |
| `date`      | Календарная дата без времени.                     | `'2024-01-15'`                                            |
| `time`      | Время суток с точностью до наносекунды.           | `'10:30:00.000000000'`                                    |

### Типы UUID {: #uuid_types }

| <div style="width:100px">Тип</div> | Описание                                                                                                           | <div style="width:400px">Формат литерала</div>                        |
|------------------------------------|--------------------------------------------------------------------------------------------------------------------|----------------------------------------|
| `uuid`                             | UUID версии 4, случайный.                                                                                          | `123e4567-e89b-12d3-a456-426614174000` |
| `timeuuid`                         | UUID версии 1, включающий временну́ю метку. Уникален даже при одинаковом времени. Используется для временны́х рядов. | `123e4567-e89b-12d3-a456-426614174000` |

### Сетевые типы {: #network_types }

| Тип    | Описание                                                                                   | Пример                   |
|--------|--------------------------------------------------------------------------------------------|--------------------------|
| `inet` | IP-адрес IPv4 или IPv6 в текстовом представлении. Резолвинг имён хостов не поддерживается. | `'192.168.1.1'`, `'::1'` |

### Булев тип {: #boolean_type }

| Тип | Описание |
|---|---|
| `boolean` | Логическое значение: `true` или `false`. |

### Коллекции {: #collection_types }

Коллекции позволяют хранить несколько значений в одном столбце. Вложенные коллекции не поддерживаются.

| Тип         | Описание                                             | Пример литерала          |
|-------------|------------------------------------------------------|--------------------------|
| `map<K, V>` | Набор пар ключ-значение. Ключи уникальны.            | `{'key1': 1, 'key2': 2}` |
| `set<T>`    | Набор уникальных значений.                           | `{1, 2, 3}`              |
| `list<T>`   | Упорядоченный список значений, допускающий повторы.  | `[1, 2, 2, 3]`           |

Элементы `set` и пары `map` возвращаются отсортированными по значению элемента и ключу
соответственно. Список `list` сохраняет порядок элементов, заданный при записи.

Столбцы-коллекции не могут входить в первичный ключ.

Пустая коллекция равнозначна `null`: запись `[]` или `{}` удаляет значение столбца, а при чтении
такой столбец возвращает `null`. Значение `null` внутри коллекции не допускается — например,
литерал `[1, null]` отклоняется.

См. [подробнее](#collection_operations) об операциях над коллекциями в `UPDATE` и `DELETE`.

!!! note "Ограничения коллекций"
    Вложенные коллекции и `frozen<>` не поддерживаются.

## Поддерживаемые возможности CQL {: #supported_cql_features }

### Определение схемы (DDL) {: #schema_definition }

#### CREATE KEYSPACE {: #create_keyspace }

Создаёт пространство имён — логический контейнер для таблиц. Параметры репликации проверяются и
сохраняются в схеме, но не применяются: репликацией управляет Picodata.

```bnf
<create-keyspace-stmt> ::= CREATE KEYSPACE [IF NOT EXISTS] <ks-name>
                           WITH REPLICATION = <map-literal>
                           [AND DURABLE_WRITES = <boolean>]
```

- `IF NOT EXISTS` — не возвращает ошибку, если пространство имён уже существует
- `durable_writes` игнорируется; запись всегда производится в журнал фиксации
- другие параметры, кроме `REPLICATION` и `DURABLE_WRITES`, не допускаются — запрос отклоняется
  с синтаксической ошибкой

Несмотря на то что параметры `REPLICATION` не применяются, Sirin проверяет их корректность и отклоняет запрос,
если они заданы неверно. Допустимые значения:

| Класс (`class`)           | Обязательные параметры                        | Допустимые значения                              |
|---------------------------|-----------------------------------------------|--------------------------------------------------|
| `SimpleStrategy`          | `replication_factor`                          | Неотрицательное целое. Прочие ключи игнорируются |
| `NetworkTopologyStrategy` | `'<имя DC>': <число>` для каждого дата-центра | Каждое значение — неотрицательное целое          |
| `LocalStrategy`           | —                                             | —                                                |

- `class` обязателен; имя класса можно указать коротко (`SimpleStrategy`) или полностью
  (`org.apache.cassandra.locator.SimpleStrategy`)
- неизвестный класс или некорректное значение приводят к ошибке `Invalid`

Пример:

```sql
CREATE KEYSPACE IF NOT EXISTS mykeyspace
    WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'};
```

#### DROP KEYSPACE {: #drop_keyspace }

Удаляет пространство имён и все его таблицы.

```bnf
<drop-keyspace-stmt> ::= DROP KEYSPACE [IF EXISTS] <ks-name>
```

- `IF EXISTS` — не возвращает ошибку, если пространство имён не существует

Пример:

```sql
DROP KEYSPACE IF EXISTS mykeyspace;
```

#### ALTER KEYSPACE {: #alter_keyspace }

Проверяет существование пространства имён (с учётом `IF EXISTS`), но не изменяет его —
`replication` и `durable_writes` принимаются синтаксически, их содержимое не проверяется и не
применяется: репликацией управляет Picodata. Другие параметры не допускаются — запрос
отклоняется с синтаксической ошибкой.

```bnf
<alter-keyspace-stmt> ::= ALTER KEYSPACE [IF EXISTS] <ks-name>
                          [WITH REPLICATION = <map-literal>]
                          [AND DURABLE_WRITES = <boolean>]
```

- `IF EXISTS` — не возвращает ошибку, если пространство имён не существует

Пример:

```sql
ALTER KEYSPACE mykeyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '3'};
```

#### CREATE TABLE {: #create_table }

Создаёт таблицу в указанном пространстве имён.

Каждая таблица должна иметь первичный ключ, состоящий из двух частей:

- **Partition key** — определяет, на каком узле хранится строка. Может быть составным.
  Все строки с одинаковым ключом партиционирования хранятся физически рядом.
- **Clustering key** — определяет порядок строк внутри партиции. Необязателен.

```sql
-- Простой первичный ключ (только partition key)
PRIMARY KEY (user_id)

-- Составной первичный ключ (partition key + clustering key)
PRIMARY KEY (device_id, event_time)

-- Составной partition key
PRIMARY KEY ((country, city), user_id)
```

```bnf
<create-table-stmt> ::= CREATE TABLE [IF NOT EXISTS] <table-name>
                        '(' <column-definitions> ',' <primary-key> ')'
                        [WITH <table-options>]

<table-options> ::= <table-option> [AND <table-option>]*

<table-option> ::= CLUSTERING ORDER BY '(' <clustering-order> [',' <clustering-order>]* ')'
                 | <option-name> '=' <value>
```

Допускается только фиксированный набор параметров, перечисленных ниже. Любой другой параметр
(например, `additional_write_policy`, `extensions`, `memtable`, `tablets`, `tombstone_gc`)
отклоняется с синтаксической ошибкой. Значения параметров проверяются; при некорректном
значении запрос также отклоняется с синтаксической ошибкой.

Применяемые параметры:

| Параметр | Описание |
|---|---|
| <div style="width:200px">`CLUSTERING ORDER BY`</div> | Задаёт порядок сортировки для столбцов кластеризующего ключа. По умолчанию — `ASC`. |
| `default_time_to_live` | Время жизни строк в секундах, целое от `0` до `630720000`. Значение `0` (по умолчанию) означает, что строки не удаляются автоматически. Применяется к каждой вставляемой строке, если в запросе не указан `USING TTL`. |

Параметры, которые проверяются и сохраняются в схеме (возвращаются в `DESCRIBE` и
`system_schema.tables`), но не влияют на работу таблицы:

| Параметр                      | Допустимые значения                                  | По умолчанию     |
|-------------------------------|------------------------------------------------------|------------------|
| `comment`                     | Строка                                               | `''`             |
| `cdc`                         | `true`, `false`                                      | `false`          |
| `bloom_filter_fp_chance`      | Число от `0` до `1`                                  | `0.01`           |
| `crc_check_chance`            | Число от `0` до `1`                                  | `1.0`            |
| `gc_grace_seconds`            | Целое от `0` до `2147483647`                         | `604800`         |
| `memtable_flush_period_in_ms` | Целое от `0` до `2147483647`                         | `0`              |
| `min_index_interval`          | Целое от `1` до `2147483647`                         | `128`            |
| `max_index_interval`          | Целое от `1` до `2147483647`                         | `2048`           |
| `speculative_retry`           | Строка, значение не проверяется                      | `'99PERCENTILE'` |
| `read_repair`                 | Строка, значение не проверяется                      | `'BLOCKING'`     |
| `caching`                     | Map, см. ниже                                        | `{'keys': 'ALL', 'rows_per_partition': 'NONE'}` |
| `compaction`                  | Map, см. ниже                                        | `{'class': 'SizeTieredCompactionStrategy'}` |
| `compression`                 | Map, см. ниже                                        | `{'class': 'LZ4Compressor'}` |

**caching**

- допустимы только ключи `keys` и `rows_per_partition`
- `keys` — `ALL` или `NONE`
- `rows_per_partition` — `ALL`, `NONE` или неотрицательное целое
- значения нечувствительны к регистру

**compaction**

- `class` обязателен и задаётся только коротким именем: `SizeTieredCompactionStrategy`,
  `TimeWindowCompactionStrategy` или `LeveledCompactionStrategy`. Полные имена
  (`org.apache.cassandra.db.compaction.*`) и другие классы (`UnifiedCompactionStrategy`,
  `IncrementalCompactionStrategy` и т. д.) отклоняются
- общие подпараметры для всех классов: `enabled`, `log_all`, `only_purge_repaired_tombstone`,
  `unchecked_tombstone_compaction`, `tombstone_threshold`, `tombstone_compaction_interval`,
  `min_threshold`, `max_threshold` (`min_threshold` не может быть больше `max_threshold`)
- подпараметры `SizeTieredCompactionStrategy`: `bucket_low`, `bucket_high` (`bucket_low` не может
  быть больше `bucket_high`), `min_sstable_size`
- подпараметры `TimeWindowCompactionStrategy`: `compaction_window_unit` (`MINUTES`, `HOURS`, `DAYS`),
  `compaction_window_size`, `split_during_flush`
- подпараметры `LeveledCompactionStrategy`: `sstable_size_in_mb`

**compression**

- `class` задаётся только коротким именем: `LZ4Compressor`, `SnappyCompressor` или `DeflateCompressor`.
  Полные имена (`org.apache.cassandra.io.compress.*`) и другие классы (например, `ZstdCompressor`)
  отклоняются. Вместо `class` допускается устаревший `sstable_compression`, но не оба сразу
- `chunk_length_in_kb` — положительная степень двойки
- `crc_check_chance` — число от `0` до `1`; переопределяет параметр таблицы `crc_check_chance`
- `{'enabled': false}` отключает сжатие; другие подпараметры вместе с ним не допускаются

Изменение параметров таблицы после создания (`ALTER TABLE ... WITH`) не поддерживается.

**Статические столбцы:**

Статический столбец объявляется с ключевым словом `STATIC` после типа. Значение статического столбца
едино для всей партиции: оно разделяется между всеми строками с одинаковым ключом партиционирования. Запись
нового значения в любую строку партиции перезаписывает его для всех строк.

Ограничения:

- статические столбцы допустимы только в таблицах с кластеризующим ключом
- столбцы первичного ключа не могут быть статическими

```sql title="Таблица со статическим столбцом"
CREATE TABLE t (
    pk  int,
    t   int,
    v   text,
    s   text STATIC,
    PRIMARY KEY (pk, t)
);
```

```sql title="Вставка двух строк в одну партицию — второй INSERT перезаписывает статическое значение"
INSERT INTO t (pk, t, v, s) VALUES (0, 0, 'val0', 'static0');
INSERT INTO t (pk, t, v, s) VALUES (0, 1, 'val1', 'static1');
```

После этих двух INSERT `s` имеет значение `'static1'` для обеих строк:

```
SELECT * FROM t;

 pk | t | s       | v
----+---+---------+------
  0 | 0 | static1 | val0
  0 | 1 | static1 | val1
```

Добавить статический столбец после создания таблицы можно через `ALTER TABLE ... ADD`:

```sql
ALTER TABLE t ADD pinned boolean STATIC;
```

**Примеры:**

```sql title="Таблица без дополнительных параметров"
CREATE TABLE users (
    user_id uuid,
    email text,
    name text,
    PRIMARY KEY (user_id)
);
```

```sql title="Таблица с порядком сортировки и временем жизни строк"
CREATE TABLE events (
    device_id uuid,
    event_time timestamp,
    payload text,
    PRIMARY KEY (device_id, event_time)
) WITH CLUSTERING ORDER BY (event_time DESC)
  AND default_time_to_live = 86400;
```

#### ALTER TABLE {: #alter_table }

Изменяет структуру таблицы: добавляет, переименовывает или удаляет столбцы.

```bnf
<alter-table-stmt> ::= ALTER TABLE [IF EXISTS] [<keyspace-name> '.'] <table-name>
                           ADD [IF NOT EXISTS] <column-definition> [',' <column-definition>]*
                     | ALTER TABLE [IF EXISTS] [<keyspace-name> '.'] <table-name>
                           RENAME [IF EXISTS] <col-name> TO <new-col-name>
                               [AND <col-name> TO <new-col-name>]*
                     | ALTER TABLE [IF EXISTS] [<keyspace-name> '.'] <table-name>
                           DROP [IF EXISTS] <col-name> [',' <col-name>]*

<column-definition> ::= <col-name> <type> [STATIC]
```

**ADD** — добавляет один или несколько столбцов:

- `IF NOT EXISTS` — пропускает столбцы, которые уже существуют, вместо возврата ошибки
- добавляемые столбцы могут быть статическими (`STATIC`); статические столбцы допускаются только в таблицах с кластеризующим ключом
- в таблицу со столбцами `counter` можно добавить только столбцы `counter`, а в таблицу без них — только
  столбцы других типов

**RENAME** — переименовывает столбцы первичного ключа:

- `IF EXISTS` — пропускает пары, у которых исходный столбец отсутствует, вместо возврата ошибки
- переименование обычных и статических столбцов не поддерживается

**DROP** — удаляет столбцы (на уровне БД помечает их как удалённые; данные при этом не уничтожаются физически):

- `IF EXISTS` — пропускает столбцы, которых нет, вместо возврата ошибки
- удаление столбцов первичного ключа не поддерживается
- после удаления столбец с тем же именем можно добавить заново; старые данные при этом не восстанавливаются

!!! note "Ограничение"
    Изменение параметров таблицы через `ALTER TABLE ... WITH` (например, `default_time_to_live`,
    `gc_grace_seconds`) не поддерживается.

**Примеры:**

```sql title="Добавить столбец"
ALTER TABLE mykeyspace.users ADD phone text;
```

```sql title="Добавить несколько столбцов, включая статический"
ALTER TABLE mykeyspace.events
    ADD region text STATIC, payload blob;
```

```sql title="Переименовать столбец кластеризующего ключа"
ALTER TABLE mykeyspace.events RENAME event_time TO ts;
```

```sql title="Удалить столбец"
ALTER TABLE mykeyspace.users DROP phone;
```

```sql title="Удалить несколько столбцов"
ALTER TABLE mykeyspace.users DROP phone, nickname;
```

#### DROP TABLE {: #drop_table }

Удаляет таблицу и все её данные.

```bnf
<drop-table-stmt> ::= DROP TABLE [IF EXISTS] [<keyspace-name> '.'] <table-name>
```

- `IF EXISTS` — не возвращает ошибку, если таблица не существует

Пример:

```sql
DROP TABLE IF EXISTS mykeyspace.events;
```

#### TRUNCATE TABLE {: #truncate_table }

Удаляет все строки из таблицы, сохраняя её схему.

```bnf
<truncate-stmt> ::= TRUNCATE [TABLE] [<keyspace-name> '.'] <table-name>
```

Пример:

```sql
TRUNCATE TABLE mykeyspace.events;
```

#### DESCRIBE {: #describe }

Возвращает список пространств имён или DDL-описание пространства имён/таблицы.

```bnf
<describe-stmt> ::= DESCRIBE KEYSPACES
                  | DESCRIBE [TABLE] <keyspace-name> '.' <table-name>
                  | DESCRIBE [TABLE] <table-name>
                  | DESCRIBE <keyspace-name>
```

- `DESCRIBE KEYSPACES` — возвращает имена всех пространств имён, включая системные. Не требует
  привилегий
- `DESCRIBE <keyspace-name>.<table-name>` — возвращает DDL-описание таблицы. Требует привилегию `DESCRIBE`
  на таблицу
- `DESCRIBE <name>` — если в текущем пространстве имён есть таблица `<name>`, возвращает её DDL-описание,
  иначе — DDL-описание пространства имён `<name>` и всех его таблиц. Требует привилегию `DESCRIBE` на
  таблицу или пространство имён соответственно

Возвращаемый набор DDL-команд можно выполнить повторно, чтобы воссоздать объект с той же схемой.

Примеры:

```sql
DESCRIBE KEYSPACES;
DESCRIBE mykeyspace;
DESCRIBE TABLE mykeyspace.events;
```

### Операции с данными (DML) {: #data_operations }

#### INSERT {: #insert }

Вставляет строку в таблицу. Если строка с таким первичным ключом уже существует, она
**полностью заменяется** (upsert-семантика): незаданные столбцы получают значение `null`.

Должны быть указаны все компоненты первичного ключа.

```bnf
<insert-stmt> ::= INSERT INTO [<keyspace-name> '.'] <table-name>
                      '(' <column-names> ')'
                  VALUES '(' <values> ')'
                  [IF NOT EXISTS]
                  [USING <using-param> [AND <using-param>]]

<using-param> ::= TTL <int>
                | TIMESTAMP <int>
```

- `IF NOT EXISTS` — вставляет строку только если она не существует (см. [LWT](#lwt))
- `USING TTL <seconds>` — задаёт время жизни строки в секундах; переопределяет `default_time_to_live` таблицы
- `USING TIMESTAMP <microseconds>` — задаёт метку времени записи (см. [USING TIMESTAMP](#using_timestamp))

В `VALUES` вместо литералов можно использовать [функции](#functions) и маркеры параметров (`?`, `:name`).

Примеры:

```sql title="Простая вставка"
INSERT INTO mykeyspace.users (user_id, email, name)
    VALUES (uuid(), 'alice@example.com', 'Alice');
```

```sql title="Вставка с TTL (строка удалится через 1 час)"
INSERT INTO mykeyspace.sessions (session_id, user_id)
    VALUES (uuid(), 123e4567-e89b-12d3-a456-426614174000)
    USING TTL 3600;
```

```sql title="Вставка только при отсутствии строки"
INSERT INTO mykeyspace.users (user_id, email)
    VALUES (uuid(), 'bob@example.com')
    IF NOT EXISTS;
```

```sql title="Вставка с TTL и явной меткой времени"
INSERT INTO mykeyspace.sessions (session_id, user_id)
    VALUES (uuid(), 123e4567-e89b-12d3-a456-426614174000)
    USING TTL 3600 AND TIMESTAMP 1767225600000000;
```

#### UPDATE {: #update }

Обновляет один или несколько столбцов строки. Строка идентифицируется по полному первичному ключу
в `WHERE`. Если строки с указанным ключом не существует, она будет **создана** (upsert-семантика) —
за исключением случая, когда указан `IF EXISTS`.

```bnf
<update-stmt> ::= UPDATE [<keyspace-name> '.'] <table-name>
                  [USING <using-param> [AND <using-param>]]
                  SET <assignment> [',' <assignment>]*
                  WHERE <where-clause>
                  [<if-clause>]

<using-param> ::= TTL <int>
                | TIMESTAMP <int>

<assignment> ::= <column-name> '=' <value>
               | <column-name> '=' <column-name> '+' <value>
               | <column-name> '=' <column-name> '-' <value>
               | <column-name> '=' <value> '+' <column-name>
               | <column-name> '[' <index> ']' '=' <value>
```

- `<if-clause>` — делает выполнение `UPDATE` условным (см. [LWT](#lwt)): `IF EXISTS` либо
  `IF <condition> [AND <condition> ...]`
- `USING TTL <int>` — устанавливает время жизни (в секундах) для столбцов, изменяемых этим `UPDATE`.
  Время жизни остальных столбцов строки не меняется. Допустимый диапазон: от `0` до `630720000`
  (20 лет). Значение `0` означает, что записанные значения хранятся бессрочно. Не применяется к
  таблицам с колонками типа `counter`
- `USING TIMESTAMP <microseconds>` — задаёт метку времени записи (см. [USING TIMESTAMP](#using_timestamp))

**Ограничения Sirin:**

- ограничения `IF`-условий описаны в разделе [LWT](#lwt)

**Виды присваиваний:**

| Форма               | Применение                                        | Пример                        |
|---------------------|---------------------------------------------------|-------------------------------|
| `col = value`       | Установить значение                               | `name = 'Bob'`                |
| `col = col + value` | Инкремент счётчика, добавление в коллекцию        | `visits = visits + 1`         |
| `col = col - value` | Декремент счётчика, удаление из коллекции         | `visits = visits - 1`         |
| `col = value + col` | Добавление элементов в начало `list`              | `events = ['start'] + events` |
| `col[i] = value`    | Замена элемента `list` по индексу                 | `events[0] = 'init'`          |

##### Операции над коллекциями {: #collection_operations }

| Тип        | Операция                     | Описание                                                                      |
|------------|------------------------------|-------------------------------------------------------------------------------|
| `set<T>`   | `s = s + {v1, v2}`           | Добавляет элементы в множество                                                |
| `set<T>`   | `s = s - {v1, v2}`           | Удаляет элементы из множества                                                 |
| `map<K,V>` | `m = m + {k1: v1, k2: v2}`   | Добавляет пары; значения существующих ключей перезаписываются                 |
| `map<K,V>` | `m = m - {k1, k2}`           | Удаляет пары с указанными ключами. Правый операнд — множество ключей `set<K>` |
| `list<T>`  | `l = l + [v1, v2]`           | Добавляет элементы в конец списка                                             |
| `list<T>`  | `l = [v1, v2] + l`           | Добавляет элементы в начало списка                                            |
| `list<T>`  | `l = l - [v1, v2]`           | Удаляет все вхождения указанных значений                                      |
| `list<T>`  | `l[i] = v`                   | Заменяет элемент с индексом `i` (нумерация с `0`); `l[i] = null` удаляет его  |
| `list<T>`  | `DELETE l[i] FROM ...`       | Удаляет элемент с индексом `i`, сдвигая последующие (см. [DELETE](#delete))   |

- присваивание `col = value` заменяет коллекцию целиком
- добавление или удаление пустой коллекции либо `null` (`s = s + {}`, `l = l - null`) ничего не меняет
- если список пуст или индекс выходит за его границы, `l[i] = v` и `DELETE l[i]` возвращают ошибку
- при удалении единственного элемента столбец получает значение `null`
- запись и удаление отдельного элемента поддерживаются только для `list`: `m[key] = value`;
  `DELETE m[key]` для `map` и `DELETE s[value]` для `set` не поддерживаются

Примеры:

```sql title="Обновление обычных столбцов"
UPDATE mykeyspace.users
    SET name = 'Alice Smith', email = 'alice.smith@example.com'
    WHERE user_id = 123e4567-e89b-12d3-a456-426614174000;
```

```sql title="Обновление с TTL"
UPDATE mykeyspace.users USING TTL 3600
    SET session_token = 'abc'
    WHERE user_id = 123e4567-e89b-12d3-a456-426614174000;
```

```sql title="Добавление и удаление элементов коллекций"
UPDATE mykeyspace.users
    SET tags = tags + {'vip'}, settings = settings - {'theme'}, history = history + ['login']
    WHERE user_id = 123e4567-e89b-12d3-a456-426614174000;
```

```sql title="Замена элемента списка по индексу"
UPDATE mykeyspace.users
    SET history[0] = 'signup'
    WHERE user_id = 123e4567-e89b-12d3-a456-426614174000;
```

!!! warning "Особенности операций над list"
    - добавление в начало и в конец списка неидемпотентно: если запрос завершился по таймауту,
      его повтор может добавить элементы дважды
    - запись и удаление по индексу (`l[i] = v`, `DELETE l[i]`), а также удаление по значению
      (`l = l - [...]`) читают текущее значение списка перед записью и выполняются медленнее
      обычного обновления
    - TTL из `USING TTL` применяется только к элементам, добавленным этим запросом

    Если порядок элементов и повторы не нужны, используйте `set`.

```sql title="Инкремент счётчика"
UPDATE mykeyspace.stats
    SET page_views = page_views + 1
    WHERE page_id = 'home';
```

```sql title="Обновление только при существовании строки"
UPDATE mykeyspace.users
    SET name = 'Bob'
    WHERE user_id = 123e4567-e89b-12d3-a456-426614174000
    IF EXISTS;
```

```sql title="Обновление только при выполнении условия на значение столбца"
UPDATE mykeyspace.users
    SET name = 'Bob'
    WHERE user_id = 123e4567-e89b-12d3-a456-426614174000
    IF status = 'pending';
```

#### DELETE {: #delete }

Удаляет строку целиком или значения отдельных столбцов. Операция всегда задаётся
по первичному ключу.

```bnf
<delete-stmt> ::= DELETE [<target> [',' <target>]*]
                  FROM [<keyspace-name> '.'] <table-name>
                  [USING TIMESTAMP <int>]
                  WHERE <where-clause>
                  [<if-clause>]

<target> ::= <column-name>
           | <column-name> '[' <index> ']'
```

- Если список столбцов не указан — удаляется вся строка.
- Если список столбцов указан — удаляются только значения этих столбцов (столбцы
  получают значение `null`). Столбцы первичного ключа удалить нельзя.
- `<column-name>[<index>]` — удаляет из `list` элемент с указанным индексом (нумерация с `0`);
  последующие элементы сдвигаются
- `<if-clause>` — делает выполнение `DELETE` условным (см. [LWT](#lwt)): `IF EXISTS` либо
  `IF <condition> [AND <condition> ...]`
- `USING TIMESTAMP <microseconds>` — задаёт метку времени удаления (см. [USING TIMESTAMP](#using_timestamp))
- `WHERE` должен содержать как минимум полный ключ партиционирования
- удаление отдельной пары из `map` (`DELETE col[key] FROM ...`) и элемента из `set`
  (`DELETE col[value] FROM ...`) не поддерживается; вместо этого используйте
  `UPDATE ... SET col = col - {key}`

Примеры:

```sql title="Удаление строки по первичному ключу"
DELETE FROM mykeyspace.users
    WHERE user_id = 123e4567-e89b-12d3-a456-426614174000;
```

```sql title="Удаление строки с проверкой существования"
DELETE FROM mykeyspace.users
    WHERE user_id = 123e4567-e89b-12d3-a456-426614174000
    IF EXISTS;
```

```sql title="Удаление строки при выполнении условия на значение столбца"
DELETE FROM mykeyspace.users
    WHERE user_id = 123e4567-e89b-12d3-a456-426614174000
    IF status = 'inactive';
```

```sql title="Удаление значения отдельного столбца"
DELETE email FROM mykeyspace.users
    WHERE user_id = 123e4567-e89b-12d3-a456-426614174000;
```

```sql title="Удаление элемента списка по индексу"
DELETE history[0] FROM mykeyspace.users
    WHERE user_id = 123e4567-e89b-12d3-a456-426614174000;
```

```sql title="Удаление нескольких строк (по диапазону clustering key)"
DELETE FROM mykeyspace.events
    WHERE device_id = 123e4567-e89b-12d3-a456-426614174000
      AND event_time < '2024-01-01 00:00:00+0000';
```

#### SELECT {: #select }

Читает данные из таблицы.

```bnf
<select-stmt> ::= SELECT [DISTINCT] <select-clause>
                  FROM [<keyspace-name> '.'] <table-name>
                  [WHERE <where-clause>]
                  [ORDER BY <column-name> [ASC | DESC] [',' ...]]
                  [LIMIT <number>]
                  [ALLOW FILTERING]

<select-clause> ::= '*'
                  | <selector> [AS <alias>] [',' <selector> [AS <alias>]]*

<selector> ::= <column-name>
             | <column-name> '[' <literal> ']'
```

Селектор `<column-name>[<literal>]` возвращает значение `map` по ключу, который задаётся только литералом.
Выборка элемента `set` и выборка диапазона (`m['a'..'c']`) не поддерживаются.

**WHERE**

Задаёт условие фильтрации строк. Поддерживаемые операторы: `=`, `<`, `>`, `<=`, `>=`, `!=`, `IN`,
`CONTAINS`. В правой части условия можно использовать литералы, маркеры параметров и
[функции](#functions).

Для эффективной работы рекомендуется всегда указывать полный ключ партиционирования. Фильтрация по
неключевым столбцам требует `ALLOW FILTERING`.

`col CONTAINS value` отбирает строки, в которых коллекция содержит значение: элемент `list` или `set`,
значение (не ключ) `map`. Условие `CONTAINS` всегда требует `ALLOW FILTERING`. `CONTAINS KEY` в
`WHERE` не поддерживается.

```sql
WHERE device_id = 123e4567-e89b-12d3-a456-426614174000
  AND event_time > '2024-01-01 00:00:00+0000'
```

**ORDER BY**

Порядок сортировки можно задавать только по столбцам кластеризующего ключа, и только
в том направлении, которое задано в `CLUSTERING ORDER BY` таблицы или в обратном ему — одновременно
для всех перечисленных столбцов.

Примеры для таблицы со смешанным порядком кластеризации:

```sql
CREATE TABLE readings (
    sensor_id int,
    day       date,
    seq       int,
    value     double,
    PRIMARY KEY (sensor_id, day, seq)
) WITH CLUSTERING ORDER BY (day DESC, seq ASC);
```

```sql title="Прямой порядок — совпадает с CLUSTERING ORDER BY (результат тот же, что и без ORDER BY)"
SELECT * FROM readings WHERE sensor_id = 1 ORDER BY day DESC, seq ASC;
```

```sql title="Обратный порядок — направление инвертировано для всех столбцов"
SELECT * FROM readings WHERE sensor_id = 1 ORDER BY day ASC, seq DESC;
```

```sql title="Ошибка — направление инвертировано только для одного столбца"
SELECT * FROM readings WHERE sensor_id = 1 ORDER BY day DESC, seq DESC;
```

Столбцы перечисляются в порядке их объявления в кластеризующем ключе. Столбец можно пропустить, если
в `WHERE` он ограничен единственным значением (`=` или `IN` с одним элементом):

```sql title="Столбец day зафиксирован в WHERE, сортировка только по seq"
SELECT * FROM readings WHERE sensor_id = 1 AND day = '2026-01-15' ORDER BY seq DESC;
```

**LIMIT**

Ограничивает общее число возвращаемых строк.

**ALLOW FILTERING**

По умолчанию Sirin, как и Cassandra, запрещает запросы, требующие полного сканирования всех
партиций. Такие запросы необходимо явно пометить `ALLOW FILTERING`. Следует использовать
осторожно на больших таблицах.

**DISTINCT**

`SELECT DISTINCT` возвращает по одной строке на уникальную партицию.

- в списке столбцов должен быть указан весь ключ партиционирования целиком; дополнительно
  можно указать статические столбцы — колонки кластеризации и обычные колонки не допускаются
- `WHERE` может фильтровать только по столбцам ключа партиционирования и статическим столбцам
- `ORDER BY` допускается, только если ключ партиционирования полностью зафиксирован через `=` или `IN`
- не поддерживается для системных таблиц (`system.*`, `system_schema.*`)

```sql title="Уникальные партиции таблицы"
SELECT DISTINCT region, sensor_id FROM sensors;
```

```sql title="Уникальные партиции вместе со статическим столбцом"
SELECT DISTINCT region, sensor_id, firmware FROM sensors;
```

**Ограничения:**

- `GROUP BY` не поддерживается
- `USING CONSISTENCY` не поддерживается
- функции в проекции — между `SELECT` и `FROM` (`COUNT(*)`, `CAST()`, `token()`, `writetime()`, `ttl()`
  и т. д.) — не поддерживаются (например, запрос `SELECT token(id) FROM t` не будет работать)
- сравнение кортежей, например `(a, b) > (1, 2)`, не поддерживается

Примеры:

```sql title="Все строки партиции"
SELECT * FROM mykeyspace.events
    WHERE device_id = 123e4567-e89b-12d3-a456-426614174000;
```

```sql title="С сортировкой и ограничением"
SELECT device_id, event_time, payload
    FROM mykeyspace.events
    WHERE device_id = 123e4567-e89b-12d3-a456-426614174000
    ORDER BY event_time DESC
    LIMIT 10;
```

```sql title="С псевдонимом столбца"
SELECT user_id AS id, name AS username FROM mykeyspace.users
    WHERE user_id = 123e4567-e89b-12d3-a456-426614174000;
```

```sql title="Полное сканирование (требует ALLOW FILTERING)"
SELECT * FROM mykeyspace.users
    WHERE name = 'Alice'
    ALLOW FILTERING;
```

```sql title="Значение map по ключу и фильтрация по элементу коллекции"
SELECT user_id, settings['theme'] FROM mykeyspace.users
    WHERE tags CONTAINS 'vip'
    ALLOW FILTERING;
```

#### BATCH {: #batch }

Объединяет несколько DML-операций в один пакетный запрос (батч). Все операции батча применяются атомарно.

```bnf
<batch-stmt> ::= BEGIN [UNLOGGED] BATCH [USING TIMESTAMP <int>]
                     <dml-stmt> ';'
                     [<dml-stmt> ';']*
                 APPLY BATCH
```

Батч может содержать операторы `INSERT`, `UPDATE` и `DELETE`. Операции могут
производиться над разными таблицами.

- `UNLOGGED` — в Cassandra отключает журнал батча для повышения производительности; в Sirin
  принимается синтаксически, поведение не меняется.
- `USING TIMESTAMP <microseconds>` — задаёт единую метку времени для всех операций батча
  (см. [USING TIMESTAMP](#using_timestamp)).

Пример:

```sql
BEGIN BATCH
    INSERT INTO mykeyspace.users (user_id, name) VALUES (uuid(), 'Alice');
    UPDATE mykeyspace.users SET email = 'alice@example.com'
        WHERE user_id = 123e4567-e89b-12d3-a456-426614174000;
    DELETE FROM mykeyspace.users
        WHERE user_id = 00000000-0000-0000-0000-000000000001;
APPLY BATCH;
```

#### USING TIMESTAMP {: #using_timestamp }

Каждое записанное значение хранит метку времени записи. По умолчанию это текущее время узла,
обрабатывающего запрос. `USING TIMESTAMP` позволяет задать метку явно — целым числом микросекунд
с начала эпохи Unix (1970-01-01 00:00:00 UTC).

При конкурирующих записях одного и того же значения сохраняется запись с бóльшей меткой времени.
Запись/удаление с меткой времени меньше, чем у уже сохранённого значения, это значение не
изменяет.

`USING TIMESTAMP` поддерживается в `INSERT`, `UPDATE`, `DELETE` и `BATCH`. Использовать его вместе с
условиями LWT (`IF NOT EXISTS`, `IF EXISTS`, `IF <condition>`) нельзя.

```sql
UPDATE mykeyspace.users USING TIMESTAMP 1767225600000000
    SET name = 'Alice'
    WHERE user_id = 123e4567-e89b-12d3-a456-426614174000;
```

#### LWT (Lightweight transactions) {: #lwt }

Lightweight transactions позволяют выполнять операции условно — только при выполнении
указанного условия. Это обеспечивает семантику «сравни и замени» (compare-and-set) без полного
распределённого консенсуса.

Так как реплики в Picodata объединены синхронной репликацией, проверка условия выполняется
локально на узле, обрабатывающем запрос.

На данный момент поддерживаются:

| Оператор | Конструкция                            | Описание                                                             |
|----------|----------------------------------------|----------------------------------------------------------------------|
| `INSERT` | `IF NOT EXISTS`                        | Вставить строку только если её нет                                   |
| `UPDATE` | `IF EXISTS`                            | Обновить строку только если она есть                                 |
| `UPDATE` | `IF <condition> [AND <condition> ...]` | Обновить строку только если выполняются условия на значения столбцов |
| `DELETE` | `IF EXISTS`                            | Удалить строку только если она есть                                  |
| `DELETE` | `IF <condition> [AND <condition> ...]` | Удалить строку только если выполняются условия на значения столбцов  |

**Условия (`IF <condition>`)**

Каждое условие сравнивает значение обычного или статического столбца с выражением:

| Форма                    | Описание                                           |
|--------------------------|----------------------------------------------------|
| `col = value`            | Равно                                              |
| `col != value`           | Не равно                                           |
| `col > value`            | Больше                                             |
| `col >= value`           | Больше или равно                                   |
| `col < value`            | Меньше                                             |
| `col <= value`           | Меньше или равно                                   |
| `col IN (value, ...)`    | Значение столбца входит в список                   |
| `col CONTAINS value`     | Значение входит в коллекцию (`list`, `set`, `map`) |
| `col CONTAINS KEY value` | Ключ входит в `map`                                |

Несколько условий объединяются через `AND`.

**Ограничения:**

- в условии нельзя использовать столбцы первичного ключа — они уже заданы в `WHERE`
- условия на столбцы типа `counter` не поддерживаются; более того, `IF EXISTS` тоже
  не поддерживается для таблиц, содержащих `counter`-столбцы
- `CONTAINS` применим только к коллекциям, `CONTAINS KEY` — только к `map`
- сравнение с `NULL` допустимо только через `=` и `!=`; операторы `>`, `>=`, `<`, `<=`
  со значением `NULL` возвращают ошибку

Каждый LWT-запрос возвращает результирующий набор с псевдостолбцом `[applied]`:

- `true` — условие выполнено, операция применена
- `false` — условие не выполнено, операция не применена

Примеры:

```sql title="Вставить только если строки нет"
INSERT INTO mykeyspace.users (user_id, name)
    VALUES (123e4567-e89b-12d3-a456-426614174000, 'Alice')
    IF NOT EXISTS;
```

```
 [applied]
-----------
      True
```

```sql title="Обновить только если строка существует"
UPDATE mykeyspace.users SET name = 'Bob'
    WHERE user_id = 123e4567-e89b-12d3-a456-426614174000
    IF EXISTS;
```

```
 [applied]
-----------
     False
```

```sql title="Обновить только при выполнении условий на несколько столбцов"
UPDATE mykeyspace.users SET balance = 100
    WHERE user_id = 123e4567-e89b-12d3-a456-426614174000
    IF balance = 50 AND status = 'active';
```

```sql title="Удалить только если значение столбца входит в список"
DELETE FROM mykeyspace.users
    WHERE user_id = 123e4567-e89b-12d3-a456-426614174000
    IF status IN ('inactive', 'banned');
```

### Аутентификация и управление правами доступа {: #security }

Sirin использует ролевую модель управления доступом (RBAC), аналогичную Cassandra. Доступ к данным
определяется набором привилегий, назначенных ролям. Роли могут наследоваться друг от друга, образуя
иерархию. Аутентификация выполняется по протоколу Cassandra Native Protocol v4 с передачей
учётных данных (логин и пароль) при установке соединения.

По умолчанию аутентификация отключена. Как её включить — см. раздел [Включение аутентификации](#enable_auth).

По умолчанию создаётся суперпользователь `cassandra` с паролем `cassandra`. Суперпользователь
обладает полными правами на все ресурсы и может управлять другими ролями.

!!! warning "Внимание!"
    Рекомендуем сменить пароль суперпользователя `cassandra` сразу после развёртывания.

#### CREATE ROLE {: #create_role }

Создаёт новую роль. Роль может быть учётной записью пользователя (с возможностью входа) или
группой для делегирования прав.

```bnf
<create-role-stmt> ::= CREATE ROLE [IF NOT EXISTS] <role_name>
                       [WITH <role_options>]

<role_options>     ::= <role_option> [AND <role_option>]*

<role_option>      ::= PASSWORD = '<string>'
                     | HASHED PASSWORD = '<bcrypt-hash>'
                     | LOGIN = (true | false)
                     | SUPERUSER = (true | false)
                     | OPTIONS = <map_literal>
                     | ACCESS TO DATACENTERS <set-literal>
                     | ACCESS TO ALL DATACENTERS
                     | ACCESS FROM CIDRS <set-literal>
                     | ACCESS FROM ALL CIDRS
```

- `PASSWORD` — пароль в открытом виде, хешируется при сохранении. Задаётся либо `PASSWORD`, либо `HASHED PASSWORD` — одновременно оба параметра не допускаются
- `HASHED PASSWORD` — предварительно вычисленный bcrypt-хеш пароля. Используется при переносе учётных записей из другой Cassandra-совместимой системы
- `LOGIN` — разрешает вход в систему. По умолчанию `false`. При `LOGIN = true` обязательно указать `PASSWORD` или `HASHED PASSWORD`
- `SUPERUSER` — наделяет роль правами суперпользователя. По умолчанию `false`
- `IF NOT EXISTS` — не возвращает ошибку, если роль уже существует
- `ACCESS TO DATACENTERS {'dc1', 'dc2', ...}` — ограничивает вход только с узлов указанных дата-центров. Каждый узел Picodata имеет параметр [`instance.failure_domain`](../tutorial/deploy.md#failure_domains) — словарь произвольных ключей, описывающих его физическое размещение (например, `{"DC": "DC1", "HOST": "node1"}`). Параметр конфигурации плагина `router.auth.network.dc_failure_domain_key` задаёт, какой именно ключ из этого словаря считать именем дата-центра (по умолчанию `dc`)
- `ACCESS TO ALL DATACENTERS` — разрешает вход с узлов любого дата-центра (поведение по умолчанию)
- `ACCESS FROM CIDRS {'group1', 'group2', ...}` — разрешает вход только с IP-адресов, принадлежащих указанным CIDR-группам (см. [CIDR-группы](#cidr_groups)). Требует `router.auth.is_cidr_authorizer_enabled: true`
- `ACCESS FROM ALL CIDRS` — разрешает вход с любых IP-адресов (поведение по умолчанию)

Примеры:

```sql title="Создание роли пользователя с паролем"
CREATE ROLE alice WITH PASSWORD = 'secret' AND LOGIN = true;
```

```sql title="Создание роли-группы без права входа"
CREATE ROLE data_readers;
```

```sql title="Создание суперпользователя"
CREATE ROLE admin WITH PASSWORD = 'str0ng' AND LOGIN = true AND SUPERUSER = true;
```

```sql title="Роль с ограничением по дата-центру и CIDR-группам"
CREATE ROLE app_user
  WITH PASSWORD = 'pass'
  AND LOGIN = true
  AND ACCESS TO DATACENTERS {'dc-msk'}
  AND ACCESS FROM CIDRS {'office_net'};
```

#### ALTER ROLE {: #alter_role }

Изменяет параметры существующей роли. Поддерживает те же опции, что и `CREATE ROLE`,
за исключением `IF NOT EXISTS`. Незаданные параметры остаются без изменений.

```bnf
<alter-role-stmt> ::= ALTER ROLE <role_name>
                      [WITH <role_option> [AND <role_option>]*]
```

Примеры:

```sql title="Смена пароля"
ALTER ROLE alice WITH PASSWORD = 'newpassword';
```

```sql title="Повышение до суперпользователя"
ALTER ROLE alice WITH SUPERUSER = true;
```

```sql title="Ограничение доступа по дата-центру"
ALTER ROLE alice WITH ACCESS TO DATACENTERS {'dc-spb'};
```

#### DROP ROLE {: #drop_role }

Удаляет роль. При удалении атомарно очищаются все связанные записи: членство в других
ролях, разрешения, сетевые ограничения.

```bnf
<drop-role-stmt> ::= DROP ROLE [IF EXISTS] <role_name>
```

Пример:

```sql
DROP ROLE IF EXISTS alice;
```

#### CIDR-группы {: #cidr_groups }

CIDR-группы — именованные наборы IP-подсетей, используемые в опции `ACCESS FROM CIDRS`
при создании или изменении ролей. Для применения CIDR-авторизации необходимо включить
параметр `router.auth.is_cidr_authorizer_enabled: true` в конфигурации плагина.

!!! note "Отличие от Cassandra"
    В Apache Cassandra CIDR-группы не управляются через CQL: они задаются
    утилитой `nodetool updatecidrgroup` и обновляются командой
    `nodetool reloadcidrgroupscache`. В Sirin управление CIDR-группами реализовано
    в виде CQL-операторов DCL, описанных ниже.

**CREATE CIDR GROUP**

```bnf
<create-cidr-group-stmt> ::= CREATE CIDR GROUP <name>
                              WITH ADDRESSES = {'<cidr>', ...}
```

**ALTER CIDR GROUP**

```bnf
<alter-cidr-group-stmt> ::= ALTER CIDR GROUP <name> ADD ADDRESSES {'<cidr>', ...}
                           | ALTER CIDR GROUP <name> DROP ADDRESSES {'<cidr>', ...}
```

**DROP CIDR GROUP**

```bnf
<drop-cidr-group-stmt> ::= DROP CIDR GROUP <name>
```

Примеры:

```sql
-- Создание группы
CREATE CIDR GROUP office_net WITH ADDRESSES = {'10.0.0.0/8', '192.168.1.0/24'};

-- Добавление адресов
ALTER CIDR GROUP office_net ADD ADDRESSES {'172.16.0.0/12'};

-- Удаление адресов
ALTER CIDR GROUP office_net DROP ADDRESSES {'192.168.1.0/24'};

-- Удаление группы
DROP CIDR GROUP office_net;
```

Права на управление CIDR-группами выдаются через
`GRANT ... ON CIDR GROUP <name>` или `GRANT ... ON ALL CIDR GROUPS`
(см. раздел [GRANT PERMISSION](#grant_permission)).

#### GRANT ROLE / REVOKE ROLE {: #grant_revoke_role }

Назначает или отзывает членство одной роли в другой. Роль наследует все права родительской роли.

```bnf
<grant-role-stmt>  ::= GRANT <role_name> TO <grantee_role>
<revoke-role-stmt> ::= REVOKE <role_name> FROM <grantee_role>
```

Примеры:

```sql title="Назначить роль data_readers пользователю alice"
GRANT data_readers TO alice;
```

```sql title="Отозвать роль data_readers у пользователя alice"
REVOKE data_readers FROM alice;
```

#### GRANT PERMISSION {: #grant_permission }

Предоставляет привилегии роли на указанный ресурс.

```bnf
<grant-permission-stmt> ::= GRANT <permissions> ON <resource> TO <role_name>

<permissions> ::= ALL [PERMISSIONS]
                | <permission> [, <permission>]* [PERMISSION[S]]

<permission>  ::= CREATE | ALTER | DROP | SELECT | MODIFY | AUTHORIZE | DESCRIBE

<resource>    ::= ALL KEYSPACES
                | KEYSPACE <keyspace_name>
                | [TABLE] <table_name>
                | ALL ROLES
                | ROLE <role_name>
                | ALL CIDR GROUPS
                | CIDR GROUP <group_name>
```

Применимые привилегии зависят от типа ресурса:

| Ресурс                      | Применимые привилегии                                    |
|-----------------------------|----------------------------------------------------------|
| ALL KEYSPACES, KEYSPACE     | CREATE, ALTER, DROP, SELECT, MODIFY, AUTHORIZE, DESCRIBE |
| TABLE                       | ALTER, DROP, SELECT, MODIFY, AUTHORIZE, DESCRIBE         |
| ALL ROLES, ROLE             | CREATE, ALTER, DROP, AUTHORIZE, DESCRIBE                 |
| ALL CIDR GROUPS, CIDR GROUP | CREATE, ALTER, DROP, AUTHORIZE, DESCRIBE                 |

Описание привилегий:

| Привилегия  | Описание                                              |
| ----------- | ----------------------------------------------------- |
| `CREATE`    | Создание таблиц и пространств имён                    |
| `ALTER`     | Изменение схемы                                       |
| `DROP`      | Удаление таблиц и пространств имён                    |
| `SELECT`    | Чтение данных                                         |
| `MODIFY`    | Запись данных (INSERT, UPDATE, DELETE)                |
| `AUTHORIZE` | Управление правами доступа (GRANT, REVOKE)            |
| `DESCRIBE`  | Просмотр ролей (LIST ROLES) и схемы (DESCRIBE)        |

Примеры:

```sql title="Разрешить чтение из всех таблиц пространства имён ks1"
GRANT SELECT ON KEYSPACE ks1 TO data_readers;
```

```sql title="Разрешить запись в конкретную таблицу"
GRANT MODIFY ON TABLE ks1.events TO writer;
```

```sql title="Выдать все права на пространство имён"
GRANT ALL PERMISSIONS ON KEYSPACE ks1 TO admin;
```

```sql title="Выдать несколько привилегий одним запросом"
GRANT SELECT, MODIFY ON ALL KEYSPACES TO app_role;
```

```sql title="Управление CIDR-группами"
GRANT ALTER ON ALL CIDR GROUPS TO netadmin;
```

#### LIST ROLES {: #list_roles }

Возвращает список ролей в системе. При указании конкретной роли возвращает роли, членом которых
она является.

```bnf
<list-roles-stmt> ::= LIST ROLES [OF <role_name>] [NORECURSIVE]
```

- `OF <role_name>` — показать роли, в которые входит указанная роль
- `NORECURSIVE` — только прямые членства, без транзитивных

Примеры:

```sql title="Все роли в системе"
LIST ROLES;
```

```sql title="Роли, которые наследует alice (транзитивно)"
LIST ROLES OF alice;
```

```sql title="Только прямые родительские роли alice"
LIST ROLES OF alice NORECURSIVE;
```

#### LIST PERMISSIONS {: #list_permissions }

Возвращает список выданных привилегий. Результат можно фильтровать по типу привилегии, ресурсу
и роли.

```bnf
<list-permissions-stmt> ::= LIST <permissions> [ON <resource>] [OF <role_name>] [NORECURSIVE]

<permissions> ::= ALL [PERMISSIONS]
                | <permission> [, <permission>]* [PERMISSION[S]]
```

- `OF <role_name>` — показать привилегии указанной роли и её родительских ролей (по умолчанию рекурсивно)
- `NORECURSIVE` — только собственные привилегии роли, без унаследованных
- `ON <resource>` — фильтрация по ресурсу

Примеры:

```sql title="Все привилегии в системе"
LIST ALL PERMISSIONS;
```

```sql title="Все привилегии роли alice (включая унаследованные)"
LIST ALL PERMISSIONS OF alice;
```

```sql title="Только собственные привилегии alice"
LIST ALL PERMISSIONS OF alice NORECURSIVE;
```

```sql title="Привилегии на конкретное пространство имён"
LIST ALL PERMISSIONS ON KEYSPACE ks1 OF alice;
```

```sql title="Только SELECT-привилегии alice"
LIST SELECT PERMISSIONS OF alice;
```

#### REVOKE PERMISSION {: #revoke_permission }

Отзывает ранее выданные привилегии роли на ресурс.

```bnf
<revoke-permission-stmt> ::= REVOKE <permissions> ON <resource> FROM <role_name>
```

Примеры:

```sql title="Отозвать право чтения"
REVOKE SELECT ON TABLE ks1.users FROM reporter;
```

```sql title="Отозвать все привилегии на keyspace"
REVOKE ALL PERMISSIONS ON KEYSPACE ks1 FROM alice;
```

### TTL и механизм экспирации {: #ttl_and_expiration }

TTL (Time To Live) определяет время жизни записанных значений в секундах. По истечении TTL
значения становятся невидимыми для чтения; строка, у которой истекли все значения, помечается
для физического удаления.

#### Задание TTL {: #ttl_set }

TTL можно задать на двух уровнях:

**На уровне таблицы** — через параметр `default_time_to_live` в `CREATE TABLE`. Применяется
ко всем вставляемым строкам, если `USING TTL` не указан явно в запросе.

```sql
CREATE TABLE mykeyspace.sessions (
    session_id uuid PRIMARY KEY,
    user_id    uuid
) WITH default_time_to_live = 86400;   -- 24 часа
```

**На уровне запроса** — через `USING TTL` в операторах `INSERT` и `UPDATE`. Переопределяет
`default_time_to_live`. TTL применяется к значениям, записанным этим запросом: `UPDATE ... USING TTL`
не меняет время жизни столбцов, которые он не затрагивает.

```sql title="Строка удалится через 10 минут"
INSERT INTO mykeyspace.sessions (session_id, user_id)
    VALUES (uuid(), 123e4567-e89b-12d3-a456-426614174000)
    USING TTL 600;
```

```sql title="Отключить TTL для конкретной строки (строка бессмертна)"
INSERT INTO mykeyspace.sessions (session_id, user_id)
    VALUES (uuid(), 123e4567-e89b-12d3-a456-426614174000)
    USING TTL 0;
```

```sql title="Значение столбца token истечёт через 5 минут, остальные столбцы не изменятся"
UPDATE mykeyspace.sessions USING TTL 300
    SET token = 'abc'
    WHERE session_id = 123e4567-e89b-12d3-a456-426614174000;
```

#### Механизм удаления {: #ttl_expiration_mechanism }

Sirin использует двухэтапный механизм удаления:

1. **Логическая экспирация.** При чтении строка проверяется на истечение TTL. Истёкшие строки
   не возвращаются клиенту — они невидимы немедленно после истечения срока.

2. **Физическое удаление.** Фоновая задача периодически обходит таблицы и физически удаляет
   истёкшие строки. Интервал и размер пакета настраиваются через конфигурацию плагина
   (`storage.ttl.timeout`, `storage.ttl.batch_size`).

Такой подход позволяет не влиять на производительность операций чтения и записи.

### Функции {: #functions }

Функции можно использовать в качестве значений: в `VALUES` оператора `INSERT`, в правой части
присваиваний `UPDATE`, в правой части условий `WHERE` и `IF`. Аргументами функций могут быть литералы
и маркеры параметров (`?`, `:name`) — тип маркера определяется типом аргумента функции.

#### Функция uuid {: #uuid_function }

`uuid()` генерирует случайный UUID версии 4 (тип `uuid`).

#### Функции timeuuid {: #timeuuid_functions }

| Функция          | Описание                                                                                                                        |
|------------------|---------------------------------------------------------------------------------------------------------------------------------|
| `now()`          | Генерирует новый уникальный `timeuuid` для текущего момента времени. Синонимы: `currentTimeuuid()`, `current_timeuuid()`.       |
| `minTimeuuid(t)` | Возвращает минимально возможный `timeuuid` для заданного момента времени `t` (типа `timestamp`). Синоним: `min_timeuuid()`.     |
| `maxTimeuuid(t)` | Возвращает максимально возможный `timeuuid` для заданного момента времени `t` (типа `timestamp`). Синоним: `max_timeuuid()`.    |

`minTimeuuid()` и `maxTimeuuid()` удобно использовать в `WHERE` для выборки по временнóму диапазону.

#### Функция token {: #token_function }

`token(v1, ..., vN)` вычисляет токен партиции (хеш Murmur3, тип `bigint`) по значениям столбцов ключа
партиционирования. Число и типы аргументов должны совпадать со столбцами ключа партиционирования
таблицы, к которой относится запрос.

!!! note "Примечание"
    `token()` вычисляет значение, но не может стоять в левой части условия: фильтрация вида
    `WHERE token(pk) > ...` не поддерживается.

**Примеры:**

```sql title="Фильтрация событий по временнóму диапазону"
SELECT * FROM ks1.events
WHERE device_id = 123e4567-e89b-12d3-a456-426614174000
  AND event_id > minTimeuuid('2024-01-01 00:00:00+0000')
  AND event_id < maxTimeuuid('2024-01-02 00:00:00+0000');
```

```sql title="Фильтрация по текущему моменту времени"
SELECT * FROM ks1.events
WHERE device_id = 123e4567-e89b-12d3-a456-426614174000
  AND event_id < now();
```

```sql title="Генерация идентификаторов при вставке"
INSERT INTO ks1.events (device_id, event_id, payload)
    VALUES (uuid(), now(), 'boot');
```

```sql title="Маркеры параметров в аргументах функции (prepared statement)"
SELECT * FROM ks1.events
WHERE device_id = ?
  AND event_id > minTimeuuid(?);
```

#### Ограничения {: #functions_limitations }

- функции не поддерживаются в проекции `SELECT` (между `SELECT` и `FROM`)
- агрегатные функции (`count`, `sum`, `avg`, `min`, `max`) не поддерживаются
- пользовательские функции (UDF) и пользовательские агрегаты (UDA) не поддерживаются
- триггеры не поддерживаются

## Примеры использования {: #usage_examples }

```sql title="Создание таблицы"
CREATE TABLE users (
    id uuid PRIMARY KEY,
    name text,
    age int,
    country text
);
```

```sql title="Добавление данных"
INSERT INTO users (id, name, age, country) VALUES (2f0cff20-967f-4dfa-a8a1-6140b5fd9255, 'Ivan', 32, 'Russia');
```

```sql title="Выборка данных с LIMIT"
SELECT id, name FROM users LIMIT 10;
```

```sql title="TTL"
INSERT INTO sessions (id, user_id) VALUES (uuid(), 42) USING TTL 600;
```

## Протоколы и драйверы {: #protocols_and_drivers }

- поддерживается протокол Cassandra Native Protocol v4
- совместимость с драйверами Apache Cassandra для популярных языков:
    - Java — [DataStax Java Driver](https://github.com/datastax/java-driver)
    - Python — [Python Cassandra Driver](https://github.com/datastax/python-driver)
    - Go — [gocql](https://github.com/gocql/gocql)
    - Node.js — [cassandra-driver for Node.js](https://github.com/datastax/nodejs-driver)
    - Rust — [ScyllaDB Rust Driver](https://github.com/scylladb/scylla-rust-driver)
- совместимость с инструментами:
    - `cqlsh` — стандартный CLI для Cassandra
    - DBeaver — GUI для управления базами данных

### Маршрутизация запросов на мастер {: #master_routing }

Запросы обрабатываются на мастерах репликасетов. Реплики сообщают драйверу топологию так,
чтобы трафик уходил на мастера:

- в `system.local` реплика описывает себя, но с пустым набором токенов,
  поэтому драйвер, учитывающий токены при маршрутизации (token-aware), не направляет на неё запросы
- в `system.peers` и `system.peers_v2` реплика возвращает всех мастеров кластера, включая мастера
  своего репликасета

Драйвер, подписанный на события `TOPOLOGY_CHANGE` (запрос `REGISTER`), получает push-события:

- при подключении к реплике — `NEW_NODE` для мастера и `REMOVED_NODE` для самой реплики
- при смене мастера — бывший мастер отправляет своим клиентам те же события, а новый мастер —
  `NEW_NODE` для себя

События `STATUS_CHANGE` и `SCHEMA_CHANGE` принимаются в подписке, но не отправляются.

### Уровни согласованности {: #consistency_levels }

Уровни согласованности, которые драйвер передаёт в кадрах `QUERY`, `EXECUTE` и `BATCH`
(`consistency` и `serial_consistency`), принимаются, но не влияют на выполнение запроса. Запрос
всегда выполняется на мастере репликасета, а гарантии согласованности определяются синхронной
репликацией Picodata. Например, запросы с `LOCAL_QUORUM` и `ONE` выполняются одинаково, а условные
запросы (LWT) с `SERIAL` и `LOCAL_SERIAL` — тоже.

Поэтому ошибки `Unavailable`, `ReadTimeout` и `WriteTimeout`, связанные с недостаточным числом
ответивших реплик, не возвращаются.

Запрос отклоняется с ошибкой `Invalid` (`0x2200`), только если кадр содержит некорректное значение:

- неизвестный код уровня согласованности
- в поле `serial_consistency` передано значение, отличное от `SERIAL` и `LOCAL_SERIAL`

### Prepared statements {: #prepared_statements }

Подготовленные запросы кешируются на узле отдельно для каждого пространства имён. Если запроса нет
в кеше (например, после перезапуска узла), `EXECUTE` и `BATCH` возвращают ошибку `Unprepared`
(`0x2500`), и драйвер должен автоматически подготовить запрос заново.

## Развёртывание, эксплуатация и восстановление {: #deployment_operations_recovery }

Все процедуры полностью соответствуют инфраструктуре Picodata.

См. разделы документации Picodata:

- [Установка плагинов](../overview/glossary.md#plugin)
- [Создание кластера](../tutorial/deploy.md)
- [Получение данных о кластере](../admin/local_monitoring.md)
- [Резервное копирование и восстановление](../admin/backup_and_restore.md)

### Конфигурация плагина {: #plugin_configuration }

#### Сетевой адрес (advertise url) {: #router_listener }

Адрес и порт, на которых сервис `router` принимает соединения по
протоколу Cassandra, задаются не в `plugin_config.yaml`, а в
конфигурации самого инстанса Picodata (`picodata.yaml`) через блок
параметров `listener` для плагина — см.
[`instance.plugin`](../reference/config.md#instance_plugin).

```yaml
instance:
  # ...
  plugin:
    sirin:
      service:
        router:
          listener:
            enabled: true
            listen: "0.0.0.0:9042"
            advertise: "10.20.1.1:9042"
            tls:
              enabled: false
```

- `listen` — адрес, на котором сервис слушает входящие соединения
- `advertise` — адрес, который клиенты получают через `system.local` /
  `system.peers` / `system.peers_v2` и используют для прямых подключений
  к узлу, в том числе для того, чтобы драйвер мог маршрутизировать
  запросы с учётом токенов. Если не задан, равен `listen`
- `tls` — настройка TLS-соединения. На данный момент не поддерживается.

Без этого блока сервис `router` не запустится — Picodata потребует явно
указать параметры `listener`
для сервиса плагина.

Остальные параметры плагина задаются, как и раньше, в `plugin_config.yaml`:

```yaml
router:
  auth:
    is_required: false      # Включить обязательную аутентификацию. По умолчанию false.
    permissions_validity: 2s  # Интервал обновления кеша прав пользователя в рамках
                              # активной сессии. По умолчанию 2s.
    is_cidr_authorizer_enabled: false  # Включить проверку ACCESS FROM CIDRS при входе
    network:
      # Ключ failure_domain Picodata, значение которого трактуется как имя
      # дата-центра для проверки ACCESS TO DATACENTERS.
      dc_failure_domain_key: dc

  dispatcher:
    # Ёмкость канала между IO-потоком и файбер-луп Tarantool. По умолчанию 4096.
    channel_capacity: 4096

    # Режим Tokio-рантайма IO-потока:
    #   single (по умолчанию) — однопоточный рантайм
    #   multi                 — многопоточный рантайм; дополнительный параметр:
    #     worker_threads: <N>  — число рабочих потоков
    #                           (по умолчанию — по одному на логическое ядро)
    #
    # Пример — многопоточный рантайм с 4 потоками:
    #   tokio_runtime:
    #     mode: multi
    #     worker_threads: 4
    tokio_runtime:
      mode: single

storage:
  ttl:
    timeout: 10             # Частота срабатывания в секундах фоновой задачи, которая
                            # физически удаляет записи с истёкшим сроком жизни
    batch_size: 100         # Размер пакета на одну операцию удаления записей
```

#### Включение аутентификации {: #enable_auth }

По умолчанию аутентификация отключена (`is_required: false`). Включить её можно двумя способами:

**Через файл конфигурации** `plugin_config.yaml`:

```yaml
router:
  auth:
    is_required: true
    permissions_validity: 2s
```

**Через SQL-команду** [ALTER PLUGIN](https://docs.picodata.io/picodata/stable/reference/sql/alter_plugin/)
без перезапуска кластера:

```sql
ALTER PLUGIN sirin 2.0.0 SET router.auth.is_required='true';
```

Параметр `permissions_validity` задаёт интервал, с которым Sirin проверяет изменения прав в рамках
активного соединения. При значении `2s` изменения, внесённые через `GRANT`/`REVOKE`, начнут
действовать не позднее чем через 2 секунды — без разрыва соединения.

```sql
ALTER PLUGIN sirin 2.0.0 SET router.auth.permissions_validity='5s';
```

### Метрики {: #metrics }

Sirin публикует метрики в формате Prometheus через HTTP-эндпоинт Picodata `/metrics` (см.
[Метрики](../reference/metrics.md)). Все метрики плагина имеют префикс `sirin_`.

**Протокол и соединения:**

| Метрика                                   | Тип       | Метки              | Описание                                                          |
|-------------------------------------------|-----------|--------------------|-------------------------------------------------------------------|
| `sirin_proto_active_connections`          | gauge     | —                  | Число активных клиентских соединений                              |
| `sirin_proto_request_latency_seconds`     | histogram | `opcode`, `status` | Время обработки запросов протокола                                |
| `sirin_proto_outgoing_queue_depth_frames` | histogram | —                  | Число фреймов в исходящей очереди соединения                      |
| `sirin_prepared_statements_active`        | gauge     | —                  | Число подготовленных запросов в кеше                              |

**Обработка запросов (router):**

| Метрика                                      | Тип       | Метки                          | Описание                                                                                   |
|----------------------------------------------|-----------|--------------------------------|--------------------------------------------------------------------------------------------|
| `sirin_statements_processed_total`           | counter   | `operation_type`, `result`     | Число обработанных запросов. `operation_type`: `regular`, `prepare`, `execute`, `batch`; запросы внутри `BATCH` считаются по отдельности |
| `sirin_statement_processing_latency_seconds` | histogram | `type`, `status`               | Время обработки запроса. `type`: `select`, `insert`, `update`, `delete`, `batch`, `other`  |
| `sirin_statement_select_processed_total`     | counter   | `allow_filtering`, `status`    | Число обработанных `SELECT`                                                                |
| `sirin_statement_select_rows_returned_total` | counter   | —                              | Число строк, возвращённых успешными `SELECT`                                               |
| `sirin_router_rpc_batch_outbound_dmls_total` | counter   | `dml_kind`                     | Число DML-операций, отправленных из router в storage в составе батчей                      |

Метка `status` принимает значения `ok` и `error`.

**Хранение (storage):**

| Метрика                                   | Тип       | Метки            | Описание                                          |
|-------------------------------------------|-----------|------------------|---------------------------------------------------|
| `sirin_storage_rpc_latency_seconds`       | histogram | `path`, `status` | Время обработки RPC-запросов на storage           |
| `sirin_ttl_expired_records_deleted_total` | counter   | `table`          | Число строк, физически удалённых по истечении TTL |

**Ресурсы процесса:**

| Метрика                                     | Тип     | Метки  | Описание                                                            |
|---------------------------------------------|---------|--------|---------------------------------------------------------------------|
| `sirin_cpu_tx_thread_time_seconds`          | gauge   | `kind` | Процессорное время TX-потока                                        |
| `sirin_cpu_tokio_thread_time_seconds_total` | counter | `kind` | Суммарное процессорное время потоков, обслуживающих соединения      |
| `sirin_process_resident_memory_bytes`       | gauge   | —      | Резидентная память процесса (RSS)                                   |
