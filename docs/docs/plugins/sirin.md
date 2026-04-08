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

| Возможность Cassandra                | Статус в Sirin      |
| ------------------------------------ | ------------------- |
| CQL (основные операторы)             | ✅ Поддерживается   |
| Prepared statements                  | ✅ Поддерживается    |
| Named parameter markers              | ✅ Поддерживается      |
| Pagination                           | ✅ Поддерживается      |
| TTL                                  | ✅ Поддерживаются      |
| Static columns                       | ✅ Поддерживаются      |
| LIMIT в SELECT                       | ✅ Поддерживается      |
| Телеметрия                           | ✅ Поддерживается      |
| Инструменты (cqlsh, picodata admin)  | ✅ Поддерживается      |
| BATCH                                | ✅ Поддерживается      |
| Аутентификация и управление правами  | 🟡 Частично         |
| Lightweight transactions (LWT)       | 🟡 Частично |
| User-defined types (UDT)             | ❌ Не поддерживаются |
| Материализованные представления (MV) | ❌ Не поддерживаются |
| GROUP BY                             | ❌ Не поддерживается |
| ALTER TABLE                          | ❌ Не поддерживается |


### Ограничения {: #limitations }

- `CREATE KEYSPACE` и `DROP KEYSPACE` поддерживаются синтаксически, но параметры replication_strategy
  и replication_factor игнорируются. Настройки берутся из конфигурации Picodata
- репликация управляется средствами Picodata
- [движок хранения] по умолчанию — vinyl. Все настройки vinyl влияют на характеристики хранения
- материализованные представления (MV) отсутствуют
- UDT (User-Defined Type) и вложенные коллекции отсутствуют

[движок хранения]: ../overview/glossary.md#db_engine

## Типы данных {: #data_types }

Sirin поддерживает основные типы данных Cassandra. Ниже приведены поддерживаемые типы,
их допустимый диапазон значений и формат литералов в CQL.

### Числовые типы {: #numeric_types }

| Тип | Описание | Диапазон |
|---|---|---|
| `tinyint` | 8-битное целое со знаком | от -128 до 127 |
| `smallint` | 16-битное целое со знаком | от -32 768 до 32 767 |
| `int` | 32-битное целое со знаком | от -2 147 483 648 до 2 147 483 647 |
| `bigint` | 64-битное целое со знаком | от -2^63 до 2^63 - 1 |
| `float` | 32-битное число с плавающей точкой (IEEE 754) | ≈ ±3.4 × 10^38 |
| `double` | 64-битное число с плавающей точкой (IEEE 754) | ≈ ±1.8 × 10^308 |
| `counter` | 64-битный атомарный счётчик; поддерживает только операции `+` и `-` | от -2^63 до 2^63 - 1 |

### Строковые и бинарные типы {: #string_types }

| Тип | Описание |
|---|---|
| `text`, `varchar` | UTF-8 строка произвольной длины. `text` и `varchar` являются синонимами. |
| `ascii` | Строка, содержащая только символы US-ASCII. |
| `blob` | Произвольные двоичные данные. В CQL записываются в hex-формате: `0x<hex>`. |

### Временны́е типы {: #temporal_types }

| Тип | Описание | Формат литерала |
|---|---|---|
| `timestamp` | Момент времени с точностью до миллисекунды (UTC). | `'2024-01-15 10:30:00+0000'` или `'2024-01-15T10:30:00Z'` |
| `date` | Календарная дата без времени. | `'2024-01-15'` |
| `time` | Время суток с точностью до наносекунды. | `'10:30:00.000000000'` |

### Типы UUID {: #uuid_types }

| Тип | Описание | Формат литерала |
|---|---|---|
| `uuid` | UUID версии 4, случайный. | `123e4567-e89b-12d3-a456-426614174000` |
| `timeuuid` | UUID версии 1, включающий временну́ю метку. Уникален даже при одинаковом времени. Используется для временны́х рядов. | `123e4567-e89b-12d3-a456-426614174000` |

### Сетевые типы {: #network_types }

| Тип | Описание | Пример |
|---|---|---|
| `inet` | IP-адрес IPv4 или IPv6 в текстовом представлении. Резолвинг имён хостов не поддерживается. | `'192.168.1.1'`, `'::1'` |

### Булев тип {: #boolean_type }

| Тип | Описание |
|---|---|
| `boolean` | Логическое значение: `true` или `false`. |

### Коллекции {: #collection_types }

Коллекции позволяют хранить несколько значений в одном столбце. Вложенные коллекции не поддерживаются.

| Тип | Описание | Пример литерала |
|---|---|---|
| `map<K, V>` | Набор пар ключ-значение. Ключи уникальны. | `{'key1': 1, 'key2': 2}` |
| `set<T>` | Неупорядоченный набор уникальных значений. | `{1, 2, 3}` |

!!! note "Ограничения коллекций"
    Вложенные коллекции и `frozen<>` не поддерживаются.

## Поддерживаемые возможности CQL {: #supported_cql_features }

### Определение схемы (DDL) {: #schema_definition }

#### CREATE KEYSPACE {: #create_keyspace }

Создаёт пространство имён — логический контейнер для таблиц. Параметры репликации принимаются
синтаксически для совместимости с Cassandra, но не применяются: репликацией управляет Picodata.

```bnf
<create-keyspace-stmt> ::= CREATE KEYSPACE [IF NOT EXISTS] <ks-name>
                           WITH REPLICATION = <map-literal>
                           [AND DURABLE_WRITES = <boolean>]
```

- `IF NOT EXISTS` — не возвращает ошибку, если пространство имён уже существует
- `replication_strategy` и `replication_factor` игнорируются
- `durable_writes` игнорируется; запись всегда производится в журнал фиксации

Пример:

```cql
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

```cql
DROP KEYSPACE IF EXISTS mykeyspace;
```

#### CREATE TABLE {: #create_table }

Создаёт таблицу в указанном пространстве имён.

Каждая таблица должна иметь первичный ключ, состоящий из двух частей:

- **Partition key** — определяет, на каком узле хранится строка. Может быть составным.
  Все строки с одинаковым partition key хранятся физически рядом.
- **Clustering key** — определяет порядок строк внутри partition. Необязателен.

```cql
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
                 | default_time_to_live '=' <integer>
                 | COMMENT '=' <string>
                 | <other-cassandra-option>
```

Поддерживаемые параметры таблицы:

| Параметр | Описание |
|---|---|
| `CLUSTERING ORDER BY` | Задаёт порядок сортировки для столбцов кластеризующего ключа. По умолчанию — `ASC`. |
| `default_time_to_live` | Время жизни строк в секундах. Значение `0` означает, что строки не удаляются автоматически. Если задано, применяется к каждой вставляемой строке, если явное значение `TTL` не указано в самом запросе `INSERT`. |

Остальные параметры Cassandra (`compaction`, `compression`, `gc_grace_seconds`, `caching` и т. д.) принимаются синтаксически для обеспечения совместимости, но не применяются.

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

На данный момент не поддерживается.

#### DROP TABLE {: #drop_table }

Удаляет таблицу и все её данные.

```bnf
<drop-table-stmt> ::= DROP TABLE [IF EXISTS] [<keyspace-name> '.'] <table-name>
```

- `IF EXISTS` — не возвращает ошибку, если таблица не существует

Пример:

```cql
DROP TABLE IF EXISTS mykeyspace.events;
```

#### TRUNCATE TABLE {: #truncate_table }

Удаляет все строки из таблицы, сохраняя её схему.

```bnf
<truncate-stmt> ::= TRUNCATE [TABLE] [<keyspace-name> '.'] <table-name>
```

Пример:

```cql
TRUNCATE TABLE mykeyspace.events;
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
                  [USING TTL <int>]
```

- `IF NOT EXISTS` — вставляет строку только если она не существует (см. [LWT](#lwt))
- `USING TTL <seconds>` — задаёт время жизни строки в секундах; переопределяет `default_time_to_live` таблицы
- `USING TIMESTAMP` — не поддерживается

Примеры:

```sql title="Простая вставка"
INSERT INTO mykeyspace.users (user_id, email, name)
    VALUES (uuid(), 'alice@example.com', 'Alice');
```

```sql title="Вставка с TTL (строка удалится через 1 час)"--
INSERT INTO mykeyspace.sessions (session_id, user_id)
    VALUES (uuid(), 123e4567-e89b-12d3-a456-426614174000)
    USING TTL 3600;
```

```sql title="Вставка только при отсутствии строки"
INSERT INTO mykeyspace.users (user_id, email)
    VALUES (uuid(), 'bob@example.com')
    IF NOT EXISTS;
```

#### UPDATE {: #update }

Обновляет один или несколько столбцов строки. Строка идентифицируется по полному первичному ключу
в `WHERE`. Если строки с указанным ключом не существует, она будет **создана** (upsert-семантика) —
за исключением случая, когда указан `IF EXISTS`.

```bnf
<update-stmt> ::= UPDATE [<keyspace-name> '.'] <table-name>
                  SET <assignment> [',' <assignment>]*
                  WHERE <where-clause>
                  [IF EXISTS]

<assignment> ::= <column-name> '=' <value>
               | <column-name> '=' <column-name> '+' <value>
               | <column-name> '=' <column-name> '-' <value>
```

- `IF EXISTS` — обновляет строку только если она существует (см. [LWT](#lwt))
- `USING TIMESTAMP` — не поддерживается

**Виды присваиваний:**

| Форма | Применение | Пример |
|---|---|---|
| `col = value` | Установить значение | `name = 'Bob'` |
| `col = col + value` | Инкремент счётчика или добавление элементов в коллекцию | `visits = visits + 1` |
| `col = col - value` | Декремент счётчика или удаление элементов из коллекции | `visits = visits - 1` |

Примеры:

```sql title="Обновление обычных столбцов"
UPDATE mykeyspace.users
    SET name = 'Alice Smith', email = 'alice.smith@example.com'
    WHERE user_id = 123e4567-e89b-12d3-a456-426614174000;
```

```sql title="Инкремент счётчика"
UPDATE mykeyspace.stats
    SET page_views = page_views + 1
    WHERE page_id = 'home';
```

```sql title="Добавление записей в map-коллекцию"
UPDATE mykeyspace.users
    SET metadata = metadata + {'city': 'Moscow'}
    WHERE user_id = 123e4567-e89b-12d3-a456-426614174000;
```

```sql title="Удаление записей из map-коллекции"
UPDATE mykeyspace.users
    SET metadata = metadata - {'city': 'Moscow'}
    WHERE user_id = 123e4567-e89b-12d3-a456-426614174000;
```

```sql title="Обновление только при существовании строки"
UPDATE mykeyspace.users
    SET name = 'Bob'
    WHERE user_id = 123e4567-e89b-12d3-a456-426614174000
    IF EXISTS;
```

#### DELETE {: #delete }

Удаляет строку целиком или значения отдельных столбцов. Операция всегда задаётся
по первичному ключу.

```bnf
<delete-stmt> ::= DELETE [<column-name> [',' <column-name>]*]
                  FROM [<keyspace-name> '.'] <table-name>
                  WHERE <where-clause>
                  [IF EXISTS]
```

- Если список столбцов не указан — удаляется вся строка.
- Если список столбцов указан — удаляются только значения этих столбцов (столбцы
  получают значение `null`). Столбцы первичного ключа удалить нельзя.
- `IF EXISTS` — удаляет строку только если она существует (см. [LWT](#lwt))
- `USING TIMESTAMP` — не поддерживается
- `WHERE` должен содержать как минимум полный partition key

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

```sql title="Удаление значения отдельного столбца"
DELETE email FROM mykeyspace.users
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
<select-stmt> ::= SELECT <select-clause>
                  FROM [<keyspace-name> '.'] <table-name>
                  [WHERE <where-clause>]
                  [ORDER BY <column-name> [ASC | DESC] [',' ...]]
                  [LIMIT <number>]
                  [ALLOW FILTERING]

<select-clause> ::= '*'
                  | <column-name> [AS <alias>] [',' <column-name> [AS <alias>]]*
```

**WHERE**

Задаёт условие фильтрации строк. Поддерживаемые операторы: `=`, `<`, `>`, `<=`, `>=`, `!=`, `IN`.

Для эффективной работы рекомендуется всегда указывать полный partition key. Фильтрация по
неключевым столбцам требует `ALLOW FILTERING`.

```cql
WHERE device_id = 123e4567-e89b-12d3-a456-426614174000
  AND event_time > '2024-01-01 00:00:00+0000'
```

**ORDER BY**

Порядок сортировки можно задавать только по столбцам кластеризующего ключа, и только
в том направлении, которое задано в `CLUSTERING ORDER BY` таблицы или в обратном ему.

**LIMIT**

Ограничивает общее число возвращаемых строк.

**ALLOW FILTERING**

По умолчанию Sirin, как и Cassandra, запрещает запросы, требующие полного сканирования всех
partition. Такие запросы необходимо явно пометить `ALLOW FILTERING`. Следует использовать
осторожно на больших таблицах.

**Ограничения:**

- `GROUP BY` не поддерживается
- `USING CONSISTENCY`, `USING TIMESTAMP` не поддерживаются
- функции в списке столбцов (`COUNT(*)`, `CAST()`, вызовы функций) не поддерживаются
- сравнение кортежей, например `(a, b) > (1, 2)`, не поддерживается

Примеры:

```sql title="Все строки partition"
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

#### BATCH {: #batch }

Объединяет несколько DML-операций в один запрос. Все операции батча применяются атомарно.

```bnf
<batch-stmt> ::= BEGIN [UNLOGGED] BATCH
                     <dml-stmt> ';'
                     [<dml-stmt> ';']*
                 APPLY BATCH
```

Батч может содержать операторы `INSERT`, `UPDATE` и `DELETE`. Операции могут
производится над разными таблицами.

- `UNLOGGED` — в Cassandra отключает журнал батча для повышения производительности; в Sirin
  принимается синтаксически, поведение не меняется.
- `USING TIMESTAMP` на уровне батча не поддерживается.

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

#### LWT (Lightweight transactions) {: #lwt }

Lightweight transactions позволяют выполнять операции условно — только при выполнении
указанного условия. Это обеспечивает семантику «сравни и замени» (compare-and-set) без полного
распределённого консенсуса.

Так как реплики в Picodata объединены синхронной репликацией, проверка условия выполняется
локально на узле, обрабатывающем запрос.

На данный момент поддерживаются:

| Оператор | Конструкция | Описание |
|---|---|---|
| `INSERT` | `IF NOT EXISTS` | Вставить строку только если её нет |
| `UPDATE` | `IF EXISTS` | Обновить строку только если она есть |
| `DELETE` | `IF EXISTS` | Удалить строку только если она есть |

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

### Аутентификация и управление правами доступа {: #security }

Sirin использует ролевую модель управления доступом (RBAC), аналогичную Cassandra. Доступ к данным
определяется набором привилегий, назначенных ролям. Роли могут наследоваться друг от друга, образуя
иерархию. Аутентификация выполняется по протоколу Cassandra Native Protocol v4 с передачей
учётных данных (логин и пароль) при установке соединения.

По умолчанию аутентификация отключена. Как её включить — см. раздел [Включение аутентификации](#enable_auth).

По умолчанию создаётся суперпользователь `cassandra` с паролем `cassandra`. Суперпользователь
обладает полными правами на все ресурсы и может управлять другими ролями.

!!! warning "Безопасность"
    Рекомендуется сменить пароль суперпользователя `cassandra` сразу после развёртывания.

#### CREATE ROLE {: #create_role }

Создаёт новую роль. Роль может быть учётной записью пользователя (с возможностью входа) или
группой для делегирования прав.

```bnf
<create-role-stmt> ::= CREATE ROLE [IF NOT EXISTS] <role_name>
                       [WITH <role_options>]

<role_options>     ::= <role_option> [AND <role_option>]*

<role_option>      ::= PASSWORD = '<string>'
                     | LOGIN = (true | false)
                     | SUPERUSER = (true | false)
                     | OPTIONS = <map_literal>
```

- `LOGIN = true` — разрешает вход в систему. По умолчанию `false`
- `SUPERUSER = true` — наделяет ролью суперпользователя с полными правами. По умолчанию `false`
- `PASSWORD` — пароль; обязателен, если `LOGIN = true`
- `IF NOT EXISTS` — не возвращает ошибку, если роль уже существует

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

!!! note "Ограничения"
    `DROP ROLE` и `ALTER ROLE` на данный момент не поддерживаются.

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
```

Применимые привилегии зависят от типа ресурса:

| Ресурс          | Применимые привилегии                              |
| --------------- | -------------------------------------------------- |
| ALL KEYSPACES, KEYSPACE | CREATE, ALTER, DROP, SELECT, MODIFY, AUTHORIZE |
| TABLE           | ALTER, DROP, SELECT, MODIFY, AUTHORIZE             |
| ALL ROLES, ROLE | CREATE, ALTER, DROP, AUTHORIZE, DESCRIBE           |

Описание привилегий:

| Привилегия  | Описание                                              |
| ----------- | ----------------------------------------------------- |
| `CREATE`    | Создание таблиц и пространств имён                    |
| `ALTER`     | Изменение схемы                                       |
| `DROP`      | Удаление таблиц и пространств имён                    |
| `SELECT`    | Чтение данных                                         |
| `MODIFY`    | Запись данных (INSERT, UPDATE, DELETE)                |
| `AUTHORIZE` | Управление правами доступа (GRANT, REVOKE)            |
| `DESCRIBE`  | Просмотр информации о ролях (LIST ROLES)              |

Примеры:

```sql
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

!!! note "Ограничение"
    `REVOKE PERMISSION` на данный момент не поддерживается.

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

### TTL и механизм экспирации {: #ttl_and_expiration }

TTL (Time To Live) определяет время жизни строки в секундах. По истечении TTL строка
становится невидимой для чтения и помечается для физического удаления.

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

**На уровне строки** — через `USING TTL` в операторе `INSERT`. Переопределяет `default_time_to_live`.

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

#### Механизм удаления {: #ttl_expiration_mechanism }

Sirin использует двухэтапный механизм удаления:

1. **Логическая экспирация.** При чтении строка проверяется на истечение TTL. Истёкшие строки
   не возвращаются клиенту — они невидимы немедленно после истечения срока.

2. **Физическое удаление.** Фоновая задача периодически обходит таблицы и физически удаляет
   истёкшие строки. Интервал и размер пакета настраиваются через конфигурацию плагина
   (`storage.ttl.timeout`, `storage.ttl.batch_size`).

Такой подход позволяет не влиять на производительность операций чтения и записи.

### Функции {: #functions }

На данный момент Sirin поддерживает функции для работы со значениями типа `timeuuid`. Эти функции
используются в условиях `WHERE` для фильтрации строк по временно́му диапазону.

#### Функции timeuuid {: #timeuuid_functions }

| Функция | Описание |
|---|---|
| `now()` | Генерирует новый уникальный `timeuuid`, соответствующий текущему моменту времени. Синонимы: `currentTimeuuid()`, `current_timeuuid()`. |
| `minTimeuuid(t)` | Возвращает минимально возможный `timeuuid` для заданного момента времени `t` (типа `timestamp`). Синонимы: `min_timeuuid()`. |
| `maxTimeuuid(t)` | Возвращает максимально возможный `timeuuid` для заданного момента времени `t` (типа `timestamp`). Синонимы: `max_timeuuid()`. |

!!! warning "Ограничение"
    Функции `now()`, `minTimeuuid()`, `maxTimeuuid()` поддерживаются только в условии `WHERE`. Использование в списке столбцов оператора `SELECT` не поддерживается.

**Примеры:**

```sql title="Фильтрация событий за последние сутки"
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

#### Ограничения {: #functions_limitations }

- функции поддерживаются только в условии `WHERE`, не в списке столбцов `SELECT`
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

## Развёртывание, эксплуатация и восстановление {: #deployment_operations_recovery }

Все процедуры полностью соответствуют инфраструктуре Picodata.

См. разделы документации Picodata:

- [Установка плагинов](../overview/glossary.md#plugin)
- [Создание кластера](../tutorial/deploy.md)
- [Получение данных о кластере](../admin/local_monitoring.md)
- [Резервное копирование и восстановление](../admin/backup_and_restore.md)

### Конфигурация плагина {: #plugin_configuration }

```yaml
router:
  addr: 0.0.0.0:9042        # Адрес, на котором будет доступен протокол cassandra
  max_clients: 100          # Максимальное количество конкурентных соединений на узел
  auth:
    is_required: false      # Включить обязательную аутентификацию. По умолчанию false.
    permissions_validity: 2s  # Интервал обновления кэша прав пользователя в рамках
                              # активной сессии. По умолчанию 2s.
  peers_cleaner:
    interval_secs: 15       # Интервал в секундах, по истечении которого из таблиц
                            # peers и peers_v2 удаляются неисправные узлы.

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
ALTER PLUGIN sirin 1.1.0 SET router.auth.is_required='true';
```

Параметр `permissions_validity` задаёт интервал, с которым Sirin проверяет изменения прав в рамках
активного соединения. При значении `2s` изменения, внесённые через `GRANT`/`REVOKE`, начнут
действовать не позднее чем через 2 секунды — без разрыва соединения.

```sql
ALTER PLUGIN sirin 1.1.0 SET router.auth.permissions_validity='5s';
```
