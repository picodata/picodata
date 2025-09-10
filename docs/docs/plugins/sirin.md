# Sirin

В данном разделе приведены сведения о Sirin, плагине для СУБД Picodata.

## Введение {: #introduction }

Picodata Sirin — закрытый плагин для корпоративной версии Picodata, реализующий поддержку
API Apache Cassandra (CQL и протокол Cassandra v4) поверх резидентной СУБД Picodata. Sirin
позволяет использовать приложения и драйверы из экосистемы Cassandra без изменений, сохраняя
при этом производительность и отказоустойчивость Picodata, что упрощает миграцию и интеграцию.

## Лицензирование и доступность {: #licensing_and_availability }

Модуль Sirin входит только в корпоративную версию Picodata и не распространяется как часть открытого ПО.
Для получения лицензии и доступа к плагину свяжитесь с вашим менеджером Picodata.

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
| Lightweight transactions (LWT)       | 🟡 Частично |
| User-defined types (UDT)             | ❌ Не поддерживаются |
| Материализованные представления (MV) | ❌ Не поддерживаются |
| GROUP BY                             | ❌ Не поддерживается |
| ALTER TABLE                          | ❌ Не поддерживается |
| BATCH                                | ❌ Не поддерживается |

### Ограничения {: #limitations }

- CREATE KEYSPACE и DROP KEYSPACE поддерживаются синтаксически, но параметры replication_strategy
  и replication_factor игнорируются. Настройки берутся из конфигурации Picodata.
- Репликация управляется средствами Picodata.
- Storage engine по умолчанию — vinyl. Все настройки vinyl влияют на характеристики хранения.
- Материализованные представления (MV) отсутствуют.
- UDT(User-Defined Type) и вложенные коллекции отсутствуют.
- Система разграничения прав пользователей отсутствует.

## Типы данных {: #data_types }

Sirin поддерживает те же основные типы данных, что и Cassandra. Ниже приведены поддерживаемые
типы (описания адаптированы из официальной документации Cassandra):

- `ascii` — строки US-ASCII.
- `boolean` — логический тип (true/false).
- `blob` — двоичные данные в hex-представлении.
- `double` — 64-битное число с плавающей точкой.
- `float` — 32-битное число с плавающей точкой.
- `int` — 32-битное целое число.
- `bigint` — 64-битное целое число.
- `varchar`, `text` — UTF-8 строки (синонимы).
- `inet` — IP-адрес IPv4 или IPv6.
- `smallint` — 16-битное целое.
- `tinyint` — 8-битное целое.
- `timeuuid` — времезависимый UUID (v1).
- `uuid` — случайный UUID (v4).
- `map<k,v>` — коллекция ключ-значение (без вложенности).
- `set<t>` — множество (без вложенности).
- `list<t>` — список (без вложенности).

## Поддерживаемые возможности CQL {: #supported_cql_features }

### Определение схемы (DDL) {: #schema_definition }

#### CREATE KEYSPACE {: #create_keyspace }

```bnf
<create-keyspace-stmt> ::= CREATE KEYSPACE [IF NOT EXISTS] <ks-name>
                           WITH REPLICATION = <map-literal>
                           [AND DURABLE_WRITES = <boolean>]
```

- В Sirin ключевые слова и параметры поддерживаются синтаксически.
- `replication_strategy` и `replication_factor` игнорируются.
- `durable_writes` игнорируется, запись всегда происходит в журнал фиксации.
- Пространства имён отображаются в пространство имён Picodata

#### CREATE TABLE {: #create_table }

```bnf
<create-table-stmt> ::= CREATE TABLE [IF NOT EXISTS] <table-name>
                        '(' <column-definitions> ',' <primary-key> ')'
                        [WITH <table-options>]
```

- `table-options` игнорируются.

#### ALTER TABLE {: #alter_table }

На данный момент не поддерживается

#### DROP TABLE {: #drop_table }

```bnf
<drop-table-stmt> ::= DROP TABLE [<keyspace-name> '.'] <table-name>
                     [IF EXISTS]

<keyspace-name> ::= <identifier>

<table-name> ::= <identifier>
```

### Операции с данными (DML) {: #data_operations }

#### INSERT {: #insert }

Оператор INSERT используется для вставки новой строки или обновления (upsert) существующей
записи по полному первичному ключу. Обязательными являются значения для всех компонентов
первичного ключа; остальные столбцы при отсутствии значения получают null.

**Ограничения Sirin:**

- Клауза `USING TIMESTAMP` не поддерживается
- Counter-столбцы не поддерживаются

**Синтаксис:**

```bnf
<insert-stmt> ::= INSERT INTO [<keyspace-name> '.'] <table-name> '(' <column-names> ')'
                  VALUES '(' <values> ')' [USING TTL <int>] [USING TIMESTAMP <int>] [IF NOT EXISTS]
```

- `keyspace_name` — необязательный префикс, если выбрано ключевой пространство по умолчанию оператором `USE`;
- `column_list` — перечень вставляемых столбцов, включая все компоненты первичного ключа;
- `value_list` — соответствующие значения, порядок должен совпадать с column_list;
- `IF NOT EXISTS` — опционально, предотвращает вставку, если строка уже существует. (см раздел [LWT](#lwt))

Пример запроса:
```sql
INSERT INTO users (id, name, age)
  VALUES (uuid(), 'Alice', 30)
  USING TTL 86400
  IF NOT EXISTS;
```

#### DELETE {: #delete }

Оператор DELETE удаляет одну или несколько строк, либо значения отдельных столбцов из строки.
Удаление всегда задаётся относительно первичного ключа (полностью или частично).

**Ограничения sirin**:

- `USING TIMESTAMP` не поддерживается.
- Сравнение кортежей в WHERE (например, `(a, b) > (1, 2)`) не поддерживается.

**Синтаксис:**
```bnf
<delete-stmt> ::= DELETE [<column-names>] FROM [<keyspace-name> '.'] <table-name>
                  WHERE <where-clause> [USING TIMESTAMP <int>]
```

- `column-names` — список удаляемых столбцов (опционально). Если указан, из строки
  удаляются только эти столбцы. Если не указан — удаляется вся строка.
- `WHERE` — задаёт, какие строки удалить; должен включать полный partition key.
- `IF` — опциональные условия (lightweight transaction, CAS), позволяющие удалять
  только при выполнении условия. (см раздел [LWT](#lwt))

**WHERE:**

- Необходимо указать хотя бы полный **partirion key**.
- Допускается уточнять кластеризующие столбцы.
- Можно использовать операторы =, <, >, <=, >=, !=.
  IN, CONTAINS, CONTAINS KEY на данный момент не поддерживаются.
- Не поддерживается: сравнение кортежей в sirin.

**Примеры:**

Удаление строки по ключу:
```
DELETE FROM users WHERE id = 123;
```

Удаление отдельного столбца:
```
DELETE age FROM users WHERE id = 123;
```

#### SELECT {: #select }

Оператор `SELECT` используется для получения данных из таблиц.

```bnf
<select-stmt> ::= SELECT [DISTINCT] <select-list>
                 FROM [<keyspace-name> '.'] <table-name>
                 [WHERE <condition-list>]
                 [USING <using-clause>]
                 [LIMIT <number>]
                 [ALLOW FILTERING]

<select-list> ::= '*' | <column-list>

<column-list> ::= <column-name> [',' <column-list>]

<condition-list> ::= <condition> [AND <condition-list>]

<condition> ::= <column-name> <comparison-op> <value>
             | <column-name> IN '(' <value-list> ')'

<comparison-op> ::= '=' | '<' | '>' | '<=' | '>=' | '!='

<value-list> ::= <value> [',' <value-list>]

<using-clause> ::= <consistency-level>
                | PAGINATION
                | TIMESTAMP <timestamp>

<consistency-level> ::= CONSISTENCY <level>

<level> ::= ONE | QUORUM | ALL | LOCAL_ONE | LOCAL_QUORUM | EACH_QUORUM | TWO | THREE

<timestamp> ::= <integer>

<allow-filtering> ::= ALLOW FILTERING
```

**Ограничения:**

- `USING` не поддерживается.
- `GROUP BY` не поддерживается.
- Сравнение кортежей, например `(column1, column2) > (1, 2)`, не поддерживается.
- Альясы(оператор `AS`) не поддерживаются.

**select_clause**

`select_clause` определяет, какие столбцы запрашиваются и возвращаются в результирующем наборе.
Также могут применяться примитивные преобразования перед возвратом.

```bnf
select_clause ::= selector [ AS identifier ]
                 (',' selector [ AS identifier ])*
```

Здесь selector может быть:

- именем столбца таблицы;
- термином (term);
- кастингом: `CAST(selector AS cql_type)`, **на данный момент не поддерживается**;
- вызовом функции: `function_name(selector, ...)`, **на данный момент не поддерживается**;
- специальной формой `COUNT(*)`, **на данный момент не поддерживается**.

**Aliases (псевдонимы)**

Каждый верхнеуровневый селектор может быть переименован с помощью AS. В таком случае имя столбца
в результате будет задано псевдонимом, а не исходным именем.

**WHERE clause**

Оператор WHERE задаёт фильтрацию строк:
```bnf
where_clause ::= relation ( AND relation )*
relation ::= column_name operator term
```
Поддерживаемые операторы: =, <, >, <=, >=, !=, IN, CONTAINS, CONTAINS KEY.

Однако:

Сравнение кортежей (например, (column1, column2) > (1, 2)) не поддерживается в sirin.

**ORDER BY**

Оператор ORDER BY определяет порядок возвращаемых строк:
```bnf
ordering_clause ::= column_name [ASC | DESC] (',' column_name [ASC | DESC])*
```

Порядок ограничен теми столбцами кластеризации, которые определены в таблице.

**LIMIT и PER PARTITION LIMIT**

- `LIMIT` ограничивает общее число возвращаемых строк.
- `PER PARTITION LIMIT` — максимальное число строк в каждом разделе.

**ALLOW FILTERING**

По умолчанию sirin, как и cassandra, не допускает выполнения полного сканирования
всех разделов. Чтобы принудительно разрешить запросам сканирование всех данных, используется
`ALLOW FILTERING`.

Пример допустимого запроса с учётом ограничений:
```sql
SELECT id, name
FROM users
WHERE age >= 18
ORDER BY name ASC
LIMIT 100
ALLOW FILTERING;
```

#### LWT (Lightweight transactions) {: #lwt }

Sirin частично поддерживает механизм LWT. Так как запись производится в конкретный набор реплик, а сами
реплики объединены синхронной репликацией, то проверка условия происходит на узле, выполняющем запрос.

На данный момент поддерживается конструкция `IF NOT EXISTS` для запроса типа `INSERT`. При указании
условия, на запрос будет возвращён ответ вида
```
[applied] = false
```
где `false` будет возвращено, если условие ложно и данные не были вставлены. Соответственно `true`
будет возвращено в случае применения(вставки) данных.

Пример:
```sql
INSERT INTO tbl (key, value) VALUES ('key1', 0e568df0-7eb1-11f0-1cef-2593d20a0950) IF NOT EXISTS;
```

Вернёт:
```
 [applied]
-----------
      True
```

### TTL и механизм экспирации {: #ttl_and_expiration }

Sirin поддерживает TTL для строк и отдельных колонок:
```sql
INSERT INTO users (id, name) VALUES (1, 'Ivan') USING TTL 86400;
```

При выполнении запроса система автоматически исключает записи, срок хранения которых истёк.
Физический процесс удаления таких данных происходит в фоновом режиме: фоновым процесс обходит таблицы
и удаляет данные с истёкшим временем жизни. Таким образом, хотя данные и помечаются на удаление сразу
после истечения срока жизни, их фактическое удаление выполняется асинхронно, не влияя на производительность
основных операций чтения.

### Функции {: #functions }

В дополнение к базовым операторам CQL, модуль Sirin поддерживает стандартные скалярные функции Cassandra.
Это позволяет использовать привычный синтаксис и возможности для обработки данных на стороне запроса.

#### Категории поддерживаемых функций {: #supported_functions }

**Арифметические функции:**

- `abs()` — абсолютное значение
- `ceil()`, `floor()` — округление вверх/вниз
- `exp()`, `ln()`, `log()`, `log10()`
- `pow(base, exponent)`
- `round()`
- `sqrt()`

**Функции работы со строками:**

- `blobAsText()`, `textAsBlob()`
- `uuidAsText()`
- `toTimestamp()`, `toDate()`
- `minTimeuuid()`, `maxTimeuuid()`
- `substr()`
- `upper()`, `lower()`

**Функции работы со строками:**

- `blobAsText()`, `textAsBlob()`
- `uuidAsText()`
- `toTimestamp()`, `toDate()`
- `minTimeuuid()`, `maxTimeuuid()`
- `substr()`
- `upper()`, `lower()`

**Функции работы с коллекциями:**

- `size()` — количество элементов в коллекции (map, set, list).
- `element_at(collection, index)` — элемент по индексу (для list).

#### Примеры использования {: #functions_examples }

```sql
-- Пример 1. Арифметические функции
SELECT abs(-15), sqrt(16), pow(2, 8) FROM ks1.tbl1 LIMIT 1;

-- Пример 2. Функции со строками
SELECT upper(name), substr(description, 1, 10) FROM ks1.tbl2;

-- Пример 3. Работа с UUID и временем
SELECT now(), currentTimestamp(), dateOf(now()) FROM ks1.tbl3;

-- Пример 4. Работа с коллекциями
SELECT size(tags), element_at(tags, 0) FROM ks1.tbl4;
```

#### Ограничения {: #functions_limitations }

- Агрегатные функции (`count`, `sum`, `avg`, `min`, `max`) не поддерживаются.
- Пользовательские функции (UDF) и пользовательские агрегаты (UDA) не поддерживаются.
- Триггеры не поддерживаются.

## Примеры использования {: #usage_examples }

### Создание таблицы {: #create_table_example }

```sql
CREATE TABLE users (
    id uuid PRIMARY KEY,
    name text,
    age int,
    country text
);
```

### Добавление данных {: #insert_data_example }

```sql
INSERT INTO users (id, name, age, country) VALUES (2f0cff20-967f-4dfa-a8a1-6140b5fd9255, 'Ivan', 32, 'Russia');
```

### Выборка данных с LIMIT {: #select_with_limit_example }

```sql
SELECT id, name FROM users LIMIT 10;
```

### TTL {: #ttl_example }

```sql
INSERT INTO sessions (id, user_id) VALUES (uuid(), 42) USING TTL 600;
```

## Протоколы и драйверы {: #protocols_and_drivers }

- Поддерживается протокол Cassandra Native Protocol v4.
- Совместимость с драйверами Apache Cassandra для популярных языков:
  - Java — [DataStax Java Driver](https://github.com/datastax/java-driver)
  - Python — [Python Cassandra Driver](https://github.com/datastax/python-driver)
  - Go — [gocql](https://github.com/gocql/gocql)
  - Node.js — [cassandra-driver for Node.js](https://github.com/datastax/nodejs-driver)
  - Rust - [ScyllaDB Rust Driver](https://github.com/scylladb/scylla-rust-driver)

## Развёртывание, эксплуатация и восстановление  {: #deployment_operations_recovery }

Все процедуры полностью соответствуют инфраструктуре Picodata.
См. разделы документации Picodata:
- [Установка плагинов](https://docs.picodata.io/picodata/stable/overview/glossary/#plugin)
- [Развёртывание кластера](https://docs.picodata.io/picodata/stable/#run_deploy)
- [Мониторинг и телеметрия](https://docs.picodata.io/picodata/stable/admin/monitoring/)
- [Резервное копирование и восстановление](https://docs.picodata.io/picodata/stable/admin/backup/)

### Конфигурация плагина {: #plugin_configuration }

```yaml
router:
  addr: 0.0.0.0:9042        # Адрес, на котором будет доступен протокол cassandra
  max_clients: 100          # Максимальное количество конкурентных соединений на узел
  peers_cleaner:
    interval_secs: 15       # Интервал в секундах, по истечении которого из таблиц
                            # peers и peers_v2 удаляются неисправные узлы.

storage:
  ttl:
    timeout: 10             # Частота срабатывания в секундах фоновой задачи, которая
                            # физически удаляет записи с истёкшим сроком жизни
    batch_size: 100         # Размер пакета на одну операцию удаления записей
```
