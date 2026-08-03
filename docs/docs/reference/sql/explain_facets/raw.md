# Фасет RAW

В Picodata распределенные SQL-запросы компилируются во множество локальных
SQL-запросов, исполняющихся на узлах. Фасет `RAW` показывает каждый локальный
SQL-запрос, его тип и место исполнения, а также соответствующий ему план
исполнения.

В качестве типа запроса могут быть следующие значения:

- `Let query` — `DQL`-запрос, привязанный к переменной `LET`.
- `Return query` — `DQL`-запрос, который возвращает строки из транзакционного
  блока.
- `If cond` — `DQL`-запрос.
- `If body` — `DML`-запрос, входящий в тело `IF`-блока.
- `Query` — `DML`-запрос из транзакционного блока или `DQL`-запрос вне
  транзакционного блока.

Возможные места исполнения запроса:

- `ROUTER` — запрос исполняется локально на узле.
- `WHOLE STORAGE` — запрос исполняется на каждом репликасете в кластере.
- `CONST-FILTERED STORAGE, N/M` — для запроса удалось вычислить конкретные
  репликасеты, на которых будет исполняться запрос. Также рядом указывается
  количество таких репликасетов (N) и количество всех доступных репликасетов (M)
  в кластере.
- `DYN-FILTERED STORAGE` — репликасеты, на которые нужно отправить запрос,
  определяются во время исполнения запроса. В некоторых случаях также может быть
  указано `<= N/M`, если перед динамической фильтрацией бакетов удалось
  применить статическую фильтрацию бакетов (см. `CONST-FILTERED STORAGE` выше).

План исполнения локального SQL-запроса содержит информацию о том, как будет
исполняться запрос. Например, с помощью него можно узнать, будет ли использован
индекс при сканировании таблицы.


## Примеры {: #raw-examples }

Ниже представлены примеры вывода `EXPLAIN (RAW)` с объяснением.

??? example "Подготовка тестового окружения"
    Примеры использования команд включают в себя запросы к [тестовым
    таблицам](../../legend.md).

### Последовательное сканирование {: #seq-scan }

```sql
EXPLAIN (RAW) SELECT * FROM warehouse;
```

```sql
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯

SELECT "warehouse"."id", "warehouse"."item", "warehouse"."type" FROM "warehouse"

plan:
    [0] SCAN TABLE warehouse (~1048576 rows)
```

Запрос будет разослан на все репликасеты кластера, и на каждом будет произведено
последовательное сканирование таблицы `warehouse`.

### Сканирование индекса {: #index-scan }

```sql
EXPLAIN (RAW) SELECT * FROM warehouse WHERE id = 42;
```

```sql
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/4) │
╰────────────────────────────────────────╯

SELECT "warehouse"."id", "warehouse"."item", "warehouse"."type" FROM "warehouse" WHERE "warehouse"."id" = CAST(42 AS int)

plan:
    [0] SEARCH TABLE warehouse USING PRIMARY KEY (id=?) (~1 row)
```

Из вывода следует, что запрос исполнится на одном из четырех репликасетов. Для
поиска в таблице будет использован индекс первичного ключа.

Фасет `RAW` также отражает информацию об использовании вторичных индексов.
Например:

```sql
EXPLAIN (RAW) SELECT * FROM warehouse WHERE item = 'kek';
```

```sql
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯

SELECT "warehouse"."id", "warehouse"."item", "warehouse"."type" FROM "warehouse" WHERE "warehouse"."item" = CAST('kek' AS string)

plan:
    [0] SEARCH TABLE warehouse USING COVERING INDEX item_idx (item=?) (~10 rows)
```

`USING ... INDEX` в выводе указывает на то, что при исполнении запроса
будет задействован вторичный индекс.

### Использование временной таблицы для агрегации {: #tmp-table-aggr }

Для исполнения части SQL запросов Picodata материализует промежуточные данные во
временную таблицу. Например:

```sql
EXPLAIN (RAW, FMT) SELECT * FROM warehouse ORDER BY 1;
```

```sql
 ╭──────────────────────────╮
 │ 1. Query (WHOLE STORAGE) │
 ╰──────────────────────────╯
 
 SELECT
   "warehouse"."id",
   "warehouse"."item",
   "warehouse"."type"
 FROM
   "warehouse"
 
 plan:
     [0] SCAN TABLE warehouse (~1048576 rows)
 
 ╭───────────────────╮
 │ 2. Query (ROUTER) │
 ╰───────────────────╯
 
 SELECT
   "COL_0" as "id",
   "COL_1" as "item",
   "COL_2" as "type"
 FROM
   (
     SELECT
       "COL_0",
       "COL_1",
       "COL_2"
     FROM
       "_tmp_6176347154012311129_0136"
   )
 ORDER BY
   1
 
 plan:
     [0] SCAN TABLE _tmp_6176347154012311129_0136 (~1048576 rows)
     [0] USE TEMP B-TREE FOR ORDER BY
```

Сначала Picodata выполнит сканирование таблицы `warehouse` на каждом репликасете
в кластере, а далее отсортирует вернувшиеся строки на узле-координаторе,
используя для этого временную таблицу.

### Использование в транзакционных блоках {: #transactions-usage }

```sql
EXPLAIN (RAW)
DO $$ BEGIN
  LET a = (SELECT id FROM foo WHERE id = 42);
  IF a > 5 THEN
    UPDATE foo SET val = 'kek' WHERE id = 42;
  END IF;
END $$;
```

```sql
╭──────────────────────────────────────────╮
│ 1. Let "a" (CONST-FILTERED STORAGE, 1/4) │
╰──────────────────────────────────────────╯

SELECT "foo"."id" FROM "foo" WHERE "foo"."id" = CAST(42 AS int)

plan:
    [0] SEARCH TABLE foo USING PRIMARY KEY (id=?) (~1 row)

╭──────────────────────────────────────────╮
│ 2. If cond (CONST-FILTERED STORAGE, 1/4) │
╰──────────────────────────────────────────╯

SELECT CAST(:a AS int) > CAST(5 AS int) as "cond"

plan:
    [0] TRIVIAL

╭──────────────────────────────────────────╮
│ 3. If body (CONST-FILTERED STORAGE, 1/4) │
╰──────────────────────────────────────────╯

UPDATE "foo" SET "val" = CAST('kek' AS string) WHERE "foo"."id" = CAST(42 AS int)

plan:
    [0] SEARCH TABLE foo USING PRIMARY KEY (id=?) (~1 row)
```

В случае если `LET`-выражение не используется, это отражается в выводе:

```sql
EXPLAIN (RAW)
DO $$ BEGIN
  LET a = (SELECT 1);
  LET a = (SELECT id FROM foo WHERE id = 42);
  RETURN QUERY SELECT a;
END $$;
```

```sql
╭─────────────────────────────────────────────────────╮
│ 1. **Unused** let "a" (CONST-FILTERED STORAGE, 1/4) │
╰─────────────────────────────────────────────────────╯

SELECT CAST(1 AS int) as "col_1"

plan:
    [0] TRIVIAL

╭──────────────────────────────────────────╮
│ 2. Let "a" (CONST-FILTERED STORAGE, 1/4) │
╰──────────────────────────────────────────╯

SELECT "foo"."id" FROM "foo" WHERE "foo"."id" = CAST(42 AS int)

plan:
    [0] SEARCH TABLE foo USING PRIMARY KEY (id=?) (~1 row)

╭───────────────────────────────────────────────╮
│ 3. Return query (CONST-FILTERED STORAGE, 1/4) │
╰───────────────────────────────────────────────╯

SELECT CAST(:a AS int) as "col_1"

plan:
    [0] TRIVIAL
```

Слово **Unused** рядом с именем `LET`-выражения свидетельствует о том, что оно не
используется в транзакционном блоке.
