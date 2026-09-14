# EXPLAIN

Для анализа производительности запросов в Picodata можно использовать команду
`EXPLAIN`, с помощью которой можно узнать, как будет выглядеть план исполнения
запроса. План позволяет наглядно оценить структуру и последовательность действий
при выполнении запроса.

В реализацию `EXPLAIN` заложены следующие принципы:

- [Фасеты (режимы отображения)](#facets)
- [Достоверность плана](#plan-faithfulness)
- [Возможность применения к проблемным запросам](#explain-for-err-queries)
- [Возможности форматирования](#explain-fmt)
- [Подробный вывод](#explain-verbose)

## Синтаксис {: #syntax }

![Explain](../../images/ebnf/explain.svg)

## Фасеты (режимы отображения) {: #facets }

![Facet](../../images/ebnf/facet.svg)

Команда `EXPLAIN` позволяет указывать определенные режимы вывода `EXPLAIN`,
также называемые фасетами. В настоящий момент можно указывать фасеты `RAW`,
`LOGICAL`, `BUCKETS`, `FORWARD` и `CONTEXT` в любом порядке. Каждый фасет
отвечает за конкретную информацию о запросе.

При указании нескольких фасетов одновременно также печатаются их заголовки,
позволяющие быстро ориентироваться в выводе. Например:

```sql
EXPLAIN (LOGICAL, BUCKETS) SELECT name, id FROM _pico_table;
```

```
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────

projection (_pico_table.name::string -> name, _pico_table.id::int -> id)
  scan _pico_table

──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────

buckets = any
```

Если для `EXPLAIN` не указано ни одного фасета, то фасетами по умолчанию являются
`LOGICAL` и `BUCKETS`. Таким образом, запросы

```sql
EXPLAIN (LOGICAL, BUCKETS) SELECT name, id FROM _pico_table;
```

и

```sql
EXPLAIN SELECT name, id FROM _pico_table;
```

эквивалентны.

С подробным описанием каждого фасета `EXPLAIN` можно ознакомиться тут:

- [RAW](./explain_facets/raw.md) — низкоуровневый план исполнения запроса.
- [LOGICAL](./explain_facets/logical.md) — высокоуровневый план исполнения
  запроса.
- [BUCKETS](./explain_facets/buckets.md) — информация о бакетах, участвующих в
  исполнении запроса.
- [FORWARD](./explain_facets/forward.md) — анализ сетевых пересылок, необходимых
  для исполнения запроса.
- [CONTEXT](./explain_facets/context.md) — прочие переменные среды, влияющие на
  исполнение запроса.


## Достоверность плана {: #plan-faithfulness }

Команда `EXPLAIN` в Picodata показывает полный план запроса — со всеми операциями,
которые реально выполняются, даже если они не следуют напрямую из текста
запроса. Например, `COUNT(*)` в Picodata исполняется путем частичного подсчёта
на узлах хранения и финального суммирования результатов. Это следует из вывода
фасета `LOGICAL`:

```sql
EXPLAIN (LOGICAL) SELECT COUNT(*) FROM t;
```

```
projection (sum(count_1::int)::int -> col_1)
  motion [policy: full, program: ReshardIfNeeded]
    projection (count(*)::int -> count_1)
      scan t
```

Узел `motion` отражает перераспределение данных между узлами кластера. Подробнее
об этом можно почитать [здесь](./explain_facets/logical.md#data_motion_types).

## Возможность применения к проблемным запросам {: #explain-for-err-queries }

Важной особенностью `EXPLAIN` в Picodata является возможность получить `EXPLAIN` для
запросов, которые приводят к ошибке при исполнении. Так, Picodata всегда
старается построить `EXPLAIN` для синтаксически корректных запросов, даже если они
семантически некорректные. Например:

```sql
SELECT name, id FROM _pico_table WHERE MAX(id) = 5;
```

```sql
ERROR:  Query 1 from EXPLAIN (RAW): Failed to compile SQL statement: misuse of aggregate function MAX()
```

Тем не менее, Picodata выполнит `EXPLAIN` от такого запроса:

```sql
EXPLAIN (RAW) SELECT name, id FROM _pico_table WHERE MAX(id) = 5;
```

```sql
╭───────────────────╮
│ 1. Query (ROUTER) │
╰───────────────────╯

SELECT "_pico_table"."name", "_pico_table"."id" FROM "_pico_table" WHERE max (CAST ("_pico_table"."id" as int)) = CAST(5 AS int)

plan:
Failed to compile SQL statement: misuse of aggregate function MAX()
```

На данный момент у возможности построить `EXPLAIN` для проблемных запросов есть
ограничения:

- Нельзя получить `EXPLAIN` для запроса с транзакционным блоком, если он затрагивает более одного бакета.

## Возможности форматирования {: #explain-fmt }

В Picodata для `EXPLAIN` также существует опция `FMT`, которая применяется к
фасетам. В случае `LOGICAL` форматирование применяется к плану. В случае `RAW`
форматирование применяется к локальным SQL-запросам и их планам. В случае
`BUCKETS` список бакетов разбивается на строки, но только вместе с опцией
[`VERBOSE`](#explain-verbose). Ниже приведены примеры.

Пример с `LOGICAL`:

```sql
EXPLAIN (LOGICAL) SELECT id, item FROM warehouse;
```

```sql
projection (warehouse.id::int -> id, warehouse.item::string -> item)
  scan warehouse
```

При указании опции `FMT` вывод форматируется:

```sql
EXPLAIN (LOGICAL, FMT) SELECT id, item FROM warehouse;
```

```sql
projection (
  warehouse.id::int -> id,
  warehouse.item::string -> item
)
  scan warehouse
```

Пример с `RAW`:

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

При указании опции `FMT` вывод будет отформатирован:

```sql
EXPLAIN (RAW, FMT) SELECT * FROM warehouse;
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
```

Пример с `BUCKETS` (опция [`VERBOSE`](#explain-verbose) выводит список бакетов
целиком):

```sql
EXPLAIN (BUCKETS, VERBOSE) SELECT * FROM warehouse
WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
             13, 14, 15, 16);
```

```sql
buckets = [219, 626, 653, 799, 1403, 1410, 1418, 1860, 1934, 1948, 1958, 2564, 2640, 2752, 2802, 2852]
```

При указании опции `FMT` список бакетов будет отформатирован:

```sql
EXPLAIN (BUCKETS, VERBOSE, FMT) SELECT * FROM warehouse
WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
             13, 14, 15, 16);
```

```sql
buckets = [
  219, 626, 653, 799, 1403, 1410, 1418, 1860,
  1934, 1948, 1958, 2564, 2640, 2752, 2802,
  2852
]
```

Списки, которые умещаются в одну строку, не разбиваются.

Опцию `FMT` полезно использовать при анализе вывода `EXPLAIN` для сложных запросов.
В будущем могут быть добавлены как новые фасеты, так и новые способы
форматирования их элементов.

## Подробный вывод {: #explain-verbose }

Для `EXPLAIN` также существует опция `VERBOSE`, которая управляет полнотой
вывода фасетов. В настоящий момент она применяется только к фасету `BUCKETS`.
По умолчанию этот фасет выводит лишь начало списка бакетов — столько элементов,
сколько умещается в одну строку, — и указывает, сколько бакетов осталось за
ними. Опция `VERBOSE` выводит список целиком. Ниже приведен пример.

Пример с `BUCKETS`:

```sql
EXPLAIN (BUCKETS) SELECT * FROM warehouse
WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
             13, 14, 15, 16);
```

```sql
buckets = [219, 626, 653, 799, 1403, ... (11 more)]
```

При указании опции `VERBOSE` список бакетов выводится целиком:

```sql
EXPLAIN (BUCKETS, VERBOSE) SELECT * FROM warehouse
WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
             13, 14, 15, 16);
```

```sql
buckets = [219, 626, 653, 799, 1403, 1410, 1418, 1860, 1934, 1948, 1958, 2564, 2640, 2752, 2802, 2852]
```

Опцию `VERBOSE` полезно использовать, когда нужно узнать точный набор бакетов,
на которых исполняется запрос. Вместе с опцией [`FMT`](#explain-fmt) полный
список бакетов будет отформатирован. Если опция `VERBOSE` не указана, то `FMT`
не применяется, т.к. список бакетов короткий и не нуждается в форматировании.
