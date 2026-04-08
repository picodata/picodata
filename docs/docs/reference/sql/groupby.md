# Группировка {: #groupby }

Оператор `GROUP BY` объединяет [кортежи] с одинаковыми значениями в группы.
Затем к каждой группе можно применять [агрегатные функции], например
`COUNT()` или `SUM()`.

[агрегатные функции]: aggregate.md
[кортежи]: ../../overview/glossary.md#tuple

## Сценарии использования {: #use_cases }

Используйте `GROUP BY`, когда нужно:

- посчитать количество кортежей по категориям
- найти сумму/среднее/максимум по группам
- получить статистику по определённым полям

См. также:

- [SELECT](select.md)

## Примеры запросов {: #examples }

Создадим таблицу и добавим в неё данные, которые подходят для группировки:

```sql
CREATE TABLE customers (
    id INTEGER NOT NULL,
    name TEXT NOT NULL,
    amount INTEGER,
    PRIMARY KEY (id))
USING memtx DISTRIBUTED BY (id)
OPTION (TIMEOUT = 3.0);
```

```sql
INSERT INTO customers VALUES
    (1, 'alice', 778),
    (2, 'bob', 22),
    (3, 'john', 62),
    (4, 'bob', 990),
    (5, 'alice', 24),
    (6, 'bob', 22);
```

### Использование без функций {: #simple_groupby }

Без агрегатных функций `GROUP BY` возвращает уникальные сочетания
значений. `ORDER BY` в примере ниже нужен только для фиксированного
порядка кортежей:

```sql
SELECT name, amount FROM customers
GROUP BY amount, name
ORDER BY amount, name;
```

Результат:

```shell
+-------+--------+
| name  | amount |
+================+
| bob   | 22     |
|-------+--------|
| alice | 24     |
|-------+--------|
| john  | 62     |
|-------+--------|
| alice | 778    |
|-------+--------|
| bob   | 990    |
+-------+--------+
(5 rows)
```

`GROUP BY` не заменяет `ORDER BY`, но меняет состав результата: два
кортежа `('bob', 22)` превращаются в одну группу. Если нужен
предсказуемый порядок вывода, используйте `ORDER BY`.

В отличие от `GROUP BY`, [агрегатная оконная
функция](window.md#aggregate) не сводит результат и не меняет
в нём количество строк. Если нужно вычислить агрегат по разделу,
но сохранить исходные кортежи, используйте выражение `OVER (...)`.

Без `ORDER BY` порядок кортежей в результате запроса в Picodata не
гарантируется.

### Группировка и фильтрация исходных строк {: #groupby_and_where }

Сначала `WHERE` отбирает исходные кортежи, затем `GROUP BY` объединяет
оставшиеся кортежи в группы:

```sql
SELECT name, COUNT(*) AS count FROM customers
WHERE id != 3
GROUP BY name
ORDER BY name;
```

Запрос вернёт число кортежей для каждого имени:

```shell
+-------+-------+
| name  | count |
+===============+
| alice | 2     |
|-------+-------|
| bob   | 3     |
+-------+-------+
(2 rows)
```

### Группировка и фильтрация итоговых групп {: #groupby_and_having }

Для фильтрации уже сформированных групп используйте `HAVING`. В отличие
от `WHERE`, он применяется не к исходным кортежам, а к группам:

```sql
SELECT name, SUM(amount) AS total FROM customers
GROUP BY name
HAVING SUM(amount) > 100
ORDER BY name;
```

Результат:

```shell
+-------+-------+
| name  | total |
+===============+
| alice | 802   |
|-------+-------|
| bob   | 1034  |
+-------+-------+
(2 rows)
```

Правило простое: каждое неагрегированное выражение из `SELECT` должно
присутствовать в `GROUP BY`. Для агрегатов (`COUNT`, `SUM` и т.д.) это
не требуется.

## Порядок обработки {: #processing_order }

Логический порядок обработки запроса с `WHERE`, `GROUP BY`, `HAVING` и
`ORDER BY` такой:

1. Фильтрация исходных кортежей (`WHERE`)
1. Группировка кортежей (`GROUP BY`)
1. Фильтрация получившихся групп (`HAVING`)
1. Сортировка итогового результата (`ORDER BY`)

??? example "План исполнения запроса"
    ```sql
    EXPLAIN(RAW,FMT) SELECT name,amount FROM customers WHERE id !=3 GROUP BY amount,name HAVING sum(amount) > 100 ORDER BY name;
    1. Query (STORAGE):
    SELECT
    "customers"."amount" as "gr_expr_1",
    "customers"."name" as "gr_expr_2",
    sum (CAST ("customers"."amount" as int)) as "sum_1"
    FROM
    "customers"
    WHERE
    "customers"."id" <> CAST(3 AS int)
    GROUP BY
    "customers"."amount",
    "customers"."name"
    +----------+-------+------+-------------------------------------+
    | selectid | order | from | detail                              |
    +===============================================================+
    | 0        | 0     | 0    | SCAN TABLE customers (~983040 rows) |
    |----------+-------+------+-------------------------------------|
    | 0        | 0     | 0    | USE TEMP B-TREE FOR GROUP BY        |
    +----------+-------+------+-------------------------------------+

    2. Query (ROUTER):
    SELECT
    "name",
    "amount"
    FROM
    (
        SELECT
        "COL_1" as "name",
        "COL_0" as "amount"
        FROM
        (
            SELECT
            "COL_0",
            "COL_1",
            "COL_2"
            FROM
            "TMP_9999946914108385841_0136"
        )
        GROUP BY
        "COL_0",
        "COL_1"
        HAVING
        sum ("COL_2") > CAST(100 AS int)
    )
    ORDER BY
    "name"
    +----------+-------+------+---------------------------------------------------------+
    | selectid | order | from | detail                                                  |
    +===================================================================================+
    | 0        | 0     | 0    | SCAN TABLE TMP_9999946914108385841_0136 (~1048576 rows) |
    |----------+-------+------+---------------------------------------------------------|
    | 0        | 0     | 0    | USE TEMP B-TREE FOR GROUP BY                            |
    |----------+-------+------+---------------------------------------------------------|
    | 0        | 0     | 0    | USE TEMP B-TREE FOR ORDER BY                            |
    +----------+-------+------+---------------------------------------------------------+
    ```

См. также:

- [Просмотр низкоуровневого плана запроса](explain.md#raw_query)

## Перемещение данных {: #groupby_motions }

В распределённом запросе `GROUP BY` может потребоваться перемещение
данных: всё зависит от того, находятся ли нужные кортежи в одном или
нескольких [бакетах].

[бакетах]: ../../overview/glossary.md#bucket

Если планировщик доказывает, что запрос выполняется в пределах одного
бакета, перемещения данных не будет. Так бывает, когда `WHERE`
фиксирует ключ шардирования:

```sql
EXPLAIN SELECT id, SUM(amount) FROM customers WHERE id = 1 GROUP BY id;
```

??? example "Существенная часть плана"
    ```sql
    projection ("customers"."id"::int -> "id", sum("customers"."amount") -> "col_1")
        group by ("customers"."id"::int)
            selection "customers"."id"::int = 1::int
                scan "customers"
    buckets = [1750]
    ```

См. также:

- [Отсутствие перемещения](explain.md#no_motion).

Важно не то, какая колонка указана в `GROUP BY`, а сколько бакетов
затрагивает запрос после фильтрации.

Если после фильтрации нужны данные из нескольких бакетов, используется
один из вариантов [перемещения данных]:

- локальная вставка/материализация (данные в пределах одного узла)
- частичное/полное перемещение (данные расположены на разных узлах)

[перемещения данных]: explain.md/#data_motion_types

Один из таких вариантов — полное перемещение данных. Оно часто
появляется, когда группировка идёт по колонке, не входящей в ключ
распределения, а запрос не ограничен одним бакетом. В примере ниже
фильтр `id <> 3` оставляет диапазон `buckets = [1-16384]`, поэтому в
плане появляется `motion [policy: full]`:

```sql
EXPLAIN SELECT name, COUNT(*) AS count FROM customers
WHERE id <> 3
GROUP BY name;
```

??? example "Существенная часть плана"
    ```sql
    projection ("gr_expr_1"::string -> "name", sum("count_1") -> "count")
        group by ("gr_expr_1"::string)
            motion [policy: full, program: ReshardIfNeeded]
                projection ("customers"."name"::string -> "gr_expr_1", count(*) -> "count_1")
                    group by ("customers"."name"::string)
                        selection "customers"."id"::int <> 3::int
                            scan "customers"
    buckets = [1-16384]
    ```

См. также:

- [Перемещение данных](explain.md/#data_motion_types)
