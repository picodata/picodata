# Общие табличные выражения

Предложение `WITH` добавляется перед [DQL](dql.md)-командой
[`SELECT`](select.md) и позволяет использовать в запросе временные
таблицы.

Такие временные таблицы называются *общими табличными выражениями*
(англ. CTE — common table expressions) и существуют только в контексте
выполнения запроса.

Предложение `WITH` агрессивно материализует результаты в памяти и почти
никогда не использует встраивание подзапроса в синтаксическое дерево
команды `SELECT`.

В область видимости общих табличных выражений и команды `SELECT` попадают
результаты выражений, описанных ранее в запросе `WITH`. Например, есть
запрос `WITH` с тремя общими табличными выражениями:

```
WITH
    <cte1>,
    <cte2>,
    <cte3>
SELECT <...>
```

В команде `SELECT` можно использовать результаты выражений `<cte1>`,
`<cte2>`, `<cte3>`. В выражении `<cte3>` — результаты выражений `<cte1>`,
`<cte2>`. В выражении `<cte2>` — результат выражения `<cte1>`. В выражении
`<cte1>` должны использоваться запросы к уже существующим таблицам.

## Синтаксис {: #syntax }

![WITH](../../images/ebnf/with.svg)

## Параметры {: #params }

* **cte** — имя общего табличного выражения. Соответствует правилам
  имен для всех [объектов](object.md) в кластере
* **column** — имя колонки общего табличного выражения. Соответствует
  правилам имен для всех [объектов](object.md) в кластере

## Примеры {: #examples }

??? example "Тестовые таблицы"
    Примеры использования команд включают в себя запросы к [тестовым
    таблицам](../legend.md).

```sql title="Запрос WITH и CTE с предложением WHERE"
WITH replenish (item, amount)
    AS (SELECT item, amount FROM orders WHERE amount <= 1000)
SELECT item, amount FROM replenish;
```

Результат:

```bash
+-------------+--------+
| item        | amount |
+======================+
| "adhesives" | 350    |
|-------------+--------|
| "moldings"  | 900    |
|-------------+--------|
| "bars"      | 100    |
+-------------+--------+
(3 rows)
```

```sql title="Запрос WITH и CTE с внутренним соединением"
WITH leftovers (item, orders_data, deliveries_data) AS (
    SELECT item, amount, deliveries.quantity
    FROM orders
    JOIN deliveries
    ON item = deliveries.product
    )
SELECT item, orders_data - deliveries_data AS amount FROM leftovers;
```

Результат:

```bash
+-------------+--------+
| item        | amount |
+======================+
| "metalware" | 3000   |
|-------------+--------|
| "adhesives" | 50     |
|-------------+--------|
| "moldings"  | 800    |
|-------------+--------|
| "bars"      | 95     |
|-------------+--------|
| "blocks"    | 5000   |
+-------------+--------+
(5 rows)
```

```sql title="Запрос WITH с двумя независимыми CTE"
WITH
    c (total) AS (SELECT count(*) FROM orders),
    m (earliest_date) AS (SELECT min(since) FROM orders)
SELECT c.total, m.earliest_date FROM c LEFT JOIN m ON true;
```

Результат:

```bash
+-------+------------------------+
| total | earliest_date          |
+================================+
| 5     | "2023-11-11T00:00:00Z" |
+-------+------------------------+
(1 rows)
```

```sql title="Запрос WITH, в котором результат CTE <i>ordered_items</i> используется в CTE <i>total_stock</i>"
WITH
    ordered_items (id, name, stock) AS (
        SELECT items.* FROM items
        JOIN orders
        ON items.name = orders.item
    ),
    total_stock (total) AS (
        SELECT sum(stock) FROM ordered_items
    )
SELECT * FROM total_stock;
```

Результат:

```bash
+-------+
| total |
+=======+
| 90227 |
+-------+
(1 rows)
```
