# COALESCE

Функция `COALESCE` возвращает первый из переданных аргументов *expression*,
который не равен `NULL`. Функция вернет `NULL`, только если все ее
аргументы равны `NULL`. Функции должно быть передано минимум два
аргумента.

Преимущества:

* исключение ошибок и проверок, связанных с обработкой `NULL`
* оптимизация и упрощение запросов
* возможность создания псевдонимов для `NULL` (`nil`) — аргументы должны
  быть приведены к одному типу данных с помощью функции [`CAST`]

[`CAST`]: cast.md

## Синтаксис {: #syntax }

![COALESCE](../../images/ebnf/coalesce.svg)

### Выражение {: #expression }

??? note "Диаграмма"
    ![Expression](../../images/ebnf/expression.svg)

### Литерал {: #literal }

??? note "Диаграмма"
    ![Literal](../../images/ebnf/literal.svg)

## Примеры {: #examples }

??? example "Тестовые таблицы"
    Примеры использования команд включают в себя запросы к [тестовым
    таблицам](../legend.md).


```sql title="Функция COALESCE возвращает первый непустой результат"
sql> SELECT COALESCE(NULL::STRING, 'First', NULL::STRING, 'Second');
+-------+
| col_1 |
+=======+
| First |
+-------+
(1 rows)
```

```sql title="Функция COALESCE возвращает единственный непустой результат"
sql> SELECT COALESCE(NULL::STRING, NULL::STRING, 'Check!');
+--------+
| col_1  |
+========+
| Check! |
+--------+
(1 rows)
```

```sql title="Функция COALESCE возвращает <code>NULL</code>"
sql> SELECT COALESCE(NULL, NULL, NULL, NULL);
+-------+
| col_1 |
+=======+
| nil   |
+-------+
(1 rows)
```

```sql title="Внешнее левое соединение — см. <a href='../join/#join_examples'>Использование JOIN</a>"
sql> SELECT
    items.name,
    COALESCE(items.stock::STRING, 'Not specified') AS "items.stock",
    COALESCE(orders.amount::STRING, 'Not specified') AS "orders.amount"
FROM items
LEFT JOIN orders
ON items.name = orders.item;
+---------+---------------+---------------+
| name    | items.stock   | orders.amount |
+=========================================+
| bricks  | 1123          | Not specified |
|---------+---------------+---------------|
| panels  | 998           | Not specified |
|---------+---------------+---------------|
| piles   | 177           | Not specified |
|---------+---------------+---------------|
| bars    | 90211         | 100           |
|---------+---------------+---------------|
| blocks  | 16            | 20000         |
+---------+---------------+---------------+
(5 rows)
```
