# SELECT

[DQL](dql.md)-команда `SELECT` используется для получения, фильтрации и
преобразования кортежей из таблиц в запросе. Если никакие данные не
удовлетворяют условиям фильтрации, то в ответе вернется только описание
метаданных колонок без самих кортежей. В контексте распределенной
системы, команда `SELECT` в Picodata получает информацию из всех частей
таблицы, которая может храниться на нескольких узлах кластера.

!!! note "Примечание"
    Кортежи в выводе идут в том порядке, в каком их
    отдают узлы Picodata. Гарантии порядка не предоставляются.

## Синтаксис {: #syntax }

![Select](../../images/ebnf/select.svg)

### Проекция {: #projection }

<details><summary>Диаграмма</summary><p>
![Expression](../../images/ebnf/projection.svg)
</p></details>

### Выражение {: #expression }

<details><summary>Диаграмма</summary><p>
![Expression](../../images/ebnf/expression.svg)
</p></details>

### Литерал {: #literal }

<details><summary>Диаграмма</summary><p>
![Literal](../../images/ebnf/literal.svg)
</p></details>

## Параметры {: #params }

* **Псевдоним** — позволяет переопределить названия получаемых столбцов
в запросах. Псевдоним вставляется после опционального слова `AS` и может
содержать произвольный текст со следующими ограничениями: он не должен
начинаться с цифры и не может содержать пробелов и специальных служебных
символов (кроме знака подчеркивания).

* **DISTINCT** — возвращаются только уникальные значения кортежей.

* **INNER JOIN** — к колонкам каждого кортежа из внутренней (правой)
  части запроса присоединяются только колонки тех кортежей внешней
  (левой) части, которые удовлетворяют условию соединения `ON`. Если во
  внешней части не нашлось подходящего кортежа, то внутренний кортеж не
  попадает в результат.

* **LEFT OUTER JOIN** — к колонкам каждого кортежа из внешней (левой)
  части запроса присоединяются только колонки тех кортежей внутренней
  (правой) части, которые удовлетворяют условию соединения `ON`. Если во
  внутренней части не нашлось подходящего кортежа, то вместо значений
  его колонок будет подставлен `NULL`.

* **WHERE** — условие фильтрации при сканировании таблицы или
  подзапроса.

* **GROUP BY** — [группировка](aggregate.md) данных по списку колонок
  или выражений.

* **HAVING** — фильтрация уже сгруппированных данных.

* **ORDER BY** — сортировка результата запроса по одной или нескольким
  колонкам. Используется для упорядочивания получаемого набора данных.
  Колонки могут быть указаны как по их именам, так и по порядковым
  номерам. Если не указан тип упорядочивания (`ASC` — по возрастанию,
  `DESC` — по убыванию), то по умолчанию подразумевается возрастание
  значений.

* **UNION** — объединение результатов с одинаковым набором колонок
  из нескольких DQL-запросов. При этом результат не содержит
  дубликаты строк.

* **UNION ALL** — объединение результатов с одинаковым набором колонок
  из нескольких DQL-запросов. При этом результат может содержать
  дубликаты строк.

* **EXCEPT DISTINCT** — исключение результатов с одинаковым набором
  колонок одного запроса из другого. При этом результат не содержит
  дубликаты строк.

См. также:

- [Использование JOIN](join.md)

## Примеры  {: #examples }

??? example "Тестовые таблицы"
    Примеры использования команд включают в себя запросы к [тестовым
    таблицам](../legend.md).

### Получение данных из таблицы с фильтрацией {: #select_with_filter }

```sql
SELECT name from items WHERE stock > 1000;
```

### Получение данных без повторов {: #select_distinct }

```sql
SELECT DISTINCT type FROM warehouse;
```

### Внутреннее соединение {: #inner_join }

```sql
SELECT
    c.item,
    c.type,
    a.stock
FROM warehouse AS c
JOIN items AS a
ON c.id = a.id
```

### Внешнее левое соединение {: #left_outer_join }

```sql
SELECT
    c.item,
    c.type,
    a.stock
FROM warehouse AS c
LEFT JOIN items AS a
ON TRUE
```

### Множественные соединения {: #multiple_joins }

```sql
SELECT
    warehouse.item,
    items.stock,
    orders.amount
FROM warehouse
INNER JOIN items
ON warehouse.item = items.name
LEFT OUTER JOIN orders
ON items.name = orders.item
```

### Агрегация {: #aggregation }

```sql
SELECT COUNT(*) FROM warehouse WHERE type = 'heavy';
```

### Группировка с предварительной фильтрацией {: #filter_and_group }

```sql
SELECT type, COUNT(*) FROM warehouse
WHERE id < 5
GROUP BY type;
```

<!--
### Группировка с последующей фильтрацией по сгруппированным данным {: #group_and_filter }

```sql
SELECT type, COUNT(*) as c FROM warehouse
GROUP BY type
HAVING c > 3;

sbroad: column with name "C" not found
```
-->

### Упорядочивание результата по убыванию значений в третьей колонке {: #order_desc }

```sql
SELECT * FROM items
ORDER BY 3 DESC
```

### Разнонаправленное упорядочивание результата по двум явно именованным колонкам {: #order_asc_and_desc }

```sql
SELECT * FROM items
ORDER BY name ASC, stock DESC
```

### Объединение только уникальных строк из результатов двух запросов {: #union_distinct }

```sql
SELECT item FROM warehouse
UNION
SELECT item FROM orders
```

### Объединение только уникальных строк из результатов многих запросов {: #union_distinct_chain }

```sql
SELECT item FROM warehouse
UNION
SELECT item FROM orders
UNION
SELECT name FROM items
```

### Полное объединение результатов с использованием подзапроса {: #union_all_subquery }

```sql
SELECT item FROM warehouse WHERE type = 'heavy'
UNION ALL
SELECT * FROM (
    SELECT name FROM items
    UNION ALL
    SELECT item FROM orders WHERE amount > 400
)
```

### Последовательное исключение результатов одного запроса из другого {: #except }

```sql
SELECT item FROM orders
EXCEPT
SELECT item FROM warehouse
```

### Последовательное исключение результатов с использованием подзапроса {: #except_with_subquery }

```sql
SELECT item FROM warehouse
EXCEPT
SELECT * FROM (
  SELECT item
  FROM orders
  EXCEPT
    SELECT NAME
    FROM items
    WHERE stock > 200
)
```
