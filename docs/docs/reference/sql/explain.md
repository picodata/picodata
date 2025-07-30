# EXPLAIN

Команда `EXPLAIN` добавляется перед [DQL](dql.md)- и
[DML](dml.md)-запросами для того, чтобы показать, как будет выглядеть
план исполнения запроса, при этом не выполняя сам запрос. План строится
на узле, к которому подключился пользователь, и позволяет наглядно
оценить структуру и последовательность действий при выполнении запроса.
`EXPLAIN` является инструментом для анализа и оптимизации запросов.

## Синтаксис {: #syntax }

![Explain](../../images/ebnf/explain.svg)

## Структура плана запроса {: #plan_structure }

План запроса выглядит как граф, исполнение которого происходит снизу
вверх.

Для DQL-запросов основными элементами являются:

- `scan` — сканирование (получение данных) таблицы
- `projection` — уточненный набор колонок таблицы для выполнения запроса

??? example "Тестовые таблицы"
    Примеры использования команд включают в себя запросы к [тестовым
    таблицам](../legend.md).

Дополнительно, запрос `EXPLAIN` показывает следующие элементы:

- текущие значения `SQL_VDBE_OPCODE_MAX` и `SQL_MOTION_ROW_MAX`, служащие для
  [ограничения запросов](non_block.md#query_limitations)
- расчетный диапазон [бакетов](../../overview/glossary.md#bucket), на
  которых будет выполнен запрос

Пример получения столбца таблицы целиком:

```sql
EXPLAIN SELECT item FROM warehouse;
```

Результат:

```sql
projection ("warehouse"."item"::string -> "item")
    scan "warehouse"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]
```

В одном запросе может быть несколько узлов `scan` и `projection`, в
зависимости от количества обращений к таблицам, наличия условий,
подзапросов и т.п. В общем случае, каждому ключевому слову `SELECT`
соответствует своя проекция, а каждому `FROM` — свое сканирование.

## Запрос с условием {: #query_with_selection }

Построение проекции (`projection`) всегда
происходит после сканирования. В рамках построения проекции планировщик
создает псевдоним для столбца: `"ORDERS"."AMOUNT" -> "AMOUNT"`.

Если в запросе есть условие (`where`), то в план добавляется узел
`selection`:

```sql
EXPLAIN SELECT amount FROM orders WHERE amount > 1000;
```

Вывод в консоль:

```
projection ("ORDERS"."AMOUNT"::int -> "AMOUNT")
    selection ROW("ORDERS"."AMOUNT"::int) > ROW(1000::int)
        scan "ORDERS"
```

Если `projection` выбирает столбцы (атрибуты таблицы), то `selection`
фильтрует данные по строкам (`ROW`).

Фраза `selection ROW("ORDERS"."AMOUNT"::int) > ROW(1000::int)` является результатом
трансформации фильтра `WHERE "AMOUNT" > 1000` в `WHERE ("AMOUNT") > (1000)`,
т.е. превращения значения в строку из одного столбца.

## Запрос с несколькими проекциями {: #multi_projection_explain }

Пример построения проекции из более сложного запроса:

```sql
EXPLAIN SELECT id,name FROM items
EXCEPT
SELECT id,item FROM orders
WHERE amount > 1000;
```

Вывод в консоль:

```
except
    projection ("ITEMS"."ID"::int -> "ID", "ITEMS"."NAME"::string -> "NAME")
        scan "ITEMS"
    projection ("ORDERS"."ID"::int -> "ID", "ORDERS"."ITEM"::string -> "ITEM")
        selection ROW("ORDERS"."AMOUNT"::int) > ROW(1000::int)
            scan "ORDERS"
```

В таком плане запроса присутствует два блока `projection`, перед
которыми стоит логическое условие (`EXCEPT`). В каждом блоке есть свое
сканирование таблицы и, опционально, дополнительный фильтр по строкам
(`selection`).

## Варианты перемещения данных {: #data_motion_types }

В плане запроса может быть указан параметр `motion`, который отражает
вариант перемещения данных между узлами хранения. Существуют следующие
четыре варианта:

1. **Локальная вставка**. Представляет собой локальную [материализацию
   данных](../../overview/glossary.md#data_materialization) с подсчетом
   значений `bucket_id` для каждого кортежа (соответственно, кортежи
   будут сгруппированы по этим бакетам). Перемещения данных на другие
   узлы хранения через узел-маршрутизатор не происходит. На текущем узле
   хранения будет локально создана виртуальная таблица из результатов
   читающего запроса или из переданных `VALUES`, а потом данные из нее
   будут вставлены локально в целевую таблицу. Планировщик отобразит
   значение `motion [policy: local segment]`.
1. **Локальная материализация**. Данный вариант аналогичен предыдущему с
   той разницей, что при материализации данных не происходит вычисление
   `bucket_id`. При таком запросе планировщик отобразит значение `motion
   [policy: local]`.
1. **Частичное перемещение**. При выполнении запроса на каждый узел
   кластера будет отправлена только востребованная часть данных (таблица
   перераспределяется по новому ключу). При таком запросе планировщик
   отобразит значение `motion [policy: segment]`.
1. **Полное перемещение**. На каждый узел кластера будет отправлена вся
   таблица. Планировщик отобразит значение `motion [policy: full]`.

Перемещение данных происходит в тех случаях, когда в запросе требуется
обработать данные из нескольких таблиц или несколько раз из одной
таблицы (`JOIN`, `EXCEPT`, подзапросы), а также при выполнении
[агрегатных функций](aggregate.md) (`SUM`, `COUNT`...). Перемещение
данных происходит по следующей схеме:

- на узле-маршрутизаторе (`router`) собираются запрошенные данные со
  всех узлов хранения (`storage`);
- в случае частичного перемещения (`motion [policy: segment]`),
  собранные данные объединяются в виртуальную таблицу с новым ключом
  шардирования;
- узел-маршрутизатор отправляет на узлы хранения только нужные им строки
  из этой виртуальной таблицы.

Таким образом, перемещение обеспечивает корректность выполнения
локальных запросов за счет копирования недостающих данных на каждый узел
хранения в кластере.

Вариант перемещения данных (`motion policy`) зависит от того, какие
данные доступны на локальных узлах хранения. При простом чтении из одной
таблицы перемещения нет никогда. При работе с несколькими таблицами
перемещения также может не быть, если в каждой части запроса адресуются
те столбцы, по которым таблица распределена (указан ключ шардирования).
При этом, использование агрегатных функций и/или соединения при работе с
одной или несколькими таблицами может потребовать частичного или полного
перемещения данных.

Примеры разных вариантов `motion policy` приведены ниже.

### Локальная вставка {: #local_segment_motion }

Для примера **локальной вставки** покажем `INSERT` со вставкой из читающего запроса другой таблицы, у
которой отличается ключ шардирования:

```sql
EXPLAIN INSERT INTO orders (id, item, amount) SELECT * FROM items WHERE id = 5;
```

Вывод в консоль:

```
insert "ORDERS" on conflict: fail
    motion [policy: local segment([ref("ID")])]
        projection ("ITEMS"."ID"::int -> "ID", "ITEMS"."NAME"::string -> "NAME", "ITEMS"."STOCK"::int -> "STOCK")
            selection ROW("ITEMS"."ID"::int) = ROW(5::int)
                scan "ITEMS"
```

### Локальная материализация {: #local_motion }

**Локальная материализация** относится к тем случаям, когда требуется
положить в память прочитанные данные из локального запроса для их
дальнейшей обработки. Перемещения данных нет и вычисление `bucket_id` не
требуется (см.
[подробнее](../../architecture/distributed_sql.md#data_distribution)).
Примером может служить удаление данных из таблицы:

```sql
EXPLAIN DELETE FROM warehouse WHERE id = 1;
```

Вывод в консоль:

```
delete "WAREHOUSE"
    motion [policy: local]
        projection ("WAREHOUSE"."ID"::int -> pk_col_0)
            selection ROW("WAREHOUSE"."ID"::int) = ROW(1::int)
                scan "WAREHOUSE"
```

Локальная материализация происходит и при обновлении данных в тех
случаях, если не затрагивается колонка, по которой таблица шардирована.
Например, если при создании таблицы было указано шардирование по колонке
`ID` (`distributed by (id)`), то обновление данных в других колонках
не приведет к их перемещению через узел-маршрутизатор. Поскольку при
`UPDATE` не происходит пересчет `bucket_id`, то планировщик использует
политику `local`:

```sql
EXPLAIN UPDATE warehouse SET type = 'N/A';
```

Вывод в консоль:

```
update "WAREHOUSE"
"TYPE" = COL_0
    motion [policy: local]
        projection ('N/A'::string -> COL_0, "WAREHOUSE"."ID"::int -> COL_1)
            scan "WAREHOUSE"
```

### Частичное перемещение {: #segment_motion }

**Частичное перемещение** происходит, когда требуется отправить на узлы
хранения недостающую часть таблицы.

Пример `INSERT` с передачей строки значений:

```sql
EXPLAIN INSERT INTO warehouse VALUES (1, 'bricks', 'heavy');
```

Вывод в консоль:

```
insert "WAREHOUSE" on conflict: fail
    motion [policy: segment([ref("COLUMN_1")])]
        values
            value row (data=ROW(1::int, 'bricks'::string, 'heavy'::string))
```

Пример `JOIN` двух таблиц с разными ключами шардирования:

```sql
EXPLAIN SELECT id,item FROM orders
JOIN
(SELECT nmbr,product FROM deliveries) AS new_table
ON orders.id=new_table.nmbr;
```

Вывод в консоль:

```
projection ("ORDERS"."ID"::int -> "ID", "ORDERS"."ITEM"::string -> "ITEM")
    join on ROW("ORDERS"."ID"::int) = ROW("NEW_TABLE"."NMBR"::int)
        scan "ORDERS"
            projection ("ORDERS"."ID"::int -> "ID", "ORDERS"."ITEM"::string -> "ITEM", "ORDERS"."AMOUNT"::int -> "AMOUNT", "ORDERS"."SINCE"::datetime -> "SINCE")
                scan "ORDERS"
        motion [policy: segment([ref("NMBR")])]
            scan "NEW_TABLE"
                projection ("DELIVERIES"."NMBR"::int -> "NMBR", "DELIVERIES"."PRODUCT"::string -> "PRODUCT")
                    scan "DELIVERIES"
```

Пример `UPDATE` с обновлением колонки, по которой шардирована таблица
(например, `distributed by (product)`):

```sql
EXPLAIN UPDATE deliveries SET product = 'metals',quantity = 4000 WHERE nmbr = 1;
```

Вывод в консоль:

```
update "DELIVERIES"
"NMBR" = COL_0
"PRODUCT" = COL_1
"QUANTITY" = COL_2
    motion [policy: segment([])]
        projection ("DELIVERIES"."NMBR"::int -> COL_0, 'metals'::string -> COL_1, 4000::int -> COL_2, "DELIVERIES"."PRODUCT"::string -> COL_3)
            selection ROW("DELIVERIES"."NMBR"::int) = ROW(1::int)
                scan "DELIVERIES"
```

### Полное перемещение {: #full_motion }

**Полное перемещение** происходит, когда требуется скопировать всю
внутреннюю таблицу (в правой части запроса) на все узлы, содержащие
внешнюю таблицу (в левой части).

Пример `JOIN` с соединениям не по колонкам шардирования для обеих
таблиц:

```sql
EXPLAIN SELECT NAME FROM items
JOIN
(SELECT item FROM orders) AS new_table
ON items.name = new_table.item;
```

Вывод в консоль:

```
projection ("ITEMS"."NAME"::string -> "NAME")
    join on ROW("ITEMS"."NAME"::string) = ROW("NEW_TABLE"."ITEM"::string)
        scan "ITEMS"
            projection ("ITEMS"."ID"::int -> "ID", "ITEMS"."NAME"::string -> "NAME", "ITEMS"."STOCK"::int -> "STOCK")
                scan "ITEMS"
        motion [policy: full]
            scan "NEW_TABLE"
                projection ("ORDERS"."ITEM"::string -> "ITEM")
                    scan "ORDERS"
```

Пример выполнения агрегатной функции.

```sql
EXPLAIN SELECT COUNT(id) FROM warehouse;
```

Вывод в консоль:

```
projection (sum(("0e660ad12ab24037a48f169fcf315549_count_11"::int))::decimal -> "COL_1")
    motion [policy: full]
        scan
            projection (count(("WAREHOUSE"."ID"::int))::int -> "0e660ad12ab24037a48f169fcf315549_count_11")
                scan "WAREHOUSE"
```

## Варианты использования бакетов {: #bucket_ranges }

Планировщик SQL-запросов в Picodata показывает, на каких
[бакетах](../../overview/glossary.md#bucket) будет исполнен запрос. В
зависимости от характера запроса, это значение будет выглядеть как:

- подмножество (`[1410,1934,1958]`)
- номер бакета (`[215]`)
- любой бакет (`any`)
- неизвестный набор бакетов (`unknown`)

### Подмножество {: #range }

Подмножество бакетов может быть точно вычислено для запросов без
перемещения данных и вызванного им добавления в план узлов `motion`. При
использовании фильтра с конкретными значениями это будет список
соответствующих им бакетов:

```sql
EXPLAIN SELECT * FROM warehouse WHERE id IN (1,2,3);
```

Вывод в консоль:

```sql
projection ("warehouse"."id"::int -> "id", "warehouse"."item"::string -> "item", "warehouse"."type"::string -> "type")
    selection ROW("warehouse"."id"::int) in ROW(1::int, 2::int, 3::int)
        scan "warehouse"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1410,1934,1958]
```

Частный случай подмножества — ситуация, когда запрос будет выполнен на
всех бакетах:

```sql
EXPLAIN UPDATE warehouse SET type = 'N/A';
```

Вывод в консоль:

```sql
update "warehouse"
"type" = "col_0"
    motion [policy: local]
        projection ('N/A'::string -> "col_0", "warehouse"."id"::int -> "col_1")
            scan "warehouse"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]
```

### Номер бакета {: #boundary }

В ряде случаев планировщик может определить конкретный номер бакета, на
котором будет выполнен распределенный запрос. Такой вариант характерен
для локальных DML-запросов. В примере ниже номер бакета выведен из
условия (`id = 5`):

```sql
EXPLAIN INSERT INTO orders (id, item, amount) SELECT * FROM items WHERE id = 5;
```

Вывод в консоль:

```sql
insert "orders" on conflict: fail
    motion [policy: local segment([ref("id")])]
        projection ("items"."id"::int -> "id", "items"."name"::string -> "name", "items"."stock"::int -> "stock")
            selection ROW("items"."id"::int) = ROW(5::int)
                scan "items"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [219]
```

### Любой бакет {: #any }

Значение `any` выводится для запросов к глобальным таблицам, которые
присутствуют на всех шардах. Запрос будет выполнен на том же узле, к
которому подключен клиент Picodata. Пример:

```sql
EXPLAIN SELECT id FROM _pico_table;
```

Вывод в консоль:

```sql
projection ("_pico_table"."id"::int -> "id")
    scan "_pico_table"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = any
```

### Неизвестный набор {: #unknown }

Значение `unknown` означает, что ни точный диапазон, ни конкретный номер
бакета не могут быть посчитаны. Такая ситуация возможна, к примеру, при
нелокальных DML-запросах, когда запрос затрагивает некоторое неизвестное
количество шардов:

```sql
EXPLAIN UPDATE deliveries SET product = 'metals',quantity = 4000 WHERE nmbr = 1;
```

Вывод в консоль:

```sql
update "deliveries"
"nmbr" = "col_0"
"product" = "col_1"
"quantity" = "col_2"
    motion [policy: segment([])]
        projection ("deliveries"."nmbr"::int -> "col_0", 'metals'::string -> "col_1", 4000::int -> "col_2", "deliveries"."product"::string -> "col_3")
            selection ROW("deliveries"."nmbr"::int) = ROW(1::int)
                scan "deliveries"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = unknown
```
