# Фасет LOGICAL

Структура логического плана запроса представляет собой дерево узлов плана.
Исполнение запроса происходит от листовых до родительских узлов. На листовом
уровне обычно находятся узлы сканирования, возвращающие начальные наборы строк
из таблиц.

Ниже представлены примеры вывода фасета `LOGICAL` и добавлены пояснения.

??? example "Подготовка тестового окружения"
    Примеры использования команд включают в себя запросы к [тестовым
    таблицам](../../legend.md).

## Сканирующий запрос {: #query-with-scan }

```sql
EXPLAIN (LOGICAL) SELECT item FROM warehouse;
```

```sql
projection (warehouse.item::string -> item)
  scan warehouse
```

В одном запросе может быть несколько узлов `scan` и `projection`, в
зависимости от количества обращений к таблицам, наличия условий,
подзапросов и т.п. В общем случае, каждому ключевому слову `SELECT`
соответствует своя проекция, а каждому `FROM` — свое сканирование.

## Запрос с условием {: #query_with_selection }

Построение проекции `projection` всегда
происходит после сканирования. В рамках построения проекции планировщик
создает псевдоним для столбца: `orders.amount" -> "amount"`.

Если в запросе есть условие `where`, то в план добавляется узел
`selection`:

```sql
EXPLAIN (LOGICAL) SELECT amount FROM orders WHERE amount > 1000;
```

```sql
projection (orders.amount::int -> amount)
  selection (orders.amount::int > 1000::int)
    scan orders
```

## Запрос с несколькими проекциями {: #multi_projection_explain }

Пример построения проекции из более сложного запроса:

```sql
EXPLAIN (LOGICAL)
SELECT id,name FROM items
EXCEPT
SELECT id,item FROM orders
WHERE amount > 1000;
```

```sql
except
  projection (items.id::int -> id, items.name::string -> name)
    scan items
  projection (orders.id::int -> id, orders.item::string -> item)
    selection (orders.amount::int > 1000::int)
      scan orders
```

В таком плане запроса присутствует два блока `projection`, перед
которыми стоит логическое условие `EXCEPT`. В каждом блоке есть свое
сканирование таблицы и, опционально, дополнительный фильтр по строкам
`selection`.

## Запросы с перемещением данных {: #data_motion_types }

В плане запроса может быть указан параметр `motion`, который отражает вариант
перемещения данных между узлами хранения. Существуют следующие четыре варианта:

1. **Локальная вставка**. Представляет собой локальную [материализацию
   данных](../../../overview/glossary.md#data_materialization) с подсчетом
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
   отобразит значение `motion [policy: segment(col)]`.
1. **Полное перемещение**. На каждый узел кластера будет отправлена вся
   таблица. Планировщик отобразит значение `motion [policy: full]`.

Если `motion` узле отсутствует в плане, это тоже значимая информация:
планировщик доказал, что соответствующее поддерево может быть выполнено
локально, без обмена между узлами. Это возможно, например, если это поддерево
затрагивает один бакет или работает только с глобальными таблицами.

Перемещение происходит тогда, когда узлу необходимо обратиться к данным,
которыми владеют другие узлы. Например, при выполнении [агрегатных
функций](../aggregate.md) (`SUM`, `COUNT`...). Перемещение данных происходит по
следующей схеме:

1. На узле-координаторе `router` собираются запрошенные данные со всех узлов
   хранения `storage`.
2. В случае частичного перемещения `motion [policy: segment]` собранные данные
   объединяются в виртуальную таблицу с новым ключом шардирования.
3. Узел-координатор отправляет на узлы хранения только нужные им строки из этой
   виртуальной таблицы.

Таким образом, перемещение обеспечивает корректность выполнения
локальных запросов за счет копирования недостающих данных на каждый узел
хранения в кластере.

Вариант перемещения данных `motion policy` зависит от того, какие данные
доступны на локальных узлах хранения. При простом чтении из одной таблицы
перемещения нет никогда. При работе с несколькими таблицами перемещения также
может не быть, если в каждой части запроса адресуются те столбцы, по которым
таблица распределена (указан ключ шардирования). При этом, использование
агрегатных функций и/или соединения при работе с одной или несколькими таблицами
может потребовать частичного или полного перемещения данных.

Если фильтр определяет значения всех колонок ключа шардирования, планировщик
может вычислить один бакет и удалить глобальные reduce-стадии для `DISTINCT`,
`GROUP BY`, агрегатов, `HAVING`, `ORDER BY ... LIMIT`, подзапросов и `WITH` в
соответствующем поддереве плана. Если нужная часть запроса затрагивает несколько
бакетов или это невозможно доказать на этапе планирования, в план добавляются
узлы `motion`.

Примеры разных вариантов `motion policy` приведены ниже.

### Отсутствие перемещения {: #no_motion }

Иногда отсутствие `motion` узла говорит больше, чем его наличие. Это означает,
что соответствующее поддерево плана может быть исполнено без обмена данными
между узлами. Для шардированных таблиц из этого следует, что фильтр определяет
значения всех колонок ключа шардирования, и планировщик вычислил один конкретный
бакет.

В простых запросах такое поддерево может совпадать со всем запросом
целиком, но в общем случае речь идет именно об отдельной части плана.

Пример одноузловой агрегации:

```sql
EXPLAIN (LOGICAL) SELECT sum(id), count(*) FROM warehouse WHERE id = 1;
```

```sql
projection (sum(warehouse.id::int::int)::decimal -> col_1, count(*)::int -> col_2)
  selection (warehouse.id::int = 1::int)
    scan warehouse
```

По тому же правилу `motion` может отсутствовать и в запросах с
`GROUP BY`, `DISTINCT`, `HAVING`, `ORDER BY ... LIMIT`, подзапросами и
`WITH`, если соответствующее поддерево гарантированно находится в одном
бакете.

Для `WITH` это означает, что одно и то же одноузловое `CTE` может
использоваться в нескольких местах плана без отдельной материализации
узлом `motion`; если глобальная стадия понадобится позже, `motion`
появится выше по плану.

### Локальная вставка {: #local_segment_motion }

Для примера **локальной вставки** покажем `INSERT` со вставкой из читающего запроса другой таблицы, у
которой отличается ключ шардирования:

```sql
EXPLAIN (LOGICAL) INSERT INTO orders (id, item, amount) SELECT * FROM items WHERE id = 5;
```

```sql
insert into orders on conflict: fail
  motion [policy: local segment([ref(id)]), program: ReshardIfNeeded]
    projection (items.id::int -> id, items.name::string -> name, items.stock::int -> stock)
      selection (items.id::int = 5::int)
        scan items
```

### Локальная материализация {: #local_motion }

**Локальная материализация** относится к тем случаям, когда требуется положить в
память прочитанные данные из локального запроса для их дальнейшей обработки.
Перемещения данных нет и вычисление `bucket_id` не требуется (см.
[подробнее](../../../architecture/distributed_sql.md#data_distribution)). Примером
может служить удаление данных из таблицы:

```sql
EXPLAIN (LOGICAL) DELETE FROM warehouse WHERE id = 1;
```

```sql
delete from warehouse
  motion [policy: local, program: [PrimaryKey(0), ReshardIfNeeded]]
    projection (warehouse.id::int -> pk_col_0)
      selection (warehouse.id::int = 1::int)
        scan warehouse
```

Локальная материализация происходит и при обновлении данных в тех
случаях, если не затрагивается колонка, по которой таблица шардирована.
Например, если при создании таблицы было указано шардирование по колонке
`ID` (`distributed by (id)`), то обновление данных в других колонках
не приведет к их перемещению через узел-маршрутизатор. Поскольку при
`UPDATE` не происходит пересчет `bucket_id`, то планировщик использует
политику `local`:

```sql
EXPLAIN (LOGICAL) UPDATE warehouse SET type = 'N/A';
```

```sql
update warehouse (type = col_0)
  motion [policy: local, program: ReshardIfNeeded]
    projection ('N/A'::string -> col_0, warehouse.id::int -> col_1)
      scan warehouse
```

### Частичное перемещение {: #segment_motion }

**Частичное перемещение** происходит, когда требуется отправить на узлы хранения
недостающую часть таблицы.

Пример `INSERT` с передачей строки значений:

```sql
EXPLAIN (LOGICAL) INSERT INTO warehouse VALUES (1, 'bricks', 'heavy');
```

```sql
insert into warehouse on conflict: fail
  motion [policy: segment([ref("COLUMN_1")]), program: ReshardIfNeeded]
    values
      value ROW(1::int, 'bricks'::string, 'heavy'::string)
```

Пример `JOIN` двух таблиц с разными ключами шардирования:

```sql
EXPLAIN (LOGICAL)
SELECT id,item FROM orders
JOIN
(SELECT nmbr,product FROM deliveries) AS new_table
ON orders.id=new_table.nmbr;
```

```sql
projection (orders.id::int -> id, orders.item::string -> item)
  join on (orders.id::int = new_table.nmbr::int)
    scan orders
    motion [policy: segment([ref(nmbr)]), program: ReshardIfNeeded]
      scan new_table
        projection (deliveries.nmbr::int -> nmbr, deliveries.product::string -> product)
          scan deliveries
```

Пример `UPDATE` с обновлением колонки, по которой шардирована таблица
(например, `distributed by (product)`):

```sql
EXPLAIN (LOGICAL, FMT) UPDATE deliveries SET product = 'metals', quantity = 4000 WHERE nmbr = 1;
```

```sql
update deliveries (
  bucket_id = col_1,
  quantity = col_3,
  nmbr = col_0,
  product = col_2
)
  motion [policy: segment([]), program: [PrimaryKey(0), RearrangeForShardedUpdate(2)]]
    projection (
      deliveries.nmbr::int -> col_0,
      deliveries.bucket_id::int -> col_1,
      'metals'::string -> col_2,
      4000::int -> col_3,
      deliveries.product::string -> col_4
    )
      selection (deliveries.nmbr::int = 1::int)
        scan deliveries
```

### Полное перемещение {: #full_motion }

**Полное перемещение** происходит, когда требуется скопировать всю внутреннюю
таблицу (в правой части запроса) на все узлы, содержащие внешнюю таблицу (в
левой части).

Пример `JOIN` с соединениям не по колонкам шардирования для обеих
таблиц:

```sql
EXPLAIN (LOGICAL)
SELECT NAME FROM items
JOIN
(SELECT item FROM orders) AS new_table
ON items.name = new_table.item;
```

```sql
projection (items.name::string -> name)
  join on (items.name::string = new_table.item::string)
    scan items
    motion [policy: full, program: ReshardIfNeeded]
      scan new_table
        projection (orders.item::string -> item)
          scan orders
```

Пример выполнения агрегатной функции.

```sql
EXPLAIN (LOGICAL) SELECT COUNT(id) FROM warehouse;
```

```sql
projection (sum(count_1::int)::int -> col_1)
  motion [policy: full, program: ReshardIfNeeded]
    projection (count(warehouse.id::int::int)::int -> count_1)
      scan warehouse
```

## Обработка материализованных данных {: #motion_programs }

После материализации данные помещаются в виртуальные таблицы, которые в
дальнейшем преобразуются в соответствии с программой. Программа представляет
собой последовательность инструкций, которые определяют, как преобразовывать
виртуальные таблицы во время выполнения запроса.

Каждая инструкция представляет собой конкретное действие, применяемое к
виртуальной таблице:

### PrimaryKey {: #primary_key }

Устанавливает уникальный первичный ключ материализованной таблицы.

Обновляет поле primary key виртуальной таблицы указанными позициями столбцов.

### ReshardIfNeeded {: #reshard_if_needed }

Вычисляет ключ распределения для каждой строки в виртуальной таблице, если
политика перемещения данных — Segment или LocalSegment. Используется, когда
данные нужно перераспределить по разным сегментам.

### RearrangeForShardedUpdate {: #rearrange_for_sharded_update }

Перевычисляет ключи распределения для виртуальной таблицы для дальнейшей
операции обновления данных.

### AddMissingRowsForLeftJoin {: #add_missing_rows_for_left_join }

Добавляет отсутствующие строки для сохранения семантики LEFT JOIN

Гарантирует, что все строки с левой стороны LEFT JOIN сохраняются, даже если у
них нет соответствующих строк с правой стороны после перемещения данных через
узлы Motion.

### SerializeAsEmptyTable {: #serialize_as_empty }

При установки в true гарантирует, что всё поддерево под этим узлом Motion будет
сериализовано в SQL, который возвращает пустой результат. В настоящий момент
используется только для запросов с UNION ALL над глобальной и шардированной
таблицей. При исполнении таких запросов false выставляется только для одного
репликасета.

### RemoveDuplicates {: #remove_duplicates }

Удаляет дублирующиеся строки из виртуальной таблицы

Гарантирует уникальность строк в виртуальной таблице. Обычно используется, когда
дубликаты могут появиться во время перемещения данных или при подготовке данных
для операций, требующих уникального ввода.
