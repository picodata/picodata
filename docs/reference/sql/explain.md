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

### Обязательные элементы {: #essential_elements }

Для DQL-запросов обязательными элементами являются:

- `scan` — сканирование (получение данных) таблицы
- `projection` — уточненный набор колонок таблицы для выполнения запроса

Пример получения столбца таблицы целиком:

```sql
EXPLAIN SELECT "name" FROM "stars"
```

Результат:

```sql
projection ("stars"."name"::string -> "name")
    scan "stars"
```

В одном запросе может быть несколько узлов `scan` и `projection`, в
зависимости от количества обращений к таблицам, наличия условий,
подзапросов и т.п. В общем случае, каждому ключевому слову `SELECT`
соответствует своя проекция, а каждому `FROM` — свое сканирование.

## Запрос с условием {: #query_with_selection }

Построение проекции (`projection`) всегда
происходит после сканирования. В рамках построения проекции планировщик
создает псевдоним для столбца: `"scoring"."score" -> "score"`.

Если в запросе есть условие (`where`), то в план добавляется узел
`selection`:

```sql
EXPLAIN SELECT "score" FROM "scoring" WHERE "score" > 70;
```

Вывод в консоль:

```
---
- - projection ("scoring"."score"::decimal -> "score")
    selection ROW("scoring"."score"::decimal) > ROW(70::unsigned)'
        scan "scoring"'
...
```

Если `projection` выбирает столбцы (атрибуты таблицы), то `selection`
фильтрует данные по строкам (`ROW`).

Фраза `selection ROW("scoring"."score") > ROW(70)'` является результатом
трансформации фильтра `where "score" > 70` в `where ("score") > (70)`,
т.е. превращения значения в строку из одного столбца.

## Запрос с несколькими проекциями {: #multi_projection_explain }

Пример построения проекции из более сложного запроса:

```sql
EXPLAIN SELECT "id","name" FROM "characters"
EXCEPT
SELECT "id","name" FROM "assets"
WHERE "stock" > 1000;
```

Вывод в консоль:

```
---
- - except
    projection ("characters"."id"::integer -> "id", "characters"."name"::string -> "name")'
        scan "characters"'
    projection ("assets"."id"::integer -> "id", "assets"."name"::string -> "name")'
        selection ROW("assets"."stock"::integer) > ROW(1000::unsigned)'
            scan "assets"'
...
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
   [policy:   local]`.
1. **Частичное перемещение**. При выполнении запроса на каждый узел
   кластера будет отправлена только востребованная часть данных (таблица
   перераспределяется по новому ключу). При таком запросе планировщик
   отобразит значение `motion [policy:   segment]`.
1. **Полное перемещение**. На каждый узел кластера будет отправлена вся
   таблица. Планировщик отобразит значение `motion [policy:   full]`.

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

**Локальная вставка** характерна для `INSERT` с передачей строки
значений:

```sql
EXPLAIN INSERT INTO "assets" VALUES (1, 'Woody', 2561);
```

Вывод в консоль:

```
insert "assets" on conflict: fail
    motion [policy: segment([ref("COLUMN_1")])]
        values
            value row (data=ROW(1::unsigned, 'Woody'::string, 2561::unsigned))
```

### Локальная материализация {: #local_motion }

**Локальная материализация** относится к тем случаям, когда требуется
положить в память прочитанные данные из локального запроса для их
дальнейшей обработки. Перемещения данных нет и вычисление `bucket_id` не
требуется (см.
[подробнее](../../architecture/distributed_sql.md#data_distribution)).
Примером может служить удаление данных из таблицы:

```sql
EXPLAIN DELETE FROM "characters" WHERE "id" = 1;
```

Вывод в консоль:

```
delete "characters"
    motion [policy: local]
        projection ("characters"."id"::integer -> pk_col_0)
            selection ROW("characters"."id"::integer) = ROW(1::unsigned)
                scan "characters"
```

Локальная материализация происходит и при обновлении данных в тех
случаях, если не затрагивается колонка, по которой таблица шардирована.
Например, если при создании таблицы было указано шардирование по колонке
`id` (`distributed by ("id")`), то обновление данных в других колонках
не приведет к их перемещению через узел-маршрутизатор. Поскольку при
`UPDATE` не происходит пересчет `bucket_id`, то планировщик использует
политику `local`:

```sql
EXPLAIN UPDATE "characters" SET "year" = 2010;
```

Вывод в консоль:

```
update "characters"
"year" = COL_0
    motion [policy: local]
        projection (2010::unsigned -> COL_0, "characters"."id"::integer -> COL_1)
            scan "characters"
```

### Частичное перемещение {: #segment_motion }

**Частичное перемещение** происходит, когда требуется отправить на узлы
хранения недостающую часть таблицы.

Пример `INSERT` со вставкой из читающего запроса другой таблицы, у
которой отличается ключ шардирования:

```sql
EXPLAIN INSERT INTO "assets" SELECT * FROM "assets3" WHERE "id3" = 1;
```

Вывод в консоль:

```
insert "assets" on conflict: fail'
    motion [policy: segment([ref("id3")])]'
        projection ("assets3"."id3"::integer -> "id3", "assets3"."name3"::string ->
            "name3", "assets3"."stock3"::integer -> "stock3")'
            selection ROW("assets3"."id3"::integer) = ROW(1::unsigned)'
            scan "assets3"'
```

Пример `JOIN` двух таблиц с разными ключами шардирования:

```sql
EXPLAIN SELECT "id","name" FROM "assets"
JOIN
(SELECT "id3","name3" FROM "assets3") AS "new_assets"
ON "assets"."id" = "new_assets"."id3";
```

Вывод в консоль:

```
projection ("assets"."id"::integer -> "id", "assets"."name"::string -> "name")
    join on ROW("assets"."id"::integer) = ROW("new_assets"."id3"::integer)'
        scan "assets"'
            projection ("assets"."id"::integer -> "id", "assets"."name"::string
    -> "name", "assets"."stock"::integer -> "stock")'
                scan "assets"'
        motion [policy: segment([ref("id3")])]'
            scan "new_assets"'
                projection ("assets3"."id3"::integer -> "id3", "assets3"."name3"::string -> "name3")'
                    scan "assets3"'
```

Пример `UPDATE` с обновлением колонки, по которой шардирована таблица
(например, `distributed by ("id", "name")`):

```sql
EXPLAIN UPDATE "characters" SET "name" = 'Etch', "year" = 2010 WHERE "id" = 2;
```

Вывод в консоль:

```
update "characters"
"id" = COL_0'
"name" = COL_1'
"year" = COL_2'
    motion [policy: segment([])]'
        projection ("characters"."id"::integer -> COL_0, ''Etch''::string ->
    COL_1, 2010::unsigned -> COL_2, "characters"."id"::integer -> COL_3, "characters"."name"::string
    -> COL_4)'
            selection ROW("characters"."id"::integer) = ROW(2::unsigned)'
                scan "characters"'
```

### Полное перемещение {: #full_motion }

**Полное перемещение** происходит, когда требуется скопировать всю
внутреннюю таблицу (в правой части запроса) на все узлы, содержащие
внешнюю таблицу (в левой части).

Пример `JOIN` с соединениям не по колонкам шардирования для обеих
таблиц:

```sql
EXPLAIN SELECT "id","name","stock","year" FROM "characters"
JOIN
(SELECT "id" AS "number","stock" FROM "assets") AS stock
ON "characters"."id" = stock."number";
```

Вывод в консоль:

```
projection (
    - "characters"."id" -> "id",
    - "characters"."name" -> "name",
    - "STOCK"."stock" -> "stock",
    - "characters"."year" -> "year")',
      join on ROW("characters"."id") = ROW("STOCK"."number")',
          scan "characters"',
              projection (
    -   "characters"."id" -> "id",
    -   "characters"."name" -> "name",
    -   "characters"."year" -> "year")',
    -   '                scan "characters"',
    -   '        motion [policy: full]',
    -   '            scan "STOCK"',
    -   '                projection (
    -     "assets"."id" -> "number",
    -     "assets"."stock" -> "stock")',
    -     '                    scan "assets"'
```

Пример выполнения агрегатной функции.

```sql
EXPLAIN SELECT COUNT("id") FROM "characters";
```

Вывод в консоль:

```
projection (sum(("8278664dae744882bfeec573f427fd0d_count_11"::integer))::decimal
    -> "COL_1")
    motion [policy: full]'
        scan'
            projection (count(("characters"."id"::integer))::integer -> "8278664dae744882bfeec573f427fd0d_count_11")'
                scan "characters"'
```
