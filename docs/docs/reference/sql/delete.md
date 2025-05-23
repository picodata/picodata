# DELETE

[DML](dml.md)-команда `DELETE` используется для удаления данных из таблицы.

## Синтаксис {: #syntax }

![Delete](../../images/ebnf/delete.svg)

### Выражение {: #expression }

??? note "Диаграмма"
    ![Expression](../../images/ebnf/expression.svg)

### Литерал {: #literal }

??? note "Диаграмма"
    ![Literal](../../images/ebnf/literal.svg)

## Параметры {: #params }

* **TABLE** — имя таблицы. Соответствует правилам имен для всех [объектов](object.md)
  в кластере.

## Ограничения {: #restrictions }

При выполнении запроса на удаление нескольких строк может возникнуть
ошибка [неблокирующего SQL](non_block.md). Если виртуальная таблица на
узле, на который был отправлен запрос пользователем, не может вместить
промежуточный результат (не хватает емкости виртуальной таблицы
`sql_motion_row_max`) и/или задействовано слишком много операций с
кортежами на одном из узлов хранения (`sql_vdbe_opcode_max`), то запрос
будет отработан лишь частично. Следует увеличить значения указанных
параметров в опциях запроса и повторить его.

## Примеры {: #examples }

??? example "Тестовые таблицы"
    Примеры использования команд включают в себя запросы к [тестовым
    таблицам](../legend.md).

Простой запрос удаляет все данные из указанной таблицы:

```sql
DELETE FROM warehouse OPTION (
    SQL_MOTION_ROW_MAX = 100,
    SQL_VDBE_OPCODE_MAX = 15000
);
```

Запрос с условием позволяет удалить только нужный кортеж:

```sql
DELETE FROM warehouse where id = 1;
```

Или несколько строк:

```sql
DELETE FROM warehouse WHERE id IN (1,2,3);
```

Во всех случаях в выводе в консоль будет указано количество удаленных кортежей.
