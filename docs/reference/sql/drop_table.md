# DROP TABLE

[DDL](ddl.md)-команда `DROP TABLE` используется для удаления
существующей таблицы.

## Синтаксис {: #syntax }

![DROP TABLE](../../images/ebnf/drop_table.svg)

## Параметры {: #params }

* **TABLE** — название таблицы. Соответствует правилам имен для всех
  [объектов](object.md) в кластере.

## Примеры {: #examples }

```sql
DROP TABLE characters OPTION (TIMEOUT = 3.0);
```

