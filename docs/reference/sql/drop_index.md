# DROP INDEX

[DDL](ddl.md)-команда `DROP INDEX` используется для удаления индекса.

## Синтаксис {: #syntax }

![DROP INDEX](../../images/ebnf/drop_index.svg)

## Параметры {: #params }

* **INDEX** — имя индекса; соответствует правилам имен для всех
[объектов](object.md) в кластере

## Примеры {: #examples }

```sql
DROP INDEX name_year OPTION (TIMEOUT = 3.0);
```
