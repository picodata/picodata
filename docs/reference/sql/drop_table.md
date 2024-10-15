# DROP TABLE

[DDL](ddl.md)-команда `DROP TABLE` используется для удаления
существующей таблицы.

## Синтаксис {: #syntax }

![DROP TABLE](../../images/ebnf/drop_table.svg)

## Параметры {: #params }

* **TABLE** — имя таблицы. Соответствует правилам имен для всех
  [объектов](object.md) в кластере
* **IF EXISTS** — позволяет избежать ошибки в случае, если такой
  таблицы в кластере нет
* **WAIT APPLIED** — при использовании этого параметра контроль
  пользователю будет возвращен только после того как данная операция
  будет применена либо во всем кластере (`GLOBALLY`), либо в рамках
  текущего инстанса (`LOCALLY`)

## Примеры {: #examples }

```sql
DROP TABLE warehouse
OPTION (TIMEOUT = 3.0);
```

```sql
DROP TABLE warehouse
WAIT APPLIED GLOBALLY
OPTION (TIMEOUT = 3.0);
```
