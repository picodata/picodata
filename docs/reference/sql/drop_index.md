# DROP INDEX

[DDL](ddl.md)-команда `DROP INDEX` используется для удаления индекса.

## Синтаксис {: #syntax }

![DROP INDEX](../../images/ebnf/drop_index.svg)

## Параметры {: #params }

* **INDEX** — имя индекса; соответствует правилам имен для всех
[объектов](object.md) в кластере

* **WAIT APPLIED** — при использовании этого параметра контроль
  пользователю будет возвращен только после того как данная операция
  будет применена либо во всем кластере (`GLOBALLY`), либо в рамках
  текущего инстанса (`LOCALLY`)

## Примеры {: #examples }

```sql
DROP INDEX name_year
OPTION (TIMEOUT = 3.0);
```

```sql
DROP INDEX name_year
WAIT APPLIED GLOBALLY
OPTION (TIMEOUT = 3.0);
```

