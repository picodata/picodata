# DROP PROCEDURE

[DDL](ddl.md)-команда `DROP PROCEDURE` используется для удаления
существующей [процедуры](../../overview/glossary.md#stored_procedure).

## Синтаксис {: #syntax }

![DROP PROCEDURE](../../images/ebnf/drop_procedure.svg)

### Тип {: #type }

??? note "Диаграмма"
    ![Type](../../images/ebnf/type.svg)

## Параметры {: #params }

* **PROCEDURE** — имя процедуры. Соответствует правилам имен для всех
  [объектов](object.md) в кластере. Опционально после имени процедуры
  можно указать список ее параметров (для совместимости со стандартом)
* **IF EXISTS** — позволяет избежать ошибки в случае, если такой
  процедуры в кластере нет
* **WAIT APPLIED** — при использовании этого параметра контроль
  пользователю будет возвращен только после того как данная операция
  будет применена либо во всем кластере (`GLOBALLY`), либо в рамках
  текущего инстанса (`LOCALLY`)

## Примеры {: #examples }

```sql
DROP PROCEDURE proc
OPTION ( timeout = 4 );
```

```sql
DROP PROCEDURE proc
WAIT APPLIED GLOBALLY
OPTION ( timeout = 4 );
```
