# ALTER PROCEDURE

[DDL](ddl.md)-команда `ALTER PROCEDURE` используется для изменения
существующей [процедуры](../../overview/glossary.md#stored_procedure).

## Синтаксис {: #syntax }

![ALTER PROCEDURE](../../images/ebnf/alter_procedure.svg)

### Тип {: #unrestricted_type }

??? note "Диаграмма"
    ![Type](../../images/ebnf/unrestricted_type.svg)

## Параметры {: #params }

* **PROCEDURE** — имя процедуры. Соответствует правилам имен для всех
  [объектов](object.md) в кластере. Опционально после имени процедуры
  можно указать список ее параметров (для совместимости со стандартом).

* **RENAME TO** — команда переименования процедуры.

## Общие опции и параметры {: #common_options }

Для данной команды доступны следующие общие опции и параметры SQL:

* [TIMEOUT](common_options.md#timeout)
* [WAIT APPLIED](common_options.md#wait_applied)

## Примеры {: #examples }

```sql
ALTER PROCEDURE old RENAME TO new OPTION ( timeout = 4 );
```
