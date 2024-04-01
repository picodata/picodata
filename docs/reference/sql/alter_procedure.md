# ALTER PROCEDURE

[DDL](ddl.md)-команда `ALTER PROCEDURE` используется для изменения
существующей [процедуры](../../overview/glossary.md#stored_procedure).

## Синтаксис {: #syntax }

![ALTER PROCEDURE](../../images/ebnf/alter_procedure.svg)

### Тип {: #type }

<details><summary>Диаграмма</summary><p>
![Type](../../images/ebnf/type.svg)
</p></details>

## Параметры {: #params }

* **PROCEDURE** — имя процедуры. Соответствует правилам имен для всех
  [объектов](object.md) в кластере. Опционально после имени процедуры
  можно указать список ее параметров (для совместимости со стандартом).

* **RENAME TO** — команда переименования процедуры.

## Примеры {: #examples }

```sql
ALTER PROCEDURE old RENAME TO new OPTION ( timeout = 4 )
```
