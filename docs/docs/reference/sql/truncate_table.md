# TRUNCATE TABLE

[DDL](ddl.md)-команда `TRUNCATE TABLE` используется для удаления всех
строк в существующей таблице.

## Синтаксис {: #syntax }

![TRUNCATE TABLE](../../images/ebnf/truncate_table.svg)

## Параметры {: #params }

* **TABLE** — имя таблицы. Соответствует правилам имен для всех
  [объектов](object.md) в кластере

## Общие опции и параметры {: #common_options }

Для данной команды доступны следующие общие опции и параметры SQL:

* [TIMEOUT](common_options.md#timeout)
* [WAIT APPLIED](common_options.md#wait_applied)

## Требуемые привилегии {: #required_privileges }

Данная команда требует привилегии `WRITE TABLE`. Очистить системную
таблицу нельзя никому, в том числе администратору.

См. также:

- [Управление доступом — Таблица привилегий](../../admin/access_control.md#privileges_table)

## Примеры {: #examples }

```sql
TRUNCATE TABLE warehouse
OPTION (TIMEOUT = 3.0);
```

```sql
TRUNCATE TABLE warehouse
WAIT APPLIED GLOBALLY
OPTION (TIMEOUT = 3.0);
```
