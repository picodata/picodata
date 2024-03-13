# CREATE ROLE

[DCL](dcl.md) команда `CREATE ROLE` используется для создания новой
[роли](../../tutorial/access_control.md#roles).

## Синтаксис {: #syntax }

![CREATE ROLE](../../images/ebnf/create_role.svg)

## Параметры {: #params }

* **ROLE** — имя роли. Соответствует правилам имен для всех [объектов](object.md)
  в кластере.

## Примеры  {: #examples }

```sql
CREATE ROLE toy OPTION (TIMEOUT = 3.0);
```
