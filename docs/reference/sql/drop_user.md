# DROP USER 

[DCL](dcl.md) команда `DROP USER` используется для удаления существующего
[пользователя](../../tutorial/access_control.md#users).

## Синтаксис {: #syntax }

![DROP USER](../../images/ebnf/drop_user.svg)

## Параметры {: #params }

* **USER** — имя пользователя. Соответствует правилам имен для всех
  [объектов](object.md) в кластере.

## Ограничения {: #restrictions }

Удаление пользователя приведет к ошибке, если в системе есть объекты, владельцем которых
он является (что соответствует опции `RESTRICT` из ANSI SQL).


## Примеры  {: #examples }

```sql
DROP USER andy OPTION (TIMEOUT = 3.0);
```
