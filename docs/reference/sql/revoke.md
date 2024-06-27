# REVOKE

[DCL](dcl.md)-команда `REVOKE` используется для изъятия
[привилегий](../../tutorial/access_control.md#privileges) у пользователя
или группы.

## Синтаксис {: #syntax }

![REVOKE privilege](../../images/ebnf/revoke.svg)

### Тип {: #type }

??? note "Диаграмма"
    ![Type](../../images/ebnf/type.svg)

## Параметры {: #params }

* **ROLE** — имя роли. Соответствует правилам имен для всех
  [объектов](object.md) в кластере.

* **USER** — имя пользователя. Соответствует правилам имен для всех
  [объектов](object.md) в кластере.

## Примеры {: #examples }

Изъятие права чтения таблиц у пользователя `alice`:

```sql
REVOKE READ TABLE FROM alice;
```
