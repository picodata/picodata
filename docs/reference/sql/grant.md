# GRANT

[DCL](dcl.md)-команда `GRANT` используется для выдачи
[привилегий](../../tutorial/access_control.md#privileges) пользователю
или роли на различные объекты.

## Синтаксис {: #syntax }

![GRANT privilege](../../images/ebnf/grant.svg)

### Тип {: #type }

<details><summary>Диаграмма</summary><p>
![Type](../../images/ebnf/type.svg)
</p></details>

## Параметры {: #params }

* **ROLE** — имя роли. Соответствует правилам имен для всех
  [объектов](object.md) в кластере.

* **USER** — имя пользователя. Соответствует правилам имен для всех
  [объектов](object.md) в кластере.

## Примеры {: #examples }

Выдача права изменять данные пользователя `alice` пользователю `bob`:

```sql
GRANT ALTER ON USER alice TO bob;
```

Выдача права записи в таблицу `warehouse` для пользователя `alice`:

```sql
GRANT WRITE ON TABLE warehouse to alice;
```
