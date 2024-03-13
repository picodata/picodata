# GRANT

[DCL](dcl.md) команда `GRANT` используется для выдачи
[привилегий](../../tutorial/access_control.md#privileges) пользователю или роли 
на различные объекты.

## Синтаксис {: #syntax }

![GRANT privilege](../../images/ebnf/grant.svg)

### Тип {: #type }

<details><summary>Диаграмма</summary><p>
![Type](../../images/ebnf/type.svg)
</p></details>

## Параметры {: #params }

* **ROLE** — имя роли. Соответствует правилам имен для всех [объектов](object.md)
  в кластере.

* **USER** — имя пользователя. Соответствует правилам имен для всех
  [объектов](object.md) в кластере.

## Примеры {: #examples }

Выдача права изменять данные пользователя `woody` пользователю `andy`:

```sql
GRANT ALTER ON USER woody TO andy;
```

Выдача права записи в таблицу `characters` для пользователя `woody`:

```sql
GRANT WRITE ON TABLE characters to woody;
```
