# CURRENT_DATE {: #current_date }

Функция `CURRENT_DATE` возвращает объект типа [DATETIME] с текущей датой
и временем `00:00:00`.

[DATETIME]: ../sql_types.md#datetime

## Синтаксис {: #syntax }

![CURRENT_DATE](../../images/ebnf/current_date.svg)

## Примеры {: #examples }

```sql
picodata> VALUES (CURRENT_DATE);
+------------------------+
| COL_1                  |
+========================+
| "2024-07-10T00:00:00Z" |
+------------------------+
(1 rows)
```
