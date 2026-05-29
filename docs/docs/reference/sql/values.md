# VALUES

[DQL](dql.md)-команда`VALUES` используется для получения кортежей из
создаваемой на лету временной таблицы.

## Синтаксис {: #syntax }

![VALUES](../../images/ebnf/values.svg)

### Выражение {: #expression }

??? note "Диаграмма"
    ![Expression](../../images/ebnf/expression.svg)

### Литерал {: #literal }

??? note "Диаграмма"
    ![Literal](../../images/ebnf/literal.svg)

### Литерал массива {: #array_literal }

??? note "Диаграмма"
    ![Array literal](../../images/ebnf/array_literal.svg)

## Примеры {: #examples }

Создание таблицы из двух предопределенных кортежей:
```sql
VALUES (1, 'bricks', 'heavy'), (2, 'bars', 'light');
```

Внутри `VALUES` можно использовать литерал массива `ARRAY[...]`. Подробнее
см. [ARRAY](../sql_types.md#array):

```sql
VALUES (1, ARRAY[10, 20, 30]), (2, ARRAY[]);
```
