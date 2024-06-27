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

## Примеры {: #examples }

Создание таблицы из двух предопределенных кортежей:
```sql
VALUES (1, 'bricks', 'heavy'), (2, 'bars', 'light');
```
