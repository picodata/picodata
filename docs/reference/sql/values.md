# VALUES

[DQL](dql.md)-команда`VALUES` используется для получения кортежей из
создаваемой на лету временной таблицы.

## Синтаксис {: #syntax }

![VALUES](../../images/ebnf/values.svg)

### Выражение {: #expression }

<details><summary>Диаграмма</summary><p>
![Expression](../../images/ebnf/expression.svg)
</p></details>

### Литерал {: #literal }

<details><summary>Диаграмма</summary><p>
![Literal](../../images/ebnf/literal.svg)
</p></details>

## Примеры {: #examples }

Создание таблицы из двух предопределенных кортежей:
```sql
VALUES (1, 'Woody', 2561), (2, 'Buzz Lightyear', 4781);
```
