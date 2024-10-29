# CAST

Функция `CAST` используется в [DQL](dql.md)-командах для приведения
выражения к определенному [типу](../sql_types.md).

## Синтаксис {: #syntax }

![CAST](../../images/ebnf/cast.svg)

### Выражение {: #expression }

??? note "Диаграмма"
    ![Expression](../../images/ebnf/expression.svg)

### Литерал {: #literal }

??? note "Диаграмма"
    ![Literal](../../images/ebnf/literal.svg)

### Тип {: #type }

??? note "Диаграмма"
    ![Type](../../images/ebnf/type.svg)

## Примеры {: #examples }

В качестве примера покажем преобразование дробных чисел в целые с
отбрасыванием дробной части. Используем следующую таблицу:

![Table_scores](../../images/table_scores.svg)

В обычном виде значения колонки `score` имеют дробную часть и определены
в схеме данных типом DECIMAL:

```sql
SELECT "score" FROM "scoring";
---
  'metadata': [
  {'name': 'score', 'type': 'decimal'}],
  'rows': [
    [78.33],
    [84.61],
    [47.28]]
...
```

Преобразуем тип значений из колонки `score` в INT с помощью функции `CAST`:

```sql
picodata> SELECT CAST("score" AS INT) FROM "scoring";
+-------+
| COL_1 |
+=======+
| 78    |
|-------|
| 84    |
|-------|
| 47    |
+-------+
(3 rows)
```

В качестве альтернативы можем использовать синтаксис PostgreSQL —
двойное двоеточие:

```sql
picodata> SELECT "score"::INT FROM "scoring";
+-------+
| COL_1 |
+=======+
| 78    |
|-------|
| 84    |
|-------|
| 47    |
+-------+
(3 rows)
```
