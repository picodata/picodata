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

В обычном виде значения столбца `score` имеют дробную часть и определены
в схеме данных типом `decimal`:

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

Преобразуем эти числа в `int`:

```sql
SELECT CAST("score" AS INT) FROM "scoring";
---
  'metadata': [
 {'name': 'COL_1', 'type': 'integer'}],
  'rows': [
  [78],
  [84],
  [47]]
...
```
