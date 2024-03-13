# CAST

Функция `CAST` используется в [DQL](dql.md) командах для приведения выражения к
определенному [типу](../sql_types.md).

## Синтаксис {: #syntax }

![CAST](../../images/ebnf/cast.svg)

### Выражение {: #expression }

<details><summary>Диаграмма</summary><p>
![Expression](../../images/ebnf/expression.svg)
</p></details>

### Литерал  {: #literal }

<details><summary>Диаграмма</summary><p>
![Literal](../../images/ebnf/literal.svg)
</p></details>

### Тип {: #type }

<details><summary>Диаграмма</summary><p>
![Type](../../images/ebnf/type.svg)
</p></details>

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
