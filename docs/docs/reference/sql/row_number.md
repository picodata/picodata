# ROW_NUMBER

Функция `row_number()` присваивает каждому кортежу номер в текущем разделе.
Нумерация начинается с 1 в порядке, определенном выражением `ORDER BY`
в определении окна, либо в произвольном порядке, если `ORDER BY` отсутствует.

Функция используется только с выражением `OVER` и разрешена только
в проекции `SELECT`. `FILTER` и `DISTINCT` не поддерживаются.

## Синтаксис {: #syntax }

![ROW_NUMBER](../../images/ebnf/row_number.svg)

Параметры окна описаны в разделе [Оконные функции](window.md).

## Пример {: #example_row_number }

```sql
CREATE TABLE t0(x INTEGER PRIMARY KEY, y TEXT);
INSERT INTO t0 VALUES (1, 'aaa'), (2, 'ccc'), (3, 'bbb');

SELECT x, y, row_number() OVER (ORDER BY y) FROM t0 ORDER BY x;

 x |  y  | col_1
---+-----+-------
 1 | aaa | 1
 2 | ccc | 3
 3 | bbb | 2
```

Функция `row_number()` присваивает каждому кортежу номер в порядке,
заданном выражением `ORDER BY` внутри определения окна (в данном случае
`ORDER BY y`). При этом, `ORDER BY` внутри `OVER` влияет только на нумерацию
кортежей, но не изменяет порядок итогового вывода, который определяется
`ORDER BY` в операторе `SELECT` (в данном случае `ORDER BY x`).
