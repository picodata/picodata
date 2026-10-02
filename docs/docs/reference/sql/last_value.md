# LAST_VALUE

Функция `LAST_VALUE` возвращает значение выражения в последней строке
рамки окна для текущего кортежа. Рамка рассчитывается для каждого кортежа
так же, как и для [агрегатных](window.md#aggregate) оконных функций.

Функция используется только с выражением `OVER` и разрешена только
в проекции `SELECT`. `FILTER` и `DISTINCT` не поддерживаются.

## Синтаксис {: #syntax }

![LAST_VALUE](../../images/ebnf/last_value.svg)

Параметры окна описаны в разделе [Оконные функции](window.md).

## Пример {: #example_last_value }

```sql
CREATE TABLE t0(x INTEGER PRIMARY KEY, y TEXT);
INSERT INTO t0 VALUES (1, 'aaa'), (2, 'aaa'), (3, 'bbb');

SELECT x, y, last_value(x) OVER (ORDER BY y) FROM t0 ORDER BY x;

 x |  y  | col_1
---+-----+-------
 1 | aaa | 2
 2 | aaa | 2
 3 | bbb | 3
```
