# Фасет FORWARD

При указании фасета FORWARD можно получить значение опции FORWARD, с которой запрос
гарантированно будет выполнен. Подробнее об опции FORWARD можно прочитать в разделе
[параметры](../dql.md#params) для DQL-запросов.

## Примеры использования {: #forward-examples}

??? example "Подготовка тестового окружения"
    Примеры использования команд включают в себя запросы к [тестовым
    таблицам](../../legend.md).

### OFF {: #forward-off}

```sql
EXPLAIN (FORWARD) SELECT * FROM warehouse WHERE id = 42;
```

```sql
forward analysis (on > ro_to_rw > off):
  forward = off
```

Из вывода следует, что запрос можно гарантированно исполнить без пересылок.
Такое возможно, когда бакет, соответствующий ключу `42`, лежит на узле, где
выполняется EXPLAIN.

### RO_TO_RW {: #forward-ro-to-rw}

```sql
EXPLAIN (FORWARD) SELECT * FROM warehouse WHERE id = 42;
```

```sql
forward analysis (on > ro_to_rw > off):
  forward = ro_to_rw
```

Вывод EXPLAIN указывает на то, что запрос можно гарантированно исполнить с одной
пересылкой. Следовательно, запрошенная порция данных не лежит на текущем узле.

### ON {: #forward-on}

Данное значение характерно для запросов, выполняющих полное сканирование шардированных
таблиц. Например:

```sql
EXPLAIN (FORWARD) SELECT * FROM warehouse ORDER BY 1;
```

```sql
forward analysis (on > ro_to_rw > off):
  forward = on
```

Из вывода фасета FORWARD следует, что при исполнении запроса будет совершено
более одной пересылки.
