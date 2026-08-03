# Фасет CONTEXT

Фасет `CONTEXT` может быть использован для просмотра параметров исполнения
запроса. В настоящий момент там присутствуют
[`sql_vdbe_opcode_max`](../../db_config.md#sql_vdbe_opcode_max) и
[`sql_motion_row_max`](../../db_config.md#sql_motion_row_max).

??? example "Подготовка тестового окружения"
    Примеры использования команд включают в себя запросы к [тестовым
    таблицам](../../legend.md).

```sql
EXPLAIN (CONTEXT) SELECT * FROM foo;
```

```sql
sql_vdbe_opcode_max = 45000
sql_motion_row_max = 5000
```
