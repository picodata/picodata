# _PICO_BUCKET

Скалярная функция `_pico_bucket` возвращает диапазоны бакетов,
принадлежащие отдельным репликасетам, в рамках указанного [тира][tier].
Ответ предоставляет в формате JSON.

## Синтаксис {: #syntax }

![_PICO_BUCKET](../../images/ebnf/_pico_bucket.svg)

## Примеры {: #examples }

```sql
SELECT _pico_bucket('default');
```

вернет диапазоны бакетов:

```json
 [{"bucket_id_start":1,"target_replicaset_uuid":"ad033267-39ac-4ee4-822f-785d021db1cc","tier_name":"default","state":"active","current_replicaset_uuid":"ad033267-39ac-4ee4-822f-785d021db1cc","bucket_id_end":8192},{"bucket_id_start":8193,"target_replicaset_uuid":"d9c7e155-9434-4760-8a69-e62c42f22e4c","tier_name":"default","state":"active","current_replicaset_uuid":"d9c7e155-9434-4760-8a69-e62c42f22e4c","bucket_id_end":16384}]
```

[tier]: ../../overview/glossary.md#tier
