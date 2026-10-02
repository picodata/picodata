# PICO_INSTANCE_HEALTH_STATUS

Скалярная функция `pico_instance_health_status` позволяет получить
сведения о состоянии и работоспособности инстанса в формате [JSON],
предоставив текстовое значение его [UUID]. Является оберткой,
предоставляющей доступ к HTTP-эндпоинту [`health/status`] из SQL.

## Синтаксис {: #syntax }

![PICO_INSTANCE_HEALTH_STATUS](../../images/ebnf/pico_instance_health_status.svg)

## Примеры {: #examples }

```sql
SELECT pico_instance_health_status(pico_instance_uuid());
```

вернет сведения о состоянии текущего инстанса.

[UUID]: ../../reference/sql_types.md#uuid
[JSON]: ../../reference/sql_types.md#json
[`health/status`]: ../../admin/local_monitoring.md#instance_health_check
