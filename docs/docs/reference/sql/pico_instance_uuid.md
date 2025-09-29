# PICO_INSTANCE_UUID

Скалярная функция `pico_instance_uuid` позволяет узнать текстовое
значение [UUID] инстанса, на котором выполняется запрос. Разрешено
использовать только в проекциях.

## Синтаксис {: #syntax }

![PICO_INSTANCE_UUID](../../images/ebnf/pico_instance_uuid.svg)

## Пример использования {: #using_example }

```sql
SELECT pico_instance_uuid();
```

вернет значение [UUID] текущего инстанса.

[UUID]: ../../reference/sql_types.md#uuid
