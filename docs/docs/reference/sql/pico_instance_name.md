# PICO_INSTANCE_NAME

Скалярная функция `pico_instance_name` позволяет узнать имя инстанса,
предоставив текстовое значение его [UUID]. Функция вернет `NULL` если инстанс с
указанным значением [UUID] не существует.

## Синтаксис {: #syntax }

![PICO_INSTANCE_NAME](../../images/ebnf/pico_instance_name.svg)

## Примеры {: #examples }

```sql
SELECT pico_instance_name(pico_instance_uuid());
```

вернет имя инстанса, на котором выполняется запрос.

[UUID]: ../../reference/sql_types.md#uuid
