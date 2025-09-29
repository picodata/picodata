# INSTANCE_UUID

Скалярная функция `instance_uuid` позволяет узнать `uuid` инстанса, на котором
выполняется запрос. Разрешено использовать только в проекциях.

!!! warning title "Внимание"
    Функция `instance_uuid` объявлена
    устаревшей и будет удалена в следующих релизах Picodata. Используйте
    вместо нее [`pico_instance_uuid`](pico_instance_uuid.md)

## Синтаксис {: #syntax }

![INSTANCE UUID](../../images/ebnf/instance_uuid.svg)

## Примеры использования {: #using_examples }

```sql
SELECT instance_uuid();
```
