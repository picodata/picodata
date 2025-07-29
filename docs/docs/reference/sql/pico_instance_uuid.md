# PICO_INSTANCE_UUID

Скалярная функция `instance_uuid` позволяет узнать `uuid` инстанса, на котором
выполняется запрос. Разрешено использовать только в проекциях.

## Синтаксис {: #syntax }

![PICO_INSTANCE UUID](../../images/ebnf/pico_instance_uuid.svg)

## Примеры использования {: #using_examples }

Вызов функции не требует привилегий и таблиц, поэтому запрос:

```sql
SELECT pico_instance_uuid();
```

вернет `uuid` инстанса, к которому подключена текущая сессия.
