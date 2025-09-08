# PICO_REPLICASET_NAME

Скалярная функция `pico_replicaset_name` позволяет узнать имя репликасета, к
которому относится текущий инстанс. Разрешено использовать только в проекциях.

## Синтаксис {: #syntax }

![PICO_REPLICASET_NAME](../../images/ebnf/pico_replicaset_name.svg)

## Примеры использования {: #using_examples }

Вызов функции не требует привилегий и таблиц, поэтому запрос:

```sql
SELECT pico_replicaset_name();
```

вернёт имя репликасета текущего инстанса.
