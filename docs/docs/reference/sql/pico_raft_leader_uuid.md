# PICO_RAFT_LEADER_UUID

Скалярная функция `pico_raft_leader_uuid` возвращает текстовое значение [UUID]
лидера raft-группы. Поскольку распространение изменений по кластеру занимает
время, в случае смены лидера функция может возвращать разные значения при
подключении к разным узлам.

Разрешено использовать только в проекциях.

[UUID]: ../../reference/sql_types.md#uuid

## Синтаксис {: #syntax }

![PICO_RAFT_LEADER_UUID](../../images/ebnf/pico_raft_leader_uuid.svg)

## Пример использования {: #using_example }

```sql
SELECT pico_raft_leader_uuid();
```
