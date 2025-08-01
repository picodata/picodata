# PICO_RAFT_LEADER_UUID

Скалярная функция `pico_raft_leader_uuid` позволяет узнать `uuid` лидера
raft-группы. Разрешено использовать только в проекциях.

## Синтаксис {: #syntax }

![PICO_RAFT_LEADER_UUID](../../images/ebnf/pico_raft_leader_uuid.svg)

## Примеры использования {: #using_examples }

Вызов функции не требует привилегий и таблиц, поэтому запрос:

```sql
SELECT pico_raft_leader_uuid();
```

вернет `uuid` инстанса, который является на данный момент raft-лидером.
