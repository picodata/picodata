# PICO_RAFT_LEADER_ID

Скалярная функция `pico_raft_leader_id` позволяет узнать идентификатор
лидера raft-группы (значение поля `raft_id` из таблицы
[_pico_peer_address]). Разрешено использовать только в проекциях.

[_pico_peer_address]: ../../architecture/system_tables.md#_pico_peer_address

## Синтаксис {: #syntax }

![PICO_RAFT_LEADER_ID](../../images/ebnf/pico_raft_leader_id.svg)

## Примеры использования {: #using_examples }

Вызов функции не требует привилегий и таблиц, поэтому запрос:

```sql
SELECT pico_raft_leader_id();
```

значение поля `raft_id` инстанса, который является на данный момент raft-лидером.
