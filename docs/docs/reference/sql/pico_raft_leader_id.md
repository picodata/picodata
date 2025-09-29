# PICO_RAFT_LEADER_ID

Скалярная функция `pico_raft_leader_id` позволяет узнать идентификатор
лидера raft-группы (значение поля `raft_id` из таблицы
[_pico_peer_address]). Поскольку распространение изменений по кластеру занимает время,
в случае смены лидера функция может возвращать разные значения при подключении к разным узлам.

Разрешено использовать только в проекциях.

[_pico_peer_address]: ../../architecture/system_tables.md#_pico_peer_address

## Синтаксис {: #syntax }

![PICO_RAFT_LEADER_ID](../../images/ebnf/pico_raft_leader_id.svg)

## Примеры использования {: #using_examples }

```sql
SELECT pico_raft_leader_id();
```

Следующий запрос выведет имя узла, являющегося лидером:

```sql
SELECT name FROM _pico_instance WHERE raft_id IN (SELECT pico_raft_leader_id());
```
