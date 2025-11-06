# Системные функции {: #system_functions }

Picodata поддерживает следующий набор системных SQL-функций.

## INSTANCE_UUID {: #instance_uuid }

Скалярная функция `instance_uuid` позволяет узнать `uuid` инстанса, на котором
выполняется запрос. Разрешено использовать только в проекциях.

!!! warning title "Внимание"
    Функция `instance_uuid` объявлена
    устаревшей и будет удалена в следующих релизах Picodata. Используйте
    вместо нее [`pico_instance_uuid`](#pico_instance_uuid)

**Синтаксис**

![INSTANCE UUID](../../images/ebnf/instance_uuid.svg)

**Примеры использования**

```sql
SELECT instance_uuid();
```

## PICO_INSTANCE_UUID {: #pico_instance_uuid }

Скалярная функция `pico_instance_uuid` позволяет узнать текстовое
значение [UUID] инстанса, на котором выполняется запрос. Разрешено
использовать только в проекциях.

[UUID]: ../../reference/sql_types.md#uuid

**Синтаксис**

![PICO_INSTANCE_UUID](../../images/ebnf/pico_instance_uuid.svg)

**Пример использования**

```sql
SELECT pico_instance_uuid();
```

вернет значение [UUID] текущего инстанса.

## PICO_INSTANCE_NAME {: #pico_instance_name }

Скалярная функция `pico_instance_name` позволяет узнать имя инстанса,
предоставив текстовое значение его [UUID]. Функция вернет `NULL` если инстанс с
указанным значением [UUID] не существует.

**Синтаксис**

![PICO_INSTANCE_NAME](../../images/ebnf/pico_instance_name.svg)

**Пример использования**

```sql
SELECT pico_instance_name(pico_instance_uuid());
```

вернет имя инстанса, на котором выполняется запрос.

## PICO_REPLICASET_NAME {: #pico_replicaset_name }

Скалярная функция `pico_replicaset_name` позволяет узнать имя [репликасета][replicaset], к
которому относится текущий инстанс, предоставив текстовое значение его [UUID].

**Синтаксис**

![PICO_REPLICASET_NAME](../../images/ebnf/pico_replicaset_name.svg)

**Примеры использования**

```sql title="Универсальный пример"
SELECT pico_replicaset_name(pico_instance_uuid());
```

```sql title="Пример для bd2eff94-9c4f-4526-a3b6-3c379b7e2c4a"
SELECT pico_replicaset_name('bd2eff94-9c4f-4526-a3b6-3c379b7e2c4a');
```

вернет имя [репликасета][replicaset], участником которого является текущий
инстанс.

[replicaset]: ../../overview/glossary.md#replicaset

## PICO_TIER_NAME {: #pico_tier_name }

Скалярная функция `pico_tier_name` позволяет узнать имя [тира][tier], к
которому относится текущий инстанс, предоставив текстовое значение его [UUID].

**Синтаксис**

![PICO_TIER_NAME](../../images/ebnf/pico_tier_name.svg)

**Примеры использования**

```sql title="Универсальный пример"
SELECT pico_tier_name(pico_instance_uuid());
```

```sql title="Пример для bd2eff94-9c4f-4526-a3b6-3c379b7e2c4a"
SELECT pico_tier_name('bd2eff94-9c4f-4526-a3b6-3c379b7e2c4a');
```

вернет имя [тира][tier], участником которого является текущий инстанс.

[tier]: ../../overview/glossary.md#tier

## PICO_INSTANCE_DIR {: #pico_instance_dir }

Скалярная функция `pico_instance_dir` позволяет узнать абсолютный или
относительный путь к рабочей директории инстанса (в зависимости от
[конфигурации] запуска), предоставив текстовое значение его [UUID]. В случае,
если рабочая директория не была задана явно, функция вернет полный путь к
расположению, из которого был запущен инстанс.

Разрешено использовать только в проекциях.

!!! warning title "Внимание"
    Функция возвращает путь к рабочей директории инстанса, даже если он
    более не существует (например, директория была удалена). Существование пути не
    валидируется — это должен выполнить администратор самостоятельно.

[конфигурации]: ../config.md

**Синтаксис**

![PICO_INSTANCE_DIR](../../images/ebnf/pico_instance_dir.svg)

**Примеры использования**

```sql title="Универсальный пример"
SELECT pico_instance_dir(pico_instance_uuid());
```

```sql title="Пример для bd2eff94-9c4f-4526-a3b6-3c379b7e2c4a"
SELECT pico_instance_dir('bd2eff94-9c4f-4526-a3b6-3c379b7e2c4a');
```

вернет путь к рабочей директории инстанса.

## PICO_CONFIG_FILE_PATH {: #pico_config_file_path }

Скалярная функция `pico_config_file_path` позволяет узнать полный путь к файлу
[конфигурации] инстанса, предоставив текстовое значение его [UUID]. Функция
вернет `NULL` если при запуске инстанса не использовался файл конфигурации.

Разрешено использовать только в проекциях.

**Синтаксис**

![PICO_CONFIG_FILE_PATH](../../images/ebnf/pico_config_file_path.svg)

**Примеры использования**

```sql title="Универсальный пример"
SELECT pico_config_file_path(pico_instance_uuid());
```

```sql title="Пример для bd2eff94-9c4f-4526-a3b6-3c379b7e2c4a"
SELECT pico_config_file_path('bd2eff94-9c4f-4526-a3b6-3c379b7e2c4a');
```

вернет путь к использованному файлу конфигурации или `NULL`.

## PICO_RAFT_LEADER_ID {: #pico_raft_leader_id }

Скалярная функция `pico_raft_leader_id` позволяет узнать идентификатор
лидера raft-группы (значение поля `raft_id` из таблицы
[_pico_peer_address]). Поскольку распространение изменений по кластеру занимает время,
в случае смены лидера функция может возвращать разные значения при подключении к разным узлам.

Разрешено использовать только в проекциях.

[_pico_peer_address]: ../../architecture/system_tables.md#_pico_peer_address

**Синтаксис**

![PICO_RAFT_LEADER_ID](../../images/ebnf/pico_raft_leader_id.svg)

**Примеры использования**

```sql
SELECT pico_raft_leader_id();
```

Следующий запрос выведет имя узла, являющегося лидером:

```sql
SELECT name FROM _pico_instance WHERE raft_id IN (SELECT pico_raft_leader_id());
```

## PICO_RAFT_LEADER_UUID {: #pico_raft_leader_uuid }

Скалярная функция `pico_raft_leader_uuid` возвращает текстовое значение [UUID]
лидера raft-группы. Поскольку распространение изменений по кластеру занимает
время, в случае смены лидера функция может возвращать разные значения при
подключении к разным узлам.

Разрешено использовать только в проекциях.

**Синтаксис**

![PICO_RAFT_LEADER_UUID](../../images/ebnf/pico_raft_leader_uuid.svg)

**Пример использования**

```sql
SELECT pico_raft_leader_uuid();
```

## VERSION {: #version }

Скалярная функция `version` позволяет узнать версию Picodata инстанса,
на котором выполняется запрос. Разрешено использовать только в
проекциях.

**Синтаксис**

![VERSION](../../images/ebnf/version.svg)

**Примеры использования**

Вызов функции не требует привилегий и таблиц, поэтому запрос:

```sql
SELECT version();
```

вернет полную версию инстанса, к которому подключена текущая сессия.
