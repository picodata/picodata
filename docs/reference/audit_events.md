# Регистрируемые события безопасности

В данном разделе перечислены события, которые фиксируются в [журнале
аудита](../tutorial/audit_log.md) Picodata. События проиллюстрированы
псевдоструктурами в формате JSON.

<!--
Релевантный код Picodata:
- https://git.picodata.io/picodata/picodata/picodata/-/blob/master/test/int/test_audit.py
- https://git.picodata.io/search?project_id=58&search=audit%21%28
- или в репозитории picodata: `grep -a7 -R 'audit!(' src/`
-->

## Структура журнала {: #audit_log_structure }

Журнал представляет собой массив строк с разметкой `json`, где каждой
строке соответствует одно событие. Пример сообщения из журнала
(переформатированный для лучшей читаемости):

```
{
     "title": "create_table",
     "message": "created table `friends_of_peppa`",
     "severity": "medium",
     "name": "friends_of_peppa",
     "initiator": "admin",
     "time": "2023-12-01T14:31:50.117+0300",
     "id": "1.0.14"
}
```

Каждая запись журнала включает следующие обязательные элементы:

- `title` – наименование
- `message` – описание
- `severity` – важность (`low` / `medium` / `high`)
- `initiator` — субъект доступа инициировавший событие
- `time` – дата и время
- `id` – идентификатор, состоящий из трех чисел:
    - `raft_id` – идентификатор инстанса в Raft
    - `gen` – счетчик перезапусков инстанса
    - `count` – внутренний счетчик событий

Помимо этого, запись может содержать дополнительные поля, относящиеся к
конкретному событию, например, имена пользователей, таблиц и т.д.

## Перечень регистрируемых событий {: #events }

<!--
Регистрации подлежат как минимум следующие события безопасности:

- [x] создание учетных записей пользователей СУБД;
     * (create_user, create_role)

- [x] изменение атрибутов учетных записей пользователей СУБД;
     * (change_password, rename_user)

- [x] успешные и неуспешные попытки аутентификации пользователей СУБД;
     * (auth_ok, auth_fail)

- [x] запуск и остановка СУБД с указанием причины остановки;
     * (local_startup, local_shutdown, change_current_grade)

- [x] изменение конфигурации СУБД;
     * (change_config, join_instance, expel_instance)

- [x] создание и удаление таблицы
     * (create_table, drop_table)

- [x] изменение правил разграничения доступа в СУБД;
     * (grant_privilege, grant_role, revoke_privilege, revoke_role)

- [x] создание и удаление БД;
     * (create_local_db, drop_local_db)

- [x] подключение, восстановление БД;
     * (connect_local_db, recover_local_db)

- [x] факты нарушения целостности объектов контроля;
     * (integrity_violation)

- [x] создание и изменение процедур (программного кода), хранимых в БД, и представлений.
     * (create_procedure, drop_procedure, rename_procedure)
-->

### create_user

Создание учетной записи пользователя СУБД.

```json
{
     "title": "create_user",
     "message": "created user `<user>`",
     "severity": "high",
     "user": ...,
     "auth_type": ...,
     ...
}
```

### create_role

Создание роли СУБД.

```json
{
     "title": "create_role",
     "message": "created role `<role>`",
     "severity": "high",
     "role": ...,
     ...
}
```

### drop_user

Удаление учетной записи пользователя СУБД.

```json
{
     "title": "drop_user",
     "message": "dropped user `<user>`",
     "severity": "medium",
     "user": ...,
     ...
}
```

### drop_role

Удаление роли СУБД.

```json
{
     "title": "drop_role",
     "message": "dropped role `<role>`",
     "severity": "medium",
     "role": ...,
     ...
}
```

### rename_user

Переименование учетной записи пользователя СУБД.

```json
{
     "title": "rename_user",
     "message": "name of user `<old_name>` was changed to `<new_name>`",
     "severity": "high",
     "old_name": ...,
     "new_name": ...,
     ...
}
```

### change_password

Изменение атрибутов учетных записей пользователей СУБД:

- метода аутентификации
- пароля

```json
{
     "title": "change_password",
     "message": "password of user `<user>` was changed",
     "severity": "high",
     "user": ...,
     "auth_type": ...,
     ...
}
```

### grant_privilege

Выдача привилегии пользователю СУБД.

```json
{
     "title": "grant_privilege",
     "message": "granted privilege <privilege>
          on <object_type> `<object>`
          to <grantee_type> `<grantee>`",
     "severity": "high",
     "privilege": ...,
     "object_type": ...,
     "object": ...,
     "grantee_type": ...,
     "grantee": ...,
     ...
}
```

### revoke_privilege

Отзыв привилегии у пользователя СУБД.

```json
{
     "title": "revoke_privilege",
     "message": "revoked privilege <privilege>
          on <object_type> `<object>`
          from <grantee_type> `<grantee>`",
     "severity": "high",
     "privilege": ...,
     "object_type": ...,
     "object": ...,
     "grantee_type": ...,
     "grantee": ...,
     ...
}
```

### grant_role

Назначение роли пользователю СУБД или другой роли.

```json
{
     "title": "grant_role",
     "message": "granted role `<role>` to <grantee_type> `<grantee>`",
     "severity": "high",
     "role": ...,
     "grantee_type": ...,
     "grantee": ...,
     ...
}
```

### revoke_role

Отзыв роли у пользователя СУБД или у другой роли.

```json
{
     "title": "revoke_role",
     "message": "revoked role `<role>` from <grantee_type> `<grantee>`",
     "severity": "high",
     "role": ...,
     "grantee_type": ...,
     "grantee": ...,
     ...
}
```

### auth_ok

Успешная попытка аутентификации.

```json
{
     "title": "auth_ok",
     "message": "successfully authenticated user `<user>`",
     "severity": "high",
     "user": ...,
     "verdict": ...,
     ...
}
```

### auth_fail

Неуспешная попытка аутентификации.

```json
{
     "title": "auth_fail",
     "message": "failed to authenticate user `<user>`",
     "severity": "high",
     "user": ...,
     "verdict": ...,
     ...,
}
```

### create_local_db

Создание базы данных. Событие фиксируется после добавления инстанса в
кластер, см. [join_instance](#join_instance). Событие фиксируется на
всех узлах кластера, включая добавляемый.

```json
{
     "title": "create_local_db",
     "message": "local database created on `<instance_id>`",
     "severity": "low",
     "raft_id": ...,
     "instance_id": ...,
     ...
}
```

### drop_local_db

Удаление базы данных. Событие фиксируется после удаления инстанса из
кластера, см. [expel_instance](#expel_instance). Событие фиксируется на
всех узлах кластера.

```json
{
     "title": "drop_local_db",
     "message": "local database dropped on `<instance_id>`",
     "severity": "low",
     "raft_id": ...,
     "instance_id": ...,
     ...
}
```

### connect_local_db

Подключение базы данных. Событие фиксируется после инициализации
локального хранилища, см. [Жизненный цикл инстанса][init_common]

<!--
start_discover -> recover_local_db + connect_local_db
start_boot/start_join -> create_local_db + connect_local_db
-->

[init_common]: ../architecture/instance_lifecycle.md/#fn_init_common

```json
{
     "title": "connect_local_db",
     "message": "local database connected on `<instance_id>`",
     "severity": "low",
     "raft_id": ...,
     "instance_id": ...,
     ...
}
```

### recover_local_db

Восстановление базы данных. Событие фиксируется после инициализации
локального хранилища в случае если имело место восстановление из
снапшота, см. [Жизненный цикл инстанса][start_discover].

[start_discover]: ../architecture/instance_lifecycle.md/#fn_start_discover

```json
{
     "title": "recover_local_db",
     "message": "local database recovered on `<instance_id>`",
     "severity": "low",
     "raft_id": ...,
     "instance_id": ...,
     ...
}
```

### shredding_started

Начало безопасного удаления рабочих файлов инстанса путем многократной
перезаписи специальными битовыми последовательностями.

```json
{
     "title": "shredding_started",
     "message": "shredding started for <filename>",
     "severity": "low",
     "filename": ...,
     ...
}
```

### shredding_finished

Успешное удаление рабочих файлов инстанса.

```json
{
     "title": "shredding_finished",
     "message": "shredding finished for <filename>",
     "severity": "low",
     "filename": ...,
     ...
}
```

### shredding_failed

Ошибка безопасного удалении рабочих файлов инстанса.

```json
{
     "title": "shredding_failed",
     "message": "shredding failed for <filename>",
     "severity": "low",
     "filename": ...,
     "error": ...,
     ...
}
```

### change_current_grade

Изменение текущего [грейда](../overview/glossary.md#grade) инстанса.

```json
{
     "title": "change_current_grade",
     "message": "current grade
          of instance `<instance_id>`
          changed to <new_grade>",
     "severity": "medium",
     "instance_id": ...,
     // TODO: "old_grade": ...,
     "new_grade": ...,
     "raft_id": ...,
     ...
}
```


### change_target_grade

Изменение целевого [грейда](../overview/glossary.md#grade) инстанса.

```json
{
     "title": "change_target_grade",
     "message": "target grade
          of instance `<instance_id>`
          changed to <new_grade>",
     "severity": "low",
     "instance_id": ...,
     // TODO: "old_grade": ...,
     "new_grade": ...,
     "raft_id": ...,
     ...
}
```

### join_instance

Изменение конфигурации СУБД связанное с добавлением инстанса в кластер.

```json
{
     "title": "join_instance",
     "message": "a new instance `<instance_id>` joined the cluster",
     "severity": "low",
     "instance_id": ...,
     "raft_id": ...,
     ...
}
```

### expel_instance

Изменение конфигурации СУБД связанное с удалением инстанса из кластера.

```json
{
     "title": "expel_instance",
     "message": "instance `<instance_id>` was expelled from the cluster",
     "severity": "low",
     "instance_id": ...,
     "raft_id": ...,
     ...
}
```

### change_config

Изменение конфигурации СУБД связанное с изменением в системной таблице
[_pico_property](../architecture/system_tables.md#_pico_property).

```json
{
     "title": "change_config",
     "message": "property `<key>` was changed to <value>",
     "severity": "high",
     "key": ...,
     "value": ...,
     ...
}
```

### create_table

Создание таблицы.

```json
{
     "title": "create_table",
     "message": "created table `<name>`",
     "severity": "medium",
     "name": ...,
     ...
}
```

### drop_table

Удаление таблицы.

```json
{
     "title": "drop_table",
     "message": "dropped table `<name>`",
     "severity": "medium",
     "name": ...,
     ...
}
```

### create_procedure

Создание хранимой процедуры.

```json
{
     "title": "create_procedure",
     "message": "created procedure `<name>`",
     "severity": "medium",
     "name": ...,
     ...
}
```

### rename_procedure

Переименование хранимой процедуры.

```json
{
     "title": "rename_procedure",
     "message": "renamed procedure `<old_name>` to `<new_name>`",
     "severity": "medium",
     "old_name": ...,
     "new_name": ...,
     ...
}
```

### drop_procedure

Удаление хранимой процедуры.

```json
{
     "title": "drop_procedure",
     "message": "dropped procedure `<name>`",
     "severity": "medium",
     "name": ...,
     ...
}
```

### access_denied

Неавторизованный запрос к БД.

<!--
TODO:
privilege_type -> privilege
object_name -> object
new field: user == initiator
-->

```json
{
     "title": "access_denied",
     "message": "<privilege> access
          to <object_type> `<object>`
          is denied for user `<user>`",
     "severity": "medium",
     "privilege": ...,
     "object_type": ...,
     "object": ...,
     ...
}
```

### local_startup

Запуск инстанса.

```json
{
     "title": "local_startup",
     "message": "instance is starting",
     "severity": "low",
     ...
}
```

### local_shutdown

Остановка инстанса.
<!--
TODO:
new field: reason
-->

```json
{
     "title": "local_shutdown",
     "message": "instance is shutting down",
     "severity": "high",
     ...
}
```

### integrity_violation

Нарушение целостности объектов контроля, см. [Контроль
целостности][integrity].

[integrity]: ../security/integrity.md

```json
{
     "title": "integrity_violation",
     "message": "integrity violation detected",
     "severity": "high",
     ...
}
```
