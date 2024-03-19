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
     * (change_password, todo:rename_user)

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

- [ ] создание и удаление БД (new_database_created)
- [ ] подключение, восстановление БД;
- [ ] факты нарушения целостности объектов контроля;
- [ ] создание и изменение процедур (программного кода), хранимых в БД, и представлений.
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

### new_database_created

Создание базы данных.

<!-- TODO: rename to "create_db" -->

```json
{
     "title": "new_database_created",
     "message": "a new database `<cluster_id>` was created",
     "severity": "low",
     "raft_id": ...,
     "instance_id": ...,
     // TODO: "cluster_id": ...,
     ...
}
```

<!--
В Picodata определена ровно одна БД, которая называется `universe`. Ее
нельзя создать повторно, удалить или подключить. Восстановление БД
эквивалентно восстановлению СУБД и может быть произведено следующими
способами:

- cредствами сертифицированной ОС в ручном режиме. Администратор ОС
может разархивировать в директорию СУБД заранее сохраненные файлы СУБД
([снапшоты](../overview/glossary.md#snapshot) и [журналы упреждающей
записи](../overview/glossary.md#persistence));
- при помощи функции восстановления узла
([rebootstrap](../overview/glossary.md#bootstrap)). В этом случае
происходит запись события "Создание БД".

В процессе выполнения [bootstrap](../overview/glossary.md#bootstrap) при создании кластера
происходит запись события "Создание БД" со следующими свойствами:
-->


### shredding_started

Начало безопасного удаления рабочих файлов инстанса путем многократной
перезаписи специальными битовыми последовательностями.

```json
{
     "title": "shredding_started",
     "message": "shredding started for <filename>",
     "severity": "low",
     "filename": ...,
     "initiator": ...,
     "time": ...,
     "id": ...
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
     "initiator": ...,
     "time": ...,
     "id": ...
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
     "initiator": ...,
     "time": ...,
     "id": ...
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
     "initiator": ...,
     "time": ...,
     "id": ...
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
     "initiator": ...,
     "time": ...,
     "id": ...
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
     "initiator": ...,
     "time": ...,
     "id": ...,
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
     "initiator": ...,
     "time": ...,
     "id": ...
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
     "initiator": ...,
     "time": ...,
     "id": ...,
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
     "initiator": ...,
     "time": ...,
     "id": ...,
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
     "initiator": ...,
     "time": ...,
     "id": ...,
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
     "initiator": ...,
     "time": ...,
     "id": ...,
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
     "privilege" ...,
     "object_type" ...,
     "object" ...,
     "initiator" ...,
     "time" ...,
     "id" ...
}
```

### local_startup

Запуск инстанса.

```json
{
     "title": "local_startup",
     "message": "instance is starting",
     "severity": "low",
     "time": ...,
     "id": ...
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
     "time": ...,
     "id": ...
}
```
