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

### create_user

Создание учетной записи пользователя СУБД

```json
{
     "title": "create_user",
     "message": "created user `<user>`",
     "severity": "high",
     "user": ...,
     "auth_type": ...,
     "initiator": ...,
     "time": ...,
     "id": ...
}
```

### create_role

Создание роли СУБД

```json
{
     "title": "create_role",
     "message": "created role `<role>`",
     "severity": "high",
     "role": ...,
     "initiator": ...,
     "time": ...,
     "id": ...
}
```

### drop_user

Удаление учетной записи пользователя СУБД

### drop_role

Удаление роли СУБД

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
     "initiator": ...,
     "time": ...,
     "id": ...
}
```

### grant_privilege

Выдача привилегии пользователю СУБД

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
     "initiator": ...,
     "time": ...,
     "id": ...
}
```

### revoke_privilege

Отзыв привилегии у пользователя СУБД

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
     "initiator": ...,
     "time": ...,
     "id": ...
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
     "initiator": ...,
     "time": ...,
     "id": ...
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
     "initiator": ...,
     "time": ...,
     "id": ...
}
```

### auth_ok

Успешная попытка аутентификации

```json
{
     "title": "auth_ok",
     "message": "successfully authenticated user `<user>`",
     "severity": "high",
     "user": ...,
     "verdict": ...,
     "initiator": ...,
     "time": ...,
     "id": ...
}
```

### auth_fail

Неуспешная попытка аутентификации

```json
{
     "title": "auth_fail",
     "message": "failed to authenticate user `<user>`",
     "severity": "high",
     "user": ...,
     "verdict": ...,
     "initiator": ...,
     "time": ...,
     "id": ...,
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
     "initiator": ...,
     "time": ...,
     "id": ...,
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

#### Добавление нового узла в кластер {: #add_instance }

При добавлении узла в кластер в его журнале
фиксируется событие создания БД `universe` с наименованием
`create_database`. Событие фиксируется ровно один раз при первом запуске
узла кластера.

#### Исключение узла из кластера {: #expel_instance }

Удаление БД `universe` не предусмотрено, поэтому события на этот случай
тоже нет.

### Запуск и остановка СУБД с указанием причины остановки {: #db_start_stop }

- наименование (`change_current_grade`)
- важность (`средняя`)
- имя [грейда](../overview/glossary.md#grade) при запуске (`online`)
- имя грейда при остановке (`offline`)
- идентификатор узла кластера (`raft_id`, `instance_id`)

К данному событию относится информация о любом изменении грейда каждого
узла кластера, как только оно было зарегистрировано. Плановые
[запуск](../reference/cli.md#run) и
[остановка](../reference/api.md#pico_exit) узла
([добавление](../overview/glossary.md#joining) в кластер и
[исключение](../reference/cli.md#expel) из него соответственно) связаны
с переоценкой грейдов и поэтому также вызывают это событие.

### Изменение конфигурации СУБД {: #db_configure }

#### Изменение значений параметров конфигурации {: #db_change_config }

- наименование (`change_config`)
- важность (`высокая`)
- название и новое значение параметра конфигурации

Отслеживается изменение следующих параметров СУБД:

- `password_min_length` – минимальная длина пароля учетной записи
- `audit_log` – настройки файла журнала безопасности
- `audit_enabled` – статус журнала безопасности (`ВКЛ`/`ВЫКЛ`)
- низкоуровневых параметров, таких как:
     - `listen`
     - `checkpoint_interval`
     - `checkpoint_count`
     - `memtx_memory`
     - `vinyl_memory`
     - `snapshot_period`
     - `log`
     - `log_level`
     - `log_format`
     - `log_nonblock`
     - `too_long_threshold`

Параметры, которые не были указаны, изменить невозможно.

#### Изменение топологии кластера (удаление и добавление узлов) {: #db_change_topology }

- наименование (`change_topology`)
- важность (`высокая`)
- адрес и идентификатор добавленного/удаленного узла кластера
- тип (`join` / `expel`)

Событие изменения топологии будет зарегистрировано на узле-лидере. При
добавлении и удалении узла в журнал попадет событие с наименованием `join`
или `expel`.

### Создание и удаление таблиц за исключением временных таблиц, создаваемых СУБД в служебных целях {: #manage_tables }

- наименование (`create_table` / `drop_table`)
- важность (`средняя`)
- имя новой таблицы
- идентификатор пользователя-владельца таблицы

Владельцем созданной таблицы автоматически становится тот, кто ее создал.

### Создание и удаление хранимых процедур {: #manage_stored_procedures }

- наименование (`create_procedure` / `drop_procedure`)
- важность (`средняя`)
- имя новой процедуры
- идентификатор пользователя-владельца процедуры

Владельцем созданной процедуры автоматически становится тот, кто ее создал.

### Факты нарушения целостности объектов контроля {: #detect_intrusion }

Объектами контроля являются:

- конфигурации СУБД
- конфигурации БД
- процедуры (программный код) СУБД
- процедуры (программный код), хранящийся в БД

Файлы снапшотов, в которых хранятся все указанные объекты контроля,
содержат контрольные суммы формата `crc32`, которые позволяют обнаружить
несанкционированное изменение объектов. В случае несовпадения
контрольных сумм при запуске узла в журнал безопасности будет записана
критическая ошибка, и приложение СУБД аварийно завершится.

### Неудачные попытки доступа к объекту {: #failed_access_attempts }

- наименование (`access_denied`)
- важность (`средняя`)
- пользователь, запросивший доступ
- имя и тип объекта

### Ротация журнала аудита {: #audit_log_rotation }

- наименование (`audit_rotate`)
- важность (`низкая`)
