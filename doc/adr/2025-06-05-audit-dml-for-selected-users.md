<!--
The template is not strict. You can use any ADR structure that feels best for a particular case.
See these ADR examples for inspiration:
- [Cassandra SEP - Ganeral Purpose Transactions](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-15%3A+General+Purpose+Transactions)
- [Rust RFC - Lifetime Ellision](https://github.com/rust-lang/rfcs/blob/master/text/0141-lifetime-elision.md)
- [TiKV - Use Joint Consensus](https://github.com/tikv/rfcs/blob/master/text/0054-joint-consensus.md)
-->

status: "accepted" <!-- Proposed status left out as we consider an MR as proposition -->

decision-makers: @kostja @d.rodionov @funbringer @darthunix

<!--
consulted: list everyone whose opinions are sought (typically subject-matter experts); and with whom there is a two-way communication
informed: list everyone who is kept up-to-date on progress; and with whom there is a one-way communication
-->

---

# Аудит DML-операций для выбранных юзеров

## Контекст и проблема

Аудит - логирование определенных операций в отдельный лог, для понимания кто что делал, когда и как.
Сейчас мы логируем в аудит лог DDL, ACL, результат аутентификации юзера, и некоторые DML, относящиеся к
изменению таблицы `_pico_instance`.

Хотим логировать все DML для конкретных юзеров (не для всех).

## Как делают другие

### PostgreSQL

Для аудирования предлагается использовать extension `pgaudit`.
Включается и настраивается через `ALTER SYSTEM SET` переменных `pgaudit`

```sql
CREATE EXTENSION pgaudit;
ALTER SYSTEM SET pgaudit.log = 'all, -misc';
```

Логирование для выбранных пользователей (фильтр по пользователям) - отсутствует.
Есть [issue](https://github.com/pgaudit/pgaudit/issues/239) на поддержку (без движения).

Логирование двух типов: SESSION (все стейтменты), OBJECT (стейтменты относящиеся к конкретному объекту).

Пример настройки OBJECT логирования (логируем только select и delete на таблицу account):

```sql
set pgaudit.role = 'auditor';

grant select, delete
   on public.account
   to auditor;
```

Пример настройки SESSION логирования см. выше (через `pgaudit.log`).

Пример записи из лога:

```csv
AUDIT: SESSION,1,1,DDL,CREATE TABLE,TABLE,public.account,create table account
(
    id int,
    name text,
    password text,
    description text
);,<not logged>
```

### Oracle Database

Настраивается и через конфиг, и через SQL. Поддерживаются фильтры,
в том числе по выбранным пользователям.

Пример задания фильра на пользователя:

```sql
CREATE AUDIT POLICY hr_admin_pol
  ACTIONS DELETE on hr.employees,
          INSERT on hr.employees,
          UPDATE on hr.employees,
          ALL on hr.departments;

AUDIT POLICY hr_admin_pol BY jdoe;
```

Пример задания такого же фильтра, но на все таблицы:

```sql
CREATE AUDIT POLICY hr_admin_pol
  ACTIONS DELETE ANY TABLE,
          INSERT ANY TABLE,
          UPDATE ANY TABLE;

AUDIT POLICY hr_admin_pol BY jdoe;
```

Пример задания фильтра на роль:

```sql
CREATE AUDIT POLICY hr_admin_pol
  ACTIONS DELETE on hr.employees,
          INSERT on hr.employees,
          UPDATE on hr.employees,
          ALL on hr.departments
  ROLES audit_admin;

AUDIT POLICY hr_admin_pol;
```

Или так:

```sql
CREATE AUDIT POLICY hr_admin_pol
  ACTIONS DELETE on hr.employees,
          INSERT on hr.employees,
          UPDATE on hr.employees,
          ALL on hr.departments;

AUDIT POLICY hr_admin_pol BY USERS WITH GRANTED ROLES audit_admin;
```

Выключение фильтра:

```
AUDIT POLICY hr_admin_pol EXCEPT jdoe;

```

### MySQL

Есть в Enterprise Edition в виде плагина audit_log.

```sql
INSTALL PLUGIN audit_log SONAME 'audit_log.so'
```

Поддерживаются фильтры, в том числе по выбранным пользователям.
Фильтры хранятся в системных таблицах.
Для работы предлагается использовать встроенные функции плагина.

Пример задания фильтра:

```sql
mysql> SELECT audit_log_filter_set_user('user1@localhost', 'SomeFilter');
+------------------------------------------------------------+
| audit_log_filter_set_user('user1@localhost', 'SomeFilter') |
+------------------------------------------------------------+
| OK                                                         |
+------------------------------------------------------------+
```

### MongoDB

Есть в Enterprise Edition. Настраивается в конфиге. Поддерживаются фильтры по
типам событий. Логирование для выбранных пользователей - отсутствует.

Поддерживают логирование в OCSF Schema (стандарт формата аудит логов).

```yaml
# mongod.conf
auditLog:
  destination: file
  path: /var/log/mongodb/audit.json
  format: JSON
```

### Cassandra

Настраивается в конфиге. Поддерживаются фильтры по
типам событий, кейспейсам и выбранным юзерам.

Опции в конфиге - `included_users`, `excluded_users`.

```yaml
# Audit logging - Logs every incoming CQL command request, authentication to a node. See the docs
# on audit_logging for full details about the various configuration options.
audit_logging_options:
  enabled: false
  logger:
    - class_name: BinAuditLogger
  # audit_logs_dir:
  # included_keyspaces:
  # excluded_keyspaces: system, system_schema, system_virtual_schema
  # included_categories:
  # excluded_categories:
  # included_users:
  # excluded_users:
  # roll_cycle: HOURLY
  # block: true
  # max_queue_weight: 268435456 # 256 MiB
  # max_log_size: 17179869184 # 16 GiB
  ## archive command is "/path/to/script.sh %path" where %path is replaced with the file being rolled:
  # archive_command:
  # max_archive_retries: 10
```

### Microsoft SQL Server

Поддерживают включение и настройку аудита на уровне SQL-команд.
Поддерживаются фильтры по
типам событий. Логирование для выбранных пользователей - отсутствует.

```sql
-- Create a server audit
CREATE SERVER AUDIT MyAudit
TO FILE (FILEPATH = 'C:\AuditLogs\');

-- Enable auditing for logins
CREATE DATABASE AUDIT SPECIFICATION LoginAudit
FOR SERVER AUDIT MyAudit
ADD (FAILED_LOGIN_GROUP);
```

### Tarantool

Есть в Enterprise Edition. Настраивается в конфиге.
Есть фильтры по типам событий, спейсам. Можно логировать
не весть тапл, а только ключ.

Логирование для выбранных пользователей - отсутствует.

```yaml
audit_log:
  to: file
  file: "audit_tarantool.log"
  filter: [user_create, data_operations, ddl, custom]
  format: json
  spaces: [bands]
  extract_key: true
```

Пример записи лога:

```json
{
  "time": "2024-01-24T11:43:21.566+0300",
  "uuid": "26af0a7d-1052-490a-9946-e19eacc822c9",
  "severity": "INFO",
  "remote": "unix/:(socket)",
  "session_type": "console",
  "module": "tarantool",
  "user": "admin",
  "type": "space_create",
  "tag": "",
  "description": "Create space Bands"
}
```

### YugabyteDB

Поддерживают плагин `pgaudit`. Так же настраивается, такой же формат на выходе.

### CockroachDB

Есть с enterprise лицензией.
Поддерживают включение и настройку аудита на уровне SQL-команд.
Логирование двух типов: table-based и role-based
Логирование для выбранных пользователей есть (role-based).
Фильтра по типам событий нет.

Пример включения role-based:

```sql
SET CLUSTER SETTING sql.log.user_audit = '
    test_role       NONE
    another_role    ALL
    ALL             NONE
';

GRANT test_role to test_user;
GRANT another_role to test_user;
```

Пример включения table-based:

```sql
ALTER TABLE customers EXPERIMENTAL_AUDIT SET READ WRITE;
```

### YDB

Есть аудит логи. Настраивается в конфиге.
Нет никаких фильтров. Т.е. логирования по выбранным пользователям
нет.

```yaml
audit_config:
  file_backend:
    format: audit_log_format
    file_path: "path_to_log_file"
  unified_agent_backend:
    format: audit_log_format
    log_name: session_meta_log_name
  stderr_backend:
    format: audit_log_format
```

### ScyllaDB

Есть аудит логи. Настраивается в конфиге.
Поддерживаются фильтры по
типам событий, кейспейсам и таблицам.
Два варианта audit storage: syslog или таблица.

Логирование для выбранных пользователей - отсутствует.

```yaml
# audit setting
# by default, Scylla does not audit anything.
# It is possible to enable auditing to the following places:
#   - audit.audit_log column family by setting the flag to "table"
audit: "table"
#
# List of statement categories that should be audited.
audit_categories: "DCL,DDL,AUTH"
#
# List of tables that should be audited.
audit_tables: "mykespace.mytable"
#
# List of keyspaces that should be fully audited.
# All tables in those keyspaces will be audited
audit_keyspaces: "mykespace"
```

## Как будем делать мы

Используем синтаксис Oracle со следующими ограничениями:

- policy создавать/изменять/удалять не позволяем
- policy будет одна, она захардкожена в коде (`dml_default`), позволяет аудировать
  все DML операции на все таблицы
- поддерживаем синтаксис включения на юзеров:
  `AUDIT POLICY dml_default BY user1;`
- поддерживаем синтаксис выключения на юзеров:
  `AUDIT POLICY dml_default EXCEPT user1;`
- не поддерживаем (пока) синтаксис включения/выключения на роли юзеров

Работа с аудитом DML:

- настройку для аудита храним в `_pico_db_config` (битсет `u128` по user_id, где каждый бит - флаг того,
  надо логировать или нет)
- при обработке закоммиченных записей из рафт лога смотрим в настройку и
  проверяем пользователя
- если для пользователя нужно логирование - то логируем (`crate::audit!(...)`)
