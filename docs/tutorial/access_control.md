# Управление доступом

В данном разделе описана ролевая модель управления доступом в Picodata и
приведены примеры необходимых для ее настройки SQL-команд.

## Ролевая модель {: #role_model }

Ролевая модель Picodata позволяет гибко разграничивать возможности
пользователей распределенной системы. Разграничение строится на базе
трех основных понятий: [пользователей](#users),
[привилегий](#privileges) и [ролей](#roles). Привилегии можно назначать
напрямую пользователям:

![Users and privileges](../images/user_priv.svg)

Также, привилегии могут быть назначены роли, которая, в свою очередь,
может быть присвоена пользователю:

![Users and roles](../images/user_roles.svg)

У каждого пользователя может быть одна или несколько ролей. Каждому
пользователю или роли может быть назначена одна или несколько
привилегий. При этом, сразу после создания новая роль не содержит
привилегий: их нужно явно ей присвоить.

Пользователь, который назначает привилегию, должен сам обладать ею.
Глобальные привилегии может назначать только [администратор СУБД](#admin).

### Объекты доступа {: #access_objects }

Picodata является распределенной СУБД, и управление доступом происходит
централизованно на всем кластере. Для управления доступом в Picodata
используются дискреционный и ролевой методы. Объектами доступа являются:

- `table` – [таблица](../overview/glossary.md#table) БД
- `user` – [пользователь](#users) СУБД
- `role` – [роль](#roles)
<!-- – `procedure` – хранимая процедура на языке SQL -->

Доступ к объектам предоставляется на основе настраиваемого списка
управления доступом (access control list, [ACL](../reference/sql_queries.md#acl)), который определяет,
какими привилегиями обладает каждый субъект (пользователь или роль).

### Привилегии {: #privileges }

В Picodata определены следующие виды привилегий:

- привилегии для работы с пользователями (`CREATE USER`, `ALTER USER`,
  `DROP USER`, `SESSION`)
- привилегии для работы с ролями (`CREATE ROLE`, `DROP ROLE`)
- привилегии для работы с таблицами (`CREATE TABLE`, `ALTER TABLE`,
  `DROP TABLE`, `READ TABLE`, `WRITE TABLE`)

_Примечание_:

`SESSION` — право подключаться по сети при помощи команд `picodata
connect` и `picodata sql`. Автоматически выдается новым пользователям
при создании, но по умолчанию отключена для [Администратора СУБД](#admin)
(`admin`). В отличие от других привилегий, для выдачи или отзыва
привилегии `SESSION` [используется](#alter_user) SQL-синтаксис `ALTER <user name> {
LOGIN / NOLOGIN }`

Информация о привилегиях хранится в системной таблице [_pico_privilege](../architecture/system_tables.md#_pico_privilege).

### Пользователи {: #users }

При подключении к системе пользователь указывает имя учетной записи.
Действия, которые пользователь может совершать в системе, определяются
выданными ему привилегиями.

Каждый объект в системе (таблица, роль, пользователь) имеет привязанного
к нему владельца — пользователя СУБД. Владелец объекта автоматически
получает все привилегии на этот объект.

Picodata предоставляет два встроенных пользователя — `admin` и `guest`.

#### Администратор СУБД {: #admin }

Администратором СУБД является встроенный пользователь `admin`.

Администратор СУБД является _суперпользователем_ и обладает следующими
привилегиями:

- создания учетных записей пользователей СУБД
- модифицировать, блокировать и удалять учетные записи пользователей СУБД
- назначать права доступа пользователям СУБД к объектам доступа СУБД
- управлять конфигурацией СУБД
- создавать, подключать БД

Данный набор привилегий эквивалентен SQL-командам:

```sql
GRANT CREATE TABLE TO "admin"
GRANT ALTER TABLE TO "admin"
GRANT DROP TABLE TO "admin"
GRANT READ TABLE TO "admin"
GRANT WRITE TABLE TO "admin"

GRANT CREATE USER TO "admin"
GRANT ALTER USER TO "admin"
GRANT DROP USER TO "admin"

GRANT CREATE ROLE TO "admin"
GRANT DROP ROLE TO "admin"
```

Однако, у него по умолчанию отсутствует привилегия `SESSION`.
Использовать учетную запись можно двумя способами:

- запустив инстанс с [интерактивной консолью](../reference/cli.md#interactive) `picodata run -i`
- подключившись к [консоли администратора](../reference/cli.md#admin_sock) командой [`picodata admin`](../reference/cli.md#admin_command)

<!--
#### Гость {: #guest }

Встроенный пользователь `guest` по умолчанию обладает минимальным
набором привилегий: у него есть роль [`public`](#public) и право подключаться к
системе. Это описывается следующими командами:

- `GRANT "public" TO "guest"`
- `ALTER USER "guest" LOGIN`
-->

#### Администратор БД {: #db_admin }

Наделить пользователя правами Администратора БД можно набором SQL-команд:

```sql
GRANT CREATE TABLE TO <grantee>
GRANT CREATE USER TO <grantee>
GRANT CREATE ROLE TO <grantee>
```

Это обеспечивает наличие у администратора БД следующих прав:

- создавать учетные записи пользователей БД
- модифицировать, блокировать и удалять учетные записи пользователей БД
- управлять конфигурацией БД
- назначать права доступа пользователям БД к объектам доступа БД
- создавать резервные копии БД и восстанавливать БД из резервной копии

При создании объекта пользователь становится его владельцем и
автоматически получает на него следующие права (в зависимости от типа
объекта):

```sql
-- CREATE TABLE <table name> ...
GRANT ALTER ON TABLE <table name> TO <owner>
GRANT DROP ON TABLE <table name> TO <owner>
GRANT READ ON TABLE <table name> TO <owner>
GRANT WRITE ON TABLE <table name> TO <owner>

-- CREATE USER <user name>
GRANT ALTER ON USER <user name> TO <owner>
GRANT DROP ON USER <user name> TO <owner>

-- CREATE ROLE <role name>
GRANT DROP ON ROLE <role name> TO <owner>
```

### Роли {: #roles }

Роль представляет собой именованную группу привилегий, что позволяет
структурировать управление доступом. Добавление привилегий в роль
производится командой `GRANT`, которая наделяет роль
привилегией. Кроме того, роли могут быть вложенными друг в друга.
Пример:

```sql
GRANT <role name> TO <role name>
```

Информация о ролях хранится системной таблице `_pico_role`. Данная
таблица имеет следующую структуру:

- `id` – уникальный идентификатор роли
- `name` – имя роли
- `owner_id` – идентификатор владельца

#### Встроенные роли {: #builtin_roles }

Помимо создаваемых пользователями ролей, Picodata предоставляет
следующие системные роли:

##### public {: #public }

Роль `public` автоматически назначается всем создаваемым
пользователям. Наделение роли `publiс` привилегией на какой-либо объект
делает этот объект общедоступным.

##### super {: #super }

Роль `super` имеет все привилегии, доступные пользователю [`admin`](#admin).

## Начало работы {: #getting_started }

Для начала работы с пользователями, привилегиями и ролями следует
[подключиться](../tutorial/connecting.md) к инстансу Picodata.

## Управление пользователями {: #user_management }

### Создание {: #create_user }

Для создания пользователя используйте SQL-команду [CREATE
USER](../reference/sql_queries.md#create_user):

```sql
CREATE USER <user name>
    [ [ WITH ] PASSWORD 'password' ]
    [ USING chap-sha1 | md5 | ldap ]
```

Пример:

```sql
CREATE USER "alice" WITH PASSWORD 'totallysecret' USING chap-sha1
CREATE USER "bob" USING ldap
```

При использовании методов аутентификации `chap-sha1` и `md5` требуется
использовать пароль не короче 8 символов, для метода `ldap` пароль не
требуется и игнорируется.

Для выполнения команды требуется привилегия `CREATE USER`:

```sql
GRANT CREATE USER TO <grantee>
```

Такое право по умолчанию есть у [Администратора СУБД](#admin) (`admin`).

### Изменение {: #alter_user }

Для изменения учетных данных пользователя используйте SQL-команду [ALTER
USER](../reference/sql_queries.md#alter_user):

```sql
ALTER USER <user name>
    [ WITH ] [ LOGIN ] | [ NOLOGIN ] |
    [ PASSWORD <password> [ USING chap-sha1 | md5 | ldap ] ]
```

Возможные действия:

- `LOGIN` / `NOLOGIN` — включение/выключение привилегии `SESSION`
- `WITH PASSWORD` — установка пароля пользователя
- `USING` — выбора метода аутентификации

Пример блокировки пользователя (отзыва привилегий `SESSION`):

```sql
ALTER USER "alice" WITH NOLOGIN
```

Обратное действие (разблокировка) с возвращением этой привилегии:

```sql
ALTER USER "alice" WITH LOGIN
```

Для использования `ALTER USER` требуется иметь эту привилегию. Ее можно выдать на конкретного
пользователя:

```sql
GRANT ALTER ON USER <user name> TO <grantee>
```

Или на всех пользователей сразу:

```sql
GRANT ALTER USER TO <grantee>
```

Привилегия `ALTER USER` по умолчанию есть у владельца пользователя и у
[Администратора СУБД](#admin).

### Удаление {: #drop_user }

Для удаление пользователя используйте SQL-команду [DROP
USER](../reference/sql_queries.md#drop_user):

```sql
DROP USER <user name>
```

Удаление пользователя приведет к ошибке если в системе есть объекты,
владельцем которых он является (что соответствует опции `RESTRICT` из
ANSI SQL).

Для удаления пользователя требуется привилегия `DROP USER` на
конкретного пользователя или на всех пользователей сразу:

```sql
GRANT DROP ON USER <user name> TO <grantee>
GRANT DROP USER TO <grantee>
```

Такие есть по умолчанию у [Администратора СУБД](#admin) и у владельца
пользователя.

### Использование ролей {: #role_management }

Для [создания](../reference/sql_queries.md#create_role) и
[удаления](../reference/sql_queries.md#drop_role) ролей используйте
следующие команды:

```sql
CREATE ROLE <role name>
DROP ROLE <role name>
```

Выполнение данных действий требует наличия привилегий `CREATE ROLE` /
`DROP ROLE` соответственно.

Для назначения роли используйте следующую команду:

```sql
GRANT <role name> TO <grantee>
```

Назначение привилегий роли происходит при помощи команды `GRANT`:

```sql
GRANT <action> ON <object name> TO <grantee>
```

В качестве `grantee` может выступать идентификатор как роли, так и
пользователя. Стоит отметить, что не все привилегии можно выдать ролям,
например, привилегия `SESSION` не может быть выдана другой роли при
помощи `GRANT`, а только командой [`ALTER USER`](#alter_user).

Выдать или отозвать роль может только ее создатель.
Исключением является [администратор СУБД](#admin),
который может выдать или отозвать любую роль.

Отозвать роль можно следующим образом:

```sql
REVOKE <role name> FROM <grantee>
```

## Управление доступом к таблицам {: #tables_access }

В Picodata доступно создание и удаление таблиц. Над таблицами можно
совершать операции чтения и записи. Для ограничения доступа к операциям
с таблицами в Picodata доступны привилегии `CREATE`, `DROP`, `READ`,
`WRITE` и `ALTER`. Все привилегии (кроме `CREATE`) могут быть выданы на
конкретную таблицу или на все таблицы сразу. Привилегия `CREATE` может быть
выдана только глобально.

Выдача привилегий осуществляется командой `GRANT`. На все таблицы:

```sql
GRANT <priv> TABLE TO <grantee>
```

На конкретную таблицу:

```sql
GRANT <priv> ON TABLE <table name> TO <grantee>
```

Отозвать выданные привилегии можно при помощи команды `REVOKE`:

```sql
REVOKE <priv> ON TABLE <table name> FROM <grantee>
```

## Дополнительные примеры SQL-запросов {: #sql_examples }

```sql
CREATE USER <user name>
    [ [ WITH ] PASSWORD 'password' ]
    [ USING chap-sha1 | md5 | ldap ]
ALTER USER <user name>
    [ [ WITH ] PASSWORD 'password' ]
    [ USING chap-sha1 | md5 | ldap ]
DROP USER <user name>

CREATE ROLE <role name>
DROP ROLE <role name>

GRANT READ ON TABLE <table name> TO <grantee>
GRANT READ TABLE TO <grantee>

GRANT WRITE TABLE TO <grantee>
GRANT WRITE ON TABLE <table name> TO <grantee>

GRANT CREATE TABLE TO <grantee>
GRANT CREATE ROLE TO <grantee>
GRANT CREATE USER TO <grantee>

GRANT ALTER TABLE TO <grantee> // alter any table
GRANT ALTER ON TABLE <table name> TO <grantee>
GRANT ALTER USER to <grantee>
GRANT ALTER ON USER <user name> to <grantee>

GRANT DROP TABLE TO <grantee>
GRANT DROP USER TO <grantee>
GRANT DROP ROLE TO <grantee>
GRANT DROP ON TABLE <table name> TO <grantee>
GRANT DROP ON USER <user name> TO <grantee>
GRANT DROP ON ROLE <role name> TO <grantee>

GRANT <role name> TO <grantee>
```

См. также:

- [Википедия — Управление доступом на основе ролей](https://ru.wikipedia.org/wiki/Управление_доступом_на_основе_ролей){:target="_blank"}
- [Википедия — Избирательное управление доступом](https://ru.wikipedia.org/wiki/Избирательное_управление_доступом){:target="_blank"}
- [Описание системных таблиц](../architecture/system_tables.md)
