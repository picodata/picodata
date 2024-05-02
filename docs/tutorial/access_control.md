# Управление доступом

В данном разделе описана ролевая модель управления доступом в Picodata и
приведены примеры необходимых для ее настройки с помощью
[DCL](../reference/sql/dcl.md)-команд языка SQL.

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

См. также:

- [Википедия — Управление доступом на основе ролей][Wiki - RBAC]
- [Википедия — Избирательное управление доступом][Wiki - DAC]
[Wiki - RBAC]: https://ru.wikipedia.org/wiki/Управление_доступом_на_основе_ролей
[Wiki - DAC]: https://ru.wikipedia.org/wiki/Избирательное_управление_доступом

### Объекты доступа {: #access_objects }

<!--
7.1 В СУБД 6, 5, 4 классов защиты должны быть реализованы дискреционный
и ролевой методы управления доступом.
-->

Picodata является распределенной СУБД, и управление доступом происходит
централизованно на всем кластере. Для управления доступом в Picodata
используются дискреционный и ролевой методы. Объектами доступа являются:

<!-- Keep in sync with overview/glossary.md#access_objects -->
- `table` – [таблица БД](#tables_access)
- `user` – учетная запись [пользователя СУБД](#users)
- `role` – [роль](#roles)
- `procedure` – [процедура](#proc_access)

<!--
Дискреционный метод управления доступом субъектов доступа к объектам
доступа СУБД (БД, таблица, запись или столбец, поле, представление,
процедура (программный код) или иные объекты доступа) должен
осуществляться на основе настраиваемых списков управления доступом
(матриц управления доступом).
-->

Доступ к объектам предоставляется на основе настраиваемого списка
управления доступом (access control list, ACL), который определяет,
какими привилегиями обладает пользователь или роль.

### Привилегии {: #privileges }

В Picodata определены следующие виды привилегий:

- привилегии для работы с пользователями (`CREATE USER`, `ALTER USER`,
  `DROP USER`)
- привилегии для работы с ролями (`CREATE ROLE`, `DROP ROLE`)
- привилегии для работы с таблицами (`CREATE TABLE`, `ALTER TABLE`,
  `DROP TABLE`, `READ TABLE`, `WRITE TABLE`)
- привилегии для работы с процедурами (`CREATE PROCEDURE`, `DROP PROCEDURE`,
  `EXECUTE PROCEDURE`)
- `LOGIN` — право подключаться к экземпляру кластера. Автоматически выдается
  новым пользователям при создании.

### Пользователи СУБД {: #users }

При подключении к системе пользователь указывает имя учетной записи.
Действия, которые пользователь может совершать в системе, определяются
выданными ему привилегиями.

Каждый объект в системе (таблица, роль, учетная запись, процедура) имеет
привязанного к нему владельца — пользователя СУБД. Владельцем объекта
является его создатель. Он обладает неотъемлемыми полными привилегиями
на этот объект.

Picodata предоставляет несколько встроенных учетных записей:

- `admin` — Администратор СУБД
- `pico_service` — служебный пользователь, от имени которого происходит
  коммуникация между инстансами кластера
- `guest` — неавторизованный пользователь

<!--
Ролевой метод управления доступом должен быть реализован для следующих
ролей пользователей СУБД:

- администратор СУБД,
- администратор БД (администратор информационной системы),
- пользователь БД (пользователь информационной системы).
-->

#### Администратор СУБД {: #admin }

<!--
СУБД должна обеспечивать наделение администратора СУБД следующими правами:

- создавать учетные записи пользователей СУБД;
- модифицировать, блокировать и удалять учетные записи пользователей СУБД;
- назначать права доступа пользователям СУБД к объектам доступа СУБД;
- управлять конфигурацией СУБД;
- создавать, подключать БД.
-->

Администратором СУБД является встроенный пользователь `admin`.

Администратор СУБД является _суперпользователем_ и обладает следующими
привилегиями:

- создавать учетные записи пользователей СУБД
- модифицировать, блокировать и удалять учетные записи пользователей СУБД
- назначать права доступа пользователям СУБД к объектам доступа СУБД
- управлять конфигурацией СУБД
- создавать, подключать БД

#### Администратор БД {: #db_admin }

<!--
СУБД должна обеспечивать наделение администратора БД следующими правами:

- создавать учетные записи пользователей БД;
- модифицировать, блокировать и удалять учетные записи пользователей БД;
- управлять конфигурацией БД;
- назначать права доступа пользователям БД к объектам доступа БД;
- создавать резервные копии БД и восстанавливать БД из резервной копии;
- создавать, модифицировать и удалять процедуры (программный код), хранимые в БД.
-->

Наделить пользователя СУБД правами Администратора БД можно следующим
набором SQL-команд:

```sql
GRANT CREATE TABLE TO <grantee>
GRANT CREATE USER TO <grantee>
GRANT CREATE ROLE TO <grantee>
GRANT CREATE PROCEDURE TO <grantee>
```

Это обеспечивает наличие у администратора БД следующих прав:

- создавать учетные записи пользователей БД
- модифицировать, блокировать и удалять учетные записи пользователей БД
- управлять конфигурацией БД <!-- схемой данных -->
- назначать права доступа пользователям БД к объектам доступа БД
- создавать резервные копии БД и восстанавливать БД из резервной копии
- создавать, модифицировать и удалять хранимые процедуры

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

-- CREATE PROCEDURE <procedure name>
GRANT ALTER ON PROCEDURE <procedure name> TO <owner>
GRANT EXECUTE ON PROCEDURE <procedure name> TO <owner>
GRANT DROP ON PROCEDURE <procedure name> TO <owner>
```

#### Пользователь БД {: #db_user }

<!--
СУБД должна обеспечивать наделение пользователя БД следующими правами:

- создавать и манипулировать объектами доступа БД (таблица, запись или
  столбец, поле, представление и иные объекты доступа);
- выполнять процедуры (программный код), хранимые в БД.
-->

Picodata позволяет наделить пользователя БД следующими правами:

- создавать и манипулировать таблицами БД
- создавать и манипулировать хранимыми процедурами
- выполнять хранимые процедуры

Для этого используйте следующие SQL-команды:

```sql
GRANT CREATE TABLE TO <grantee>
GRANT ALTER ON TABLE <table name> TO <grantee>
GRANT DROP ON TABLE <table name> TO <grantee>
GRANT READ ON TABLE <table name> TO <grantee>
GRANT WRITE ON TABLE <table name> TO <grantee>

GRANT CREATE PROCEDURE TO <grantee>
GRANT ALTER ON PROCEDURE <procedure name> TO <grantee>
GRANT DROP ON PROCEDURE <procedure name> TO <grantee>

GRANT EXECUTE ON PROCEDURE <procedure name> TO <grantee>
```

### Роли {: #roles }

Роль представляет собой именованную группу привилегий, что позволяет
структурировать управление доступом. Чтобы выдать или отозвать
привилегию у роли, используйте команды [GRANT](../reference/sql/grant.md)
и [REVOKE](../reference/sql/revoke.md).

Названия ролей `public` и `super` являются зарезервированными.

Роль `public` автоматически назначается всем создаваемым
пользователям. Наделение роли `public` привилегией на какой-либо объект
делает его общедоступным.

Роль `super` является системной и используется исключительно для
внутренних целей.

## Начало работы {: #getting_started }

Для начала работы с пользователями, привилегиями и ролями следует
[подключиться](../tutorial/connecting.md) к инстансу Picodata.

## Управление пользователями {: #user_management }

### Создание {: #create_user }

Для создания пользователя используйте SQL-команду [CREATE
USER](../reference/sql/create_user.md).

Пример:

```sql
CREATE USER alice WITH PASSWORD 'T0tallysecret' USING chap-sha1
CREATE USER bob USING ldap
```

Для имени пользователя (и в целом для объектов в Picodata) действуют
[правила использования допустимых
символов](../reference/sql/object.md). Также следует учитывать
[требования](#allowed_passwords) к длине и сложности пароля.

Для выполнения команды требуется привилегия `CREATE USER`:

```sql
GRANT CREATE USER TO <grantee>
```

Такое право по умолчанию есть у [Администратора СУБД](#admin) (`admin`).

### Модификация {: #alter_user }

Для изменения учетных данных пользователя используйте SQL-команду [ALTER
USER](../reference/sql/alter_user.md).

Пример блокировки / разблокировки пользователя:

```sql
ALTER USER alice WITH NOLOGIN
ALTER USER alice WITH LOGIN
```

Для выполнения команды требуется привилегия `ALTER USER` на
конкретную учетную запись или на все учетные записи сразу:

```sql
GRANT ALTER ON USER <user name> TO <grantee>
GRANT ALTER USER TO <grantee>
```

Привилегия `ALTER USER` есть по умолчанию у создателя данной учетной
записи и у [Администратора СУБД](#admin).

### Удаление {: #drop_user }

Для удаления пользователя используйте SQL-команду
[DROP USER](../reference/sql/drop_user.md).

Пример:

```sql
DROP USER alice
```

Для выполнения команды требуется привилегия `DROP USER` на
конкретную учетную запись или на все учетные записи сразу:

```sql
GRANT DROP ON USER <user name> TO <grantee>
GRANT DROP USER TO <grantee>
```

Привилегия `DROP USER` есть по умолчанию у создателя данной учетной
записи и у [Администратора СУБД](#admin).

### Требования к паролю {: #allowed_passwords }

При установке или изменении пароля пользователя следует учитывать
требования к его длине и сложности:

- пароль должен быть не короче 8 символов
- пароль должен одновременно содержать минимум один символ в нижнем
  регистре, символ в верхнем регистре и одну цифру

Пример некорректного пароля: `topsecret`

Пример корректного пароля: `T0psecret`

<!--
Требования к паролю хранятся в системной таблице
[_pico_property](../architecture/system_tables.md#_pico_property).

PasswordEnforceUppercase (default value: true)
PasswordEnforceLowercase (default value: true)
PasswordEnforceDigits (default value: true)
PasswordEnforceSpecialchars (default value: false)
-->

!!! note "Примечание"
    Требования к паролю применимы при использовании
    методов аутентификации `chap-sha1` и `md5`. Для метода `ldap` пароль не
    требуется и игнорируется.


### Использование ролей {: #role_management }

Для создания и удаления ролей используйте команды
[CREATE ROLE](../reference/sql/create_role.md) и
[DROP ROLE](../reference/sql/drop_role.md).

Выполнение данных действий требует наличия привилегий `CREATE ROLE` /
`DROP ROLE` соответственно.

Для назначения роли используйте команду [GRANT](../reference/sql/grant.md):

```sql
GRANT <role name> TO <grantee>
```

Назначение привилегий роли:

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

Отозвать роль можно с помощью команды [REVOKE](../reference/sql/revoke.md):

```sql
REVOKE <role name> FROM <grantee>
```

## Управление доступом к таблицам {: #tables_access }

<!--
Списки управления доступом (матрицы управления доступом) должны
позволять задавать разрешение или запрет пользователям и процедурам
(программному коду), хранимым в БД, выполнять следующие операции в
отношении объектов доступа СУБД (БД, таблица, запись или столбец, поле,
представление или иные объекты доступа): создание, модификация,
удаление, чтение.
-->

Picodata позволяет задавать разрешение пользователям и процедурам
выполнять следующие операции в отношении таблиц БД: создание,
модификация, удаление, чтение.

Для наделения пользователя и созданных им процедур указанными
привилегиями используйте SQL-команду [GRANT](../reference/sql/grant.md):

<!-- Keep in sync with #db_user -->
```sql
GRANT CREATE TABLE TO <grantee>
GRANT ALTER ON TABLE <table name> TO <grantee>
GRANT DROP ON TABLE <table name> TO <grantee>
GRANT READ ON TABLE <table name> TO <grantee>
```

Отозвать привилегию можно SQL-командой
[REVOKE](../reference/sql/revoke.md):

```sql
REVOKE <priv> ON TABLE <table name> FROM <grantee>
```

## Управление доступом к процедурам {: #proc_access }

<!--
Списки управления доступом (матрицы управления доступом) должны
позволять задавать разрешение или запрет пользователям СУБД выполнять
следующие операции в отношении процедур (программного кода), хранимых в
БД:

- создание;
- модификация, удаление, исполнение.
-->

Picodata позволяет задавать разрешение пользователям СУБД
выполнять следующие операции в отношении процедур: создание,
модификация, удаление, исполнение.

Для наделения пользователя указанными привилегиями используйте
SQL-команду [GRANT](../reference/sql/grant.md):

<!-- Keep in sync with #db_user -->
```sql
GRANT CREATE PROCEDURE TO <grantee>
GRANT ALTER ON PROCEDURE <procedure name> TO <grantee>
GRANT DROP ON PROCEDURE <procedure name> TO <grantee>
GRANT EXECUTE ON PROCEDURE <procedure name> TO <grantee>
```

Отозвать привилегию можно SQL-командой
[REVOKE](../reference/sql/revoke.md):

```sql
REVOKE <priv> ON PROCEDURE <procedure name> FROM <grantee>
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

GRANT EXECUTE ON PROCEDURE <proc name> TO <grantee>
GRANT DROP ON PROCEDURE <proc name> TO <grantee>

GRANT <role name> TO <grantee>
```
