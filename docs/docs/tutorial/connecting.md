# Подключение и работа в консоли

В данном разделе описаны способы подключения Picodata, а также первые
шаги в консоли.

По умолчанию окно терминала, в котором происходит запуск инстанса Picodata,
служит для вывода отладочного журнала инстанса. Для ввода команд
следует подключиться к Picodata из другого терминала. Возможны
два варианта:

- Подключение к [консоли администратора](#admin_console) (`picodata admin`)
- Подключение к пользовательской консоли по протоколу [PostgreSQL](#postgresql)

В последнем случае, в качестве клиента рекомендуется использовать
клиентское приложение `psql`.

<a name=psql></a>
??? note "Установка psql"
    Клиентское приложение `psql` уже поставляется в
    составе [готовых пакетов](https://picodata.io/download) Picodata. В
    остальных случаях для использования `psql` не требуется
    устанавливать сам сервер баз данных PostgreSQL. В Linux достаточно
    установить пакет `postgresql-client` (Debian/Ubuntu) или
    `postgresql` (RHEL/Fedora). В macOS установите пакет `libpq` через
    Homebrew.


См. также:

- [Справочник psql](https://www.postgresql.org/docs/current/app-psql.html)
- [Использование внешних коннекторов к Picodata](../connectors_index.md)
- [Работа с данными SQL](sql_examples.md)

## Консоль администратора {: #admin_console }

### Настройка и подключение {: #admin_console_connect }

Консоль администратора предоставляет доступ к учетной записи
[Администратора СУБД](../admin/access_control.md#admin) (`admin`). Для запуска
консоли используйте следующую команду с указанием файла unix-сокета:

```
picodata admin ./admin.sock
```

По умолчанию файл unix-сокета расположен в рабочей директории инстанса,
указанной в файле конфигурации в параметре [`instance.instance_dir`]. Путь
к этому файлу можно переопределить, задав параметр [`instance.admin_socket`].

[`instance.instance_dir`]: ../reference/config.md#instance_instance_dir
[`instance.admin_socket`]: ../reference/config.md#instance_admin_socket

При успешном подключении отобразится приглашение:

```
$ picodata admin ./admin.sock
Connected to admin console by socket path "./admin.sock"
type '\help' for interactive help
(admin) sql>
```

Консоль администратора интерпретирует вводимые команды на языке SQL.
Разделителем по умолчанию выступает знак `;`.

### Задание пароля администратора {: #set_admin_password }

По умолчанию у Администратора СУБД отсутствует пароль, поэтому
подключиться к консоли администратора возможно только при наличии
доступа к файлу сокета средствами ОС. Для того чтобы иметь возможность
подключиться к инстансу по сети, задайте пароль администратора:

```sql
ALTER USER "admin" WITH PASSWORD 'T0psecret';
```

После этого Администратор СУБД сможет подключиться, используя следующую
команду:

```shell
psql postgres://admin:T0psecret@127.0.0.1:4327
```

Кроме того, задать пароль администратора можно при инициализации
кластера, установив переменную окружения `PICODATA_ADMIN_PASSWORD`
для его первого инстанса. При повторных запусках данная переменная
будет игнорироваться.

См. также:

- [Управление доступом — Требования к паролю](../admin/access_control.md#allowed_passwords)

### Создание учетной записи пользователя {: #user_setup }

Для того чтобы использовать пользовательскую консоль, нужно сначала
создать учетную запись пользователя в административной консоли.
Используйте для этого следующую команду:

```SQL
CREATE USER "alice" WITH PASSWORD 'T0psecret';
```

Чтобы новый пользователь мог создавать таблицы, выдайте ему
соответствующую привилегию:

```SQL
GRANT CREATE TABLE TO "alice";
```

### Автоматизация первичной настройки {: #automate_setup }

Первичную настройку пользователей и их прав в консоли администратора
можно автоматизировать. Для этого сохраните следующий набор команд
в виде скрипта:

???+ example "setup.sql"
    ```sql
    ALTER USER "admin" WITH PASSWORD 'T0psecret';
    CREATE USER "alice" WITH PASSWORD 'T0psecret';
    GRANT CREATE TABLE TO "alice";
    GRANT READ TABLE TO "alice";
    GRANT WRITE TABLE TO "alice";
    ```

Запустите этот скрипт в консоли администратора:

```shell
picodata admin ./admin.sock < ../setup.sql
```

Пример вывода:

```
Connected to admin console by socket path "admin.sock"
type '\help' for interactive help
1
1
1
1
1
Bye
```

Команды будут выполнены последовательно. Числа означают количество
измененных строк после каждой команды.

## Подключение по протоколу PostgreSQL {: #postgresql }

После того как в системе создана пользовательская учетная запись, можно
подключиться к любому локальному или удаленному инстансу и выполнять
распределенные SQL-команды в рамках кластера. Picodata позволяет это
делать при помощи большого числа хорошо знакомых инструментов и
библиотек, написанных для PostgreSQL. Рекомендуемым способом подключения
к инстансу Picodata является CLI-приложение `psql`](#). По
умолчанию используется порт 4327 (его можно переопределить, задав
параметр конфигурации [`instance.pg.listen`]).

Пример для `127.0.0.1`:

```shell
psql postgres://admin:T0psecret@127.0.0.1:4327
```

[`instance.pg.listen`]: ../reference/config.md#instance_admin_socket

В соответствии с доступными ему привилегиями, пользователь сможет
работать в консоли с таблицами. Разделителем команд выступает знак
`;`.

!!! note "Примечание"
    Подключение к по протоколу PostgreSQL доступно
    только для пользователей, использующих [методы аутентификации] `md5`
    и `ldap`. В случае с `ldap` потребуется [дополнительная
    настройка](../admin/ldap.md).

[методы аутентификации]: ../admin/access_control.md#auth_types

Для выхода из консоли введите `\quit` или `\q`.

### Перенаправление команд в консоли {: #pipe }

При работе в консоли поддерживается перенаправление ввода-вывода команд
([pipe]). Таким способом можно сделать как отдельный SQL-запрос, так и
запустить файл со списком SQL-команд. Примеры:

```sql title="Отдельная команда в консоли psql"
echo "SELECT * FROM warehouse;" | psql postgres://admin:T0psecret@127.0.0.1:4327
```

```sql title="Список команд в административной консоли"
cat file.sql | picodata admin ./admin.sock
```

??? example "Содержимое файла file.sql"
    ```sql
    INSERT INTO customers VALUES (1, 'customer1');
    SELECT name FROM customers limit 10;
    DELETE FROM customers WHERE id > 100;
    ```

[pipe]: https://ru.wikipedia.org/wiki/Конвейер_(Unix)

### Ограничения протокола PostgreSQL в Picodata {: #pgproto_limitations }

* Поступающие запросы без изменений передаются в Picodata в текстовом виде,
  поэтому возможно выполнение только поддерживаемых в Picodata запросов
* [Системные каталоги PostgreSQL] реализованы частично
* При авторизации в пользовательской консоли не поддерживается метод
  аутентификации `chap-sha1`
* По умолчанию подключение по протоколу PostgreSQL использует режим
  [autocommit]. Команды [BEGIN], [COMMIT], [ROLLBACK], применяемые для
 управления интерактивными транзакциями, реализованы только как
 заглушки
* Количество одновременно исполняемых запросов ограничено системными
  параметрами [pg_portal_max] и [pg_statement_max]

[Системные каталоги PostgreSQL]: https://www.postgresql.org/docs/current/catalogs.html
[autocommit]: https://www.postgresql.org/docs/current/ecpg-sql-set-autocommit.html
[BEGIN]: https://www.postgresql.org/docs/current/sql-begin.html
[COMMIT]: https://www.postgresql.org/docs/current/sql-commit.html
[ROLLBACK]: https://www.postgresql.org/docs/current/sql-rollback.html
[pg_portal_max]: ../reference/db_config.md#pg_portal_max
[pg_statement_max]: ../reference/db_config.md#pg_statement_max
