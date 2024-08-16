# Подключение и работа в консоли

В данном разделе описаны способы подключения Picodata, а также первые
шаги в консоли.

По умолчанию консоль, в которой происходит запуск инстанса Picodata,
служит для вывода отладочного журнала инстанса. Для ввода команд
следует подключиться к Picodata из другой консоли. При этом возможны
несколько вариантов подключения:

- Подключение к [консоли администратора](#admin_console) (`picodata admin`)
- Подключение к [SQL-консоли](#sql_console) (`picodata connect`)
- Подключение [по протоколу PostgreSQL](#pgproto) (`psql` и др.)

См. также:

- [Использование внешних коннекторов к Picodata](../connectors_index.md)

## Консоль администратора {: #admin_console }

### Доступ к консоли {: #admin_console_connect }

Консоль администратора предоставляет доступ к учетной записи
[Администратора СУБД](access_control.md#admin) (`admin`). Для запуска
консоли используйте следующую команду с указанием файла unix-сокета:

```
picodata admin ./admin.sock
```

По умолчанию файл unix-сокета расположен в рабочей директории инстанса,
указанной при запуске в параметре [picodata run --data-dir]. Размещение
этого файла можно переопределить параметром [picodata run --admin-sock].

[picodata run --data-dir]: ../reference/cli.md#run_data_dir
[picodata run --admin-sock]: ../reference/cli.md#run_admin_sock

При успешном подключении отобразится приглашение:

```
$ picodata admin ./admin.sock
Connected to admin console by socket path "./admin.sock"
type '\help' for interactive help
picodata>
```

Консоль администратора интерпретирует вводимые команды на языке SQL.

### Задание пароля администратора  {: #set_admin_password }

По умолчанию у Администратора СУБД отсутствует пароль, поэтому подключиться к
консоли администратора возможно только при наличии доступа к ОС, в
которой запущен инстанс Picodata. Для того чтобы подключаться к инстансу
по сети, требуется задать пароль администратора:

```sql
ALTER USER "admin" WITH PASSWORD 'T0psecret'
```

После этого Администратор СУБД сможет подключиться, используя следующую
команду:

```shell
picodata connect admin@localhost:3301
```

### Создание учетной записи пользователя  {: #user_setup }

Для того чтобы использовать пользовательскую консоль, нужно сначала
создать учетную запись пользователя в административной консоли.

Для этого можно использовать следующую команду:

```SQL
CREATE USER "alice" WITH PASSWORD 'T0psecret';
```

Чтобы новый пользователь мог создавать таблицы, ему понадобится
соответствующая привилегия:

```SQL
GRANT CREATE TABLE TO "alice";
```

### Автоматизация первичной настройки  {: #automate_setup }

Первичную настройку пользователей и их прав в консоли администратора
можно автоматизировать: сохранить набор SQL-команд в отдельный файл, и
затем подать его на вход обработчику `picodata admin`.

Для примера подготовим следующий набор команд, предварив их
[разделителем](#backslash_commands) (для поддержки многострочного
ввода):

```shell
cat ../setup.sql
```

```sql
\s d ;
ALTER USER "admin" WITH PASSWORD 'T0psecret';
CREATE USER "alice" WITH PASSWORD 'T0psecret';
GRANT CREATE TABLE TO "alice";
GRANT READ TABLE TO "alice";
GRANT WRITE TABLE TO "alice";
```

После этого можно выполнить набор команд:

```shell
picodata admin ./admin.sock < ../setup.sql
```

Вывод:

```
Connected to admin console by socket path "admin.sock"
type '\help' for interactive help
Delimiter changed to ';'
Language switched to SQL
1
1
1
1
1
Bye
```

Команды будут выполнены последовательно. Числа в начале строк означают
количество измененных строк после каждой команды. В конце управление
возвращается терминалу (консоль Picodata не останется запущенной).

## SQL-консоль {: #sql_console }

SQL-консоль позволяет выполнять распределенные SQL-команды в рамках
кластера. После того как в системе создана пользовательская учетная
запись, можно подключиться к SQL-консоли любого локального или
удаленного инстанса. Для этого используется команда `picodata connect`.
Пример для `localhost`:

```
picodata connect alice@localhost:3301
```

В соответствии с доступными ему привилегиями, пользователь сможет
работать в консоли с таблицами. Возможность пользователя подключаться
может быть ограничена [соответствующей привилегией `LOGIN`].

[соответствующей привилегией `LOGIN`]: access_control.md#privileges

См. также:

- [Работа с данными SQL](sql_examples.md)

## Встроенная справка {: #builtin_help }

Встроенная справка Picodata доступна как в административной, так и в
SQL-консоли. Справка содержит информацию о дополнительных командах в
консоли и поддерживаемых сочетаниях клавиш. Для вызова справки
введите `\help`.

### Дополнительные команды консоли {: #backslash_commands }

В консоли Picodata доступны следующие дополнительные команды:

- `\e` — открыть текстовый редактор по умолчанию (в соответствии с
  переменной `EDITOR`)
- `\set delimiter символ` — задать `символ` для разделения строк
- `\set delimiter default` — сбросить `символ` для разделения строк
  (перенос строки по нажатию ++enter++)

!!! note "Примечание"
    Поддерживается сокращенная версия
    `\set delimiter` в виде `\s d`. Пример: `\s d ;`

### Сочетания клавиш в консоли {: #hotkeys }

В консоли Picodata поддерживаются следующие сочетания клавиш:

- ++enter++ — завершение ввода
- ++alt+enter++ — переход к новой строке
- ++ctrl+c++ — отмена ввода
- ++ctrl+d++ — выход из консоли

См. также:

- [Аргументы командной строки](../reference/cli.md)

## Pgproto {: #pgproto }

Модуль Pgproto реализует протокол PostgreSQL путем эмуляции сервера
PostgreSQL. Основная цель Pgproto — предоставить пользователям возможность
взаимодействовать с Picodata с помощью большого числа хорошо знакомых
инструментов и библиотек, написанных для PostgreSQL.

### Настройка Pgproto {: #pgproto_setup }

Для запуска сервера Pgproto, принимающего подключения PostgreSQL по адресу
`localhost:5432`, нужно [запустить](../reference/cli.md#run) инстанс Picodata,
используя [опцию](../reference/cli.md#run_pg_listen) `--pg-listen`:

```bash
picodata run --pg-listen localhost:5432
```

Подключаться к серверу Pgproto могут только пользователи с паролем,
совместимым с PostgreSQL. Для создания пользователя `postgres` с подходящим
паролем в [консоли администратора](#admin_console) нужно выполнить команду:

```sql
CREATE USER "postgres" WITH PASSWORD 'P@ssw0rd' USING md5
```

!!! note "Примечание"
    В настоящее время только аутентификация по хешу MD5 совместима
    с PostgreSQL.

Для выдачи разрешения на создание таблиц пользователю `postgres` в
[консоли администратора](#admin_console) нужно выполнить команду:

```sql
GRANT CREATE TABLE TO "postgres"
```

Полученное разрешение позволит пользователю `postgres` отправить в Picodata
запрос CREATE TABLE, пример которого приведен [далее](#examples).

### Подключение к Pgproto через psql {: #pgproto_psql_connect }

Подключение к Picodata через Pgproto осуществляется с помощью
интерактивного терминала PostgreSQL
[psql](https://www.postgresql.org/docs/current/app-psql.html).

Команда для подключения к Pgproto через psql:

```bash title="Вариант № 1"
psql -U postgres -h localhost -p 5432 -W "sslmode=disable"
```

```bash title="Вариант № 2"
psql "user=postgres host=localhost port=5432 password=P@ssw0rd sslmode=disable"
```

По умолчанию SSL отключен в Pgproto, поэтому его также следует отключить
на стороне psql, используя опцию `sslmode`.

??? abstract "Включение протокола SSL в Pgproto"
    Чтобы использовать протокол SSL при подключении к Pgproto, необходимо
    сделать следующее:

    1. Задать параметр `instance.pg.ssl: true`
    [в файле конфигурации](../reference/config.md#instance_pg_ssl)
    1. Добавить в [рабочую директорию инстанса](../reference/cli.md#run_data_dir)
    `<DATA_DIR>` SSL-сертификаты:
        * `server.crt`
        * `server.key`

### Примеры {: #examples }

После подключения к Pgproto запросы можно будет отправлять в интерактивной
сессии psql. Примеры:

```sql title="Запрос CREATE TABLE"
postgres=> CREATE TABLE WAREHOUSE (
    W_ID INTEGER NOT NULL,
    W_NAME VARCHAR(10) NOT NULL,
    W_TAX DOUBLE,
    W_YTD DOUBLE,
    PRIMARY KEY (W_ID)
)
USING MEMTX DISTRIBUTED BY (W_ID);
CREATE TABLE
```

```sql title="Запрос INSERT"
postgres=> INSERT INTO WAREHOUSE (W_ID, W_NAME) VALUES (1, 'aaaa'), (2, 'aaab'), (3, 'aaac'), (4, 'aaad');
INSERT 0 4
```

```sql title="Запрос SELECT"
postgres=> SELECT W_ID, W_NAME FROM WAREHOUSE;
 "W_ID" | "W_NAME"
--------+----------
      1 | aaaa
      2 | aaab
      3 | aaac
      4 | aaad
(4 rows)
```

```sql title="Запрос SELECT с предложением WHERE"
postgres=> SELECT W_NAME FROM WAREHOUSE WHERE W_ID=1;
 "W_NAME"
----------
 aaaa
(1 row)
```

См. также:

* [CREATE TABLE](../reference/sql/create_table.md)
* [INSERT](../reference/sql/insert.md)
* [SELECT](../reference/sql/select.md)

### Ограничения Pgproto {: #pgproto_limitations }

 * Поступающие запросы без изменений передаются в Picodata в текстовом виде,
 поэтому возможно выполнение только поддерживаемых в Picodata запросов
 * [Системные каталоги
 PostgreSQL](https://www.postgresql.org/docs/current/catalogs.html) пока
 не поддерживаются
 * Pgproto работает в режиме
[autocommit](https://www.postgresql.org/docs/current/ecpg-sql-set-autocommit.html),
т. к. Picodata не поддерживает интерактивные транзакции
