# Подключение и работа в консоли

В данном разделе описаны способы подключения Picodata, а также первые
шаги в консоли.

По умолчанию консоль, в которой происходит запуск инстанса Picodata,
служит для вывода отладочного журнала инстанса. Для ввода команд
следует подключиться к Picodata из другой консоли. Возможны
несколько вариантов:

- Подключение к [консоли администратора](#admin_console) (`picodata admin`)
- Подключение к [SQL-консоли](#sql_console) (`picodata connect`)
- Подключение по [протоколу PostgreSQL](#pgproto) (`psql` и др.)

См. также:

- [Использование внешних коннекторов к Picodata](../connectors_index.md)
- [Работа с данными SQL](sql_examples.md)

## Консоль администратора {: #admin_console }

### Настройка и подключение  {: #admin_console_connect }

Консоль администратора предоставляет доступ к учетной записи
[Администратора СУБД](access_control.md#admin) (`admin`). Для запуска
консоли используйте следующую команду с указанием файла unix-сокета:

```
picodata admin ./admin.sock
```

По умолчанию файл unix-сокета расположен в рабочей директории инстанса,
указанной при запуске в параметре [`picodata run --data-dir`]. Путь
к этому файлу можно переопределить параметром [`picodata run --admin-sock`].

[`picodata run --data-dir`]: ../reference/cli.md#run_data_dir
[`picodata run --admin-sock`]: ../reference/cli.md#run_admin_sock

При успешном подключении отобразится приглашение:

```
$ picodata admin ./admin.sock
Connected to admin console by socket path "./admin.sock"
type '\help' for interactive help
picodata>
```

Консоль администратора интерпретирует вводимые команды на языке SQL.
Разделителем по умолчанию выступает знак `;`.

См. также:

  - [Дополнительные команды](#backslash_commands)

### Задание пароля администратора  {: #set_admin_password }

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
picodata connect admin@127.0.0.1:3301
```

Кроме того, задать пароль администратора можно при инициализации
кластера, установив переменную окружения `PICODATA_ADMIN_PASSWORD`
для его первого инстанса. При повторных запусках данная переменная
будет игнорироваться.

См. также:

- [Управление доступом — Требования к паролю](../tutorial/access_control.md#allowed_passwords)

### Создание учетной записи пользователя  {: #user_setup }

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

### Автоматизация первичной настройки  {: #automate_setup }

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

## SQL-консоль {: #sql_console }

SQL-консоль позволяет выполнять распределенные SQL-команды в рамках
кластера. После того как в системе создана пользовательская учетная
запись, можно подключиться к SQL-консоли любого локального или
удаленного инстанса. Для этого используйте команду `picodata connect`.
Пример для `127.0.0.1`:

```shell
picodata connect alice@127.0.0.1:3301
```

В соответствии с доступными ему привилегиями, пользователь сможет
работать в консоли с таблицами. Разделителем по умолчанию выступает знак
`;`.

См. также:

  - [Дополнительные команды](#backslash_commands)

## Перенаправление команд в консоли {: #pipe }

При использовании административной или SQL-консоли поддерживается
перенаправление ввода-вывода команд ([pipe]). Таким способом можно сделать
как отдельный SQL-запрос, так и запустить файл со списком SQL-команд.
Примеры:

```sql title="Отдельная команда в SQL-консоли"
echo "SELECT * FROM warehouse;" | picodata connect alice@127.0.0.1:3301
```

```sql title="Список команд в административной консоли"
cat file.sql  | picodata admin ./admin.sock
```

??? example "Пример файла с командами"
    ```sql
    INSERT INTO customers VALUES (1, 'customer1');
    SELECT name FROM customers limit 10;
    DELETE FROM customers WHERE id > 100;
    ```

[pipe]: https://ru.wikipedia.org/wiki/Конвейер_(Unix)

## Встроенная справка в консоли {: #builtin_help }

Встроенная справка Picodata доступна в административной и в
SQL-консоли. Справка содержит информацию о дополнительных командах в
консоли и поддерживаемых сочетаниях клавиш. Для вызова справки
введите `\help`.

### Дополнительные команды {: #backslash_commands }

В консоли Picodata доступны следующие дополнительные команды:

- `\e` — открыть текстовый редактор по умолчанию (в соответствии с
  переменной `EDITOR`)
- `\set delimiter символ` — задать `символ` для разделения строк
- `\set delimiter enter` — задать клавишу ++enter++ для разделения строк
- `\set delimiter default` — сбросить `символ` для разделения строк на
  значение по умолчанию (разделение с помощью `;`)

### Сочетания клавиш {: #hotkeys }

В консоли Picodata поддерживаются следующие сочетания клавиш:

- ++enter++ — завершение ввода
- ++alt+enter++ — переход к новой строке
- ++ctrl+c++ — отмена ввода
- ++ctrl+d++ — выход из консоли

## Протокол PostgreSQL {: #pgproto }

Picodata позволяет пользователям взаимодействовать с кластером при
помощи большого числа хорошо знакомых инструментов и библиотек,
написанных для PostgreSQL.

### Настройка и подключение {: #setup }

Для включения протокола PostgreSQL укажите параметр [`picodata run
--pg-listen`] при запуске инстанса Picodata:

```bash
picodata run --pg-listen 127.0.0.1:5432
```

Это позволит инстансу принимать подключения в качестве сервера PostgreSQL.

[`picodata run --pg-listen`]: ../reference/cli.md#run_pg_listen

Перед подключением убедитесь, что в Picodata создана [пользовательская учетная запись](#user_setup).

Подключитесь к Picodata через [psql] — интерактивный терминал
PostgreSQL:

```shell
psql postgres://alice:T0psecret@127.0.0.1:5432?sslmode=disable
```

[psql]: https://www.postgresql.org/docs/current/app-psql.html

По умолчанию SSL для PostgreSQL-сервера в Picodata отключен, поэтому его также следует отключить
на стороне `psql`, используя опцию `sslmode`.

??? abstract "Включение протокола SSL"
    Чтобы использовать протокол SSL при подключении к Picodata по
    протоколу PostgreSQL, необходимо сделать следующее:

    1. Задайте в [файле конфигурации](../reference/config.md#instance_pg_ssl)
       параметр `instance.pg.ssl: true`

    1. Добавьте в [рабочую директорию инстанса](../reference/cli.md#run_data_dir)
       `<DATA_DIR>` SSL-сертификат и ключ `server.crt`, `server.key`

    1. (опционально) Для включения [mTLS] добавьте в
       [рабочую директорию инстанса](../reference/cli.md#run_data_dir) `<DATA_DIR>`
       SSL-сертификат `ca.crt`. В результате PostgreSQL-сервер в Picodata будет
       принимать подключения только в том случае, если клиент предоставит сертификат,
       подписанный с помощью `ca.crt`.

[mTLS]: https://en.wikipedia.org/wiki/Mutual_authentication

### Примеры запросов {: #examples }

Примеры запросов в интерактивной сессии `psql`:

```sql title="Создание таблицы"
postgres=> CREATE TABLE WAREHOUSE (
    W_ID INTEGER NOT NULL,
    W_NAME VARCHAR(10) NOT NULL,
    W_TAX DOUBLE,
    W_YTD DOUBLE,
    PRIMARY KEY (W_ID)
)
USING MEMTX DISTRIBUTED BY (W_ID);
--
CREATE TABLE
```

```sql title="Вставка строк"
postgres=> INSERT INTO WAREHOUSE (W_ID, W_NAME) VALUES (1, 'aaaa'), (2, 'aaab'), (3, 'aaac'), (4, 'aaad');
--
INSERT 0 4
```

```sql title="Получение колонок"
postgres=> SELECT W_ID, W_NAME FROM WAREHOUSE;
--
 "W_ID" | "W_NAME"
--------+----------
      1 | aaaa
      2 | aaab
      3 | aaac
      4 | aaad
(4 rows)
```

```sql title="Получение колонок с условием"
postgres=> SELECT W_NAME FROM WAREHOUSE WHERE W_ID=1;
--
 "W_NAME"
----------
 aaaa
(1 row)
```

### Ограничения {: #pgproto_limitations }

* Поступающие запросы без изменений передаются в Picodata в текстовом виде,
  поэтому возможно выполнение только поддерживаемых в Picodata запросов
* [Системные каталоги PostgreSQL](https://www.postgresql.org/docs/current/catalogs.html)
  реализованы частично
* Подключение по протоколу PostgreSQL работает в режиме
  [autocommit](https://www.postgresql.org/docs/current/ecpg-sql-set-autocommit.html),
  т.к. Picodata не поддерживает интерактивные транзакции
