# Аргументы командной строки

Picodata является консольным приложением, которое поддерживает различные
параметры запуска в виде аргументов командной строки и переменных
окружения.

<!-- ********************************************************************** -->
## picodata help {: #help }
<!-- ********************************************************************** -->

Полная справка доступна с помощью команды `picodata help`:

```txt
USAGE:
    picodata <SUBCOMMAND>

OPTIONS:
    -h, --help       Print help information
    -V, --version    Print version information

SUBCOMMANDS:
    admin        Connect to admin console of a Picodata instance
    connect      Сonnect to a Picodata instance and start interactive SQL console
    expel        Expel node from cluster
    help         Print this message or the help of the given subcommand(s)
    run          Run the picodata instance
    test         Run picodata integration tests
```

<!-- ********************************************************************** -->
## picodata run {: #run }
<!-- ********************************************************************** -->

Запускает инстанс Picodata, см. [Запуск Picodata](../tutorial/run.md).

```
picodata run [OPTIONS]
```

### --admin-sock {: #run_admin_sock }

`--admin-sock <path>`

Путь к unix-сокету для подключения с помощью команды [picodata
admin](#admin). Данный способ использует обычный текстовый протокол и
привилегии администратора. См. [Подключение и работа в консоли — Консоль
администратора](../tutorial/connecting.md#admin_console).

Значение по умолчанию: `<data_dir>/admin.sock`<br>
Аналогичная переменная окружения: `PICODATA_ADMIN_SOCK`

### --advertise {: #run_advertise }

`--advertise <[host][:port]>`

Адрес, по которому другие инстансы смогут подключиться к данному
инстансу. По умолчанию используется значение [--listen](#run_listen).

Аналогичная переменная окружения: `PICODATA_ADVERTISE`

### --audit {: #run_audit }

`--audit <file>`

Конфигурация журнала аудита. Доступны следующие варианты:

- `file:<file>` или просто `<file>` — запись в файл
- `pipe:<command>` или `| <command>` — перенаправление вывода в подпроцесс
- `syslog:` — перенаправление на службу `syslog` защищенной ОС

Аналогичная переменная окружения: `PICODATA_AUDIT_LOG`

### --cluster-id {: #run_cluster_id }

`--cluster-id <name>`

Имя кластера. Инстанс не сможет стать частью кластера, если у него
указано другое имя.

Аналогичная переменная окружения: `PICODATA_CLUSTER_ID`

### --data-dir {: #run_data_dir }

`--data-dir <path>`

Рабочая директория инстанса. Здесь Picodata хранит все данные.

Аналогичная переменная окружения: `PICODATA_DATA_DIR`

### --failure-domain {: #run_failure_domain }

`--failure-domain <key=value>`

Список параметров географического расположения сервера (через запятую).
Также этот аргумент называется _зоной доступности_. Каждый параметр
должен быть в формате КЛЮЧ=ЗНАЧЕНИЕ. Также, следует помнить о том, что
добавляемый инстанс должен обладать тем же набором доменов (т.е. ключей
данного аргумента), которые уже есть в кластере. Picodata будет избегать
помещения двух инстансов в один репликасет, если хотя бы один параметр
зоны доступности у них совпадает. Соответственно, инстансы будут
формировать новые репликасеты.

Аналогичная переменная окружения: `PICODATA_FAILURE_DOMAIN`

### --http-listen {: #run_http_listen }

`--http-listen <[host][:port]>`

Адрес
[HTTP-сервера](https://github.com/tarantool/http).
Интерфейс конфигурации сервера экспортирован в Lua-переменной
`_G.pico.httpd`. При отсутствии параметра сервер не запускается, а
указанная Lua-переменная имеет зачение `nil`.

Аналогичная переменная окружения: `PICODATA_HTTP_LISTEN`

### --init-cfg {: #run_init_cfg }

`--init-cfg <PATH>`

Путь к файлу конфигурации, используемый при бутстрапе кластера.

Данный параметр не сочетается с `--init-replication-factor`<br>
Аналогичная переменная окружения: `PICODATA_INIT_CFG`

### --init-replication-factor {: #run_init_replication_factor }

`--init-replication-factor <INIT_REPLICATION_FACTOR>`

Число реплик (инстансов с одинаковым набором хранимых данных) для
каждого репликасета.

Данный параметр не сочетается с `--init-cfg`<br>
Аналогичная переменная окружения: `PICODATA_INIT_REPLICATION_FACTOR`

### --instance-id {: #run_instance_id }

`--instance-id <name>`

Название инстанса. При отсутствии параметра, значение будет
автоматически сгенерировано raft-лидером в момент присоединения
(joining) инстанса к кластеру.

Аналогичная переменная окружения: `PICODATA_INSTANCE_ID`

### --listen {: #run_listen }

`-l, --listen <[host][:port]>`

Адрес и порт привязки инстанса.

Значение по умолчанию: `localhost:3301`<br>
Аналогичная переменная окружения: `PICODATA_LISTEN`

### --log {: #run_log }

`--log`

Конфигурация журнала логирования. Доступны следующие варианты:

- `file:<file>` или просто `<file>` — запись в файл
- `pipe:<command>` или `| <command>` — перенаправление вывода в подпроцесс
- `syslog:` — перенаправление на службу `syslog` защищенной ОС

Аналогичная переменная окружения: `PICODATA_LOG`

### --log-level {: #run_log_level }

`--log-level <LOG_LEVEL>`

Уровень важности событий, регистрируемых в отладочном журнале.

Возможные значения: `fatal`, `system`, `error`, `crit`, `warn`, `info`,
`verbose`, `debug`<br>
Значение по умолчанию: `info`<br>
Аналогичная переменная окружения: `PICODATA_LOG_LEVEL`

### --memtx_memory {: #run_memtx_memory }

`--memtx_memory`

Количество памяти предоставляемое непосредственно на хранение данных.

Значение по умолчанию: 67108864 (64M)<br>
Аналогичная переменная окружения: `PICODATA_MEMTX_MEMORY`

### --peer {: #run_peer }

`--peer <[host][:port]>`

Один или несколько адресов других инстансов через запятую.

Значение по умолчанию: `localhost:3301`<br>
Аналогичная переменная окружения: `PICODATA_PEER`

### --replicaset-id {: #run_replicaset_id }

`--replicaset-id <name>`

Имя репликасета. При отсутствии параметра, значение будет
автоматически выбрано raft-лидером в момент присоединения
(joining) инстанса к кластеру.

Аналогичная переменная окружения: `PICODATA_REPLICASET_ID`

### --script {: #run_script }

`--script <path>`

Путь к файлу Lua-скрипта, который будет выполнен после присоединения
инстанса к кластеру.

Аналогичная переменная окружения: `PICODATA_SCRIPT`

### --service-password-file {: #run_service_password_file }

`--service-password-file <PATH>`

Путь к файлу с паролем для системного пользователя `pico_service`. Этот
пароль будет использован для внутреннего взаимодействия внутри
кластера, поэтому он должен быть одинаковым у всех инстансов.

Аналогичная переменная окружения: `PICODATA_SERVICE_PASSWORD_FILE`

### --shredding {: #run_shredding }

`--shredding`

Режим многократной перезаписи специальными битовыми последовательностями
файлов `.xlog` и `.snap` при их удалении.

Аналогичная переменная окружения: `PICODATA_SHREDDING`

### --tier {: #run_tier }

`--tier <tier>`

Название [тира](../overview/glossary.md#tier), которому будет
принадлежать инстанс.

Аналогичная переменная окружения: `PICODATA_INSTANCE_TIER`

<!-- ********************************************************************** -->
## picodata admin {: #admin }
<!-- ********************************************************************** -->

Подключается к консоли администратора, cм. [Подключение и работа в
консоли — Консоль
администратора](../tutorial/connecting.md#admin_console).

```
picodata admin <PATH>
```

- `PATH`: Путь unix-сокета, соответствующий опции [picodata run
  --admin-sock](#run_admin_sock)

**Пример**

```console
$ picodata admin ./admin.sock
Connected to admin console by socket path "admin.sock"
type '\help' for interactive help
picodata>
```


<!-- ********************************************************************** -->
## picodata connect {: #connect }
<!-- ********************************************************************** -->

Подключается к кластерной SQL-консоли. См. [Пользовательская
консоль](../tutorial/connecting.md#sql_console)

```
picodata connect [OPTIONS] <ADDRESS>
```

- `ADDRESS`: Адрес инстанса Picodata в формате `[user@][host][:port]`,
  соответствующий опции [picodata run --adverse](#run_advertise).

**Пример**

```bash
$ picodata connect alice@localhost:3301
Enter password for alice:
Connected to interactive console by address "localhost:3301" under "alice" user
type '\help' for interactive help
picodata>
```

### -a, --auth-type {: #connect_auth_type }

`-a, --auth_type <METHOD>`

Метод аутентификации, см. [Аутентификация с помощью LDAP](../tutorial/ldap.md).

Возможные значения: `chap-sha1`, `ldap`, `md5`<br>
Значение по умолчанию: `chap-sha1`<br>.

### -u, --user {: #connect_user }

`-u, --user <USER>`

Имя пользователя, см. [Управление доступом](../tutorial/access_control.md#user_management).

Игнорируется при указании имени пользователя в адресе<br>
Значение по умолчанию: `guest`

### --password-file {: #connect_password_file }

`--password-file`

Путь к файлу с паролем указанного пользователя (хранится в виде обычного
текста). Без этого параметра пароль будет запрошен в интерактивном
режиме.

Значение по умолчанию: `localhost:3301`<br>
Аналогичная переменная окружения: `PICODATA_PASSWORD_FILE`

<!-- ### Число попыток неудачного входа {: #max_login_attempts }

Число допустимых неудачных попыток входа определяется параметром
`max_login_attempts`, который хранится в системной таблице
[_pico_property](../architecture/system_tables.md#_pico_property). По
умолчанию, значение равно `5`, но его можно изменить через запрос
[pico.cas()](api.md#pico.cas), например, так:

```lua
picodata> pico.cas({
    kind = 'replace',
    table = '_pico_property',
    tuple = {'max_login_attempts', 6},
})

Данная настройка не персистентна и локальна для текущего инстанса:
перезапуск инстанса сбрасывает счетчик попыток и позволяет
разблокировать пользователя.
```
-->


<!-- ********************************************************************** -->
## picodata expel {: #expel }
<!-- ********************************************************************** -->

Исключает инстанс Picodata из состава кластера.

```
picodata expel [OPTIONS]
```

### --cluster-id {: #expel_cluster_id }

`--cluster-id <name>`

Имя кластера, из которого должен быть исключен инстанс.

Значение по умолчанию: `demo`

### --instance-id {: #expel_instance_id }

`--instance-id <name>`

Имя инстанса, который следует исключить.

### --peer {: #expel_peer }

`--peer <[host][:port]>`

Адрес любого инстанса из состава кластера.

Значение по умолчанию: `localhost:3301`

<!--
## Сценарии применения команд {: #cases }

Ниже приведены типовые ситуации и подходящие для этого команды.

1. На хосте с инстансом `i4` вышел из строя жесткий диск, данные
   инстанса утрачены, сам инстанс неработоспособен. Какой-то из
   оставшихся инстансов доступен по адресу `192.168.104.55:3301`.

    ```
    picodata expel --instance-id i4 --peer 192.168.104.9:3301
    ```

2. В кластере `mycluster` из 3-х инстансов, где каждый работает на своем
   физическом сервере, происходит замена одного сервера. Выключать
   инстанс нельзя, так как оставшиеся 2 узла кластера не смогут создать
   стабильный кворум. Поэтому сначала в сеть добавляется дополнительный сервер:

    ```
    picodata run --instance-id i4 --peer 192.168.104.1 --cluster-id mycluster
    ```

    Далее, если на сервере с инстансом i3 настроен автоматический
    перезапуск Picodata в Systemd или как-либо иначе, то его нужно
    предварительно отключить. После этого c любого из уже работающих
    серверов кластера исключается инстанс i3:

    ```
    picodata expel --instance-id i3 --cluster-id mycluster
    ```

    Указанная команда подключится к `127.0.0.1:3301`, который
    самостоятельно найдет лидера кластера и отправит ему команду на
    исключение инстанса `i3`. Когда процесс `picodata` на i3 завершится
    — сервер можно выключать.
-->
