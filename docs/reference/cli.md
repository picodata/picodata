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
    admin      Connect to the Admin console of a Picodata instance
    config     Subcommands related to working with the configuration file
    connect    Connect to the Distributed SQL console
    expel      Expel node from cluster
    help       Print this message or the help of the given subcommand(s)
    run        Run the picodata instance
    test       Run picodata integration tests
```

<!-- ********************************************************************** -->
## picodata run {: #run }
<!-- ********************************************************************** -->

<!-- Описание соответствует версии Picodata `24.4.0-57-g8a306f0e`. -->

Запускает инстанс Picodata, см. [Запуск Picodata](../tutorial/run.md).

```
picodata run [OPTIONS]
```

!!! note "Примечание"
    Если в директории, из которой вызывается команда,
    присутствует [файл конфигурации](config.md) с именем `config.yaml`,
    инстанс Picodata будет запущен с указанными в нем параметрами.

### --admin-sock {: #run_admin_sock }

`--admin-sock <PATH>`

Путь к unix-сокету для подключения к консоли администратора с помощью
команды [picodata admin](#admin). В отличие от `picodata connect`,
коммуникация осуществляется в виде обычного текста и всегда происходит
под учетной записью администратора.

По умолчанию используется `admin.sock` в рабочей директории инстанса.

Аналогичная переменная окружения: `PICODATA_ADMIN_SOCK`<br>
Аналогичный параметр файла конфигурации: `instance.admin_socket`

### --advertise {: #run_advertise }

`--advertise <[HOST][:PORT]>`

Адрес, который другие инстансы должны использовать для подключения к
данному инстансу.

Значение параметра `--advertise` используется для установки публичного
IP-адреса и порта инстанса и анонсируется кластеру при запуске инстанса.
Параметр сообщает, по какому адресу остальные инстансы должны обращаться
к текущему. По умолчанию он равен [--listen](#run_listen), поэтому его
не обязательно использовать. Но, например, в случае `--listen 0.0.0.0`
его придется указать явно:

```shell
picodata run --listen 0.0.0.0:3301 --advertise 192.168.0.1:3301
```

Аналогичная переменная окружения: `PICODATA_ADVERTISE`<br>
Аналогичный параметр файла конфигурации: `instance.advertise_address`

### --audit {: #run_audit }

`--audit <PATH>`

Конфигурация [журнала аудита](../tutorial/audit_log.md). Доступны
следующие варианты:

- `file:<FILE>` или просто `<FILE>` — запись в файл
    ```
    picodata run --audit '/tmp/audit.log'
    ```

- `pipe:<COMMAND>` или `| <COMMAND>` — перенаправление вывода в подпроцесс
    ```
    picodata run --audit '| /bin/capture-from-stdin'
    ```

- `syslog:` — перенаправление вывода в службу `syslog` защищенной ОС
    ```
    picodata run --audit 'syslog:'
    ```

Аналогичная переменная окружения: `PICODATA_AUDIT_LOG`<br>
Аналогичный параметр файла конфигурации: `instance.audit`

### --cluster-id {: #run_cluster_id }

`--cluster-id <NAME>`

Имя кластера. Инстанс не сможет присоединиться к кластеру с другим именем.

По умолчанию используется имя `demo`.

Аналогичная переменная окружения: `PICODATA_CLUSTER_ID`<br>
Аналогичный параметр файла конфигурации: `cluster.cluster_id`

### --config {: #run_config }

`--config <PATH>`

Путь к [файлу конфигурации](./config.md) в формате YAML.

По умолчанию используется `./config.yaml`, если такой существует.

Аналогичная переменная окружения: `PICODATA_CONFIG_FILE`

### --config-parameter {: #run_config_parameter }

`-c, --config-parameter <PARAMETER=VALUE>`

Список пар ключ-значение, разделенных запятыми, определяющий параметры
конфигурации.

Указанные здесь значения переопределяют как значения параметров в файле
конфигурации, так и значения параметров командной строки и переменных
окружения.

В качестве `<PARAMETER>` используется путь параметра конфигурации,
разделенный точкой. Значение `<VALUE>` интерпретируется как YAML.

Пример: `picodata run -c instance.log.level=verbose`

Аналогичная переменная окружения: `PICODATA_CONFIG_PARAMETERS`

### --data-dir {: #run_data_dir }

`--data-dir <PATH>`

Рабочая директория инстанса. Здесь Picodata хранит все данные.

По умолчанию используется текущая директория `./`.

Аналогичная переменная окружения: `PICODATA_DATA_DIR`<br>
Аналогичный параметр файла конфигурации: `instance.data_dir`

### --failure-domain {: #run_failure_domain }

`--failure-domain <KEY=VALUE>`

Список пар ключ-значение, разделенных запятыми, определяющий
географическое расположение сервера — [зоны доступности]. Picodata не
будет объединять два инстанса в один репликасет, если у них совпадают
значения хотя бы в одном ключе. Вместо этого будет создан новый
репликасет. Репликасеты формируются из инстансов с разными зонами
доступности до тех пор, пока не будет достигнут желаемый [фактор
репликации].

Аналогичная переменная окружения: `PICODATA_FAILURE_DOMAIN`<br>
Аналогичный параметр файла конфигурации: `instance.failure_domain`

[зоны доступности]: ../tutorial/deploy.md#failure_domains
[фактор репликации]: ../overview/glossary.md#replication_factor

### --http-listen {: #run_http_listen }

`--http-listen <[HOST][:PORT]>`

Адрес HTTP-сервера.

Аналогичная переменная окружения: `PICODATA_HTTP_LISTEN`<br>
Аналогичный параметр файла конфигурации: `instance.http_listen`

### --init-replication-factor {: #run_init_replication_factor }

`--init-replication-factor <INIT_REPLICATION_FACTOR>`

Число реплик (инстансов с одинаковым набором хранимых данных) для
каждого репликасета.

По умолчанию используется значение `1`.

Аналогичная переменная окружения: `PICODATA_INIT_REPLICATION_FACTOR`<br>
Аналогичный параметр файла конфигурации: `cluster.default_replication_factor`

### --instance-id {: #run_instance_id }

`--instance-id <NAME>`

Имя инстанса. При отсутствии параметра значение будет автоматически
сгенерировано raft-лидером в момент присоединения инстанса к кластеру.

Аналогичная переменная окружения: `PICODATA_INSTANCE_ID`<br>
Аналогичный параметр файла конфигурации: `instance.instance_id`

### --listen {: #run_listen }

`-l, --listen <[HOST][:PORT]>`

Сетевой адрес инстанса.

По умолчанию используется значение `localhost:3301`.

Аналогичная переменная окружения: `PICODATA_LISTEN`<br>
Аналогичный параметр файла конфигурации: `instance.listen`

### --log {: #run_log }

`--log <PATH>`

Конфигурация отладочного журнала. Доступны следующие варианты:

- `file:<FILE>` или просто `<FILE>` — запись в файл
    ```
    picodata run --log '/tmp/picodata.log'
    ```

- `pipe:<COMMAND>` или `| <COMMAND>` — перенаправление вывода в подпроцесс
    ```
    picodata run --log '| /dev/capture-from-stdin'
    ```

- `syslog:` — перенаправление вывода в службу `syslog` защищенной ОС
    ```
    picodata run --log 'syslog:'
    ```

По умолчанию отладочный журнал выводится в stderr.

Аналогичная переменная окружения: `PICODATA_LOG`<br>
Аналогичный параметр файла конфигурации: `instance.log.destination`

### --log-level {: #run_log_level }

`--log-level <LOG_LEVEL>`

Уровень важности событий, регистрируемых в отладочном журнале.

Возможные значения: `fatal`, `system`, `error`, `crit`, `warn`, `info`,
`verbose`, `debug`<br>
Значение по умолчанию: `info`<br>
Аналогичная переменная окружения: `PICODATA_LOG_LEVEL`<br>
Аналогичный параметр файла конфигурации: `instance.log.level`

### --memtx-memory {: #run_memtx_memory }

`--memtx-memory <MEMTX_MEMORY>`

Объем оперативной памяти в байтах, используемый движком хранения
[memtx](../overview/glossary.md#db_engine).

По умолчанию используется 64 MБ.

Аналогичная переменная окружения: `PICODATA_MEMTX_MEMORY`<br>
Аналогичный параметр файла конфигурации: `instance.memtx.memory`

### --peer {: #run_peer }

`--peer <[HOST][:PORT]>`

Список сетевых адресов других инстансов. Используется при инициализации
кластера и присоединении инстанса к уже существующему кластеру.

По умолчанию используется значение параметра [--advertise](#run_advertise),
таким образом по умолчанию создается новый кластер.

Аналогичная переменная окружения: `PICODATA_PEER`<br>
Аналогичный параметр файла конфигурации: `instance.peers`

### --pg-listen {: #run_pg_listen }

Адрес сервера [Pgproto](../tutorial/connecting.md#pgproto).

Аналогичная переменная окружения: `PICODATA_PG_LISTEN`<br>
Аналогичный параметр файла конфигурации: `instance.pg.listen`

### --plugin-dir {: #run_plugin_dir }

`--plugin-dir <PATH>`

Путь к директории, содержащей файлы плагинов.

Аналогичная переменная окружения: `PICODATA_PLUGIN_DIR`<br>
Аналогичный параметр файла конфигурации: `instance.plugin_dir`

### --replicaset-id {: #run_replicaset_id }

`--replicaset-id <NAME>`

Имя репликасета. Используется при инициализации кластера и присоединении
инстанса к уже существующему кластеру. При отсутствии параметра
репликасет будет выбран автоматически на основе зон доступности.

Аналогичная переменная окружения: `PICODATA_REPLICASET_ID`<br>
Аналогичный параметр файла конфигурации: `instance.replicaset_id`

### --script {: #run_script }

`--script <PATH>`

Путь к файлу Lua-скрипта, который будет выполнен [после
присоединения](../architecture/instance_lifecycle.md#fn_postjoin)
инстанса к кластеру. В момент запуска скрипта локальное хранилище
инициализировано, HTTP-сервер запущен, но raft-узел не инициализирован.

Аналогичная переменная окружения: `PICODATA_SCRIPT`

### --service-password-file {: #run_service_password_file }

`--service-password-file <PATH>`

Путь к файлу с паролем для системного пользователя `pico_service`. Этот
пароль будет использован для взаимодействия с другими инстансами
кластера. При несовпадении пароля присоединение инстанса к кластеру
невозможно. При отсутствии параметра в качестве пароля используется
пустая строка, см. [Безопасный запуск](../tutorial/run.md#secure_run).

Аналогичная переменная окружения: `PICODATA_SERVICE_PASSWORD_FILE`<br>
Аналогичный параметр файла конфигурации: `instance.service_password_file`

### --shredding {: #run_shredding }

`--shredding`

Режим безопасного удаления [рабочих файлов инстанса][runfiles] путем
многократной перезаписи специальными битовыми последовательностями, см.
[Безопасный запуск](../tutorial/run.md#secure_run).

Аналогичная переменная окружения: `PICODATA_SHREDDING`<br>
Аналогичный параметр файла конфигурации: `instance.shredding`

[runfiles]: ../architecture/instance_runtime_files.md

### --tier {: #run_tier }

`--tier <TIER>`

Имя [тира](../overview/glossary.md#tier), которому будет принадлежать
инстанс. Используется при инициализации кластера и присоединении
инстанса к уже существующему кластеру.

По умолчанию используется имя `default`.

Аналогичная переменная окружения: `PICODATA_INSTANCE_TIER`<br>
Аналогичный параметр файла конфигурации: `instance.tier`

<!-- ********************************************************************** -->
## picodata admin {: #admin }
<!-- ********************************************************************** -->

Подключается к консоли администратора, cм. [Подключение и работа в
консоли — Консоль администратора](../tutorial/connecting.md#admin_console).

```
picodata admin <PATH>
```

- `PATH`: Путь к unix-сокету, соответствующий опции [picodata run
  --admin-sock](#run_admin_sock)

**Пример**

```console
$ picodata admin ./admin.sock
Connected to admin console by socket path "admin.sock"
type '\help' for interactive help
picodata>
```

<!-- ********************************************************************** -->
## picodata config default {: #config_default }
<!-- ********************************************************************** -->

Генерирует файл конфигурации Picodata со значениями по умолчанию.
Без указания опций содержимое файла конфигурации выводится в *stdout*.

```
picodata config default [OPTIONS]
```

Пример вывода `picodata config default` —
см. [Описание файла конфигурации](config.md#config_file_description)

### --output-file {: #config_default_output_file }

`-o, --output-file <FILENAME>`

Записывает файл конфигурации Picodata со значениями по умолчанию
в файл `<FILENAME>`.

**Пример**

Сохранение файла конфигурации Picodata со значениями по умолчанию
в файл `config.yaml`:

```
picodata config default -o config.yaml
```

Подробнее об имени `config.yaml` — в описании команды [picodata run](#run).

<!-- ********************************************************************** -->
## picodata connect {: #connect }
<!-- ********************************************************************** -->

Подключается к кластерной SQL-консоли. См. [Пользовательская
консоль](../tutorial/connecting.md#sql_console)

```
picodata connect [OPTIONS] <ADDRESS>
```

- `ADDRESS`: Адрес инстанса Picodata в формате `[user@][host][:port]`,
  соответствующий опции [picodata run --advertise](#run_advertise)

**Пример**

```bash
$ picodata connect alice@localhost:3301
Enter password for alice:
Connected to interactive console by address "localhost:3301" under "alice" user
type '\help' for interactive help
picodata>
```

### -a, --auth-type {: #connect_auth_type }

`-a, --auth-type <METHOD>`

Метод аутентификации, см. [Аутентификация с помощью LDAP](../tutorial/ldap.md).

Возможные значения: `chap-sha1`, `ldap`, `md5`<br>
Значение по умолчанию: `chap-sha1`<br>

### --password-file {: #connect_password_file }

`--password-file <PATH>`

Путь к файлу с паролем указанного пользователя (хранится в виде обычного
текста). Без этого параметра пароль будет запрошен в интерактивном
режиме.

Аналогичная переменная окружения: `PICODATA_PASSWORD_FILE`

### -u, --user {: #connect_user }

`-u, --user <USER>`

Имя пользователя, см. [Управление доступом](../tutorial/access_control.md#user_management).

Игнорируется при указании имени пользователя в адресе<br>
Значение по умолчанию: `guest`

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

Если инстанс хранит сегменты шардированных данных, перед его удалением
данные будет автоматически перераспределены.

Если удаляемый инстанс является голосующим в Raft, то для его замены
автоматически будет выбран новый голосующий узел.

Если удаляемый инстанс является raft-лидером, то будут организованы
новые выборы.

Данное действие требует авторизации под учетной записью Администратора
СУБД `admin`.

```
picodata expel [OPTIONS] <INSTANCE_ID>
```

- `INSTANCE_ID`: Имя инстанса

### --cluster-id {: #expel_cluster_id }

`--cluster-id <NAME>`

Имя кластера, из которого должен быть исключен инстанс.

Значение по умолчанию: `demo`

### --password-file {: #expel_password_file }

`--password-file <PATH>`

Путь к файлу с паролем Администратора СУБД `admin` (хранится в виде
обычного текста). Без этого параметра пароль будет запрошен в
интерактивном режиме.

Аналогичная переменная окружения: `PICODATA_PASSWORD_FILE`

### --peer {: #expel_peer }

`--peer <[HOST][:PORT]>`

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
