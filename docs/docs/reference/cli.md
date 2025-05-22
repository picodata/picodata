# Аргументы командной строки

Picodata является консольным приложением, которое поддерживает различные
параметры запуска в виде аргументов командной строки и переменных
окружения.

## picodata help {: #help }

Полная справка доступна с помощью команды `picodata help`:

```shell
USAGE:
    picodata <SUBCOMMAND>

OPTIONS:
    -h, --help       Print help information
    -V, --version    Print version information

SUBCOMMANDS:
    admin      Connect to the Admin console of a Picodata instance
    config     Subcommands related to working with the configuration file
    expel      Expel node from cluster
    help       Print this message or the help of the given subcommand(s)
    plugin     Subcommand related to plugin management
    run        Run the picodata instance
    status     Display the status of all instances in the cluster
    test       Run picodata integration tests
```

## picodata run {: #run }

Запускает инстанс Picodata, см. [Запуск Picodata](../tutorial/run.md).

```shell
picodata run [OPTIONS]
```

!!! note "Примечание"
    Если в директории, из которой вызывается команда,
    присутствует [файл конфигурации](config.md) с именем `picodata.yaml`,
    инстанс Picodata будет запущен с указанными в нем параметрами.

### --admin-sock {: #run_admin_sock }

`--admin-sock <PATH>`

Путь к unix-сокету для подключения к консоли администратора с помощью
команды [`picodata admin`](#admin). В отличие от [пользовательской консоли],
коммуникация осуществляется в виде обычного текста и всегда происходит
под учетной записью администратора.

По умолчанию используется `admin.sock` в рабочей директории инстанса.

Аналогичная переменная окружения: `PICODATA_ADMIN_SOCK`<br>
Аналогичный параметр файла конфигурации: [`instance.admin_socket`]

[`instance.admin_socket`]: config.md#instance_admin_socket
[пользовательской консоли]: ../tutorial/connecting.md#postgresql

### --audit {: #run_audit }

`--audit <PATH>`

Конфигурация [журнала аудита](../admin/audit_log.md). Доступны
следующие варианты:

- `file:<FILE>` или просто `<FILE>` — запись в файл
    ```shell
    picodata run --audit '/tmp/audit.log'
    ```

- `pipe:<COMMAND>` или `| <COMMAND>` — перенаправление вывода в подпроцесс
    ```shell
    picodata run --audit '| /bin/capture-from-stdin'
    ```

- `syslog:` — перенаправление вывода в службу `syslog` защищенной ОС
    ```shell
    picodata run --audit 'syslog:'
    ```

Аналогичная переменная окружения: `PICODATA_AUDIT_LOG`<br>
Аналогичный параметр файла конфигурации: [`instance.audit`]

[`instance.audit`]: config.md#instance_audit

### --cluster-name {: #run_cluster_name }

`--cluster-name <NAME>`

Имя кластера. Инстанс не сможет присоединиться к кластеру с другим именем.

По умолчанию используется имя `demo`.

Аналогичная переменная окружения: `PICODATA_CLUSTER_NAME`<br>
Аналогичный параметр файла конфигурации: [`cluster.name`]

[`cluster.name`]: config.md#cluster_name

### --config {: #run_config }

`--config <PATH>`

Путь к [файлу конфигурации](./config.md) в формате YAML.

По умолчанию используется `./picodata.yaml`, если такой существует.

Аналогичная переменная окружения: `PICODATA_CONFIG_FILE`

### --config-parameter {: #run_config_parameter }

`-c, --config-parameter <PARAMETER=VALUE>`

Список пар ключ-значение, определяющий параметры конфигурации.

Указанные здесь значения переопределяют как значения параметров в файле
конфигурации, так и значения параметров командной строки и переменных
окружения.

В качестве `<PARAMETER>` используется путь параметра конфигурации,
разделенный точкой. Значение `<VALUE>` интерпретируется как YAML.

**Пример**

```shell
picodata run -c instance.log.level=verbose -c instance.instance_dir=/path/to/dir
```

Аналогичная переменная окружения: `PICODATA_CONFIG_PARAMETERS`

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
Аналогичный параметр файла конфигурации: [`instance.failure_domain`]

[зоны доступности]: ../tutorial/deploy.md#failure_domains
[фактор репликации]: ../overview/glossary.md#replication_factor
[`instance.failure_domain`]: config.md#instance_failure_domain

### --http-listen {: #run_http_listen }

`--http-listen <HOST:PORT>`

Адрес HTTP-сервера.

Аналогичная переменная окружения: `PICODATA_HTTP_LISTEN`<br>
Аналогичный параметр файла конфигурации: [`instance.http_listen`]

[`instance.http_listen`]: config.md#instance_http_listen

### --init-replication-factor {: #run_init_replication_factor }

`--init-replication-factor <INIT_REPLICATION_FACTOR>`

Число реплик (инстансов с одинаковым набором хранимых данных) для
каждого репликасета.

По умолчанию используется значение `1`.

Аналогичная переменная окружения: `PICODATA_INIT_REPLICATION_FACTOR`<br>
Аналогичный параметр файла конфигурации: [`cluster.default_replication_factor`]

[`cluster.default_replication_factor`]: config.md#cluster_default_replication_factor

### --instance-dir {: #run_instance_dir }

`--instance-dir <PATH>`

Рабочая директория инстанса. Здесь Picodata хранит все данные.

По умолчанию используется текущая директория `./`.

Аналогичная переменная окружения: `PICODATA_INSTANCE_DIR`<br>
Аналогичный параметр файла конфигурации: [`instance.instance_dir`]

[`instance.instance_dir`]: config.md#instance_instance_dir

### --instance-name {: #run_instance_name }

`--instance-name <NAME>`

Имя инстанса. При отсутствии параметра значение будет автоматически
сгенерировано raft-лидером в момент присоединения инстанса к кластеру.
Генератор имен использует следующую схему: имя тира, номер репликасета,
номер инстанса в данном репликасете, с разделением через знак
подчеркивания.

Пример: `default_1_1`.

Аналогичная переменная окружения: `PICODATA_INSTANCE_NAME`<br>
Аналогичный параметр файла конфигурации: [`instance.name`]

[`instance.name`]: config.md#instance_name

### --iproto-advertise {: #run_iproto_advertise }

`--iproto-advertise <HOST:PORT>`

Публичный сетевой адрес инстанса. Анонсируется кластеру при запуске
инстанса и используется для подключения к нему других инстансов.

По умолчанию используется значение параметра [`--iproto-listen`](#run_iproto_listen), и
в большинстве случаев его не требуется менять. Но, например, в случае
`--iproto-listen 0.0.0.0` его придется указать явно:

```shell
picodata run --iproto-listen 0.0.0.0:3301 --iproto-advertise 192.168.0.1:3301
```

Аналогичная переменная окружения: `PICODATA_IPROTO_ADVERTISE`<br>
Аналогичный параметр файла конфигурации: [`instance.iproto_advertise`]

[`instance.iproto_advertise`]: config.md#instance_iproto_advertise

### --iproto-listen {: #run_iproto_listen }

`--iproto-listen <HOST:PORT>`

Сетевой адрес инстанса.

По умолчанию используется значение `127.0.0.1:3301`.

Аналогичная переменная окружения: `PICODATA_IPROTO_LISTEN`<br>
Аналогичный параметр файла конфигурации: [`instance.iproto_listen`]

[`instance.iproto_listen`]: config.md#instance_iproto_listen

### --log {: #run_log }

`--log <PATH>`

Конфигурация отладочного журнала. Доступны следующие варианты:

- `file:<FILE>` или просто `<FILE>` — запись в файл
    ```shell
    picodata run --log '/tmp/picodata.log'
    ```

- `pipe:<COMMAND>` или `| <COMMAND>` — перенаправление вывода в подпроцесс
    ```shell
    picodata run --log '| /dev/capture-from-stdin'
    ```

- `syslog:` — перенаправление вывода в службу `syslog` защищенной ОС
    ```shell
    picodata run --log 'syslog:'
    ```

По умолчанию отладочный журнал выводится в stderr.

Аналогичная переменная окружения: `PICODATA_LOG`<br>
Аналогичный параметр файла конфигурации: [`instance.log.destination`]

[`instance.log.destination`]: config.md#instance_log_destination

### --log-level {: #run_log_level }

`--log-level <LOG_LEVEL>`

Уровень важности событий, регистрируемых в отладочном журнале.

Возможные значения: `fatal`, `system`, `error`, `crit`, `warn`, `info`,
`verbose`, `debug`<br>
Значение по умолчанию: `info`<br>
Аналогичная переменная окружения: `PICODATA_LOG_LEVEL`<br>
Аналогичный параметр файла конфигурации: [`instance.log.level`]

[`instance.log.level`]: config.md#instance_log_level

### --memtx-memory {: #run_memtx_memory }

`--memtx-memory <MEMTX_MEMORY>`

Объем оперативной памяти в байтах, используемый движком хранения
[memtx](../overview/glossary.md#db_engine).

По умолчанию используется 64 МБ.

Поддерживаются значения в более удобном формате (`K` (Kilobytes), `M`
(Megabytes), `G` (Gigabytes), `T` (Terabytes), `1K` = 1024).

Аналогичная переменная окружения: `PICODATA_MEMTX_MEMORY`<br>
Аналогичный параметр файла конфигурации: [`instance.memtx.memory`]

[`instance.memtx.memory`]: config.md#instance_memtx_memory

### --peer {: #run_peer }

`--peer <HOST:PORT>`

Список сетевых адресов других инстансов, разделенных запятыми. Используется
при инициализации кластера и присоединении инстанса к уже существующему
кластеру.

**Пример**

```shell
picodata run --peer server-1.picodata.int:13301,server-2.picodata.int:13301
```

По умолчанию используется значение параметра [`--iproto-advertise`](#run_iproto_advertise),
таким образом по умолчанию создается новый кластер.

Аналогичная переменная окружения: `PICODATA_PEER`<br>
Аналогичный параметр файла конфигурации: [`instance.peer`]

[`instance.peer`]: config.md#instance_peer

### --pg-listen {: #run_pg_listen }

Адрес сервера для подключения по протоколу
[PostgreSQL](../tutorial/connecting.md#postgresql).

Значение по умолчанию: `127.0.0.1:4327`

Аналогичная переменная окружения: `PICODATA_PG_LISTEN`<br>
Аналогичный параметр файла конфигурации: [`instance.pg.listen`]

[`instance.pg.listen`]: config.md#instance_pg_listen

### --replicaset-name {: #run_replicaset_name }

`--replicaset-name <NAME>`

Имя репликасета. Используется при инициализации кластера и присоединении
инстанса к уже существующему кластеру. При отсутствии параметра имя
репликасета будет сгенерировано автоматически. Генератор имен использует
следующую схему: имя тира, номер репликасета в данном тире, с
разделением через знак подчеркивания.

Пример: `default_1`.

Аналогичная переменная окружения: `PICODATA_REPLICASET_NAME`<br>
Аналогичный параметр файла конфигурации: [`instance.replicaset_name`]

[`instance.replicaset_name`]: config.md#instance_replicaset_name

### --script {: #run_script }

`--script <PATH>`

Путь к файлу Lua-скрипта, который будет выполнен [после
присоединения](../architecture/instance_lifecycle.md#fn_postjoin)
инстанса к кластеру. В момент запуска скрипта локальное хранилище
инициализировано, HTTP-сервер запущен, но raft-узел не инициализирован.

Аналогичная переменная окружения: `PICODATA_SCRIPT`

### --share-dir {: #run_share_dir }

`--share-dir <PATH>`

Путь к директории, содержащей файлы плагинов.

В этой директории Picodata будет искать плагины. Требуется, чтобы
структура директорий плагина соответствовали схеме:

```
корень share-dir
    └── имя_плагина
        └── версия_плагина
```

Аналогичная переменная окружения: `PICODATA_SHARE_DIR`<br>
Аналогичный параметр файла конфигурации: [`instance.share_dir`]

[`instance.share_dir`]: config.md#instance_share_dir

### --shredding {: #run_shredding }

`--shredding`

Режим безопасного удаления [рабочих файлов инстанса][runfiles] путем
многократной перезаписи специальными битовыми последовательностями, см.
[Безопасный запуск](../tutorial/run.md#secure_run).

Аналогичная переменная окружения: `PICODATA_SHREDDING`<br>
Аналогичный параметр файла конфигурации: [`cluster.shredding`]

[runfiles]: ../architecture/instance_runtime_files.md
[`cluster.shredding`]: config.md#cluster_shredding

### --tier {: #run_tier }

`--tier <TIER>`

Имя [тира](../overview/glossary.md#tier), которому будет принадлежать
инстанс. Используется при инициализации кластера и присоединении
инстанса к уже существующему кластеру.

По умолчанию используется имя `default`.

Аналогичная переменная окружения: `PICODATA_INSTANCE_TIER`<br>
Аналогичный параметр файла конфигурации: [`instance.tier`]

[`instance.tier`]: config.md#instance_tier

## picodata admin {: #admin }

Подключается к консоли администратора, см. [Подключение и работа в
консоли — Консоль администратора](../tutorial/connecting.md#admin_console).

```shell
picodata admin <PATH>
```

- `PATH`: Путь к unix-сокету, соответствующий опции [`picodata run
  --admin-sock`](#run_admin_sock)

**Пример**

```shell
$ picodata admin ./admin.sock
Connected to admin console by socket path "admin.sock"
type '\help' for interactive help
(admin) sql>
```

## picodata config default {: #config_default }

Генерирует файл конфигурации Picodata со значениями по умолчанию.
Без указания опций содержимое файла конфигурации выводится в *stdout*.

```shell
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
в файл `picodata.yaml`:

```shell
picodata config default -o picodata.yaml
```

Подробнее об имени `picodata.yaml` — в описании команды [`picodata run`](#run).

## picodata expel {: #expel }

Исключает инстанс Picodata из состава кластера. По умолчанию (без
параметра [`--force`](#expel_force)) применяется только к неактивным
(`Offline`) инстансам.

Если удаляемый инстанс является голосующим в Raft, то для его замены
автоматически будет выбран новый голосующий узел.

Если удаляемый инстанс является raft-лидером, то будут организованы
новые выборы.

Если инстанс хранит шардированные данные, и его исключение
происходит принудительно (с параметром [`--force`](#expel_force)), то
данные инстанса будет автоматически перераспределены между другими
активными инстансами.

Данное действие требует авторизации под учетной записью Администратора
СУБД `admin`.

```shell
picodata expel [OPTIONS] <INSTANCE_UUID>
```

- `INSTANCE_UUID`: Универсальный уникальный идентификатор инстанса

### -a, --auth-type {: #expel_auth_type }

`-a, --auth-type <METHOD>`

Метод аутентификации, см. [Аутентификация с помощью LDAP](../admin/ldap.md).

Возможные значения: `md5`, `ldap`, `chap-sha1`<br>
Значение по умолчанию: `md5`<br>

### --cluster-name {: #expel_cluster_name }

`--cluster-name <NAME>`

Параметр `--cluster-name` устарел и больше не используется.
Он будет удален в будущем мажорном релизе (в версии 26).

### --force {: #expel_force }

`--force`

Принудительно исключить инстанс даже в том случае, если он находится в
состоянии `Online`.

Если удаляемый инстанс является мастером репликасета, то произойдет
автоматическая смена мастера. Если инстанс остался последним в
репликасете, то его удаление произойдет только после того, как
хранящиеся на нем сегменты данных переедут на другие репликасеты в тире.

### --password-file {: #expel_password_file }

`--password-file <PATH>`

Путь к файлу с паролем Администратора СУБД `admin` (хранится в виде
обычного текста). Без этого параметра пароль будет запрошен в
интерактивном режиме.

Аналогичная переменная окружения: `PICODATA_PASSWORD_FILE`

### --peer {: #expel_peer }

`--peer <HOST:PORT>`

Адрес любого инстанса из состава кластера.

Значение по умолчанию: `127.0.0.1:3301`

<!--
## Сценарии применения команд {: #cases }

Ниже приведены типовые ситуации и подходящие для этого команды.

1. На хосте с инстансом `i4` вышел из строя жесткий диск, данные
   инстанса утрачены, сам инстанс неработоспособен. Какой-то из
   оставшихся инстансов доступен по адресу `192.168.104.55:3301`.

    ```shell
    picodata expel --instance-id i4 --peer 192.168.104.9:3301
    ```

2. В кластере `mycluster` из 3-х инстансов, где каждый работает на своем
   физическом сервере, происходит замена одного сервера. Выключать
   инстанс нельзя, так как оставшиеся 2 узла кластера не смогут создать
   стабильный кворум. Поэтому сначала в сеть добавляется дополнительный сервер:

    ```shell
    picodata run --instance-id i4 --peer 192.168.104.1 --cluster-id mycluster
    ```

    Далее, если на сервере с инстансом i3 настроен автоматический
    перезапуск Picodata в Systemd или как-либо иначе, то его нужно
    предварительно отключить. После этого c любого из уже работающих
    серверов кластера исключается инстанс i3:

    ```shell
    picodata expel --instance-id i3 --cluster-id mycluster
    ```

    Указанная команда подключится к `127.0.0.1:3301`, который
    самостоятельно найдет лидера кластера и отправит ему команду на
    исключение инстанса `i3`. Когда процесс `picodata` на i3 завершится
    — сервер можно выключать.
-->

## picodata plugin configure {: #plugin_configure }

Обновляет конфигурацию [сервиса][s] указанного [плагина][p] на всех
инстансах кластера.

Вызывается один раз на любом инстансе.

```shell
picodata plugin configure <ADDRESS> <PLUGIN_NAME> <PLUGIN_VERSION> <PLUGIN_CONFIG>
```

- `ADDRESS`: Адрес инстанса в формате `HOST:PORT` для подключения
  системного пользователя `pico_service`
- `PLUGIN_NAME`: Имя плагина, содержащего обновляемый сервис
- `PLUGIN_VERSION`: Версия плагина, содержащего обновляемый сервис
- `PLUGIN_CONFIG`: Путь к файлу с новой конфигурацией обновляемого сервиса
  в формате YAML. Конфигурация соответствует [манифесту][m] плагина, за
  исключением полей `name` и `description`. В файле конфигурации достаточно
  указать лишь изменяемые параметры

[s]: ../overview/glossary.md#service
[p]: ../overview/glossary.md#plugin
[m]: ../architecture/plugins.md#manifest

### --service-names {: #plugin_configure_service_names }

`--service-names <SERVICE_NAMES>`

Список имен сервисов, разделенных запятыми, конфигурацию которых нужно
обновить. Если в новой конфигурации отсутствует хотя бы одно имя из данного
списка, будет выведена ошибка.

**Пример**

```shell
picodata plugin configure --service-names service_1,service_2
```

### --service-password-file {: #plugin_configure_service_password_file }

`--service-password-file <PATH>`

Путь к текстовому файлу с паролем для системного пользователя `pico_service`. Этот
пароль используется для внутреннего взаимодействия с другими инстансами Picodata и
является одинаковым для всех инстансов.

!!! note "Примечание"
    При несовпадении пароля будет выведена ошибка. При
    отсутствии параметра пароль будет запрошен в интерактивном режиме, см.
    [Безопасный запуск](../tutorial/run.md#secure_run).

Аналогичная переменная окружения: `PICODATA_SERVICE_PASSWORD_FILE`<br>

## picodata status {: #status }

Выводит статус всех инстансов в кластере.

```shell
picodata status [OPTIONS]
```

Вывод команды содержит следующие параметры:

* `CLUSTER NAME` — имя кластера
* `CLUSTER UUID` — [UUID] кластера
* `TIER/DOMAIN` — имя тира и (при наличии) также [домен отказа](../overview/glossary.md#failure_domain)
* `name` — имя инстанса
* `state` — состояние инстанса
* `uuid` — [UUID] инстанса
* `uri` — публичный сетевой адрес инстанса

[UUID]: sql_types.md#uuid

**Пример**

```shell
$ picodata status
Enter password for pico_service:
 CLUSTER NAME: my_cluster
 CLUSTER UUID: ba894c85-41cf-479f-90ff-114ae370792f
 TIER/DOMAIN: storage/MSK:RACK2

 name   state    uuid                                   uri
i1      Online   ce9870c3-e8f1-4ab3-88d4-c2ad10ef25ca   127.0.0.1:3301
i2      Online   6866d9fc-ad48-4ea1-a1e3-f35e2b9e60a9   127.0.0.1:3302
i3      Online   5d153ee3-dae3-4bea-a4de-06ac2cc4be22   127.0.0.1:3303
i4      Online   9560e6ad-445c-4334-a01f-02ee2f1fb6a8   127.0.0.1:3304
```

Для получения вывода `picodata status` требуется предоставить пароль
системного пользователя `pico_service`.

!!! note "Примечание"
    По умолчанию паролем системного пользователя `pico_service` является
    пустая строка.

### --peer {: #status_peer }

`--peer <HOST:PORT>`

Адрес любого инстанса из состава кластера.

Значение по умолчанию: `127.0.0.1:3301`

Аналогичная переменная окружения: `PICODATA_PEER`<br>
Аналогичный параметр файла конфигурации: [`instance.peer`]

### --service-password-file {: #status_service_password_file }

`--service-password-file <PATH>`

Путь к текстовому файлу с паролем для системного пользователя `pico_service`. Этот
пароль используется для внутреннего взаимодействия с другими инстансами Picodata и
является одинаковым для всех инстансов.

!!! note "Примечание"
    При несовпадении пароля будет выведена ошибка. При
    отсутствии параметра пароль будет запрошен в интерактивном режиме, см.
    [Безопасный запуск](../tutorial/run.md#secure_run).

Аналогичная переменная окружения: `PICODATA_SERVICE_PASSWORD_FILE`<br>

### --timeout {: #status_timeout }

`-t, --timeout <TIMEOUT>`

Максимальное время обработки запроса в секундах.

Значение по умолчанию: `5`

Аналогичная переменная окружения: `PICODATA_CONNECT_TIMEOUT`

**Пример**

```shell
picodata status -t 10
```
