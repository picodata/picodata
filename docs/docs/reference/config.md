# Файл конфигурации

Файл конфигурации содержит параметры кластера и инстанса для
[запуска Picodata](../tutorial/run.md).

## Назначение файла конфигурации {: #config_file_purpose }

Использование файла конфигурации является дополнительным способом
задания параметров кластера и инстанса при запуске Picodata, помимо
опций команды [`picodata run`](cli.md#run) и переменных окружения.

Команда для запуска инстанса Picodata, если файл конфигурации назван
`picodata.yaml` и расположен в директории вызова команды:

```shell
picodata run
```

Команда для запуска инстанса Picodata в остальных случаях:

```shell
picodata run --config <PATH>
```

где `<PATH>` — путь к файлу конфигурации в формате YAML.

См. также:

* [`picodata run --config`](cli.md#run_config)

## Порядок применения параметров {: #params_priority }

Параметры командной строки имеют более высокий приоритет, чем файл
конфигурации. Соответственно, используя параметр [`--config-parameter`],
можно переопределить нужные параметры запуска Picodata. Уровни вложения
параметра разделяются точкой. Например:

```shell
picodata run --config ./picodata_config.yaml -c instance.log.level=verbose
```

!!! danger "Ограничение"
    Параметры тиров нельзя переопределить отдельно — вместо этого
    следует указать в командной строке всю секцию `cluster.tier`.
    Например:
    ```shell
    picodata run --config ./picodata_config.yaml -c
    cluster.tier='{"default": {"replication_factor": 3, "can_vote":
    true}}'
    ```
    Параметры тиров, которые при этом указаны в файле
    конфигурации, будут проигнорированы

[`--config-parameter`]: cli.md#run_config_parameter

## Описание файла конфигурации {: #config_file_description }

<!-- Описание соответствует версии Picodata `24.7.0-1152-g27f3c263`. -->

Результатом выполнения команды `picodata config default -o picodata.yaml`
является файл конфигурации Picodata в формате YAML со стандартными
значениями параметров:

``` yaml title="picodata.yaml"
cluster:
  name: demo # (3)!
  tier:
    default:
      replication_factor: 1 # (7)!
      bucket_count: 3000 # (5)!
      can_vote: true # (6)!
  default_replication_factor: 1 # (1)!
  default_bucket_count: 3000 # (2)!
  shredding: false # (4)!
instance:
  instance_dir: . # (14)!
  name: null # (20)!
  replicaset_name: null # (25)!
  tier: default # (26)!
  failure_domain: {} # (12)!
  peer: # (21)!
  - 127.0.0.1:3301
  iproto_listen: 127.0.0.1:3301 # (15)!
  iproto_advertise: 127.0.0.1:3301 # (9)!
  http_listen: null # (13)!
  admin_socket: ./admin.sock # (8)!
  share_dir: null # (24)!
  audit: null # (10)!
  log:
    level: info # (18)!
    destination: null # (16)!
    format: plain # (17)!
  memtx:
    memory: 64M # (19)!
  vinyl:
    memory: 128M # (28)!
    cache: 128M # (27)!
    bloom_fpr: 0.05 # (29)!
    max_tuple_size: 1M # (30)!
    page_size: 8K # (31)!
    range_size: 1G # (32)!
    run_count_per_level: 2 # (33)!
    run_size_ratio: 3.5 # (34)!
    read_threads: 1 # (35)!
    write_threads: 4 # (36)!
    timeout: 60.0 # (37)!
  pg:
    listen: 127.0.0.1:4327 # (22)!
    ssl: false # (23)!
  boot_timeout: 7200 # (11)!
```

1. [cluster.default_replication_factor](#cluster_default_replication_factor)
2. [cluster.default_bucket_count](#cluster_default_bucket_count)
3. [cluster.name](#cluster_name)
4. [cluster.shredding](#cluster_shredding)
5. [cluster.tier.<tier_name\>.bucket_count](#cluster_tier_tier_bucket_count)
6. [cluster.tier.<tier_name\>.can_vote](#cluster_tier_tier_can_vote)
7. [cluster.tier.<tier_name\>.replication_factor](#cluster_tier_tier_replication_factor)
8. [instance.admin_socket](#instance_admin_socket)
9. [instance.iproto_advertise](#instance_iproto_advertise)
10. [instance.audit](#instance_audit)
11. [instance.boot_timeout](#instance_boot_timeout)
12. [instance.failure_domain](#instance_failure_domain)
13. [instance.http_listen](#instance_http_listen)
14. [instance.instance_dir](#instance_instance_dir)
15. [instance.iproto_listen](#instance_iproto_listen)
16. [instance.log.destination](#instance_log_destination)
17. [instance.log.format](#instance_log_format)
18. [instance.log.level](#instance_log_level)
19. [instance.memtx.memory](#instance_memtx_memory)
20. [instance.name](#instance_name)
21. [instance.peer](#instance_peer)
22. [instance.pg.listen](#instance_pg_listen)
23. [instance.pg.ssl](#instance_pg_ssl)
24. [instance.share_dir](#instance_share_dir)
25. [instance.replicaset_name](#instance_replicaset_name)
26. [instance.tier](#instance_tier)
27. [instance.vinyl.cache](#instance_vinyl_cache)
28. [instance.vinyl.memory](#instance_vinyl_memory)
29. [instance.vinyl.bloom_fpr](#instance_vinyl_bloom_fpr)
30. [instance.vinyl.max_tuple_size](#instance_vinyl_max_tuple_size)
31. [instance.vinyl.page_size](#instance_vinyl_page_size)
32. [instance.vinyl.range_size](#instance_vinyl_range_size)
33. [instance.vinyl.run_count_per_level](#instance_vinyl_run_count_per_level)
34. [instance.vinyl.run_size_ratio](#instance_vinyl_run_size_ratio)
35. [instance.vinyl.read_threads](#instance_vinyl_read_threads)
36. [instance.vinyl.write_threads](#instance_vinyl_write_threads)
37. [instance.vinyl.timeout](#instance_vinyl_timeout)

См. также:

* [`picodata config default`](cli.md#config_default)

## Параметры файла конфигурации {: #config_file_parameters }

### cluster.default_bucket_count {: #cluster_default_bucket_count }

Число бакетов в кластере по умолчанию.

Данные:

* Тип: *int*
* Значение по умолчанию: `3000`

Данный параметр задается только в файле конфигурации.

### cluster.default_replication_factor {: #cluster_default_replication_factor }

Число реплик — инстансов с одинаковым набором хранимых данных — для каждого
репликасета.

Данные:

* Тип: *int*
* Значение по умолчанию: `1`

Аналогичная переменная окружения: `PICODATA_INIT_REPLICATION_FACTOR`<br>
Аналогичная команда: [`picodata run --init-replication-factor`]

[`picodata run --init-replication-factor`]: cli.md#run_init_replication_factor

### cluster.name {: #cluster_name }

Имя кластера. Инстанс не сможет присоединиться к кластеру с другим именем.

Данные:

* Тип: *str*
* Значение по умолчанию: `demo`

Аналогичная переменная окружения: `PICODATA_CLUSTER_NAME`<br>
Аналогичная команда: [`picodata run --cluster-name`]

[`picodata run --cluster-name`]: cli.md#run_cluster_name

### cluster.shredding {: #cluster_shredding }

Режим безопасного удаления [рабочих файлов][runfiles] путем
многократной перезаписи специальными битовыми последовательностями, см.
[Безопасный запуск](../tutorial/run.md#secure_run).

[runfiles]: ../architecture/instance_runtime_files.md

Данные:

* Тип: *bool*
* Значение по умолчанию: `false`

Аналогичная переменная окружения: `PICODATA_SHREDDING`<br>
Аналогичная команда: [`picodata run --shredding`]

[`picodata run --shredding`]: cli.md#run_shredding

### cluster.tier.<tier_name\>.bucket_count {: #cluster_tier_tier_bucket_count }

Число бакетов в данном тире.

Данные:

* Тип: *int*
* Значение по умолчанию: `3000`

Данный параметр задается только в файле конфигурации.

### cluster.tier.<tier_name\>.can_vote {: #cluster_tier_tier_can_vote }

Признак [тира] *<tier_name\>*, определяющий возможность инстансов участвовать
в голосовании на выборах [raft-лидера](../overview/glossary.md#raft_leader).

Данные:

* Тип: *bool*
* Значение по умолчанию: `true`

Аналогичная команда — [`picodata run --config-parameter`]. Пример:

[`picodata run --config-parameter`]: cli.md#run_config_parameter

```bash
picodata run -c cluster.tier='{"default": {"replication_factor": 1, "can_vote": false}}'
```

См. также:

* [Динамическое переключение голосующих узлов в Raft](../architecture/raft_failover.md#raft_voter_failover)

[тира]: ../overview/glossary.md#tier

### cluster.tier.<tier_name\>.replication_factor {: #cluster_tier_tier_replication_factor }

[Фактор репликации](../overview/glossary.md#replication_factor) тира *<tier_name\>*.

Данные:

* Тип: *int*
* Значение по умолчанию: `1`

Аналогичная команда — [`picodata run --config-parameter`]. Пример:

```bash
picodata run -c cluster.tier='{"default": {"replication_factor": 3, "can_vote": true}}'
```

### instance.admin_socket {: #instance_admin_socket }

Путь к unix-сокету для подключения к консоли администратора с помощью
команды `picodata admin`. В отличие от `picodata connect`, коммуникация
осуществляется в виде обычного текста и всегда происходит под учетной
записью администратора.

Данные:

* Тип: *str*
* Значение по умолчанию: `./admin.sock`

Аналогичная переменная окружения: `PICODATA_ADMIN_SOCK`<br>
Аналогичная команда: [`picodata run --admin-sock`]

[`picodata run --admin-sock`]: cli.md#run_admin_sock

### instance.audit {: #instance_audit }

Конфигурация журнала аудита. Доступны следующие варианты:

* `file:<FILE>` или просто `<FILE>` — запись в файл
* `pipe:<COMMAND>` или `| <COMMAND>` — перенаправление вывода в подпроцесс
* `syslog:` — перенаправление вывода в службу `syslog` защищенной ОС

Данные:

* Тип: *str*
* Значение по умолчанию: `null`

Аналогичная переменная окружения: `PICODATA_AUDIT_LOG`<br>
Аналогичная команда: [`picodata run --audit`]

[`picodata run --audit`]: cli.md#run_audit

### instance.boot_timeout {: #instance_boot_timeout }

Максимальное время *в секундах*, в течение которого `instance` может находиться в ожидании загрузки перед присоединением к кластеру, после чего он автоматически отключается.

Данные:

* Тип: *int*
* Значение по умолчанию: `7200` (2 часа)

Аналогичная команда — [`picodata run --config-parameter`]. Пример:

```bash
picodata run -c instance.boot_timeout=3600
```

### instance.failure_domain {: #instance_failure_domain }

Список пар ключ-значение, разделенных запятыми, определяющий географическое
расположение сервера — [зоны доступности]. Picodata не будет объединять
два инстанса в один репликасет, если у них совпадают значения хотя бы в
одном ключе. Вместо этого будет создан новый репликасет. Репликасеты
формируются из инстансов с разными зонами доступности до тех пор, пока не
будет достигнут желаемый [фактор репликации].

[зоны доступности]: ../tutorial/deploy.md#failure_domains
[фактор репликации]: ../overview/glossary.md#replication_factor

Данные:

<!-- https://yaml.org/spec/1.2.2/#822-block-mappings -->
* Тип: Block Mappings of *{ str: str }*
* Значение по умолчанию: `{}`

Аналогичная переменная окружения: `PICODATA_FAILURE_DOMAIN`<br>
Аналогичная команда: [`picodata run --failure-domain`]

[`picodata run --failure-domain`]: cli.md#run_failure_domain

### instance.http_listen {: #instance_http_listen }

Адрес HTTP-сервера.

Данные:

* Тип: *str*
* Значение по умолчанию: `null`

Аналогичная переменная окружения: `PICODATA_HTTP_LISTEN`<br>
Аналогичная команда: [`picodata run --http-listen`]

[`picodata run --http-listen`]: cli.md#run_http_listen

### instance.instance_dir {: #instance_instance_dir }

Рабочая директория инстанса. Здесь Picodata хранит все данные.

Данные:

* Тип: *str*
* Значение по умолчанию: `.`

Аналогичная переменная окружения: `PICODATA_INSTANCE_DIR`<br>
Аналогичная команда: [`picodata run --instance-dir`]

[`picodata run --instance-dir`]: cli.md#run_instance_dir

### instance.iproto_advertise {: #instance_iproto_advertise }

Публичный сетевой адрес инстанса. Анонсируется кластеру при запуске
инстанса и используется для подключения к нему других инстансов.

Данные:

* Тип: *str*
* Значение по умолчанию: `127.0.0.1:3301`

Аналогичная переменная окружения: `PICODATA_IPROTO_ADVERTISE`<br>
Аналогичная команда: [`picodata run --iproto-advertise`]

[`picodata run --iproto-advertise`]: cli.md#run_iproto_advertise

### instance.iproto_listen {: #instance_iproto_listen }

Сетевой адрес инстанса.

Данные:

* Тип: *str*
* Значение по умолчанию: `127.0.0.1:3301`

Аналогичная переменная окружения: `PICODATA_IPROTO_LISTEN`<br>
Аналогичная команда: [`picodata run --iproto-listen`]

[`picodata run --iproto-listen`]: cli.md#run_iproto_listen

### instance.log.destination {: #instance_log_destination }

Конфигурация отладочного журнала. Доступны следующие варианты:

* `file:<FILE>` или просто `<FILE>` — запись в файл
* `pipe:<COMMAND>` или `| <COMMAND>` — перенаправление вывода в подпроцесс
* `syslog:` — перенаправление вывода в службу `syslog` защищенной ОС

По умолчанию отладочный журнал выводится в *stderr*.

Данные:

* Тип: *str*
* Значение по умолчанию: `null`

Аналогичная переменная окружения: `PICODATA_LOG`<br>
Аналогичная команда: [`picodata run --log`]

[`picodata run --log`]: cli.md#run_log

### instance.log.format {: #instance_log_format }
<!-- https://www.tarantool.io/en/doc/2.11/reference/configuration/#cfg-logging-log-format -->

Формат отладочного журнала.

Возможные значения: `plain`, `json`

Данные:

* Тип: *str*
* Значение по умолчанию: `plain`

Аналогичная переменная окружения: `PICODATA_LOG`<br>
Аналогичная команда — [`picodata run --config-parameter`]. Пример:

```bash
picodata run -c instance.log.format=json
```

### instance.log.level {: #instance_log_level }

Уровень важности событий, регистрируемых в отладочном журнале.

Возможные значения: `fatal`, `system`, `error`, `crit`, `warn`, `info`,
`verbose`, `debug`

Данные:

* Тип: *str*
* Значение по умолчанию: `info`

Аналогичная переменная окружения: `PICODATA_LOG_LEVEL`<br>
Аналогичная команда: [`picodata run --log-level`]

[`picodata run --log-level`]: cli.md#run_log_level

### instance.memtx.memory {: #instance_memtx_memory }
<!-- https://www.tarantool.io/en/doc/2.11/reference/configuration/#cfg-storage-memtx-memory -->

Объем памяти *в байтах*, выделяемый для хранения кортежей. Когда
достигается лимит использования памяти, запросы команд [INSERT](./sql/insert.md)
и [UPDATE](./sql/update.md) начинают отклоняться с ошибкой *ER_MEMORY_ISSUE*.
Сервер хранит в выделяемом объеме памяти только кортежи — для хранения индексов
и информации о соединениях используется дополнительная память.

Минимальное значение — 33,554,432 байтов (32 МБ)

Данные:

* Тип: *int*
* Значение по умолчанию: `64M` (67108864 Б)

Поддерживаются значения в более удобном формате (`K` (Kilobytes), `M`
(Megabytes), `G` (Gigabytes), `T` (Terabytes), `1K` = 1024).

Пример:

```bash
picodata run -c instance.memtx.memory=128M
```

Аналогичная переменная окружения: `PICODATA_MEMTX_MEMORY`<br>
Аналогичная команда: [`picodata run --memtx-memory`]

[`picodata run --memtx-memory`]: cli.md#run_memtx_memory

### instance.name {: #instance_name }

Имя инстанса. При отсутствии параметра значение будет автоматически
сгенерировано raft-лидером в момент присоединения инстанса к кластеру.
Генератор имен использует следующую схему: имя тира, номер репликасета,
номер инстанса в данном репликасете, с разделением через знак
подчеркивания.

Пример: `default_1_1`.

Данные:

* Тип: *str*
* Значение по умолчанию: `null`

Аналогичная переменная окружения: `PICODATA_INSTANCE_NAME`<br>
Аналогичная команда: [`picodata run --instance-name`]

[`picodata run --instance-name`]: cli.md#run_instance_name

### instance.peer {: #instance_peer }

Список сетевых адресов других инстансов, разделенных запятыми. Используется
при инициализации кластера и присоединении инстанса к уже существующему
кластеру.

Данные:

<!-- https://yaml.org/spec/1.2.2/#821-block-sequences -->
* Тип: Block Sequence of *str*
* Значение по умолчанию: `- 127.0.0.1:3301`

Пример:

```bash
picodata run -c instance.peer='["127.0.0.1:3301", "127.0.0.1:3302"]'
```

Аналогичная переменная окружения: `PICODATA_PEER`<br>
Аналогичная команда: [`picodata run --peer`]

[`picodata run --peer`]: cli.md#run_peer

### instance.pg.listen {: #instance_pg_listen }

Адрес сервера для подключения по протоколу
[PostgreSQL](../tutorial/connecting.md#pgproto).

Данные:

* Тип: *str*
* Значение по умолчанию: `127.0.0.1:4327`

Аналогичная переменная окружения: `PICODATA_PG_LISTEN`<br>
Аналогичная команда: [`picodata run --pg-listen`]

[`picodata run --pg-listen`]: cli.md#run_pg_listen

### instance.pg.ssl {: #instance_pg_ssl }

Признак использования протокола SSL при подключении к Pgproto.

Если для признака указано значение `true`, [в рабочей директории
инстанса](cli.md#run_instance_dir) `<INSTANCE_DIR>` должны находиться необходимые
SSL-сертификаты:

* `server.crt`
* `server.key`

Данные:

* Тип: *bool*
* Значение по умолчанию: `false`

Аналогичная команда — [`picodata run --config-parameter`]. Пример:

```bash
picodata run -c instance.pg.ssl=true
```

### instance.replicaset_name {: #instance_replicaset_name }

Имя репликасета. Используется при инициализации кластера и присоединении
инстанса к уже существующему кластеру. При отсутствии параметра имя
репликасета будет сгенерировано автоматически. Генератор имен использует
следующую схему: имя тира, номер репликасета в данном тире, с
разделением через знак подчеркивания.

Пример: `default_1`.

Данные:

* Тип: *str*
* Значение по умолчанию: `null`

Аналогичная переменная окружения: `PICODATA_REPLICASET_NAME`<br>
Аналогичная команда: [`picodata run --replicaset-name`]

[`picodata run --replicaset-name`]: cli.md#run_replicaset_name

### instance.share_dir {: #instance_share_dir }

Путь к директории, содержащей файлы плагинов.

Данные:

* Тип: *str*
* Значение по умолчанию: `null`

Аналогичная переменная окружения: `PICODATA_SHARE_DIR`<br>
Аналогичная команда: [`picodata run --share-dir`]

[`picodata run --share-dir`]: cli.md#run_share_dir

### instance.tier {: #instance_tier }

Имя [тира], которому будет принадлежать инстанс. Используется при
инициализации кластера и присоединении инстанса к уже существующему
кластеру.

Данные:

* Тип: *str*
* Значение по умолчанию: `default`

Аналогичная переменная окружения: `PICODATA_INSTANCE_TIER`<br>
Аналогичная команда: [`picodata run --tier`]

[`picodata run --tier`]: cli.md#run_tier

### instance.vinyl.bloom_fpr {: #instance_vinyl_bloom_fpr }
<!-- https://www.tarantool.io/en/doc/2.11/reference/configuration/#confval-vinyl_bloom_fpr -->

Вероятность ложноположительного срабатывания [фильтра Блума] для движка
хранения `vinyl`, измеряемая в долях единицы.

Предельные значения:

* `bloom_fpr: 0` — ложноположительные срабатывания отсутствуют
* `bloom_fpr: 1` — все срабатывания ложноположительные

Данные:

* Тип: *float*
* Значение по умолчанию: `0.05`

Аналогичная команда — [`picodata run --config-parameter`]. Пример:

```bash
picodata run -c instance.vinyl.bloom_fpr=0.10
```

[фильтра Блума]: https://en.wikipedia.org/wiki/Bloom_filter

### instance.vinyl.cache {: #instance_vinyl_cache }
<!-- https://www.tarantool.io/en/doc/2.11/reference/configuration/#cfg-storage-vinyl-cache -->

Размер кэша *в байтах* для движка хранения `vinyl`.

Данные:

* Тип: *int*
* Значение по умолчанию: `128M` (134217728 Б)

Поддерживаются значения в более удобном формате (`K` (Kilobytes), `M`
(Megabytes), `G` (Gigabytes), `T` (Terabytes), `1K` = 1024).

Аналогичная команда — [`picodata run --config-parameter`]. Пример:

```bash
picodata run -c instance.vinyl.cache=256M
```

### instance.vinyl.max_tuple_size {: #instance_vinyl_max_tuple_size }
<!-- https://www.tarantool.io/en/doc/2.11/reference/configuration/#confval-vinyl_max_tuple_size -->

Максимальный размер кортежа *в байтах* для движка хранения `vinyl`.

Данные:

* Тип: *int*
* Значение по умолчанию: `1M` (1048576 Б)

Поддерживаются значения в более удобном формате (`K` (Kilobytes), `M`
(Megabytes), `G` (Gigabytes), `T` (Terabytes), `1K` = 1024).

Аналогичная команда — [`picodata run --config-parameter`]. Пример:

```bash
picodata run -c instance.vinyl.max_tuple_size=2M
```

### instance.vinyl.memory {: #instance_vinyl_memory }
<!-- https://www.tarantool.io/en/doc/2.11/reference/configuration/#cfg-storage-vinyl-memory -->

Максимальное количество оперативной памяти *в байтах*, которое использует
движок хранения `vinyl`.

Данные:

* Тип: *int*
* Значение по умолчанию: `128M` (134217728 Б)

Поддерживаются значения в более удобном формате (`K` (Kilobytes), `M`
(Megabytes), `G` (Gigabytes), `T` (Terabytes), `1K` = 1024).

Аналогичная команда — [`picodata run --config-parameter`]. Пример:

```bash
picodata run -c instance.vinyl.memory=256M
```

### instance.vinyl.page_size {: #instance_vinyl_page_size }
<!-- https://www.tarantool.io/en/doc/2.11/reference/configuration/#confval-vinyl_page_size -->

Размер страницы *в байтах*, используемой движком хранения `vinyl` для
операций чтения и записи на диск.

Данные:

* Тип: *int*
* Значение по умолчанию: `8K` (8192 Б)

Поддерживаются значения в более удобном формате (`K` (Kilobytes), `M`
(Megabytes), `G` (Gigabytes), `T` (Terabytes), `1K` = 1024).

Аналогичная команда — [`picodata run --config-parameter`]. Пример:

```bash
picodata run -c instance.vinyl.page_size=16M
```

### instance.vinyl.range_size {: #instance_vinyl_range_size }
<!-- https://www.tarantool.io/en/doc/2.11/reference/configuration/#confval-vinyl_range_size -->

Максимальный размер LSM-поддерева по умолчанию *в байтах* для движка
хранения `vinyl`.

Данные:

* Тип: *int*
* Значение по умолчанию: `1G` (1073741824 Б)

Поддерживаются значения в более удобном формате (`K` (Kilobytes), `M`
(Megabytes), `G` (Gigabytes), `T` (Terabytes), `1K` = 1024).

Аналогичная команда — [`picodata run --config-parameter`]. Пример:

```bash
picodata run -c instance.vinyl.range_size=2G
```

### instance.vinyl.read_threads {: #instance_vinyl_read_threads }
<!-- https://www.tarantool.io/en/doc/2.11/reference/configuration/#confval-vinyl_read_threads -->

Максимальное количество потоков чтения для движка хранения `vinyl`.

Данные:

* Тип: *int*
* Значение по умолчанию: `1`

Аналогичная команда — [`picodata run --config-parameter`]. Пример:

```bash
picodata run -c instance.vinyl.read_threads=2
```

### instance.vinyl.run_count_per_level {: #instance_vinyl_run_count_per_level }
<!-- https://www.tarantool.io/en/doc/2.11/reference/configuration/#confval-vinyl_run_count_per_level -->

Максимальное количество файлов на уровне в LSM-дереве для движка хранения
`vinyl`.

Данные:

* Тип: *int*
* Значение по умолчанию: `2`

Аналогичная команда — [`picodata run --config-parameter`]. Пример:

```bash
picodata run -c instance.vinyl.run_count_per_level=4
```

### instance.vinyl.run_size_ratio {: #instance_vinyl_run_size_ratio }
<!-- https://www.tarantool.io/en/doc/2.11/reference/configuration/#confval-vinyl_run_size_ratio -->

Соотношение между размерами разных уровней в LSM-дереве для движка хранения
`vinyl`.

Данные:

* Тип: *float*
* Значение по умолчанию: `3.5`

Аналогичная команда — [`picodata run --config-parameter`]. Пример:

```bash
picodata run -c instance.vinyl.run_size_ratio=7.0
```

### instance.vinyl.timeout {: #instance_vinyl_timeout }
<!-- https://www.tarantool.io/en/doc/2.11/reference/configuration/#confval-vinyl_timeout -->

Максимальное время обработки запроса движком хранения `vinyl` *в секундах*.

Данные:

* Тип: *float*
* Значение по умолчанию: `60.0`

Аналогичная команда — [`picodata run --config-parameter`]. Пример:

```bash
picodata run -c instance.vinyl.timeout=120.0
```

### instance.vinyl.write_threads {: #instance_vinyl_write_threads }
<!-- https://www.tarantool.io/en/doc/2.11/reference/configuration/#confval-vinyl_write_threads -->

Максимальное количество потоков записи для движка хранения `vinyl`.

Данные:

* Тип: *int*
* Значение по умолчанию: `4`

Аналогичная команда — [`picodata run --config-parameter`]. Пример:

```bash
picodata run -c instance.vinyl.write_threads=8
```
