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


## Описание файла конфигурации {: #config_file_description }

<!-- Описание соответствует версии Picodata `24.7.0-1130-gc5067f6c`. -->

Результатом выполнения команды `picodata config default -o picodata.yaml`
является файл конфигурации Picodata в формате YAML со стандартными
значениями параметров:

``` yaml title="picodata.yaml"
cluster:
  name: demo # (2)!
  tier:
    default:
      replication_factor: 1 # (4)!
      can_vote: true # (3)!
  default_replication_factor: 1 # (1)!
instance:
  instance_dir: . # (10)!
  service_password_file: null # (25)!
  name: null # (19)!
  replicaset_name: null # (24)!
  tier: default # (27)!
  failure_domain: {} # (8)!
  peer: # (20)!
  - 127.0.0.1:3301
  listen: 127.0.0.1:3301 # (12)!
  advertise_address: 127.0.0.1:3301 # (6)!
  http_listen: null # (9)!
  admin_socket: ./admin.sock # (5)!
  plugin_dir: null # (23)!
  audit: null # (7)!
  shredding: false # (26)!
  log:
    level: info # (15)!
    destination: null # (13)!
    format: plain # (14)!
  memtx:
    memory: 64M # (18)!
    checkpoint_count: 2 # (16)!
    checkpoint_interval: 3600.0 # (17)!
  vinyl:
    memory: 128M # (29)!
    cache: 128M # (28)!
    bloom_fpr: 0.05 # (30)!
    max_tuple_size: 1M # (31)!
    page_size: 8K # (32)!
    range_size: 1G # (33)!
    run_count_per_level: 2 # (34)!
    run_size_ratio: 3.5 # (35)!
    read_threads: 1 # (36)!
    write_threads: 4 # (37)!
    timeout: 60.0 # (38)!
  iproto:
    max_concurrent_messages: 768 # (11)!
  pg:
    listen: 127.0.0.1:4327 # (21)!
    ssl: false # (22)!
```

1. [cluster.default_replication_factor](#cluster_default_replication_factor)
2. [cluster.name](#cluster_name)
3. [cluster.tier.<tier_name\>.can_vote](#cluster_tier_tier_can_vote)
4. [cluster.tier.<tier_name\>.replication_factor](#cluster_tier_tier_replication_factor)
5. [instance.admin_socket](#instance_admin_socket)
6. [instance.advertise_address](#instance_advertise_address)
7. [instance.audit](#instance_audit)
8. [instance.failure_domain](#instance_failure_domain)
9. [instance.http_listen](#instance_http_listen)
10. [instance.instance_dir](#instance_instance_dir)
11. [instance.iproto.max_concurrent_messages](#instance_iproto_max_concurrent_messages)
12. [instance.listen](#instance_listen)
13. [instance.log.destination](#instance_log_destination)
14. [instance.log.format](#instance_log_format)
15. [instance.log.level](#instance_log_level)
16. [instance.memtx.checkpoint_count](#instance_memtx_checkpoint_count)
17. [instance.memtx.checkpoint_interval](#instance_memtx_checkpoint_interval)
18. [instance.memtx.memory](#instance_memtx_memory)
19. [instance.name](#instance_name)
20. [instance.peer](#instance_peer)
21. [instance.pg.listen](#instance_pg_listen)
22. [instance.pg.ssl](#instance_pg_ssl)
23. [instance.plugin_dir](#instance_plugin_dir)
24. [instance.replicaset_name](#instance_replicaset_name)
25. [instance.service_password_file](#instance_service_password_file)
26. [instance.shredding](#instance_shredding)
27. [instance.tier](#instance_tier)
28. [instance.vinyl.cache](#instance_vinyl_cache)
29. [instance.vinyl.memory](#instance_vinyl_memory)
30. [instance.vinyl.bloom_fpr](#instance_vinyl_bloom_fpr)
31. [instance.vinyl.max_tuple_size](#instance_vinyl_max_tuple_size)
32. [instance.vinyl.page_size](#instance_vinyl_page_size)
33. [instance.vinyl.range_size](#instance_vinyl_range_size)
34. [instance.vinyl.run_count_per_level](#instance_vinyl_run_count_per_level)
35. [instance.vinyl.run_size_ratio](#instance_vinyl_run_size_ratio)
36. [instance.vinyl.read_threads](#instance_vinyl_read_threads)
37. [instance.vinyl.write_threads](#instance_vinyl_write_threads)
38. [instance.vinyl.timeout](#instance_vinyl_timeout)

См. также:

* [`picodata config default`](cli.md#config_default)

## Параметры файла конфигурации {: #config_file_parameters }

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

### instance.advertise_address {: #instance_advertise_address }

Публичный сетевой адрес инстанса. Анонсируется кластеру при запуске
инстанса и используется для подключения к нему других инстансов.

Данные:

* Тип: *str*
* Значение по умолчанию: `127.0.0.1:3301`

Аналогичная переменная окружения: `PICODATA_ADVERTISE`<br>
Аналогичная команда: [`picodata run --advertise`]

[`picodata run --advertise`]: cli.md#run_advertise

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

### instance.iproto.max_concurrent_messages {: #instance_iproto_max_concurrent_messages }
<!-- https://www.tarantool.io/en/doc/2.11/reference/configuration/#cfg-networking-net-msg-max -->

Максимальное количество сообщений, которое Picodata обрабатывает параллельно.

Для обработки сообщений Picodata использует файберы. Чтобы загруженность
файберов не влияла на производительность всей системы, Picodata ограничивает
количество сообщений, обрабатываемых файберами, блокируя некоторые ожидающие
запросы.

На мощных системах можно *увеличить* значение `max_concurrent_messages`,
тогда планировщик немедленно начнет обрабатывать ожидающие запросы.

На слабых системах можно *уменьшить* значение `max_concurrent_messages`,
тогда загруженность файберов может снизиться, хотя может потребоваться
некоторое время, пока планировщик дождется завершения уже обрабатываемых
запросов.

Когда количество сообщений достигает `max_concurrent_messages`, Picodata
приостанавливает обработку входящих пакетов, пока не обработает предыдущие
сообщения. Это не прямое ограничение количества файберов, обрабатывающих
сетевые сообщения — скорее, это общесистемное ограничение пропускной
способности канала. В свою очередь, это приводит к ограничению количества
входящих сетевых сообщений, которые обрабатывает поток процессора транзакций,
и, как следствие, косвенно влияет на файберы, обрабатывающие сетевые сообщения.

<!-- The number of fibers is smaller than the number of messages because messages
can be released as soon as they are delivered, while incoming requests might
not be processed until some time after delivery. -->

Данные:

* Тип: *int*
* Значение по умолчанию: `768`

Аналогичная команда — [`picodata run --config-parameter`]. Пример:

```bash
picodata run -c instance.iproto.max_concurrent_messages=1536
```

### instance.listen {: #instance_listen }

Сетевой адрес инстанса.

Данные:

* Тип: *str*
* Значение по умолчанию: `127.0.0.1:3301`

Аналогичная переменная окружения: `PICODATA_LISTEN`<br>
Аналогичная команда: [`picodata run --listen`]

[`picodata run --listen`]: cli.md#run_listen

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

### instance.memtx.checkpoint_count {: #instance_memtx_checkpoint_count }
<!-- https://www.tarantool.io/en/doc/2.11/reference/configuration/#cfg-checkpoint-daemon-checkpoint-count -->

Максимальное количество снапшотов, хранящихся в директории *memtx_dir*.
Если после создания нового снапшота их общее количество превысит значение
этого параметра, старые снапшоты будут удалены. Если значение параметра
равно `0`, старые снапшоты останутся нетронутыми.

Данные:

* Тип: *int*
* Значение по умолчанию: `2`

Аналогичная команда — [`picodata run --config-parameter`]. Пример:

```bash
picodata run -c instance.memtx.checkpoint_count=5
```

### instance.memtx.checkpoint_interval {: #instance_memtx_checkpoint_interval }
<!-- https://www.tarantool.io/en/doc/2.11/reference/configuration/#cfg-checkpoint-daemon-checkpoint-interval -->

Период активности службы создания снапшотов (checkpoint daemon) *в секундах*.
Если значение параметра больше нуля и произошло изменение в базе данных, служба
создания снапшотов периодически вызывает функцию, которая
создает новый снапшот. Если значение параметра равно `0.0`, служба создания
снапшотов отключается.

Данные:

* Тип: *float*
* Значение по умолчанию: `3600.0` (1 час)

Аналогичная команда — [`picodata run --config-parameter`]. Пример:

```bash
picodata run -c instance.memtx.checkpoint_interval=7200.0
```

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
инстанса](cli.md#run_instance_dir) `<DATA_DIR>` должны находиться необходимые
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

### instance.plugin_dir {: #instance_plugin_dir }

Путь к директории, содержащей файлы плагинов.

Данные:

* Тип: *str*
* Значение по умолчанию: `null`

Аналогичная переменная окружения: `PICODATA_PLUGIN_DIR`<br>
Аналогичная команда: [`picodata run --share-dir`]

[`picodata run --share-dir`]: cli.md#run_plugin_dir

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

### instance.service_password_file {: #instance_service_password_file }

Путь к файлу с паролем для системного пользователя `pico_service`.

Данные:

* Тип: *str*
* Значение по умолчанию: `null`

Аналогичная переменная окружения: `PICODATA_SERVICE_PASSWORD_FILE`<br>
Аналогичная команда: [`picodata run --service-password-file`]

[`picodata run --service-password-file`]: cli.md#run_service_password_file

### instance.shredding {: #instance_shredding }

Режим безопасного удаления [рабочих файлов инстанса][runfiles] путем
многократной перезаписи специальными битовыми последовательностями, см.
[Безопасный запуск](../tutorial/run.md#secure_run).

[runfiles]: ../architecture/instance_runtime_files.md

Данные:

* Тип: *bool*
* Значение по умолчанию: `false`

Аналогичная переменная окружения: `PICODATA_SHREDDING`<br>
Аналогичная команда: [`picodata run --shredding`]

[`picodata run --shredding`]: cli.md#run_shredding

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
