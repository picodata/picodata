# Файл конфигурации

Файл конфигурации содержит параметры кластера и инстанса для
[запуска Picodata](../tutorial/run.md).

## Назначение файла конфигурации {: #config_file_purpose }

Использование файла конфигурации является дополнительным способом
задания параметров кластера и инстанса при запуске Picodata, помимо
опций команды [picodata run](cli.md#run) и переменных окружения.

Команда для запуска инстанса Picodata, если файл конфигурации назван
`config.yaml` и расположен в директории вызова команды:

```
picodata run
```

Команда для запуска инстанса Picodata в остальных случаях:

```
picodata run --config <PATH>
```

где `<PATH>` — путь к файлу конфигурации в формате YAML.

См. также:

* [picodata run --config](cli.md#run_config)


## Описание файла конфигурации {: #config_file_description }

<!-- Описание соответствует версии Picodata `24.3.0-179-gaf8647c7`. -->

Результатом выполнения команды `picodata config default -o config.yaml`
является файл конфигурации Picodata в формате YAML со стандартными
значениями параметров:

``` yaml title="config.yaml"
cluster:
  cluster_id: demo # (4)!
  tiers:
    default:
      replication_factor: 1 # (20)!
      can_vote: true # (21)!
  default_replication_factor: 1 # (8)!
instance:
  data_dir: . # (5)!
  service_password_file: null # (16)!
  instance_id: null # (9)!
  replicaset_id: null # (15)!
  tier: default # (18)!
  failure_domain: {} # (6)!
  peers: # (13)!
  - localhost:3301
  listen: localhost:3301 # (10)!
  advertise_address: localhost:3301 # (2)!
  http_listen: null # (7)!
  admin_socket: ./admin.sock # (1)!
  plugin_dir: null # (14)!
  audit: null # (3)!
  shredding: false # (17)!
  log:
    level: info # (12)!
    destination: null # (11)!
    format: plain # (22)!
  memtx:
    memory: 67108864 # (23)!
    checkpoint_count: 2 # (24)!
    checkpoint_interval: 3600.0 # (25)!
  vinyl:
    memory: 134217728 # (26)!
    cache: 134217728 # (27)!
  iproto:
    max_concurrent_messages: 768 # (28)!
  pg:
    listen: null # (19)!
    ssl: false # (29)!
```

1.  [🔗 picodata run --admin-sock](cli.md#run_admin_sock)
2.  [🔗 picodata run --advertise](cli.md#run_advertise)
3.  [🔗 picodata run --audit](cli.md#run_audit)
4.  [🔗 picodata run --cluster-id](cli.md#run_cluster_id)
5.  [🔗 picodata run --data-dir](cli.md#run_data_dir)
6.  [🔗 picodata run --failure-domain](cli.md#run_failure_domain)
7.  [🔗 picodata run --http-listen](cli.md#run_http_listen)
8.  [🔗 picodata run --init-replication-factor](cli.md#run_init_replication_factor)
9.  [🔗 picodata run --instance-id](cli.md#run_instance_id)
10. [🔗 picodata run --listen](cli.md#run_listen)
11. [🔗 picodata run --log](cli.md#run_log)
12. [🔗 picodata run --log-level](cli.md#run_log_level)
13. [🔗 picodata run --peer](cli.md#run_peer)
14. [🔗 picodata run --plugin-dir](cli.md#run_plugin_dir)
15. [🔗 picodata run --replicaset-id](cli.md#run_replicaset_id)
16. [🔗 picodata run --service-password-file](cli.md#run_service_password_file)
17. [🔗 picodata run --shredding](cli.md#run_shredding)
18. [🔗 picodata run --tier](cli.md#run_tier)
19. [🔗 picodata run --pg-listen](cli.md#run_pg_listen)
20. [cluster.tiers.<tier_name\>.replication_factor](#cluster_tiers_tier_replication_factor)
21. [cluster.tiers.<tier_name\>.can_vote](#cluster_tiers_tier_can_vote)
22. [instance.log.format](#instance_log_format)
23. [instance.memtx.memory](#instance_memtx_memory)
24. [instance.memtx.checkpoint_count](#instance_memtx_checkpoint_count)
25. [instance.memtx.checkpoint_interval](#instance_memtx_checkpoint_interval)
26. [instance.vinyl.memory](#instance_vinyl_memory)
27. [instance.vinyl.cache](#instance_vinyl_cache)
28. [instance.iproto.max_concurrent_messages](#instance_iproto_max_concurrent_messages)
29. [instance.pg.ssl](#instance_pg_ssl)

См. также:

* [picodata config default](cli.md#config_default)

## Параметры файла конфигурации {: #config_file_parameters }

### cluster.tiers.<tier_name\>.replication_factor {: #cluster_tiers_tier_replication_factor }

[Фактор репликации](../overview/glossary.md#replication_factor) тира *<tier_name\>*.

Данные:

* Тип: *int*
* Значение по умолчанию: `1`

### cluster.tiers.<tier_name\>.can_vote {: #cluster_tiers_tier_can_vote }

Признак тира *<tier_name\>*, определяющий возможность инстансов участвовать
в голосовании на выборах [raft-лидера](../overview/glossary.md#raft_leader).

Данные:

* Тип: *bool*
* Значение по умолчанию: `true`

См. также:

* [Динамическое переключение голосующих узлов в Raft](../architecture/raft_failover.md#raft_voter_failover)

### instance.log.format {: #instance_log_format }
<!-- https://www.tarantool.io/en/doc/2.11/reference/configuration/#cfg-logging-log-format -->

Формат отладочного журнала.

Возможные значения: `plain`, `json`

Данные:

* Тип: *str*
* Значение по умолчанию: `plain`

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
* Значение по умолчанию: `67108864` (64 МБ)

См. также:

* [picodata run --memtx-memory](cli.md#run_memtx_memory)

### instance.memtx.checkpoint_count {: #instance_memtx_checkpoint_count }
<!-- https://www.tarantool.io/en/doc/2.11/reference/configuration/#cfg-checkpoint-daemon-checkpoint-count -->

Максимальное количество снапшотов, хранящихся в директории *memtx_dir*.
Если после создания нового снапшота их общее количество превысит значение
этого параметра, старые снапшоты будут удалены. Если значение параметра
равно `0`, старые снапшоты останутся нетронутыми.

Данные:

* Тип: *int*
* Значение по умолчанию: `2`

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

### instance.vinyl.memory {: #instance_vinyl_memory }
<!-- https://www.tarantool.io/en/doc/2.11/reference/configuration/#cfg-storage-vinyl-memory -->

Максимальное количество оперативной памяти *в байтах*, которое использует
движок хранения `vinyl`.

Данные:

* Тип: *int*
* Значение по умолчанию: `134217728` (128 МБ)

### instance.vinyl.cache {: #instance_vinyl_cache }
<!-- https://www.tarantool.io/en/doc/2.11/reference/configuration/#cfg-storage-vinyl-cache -->

Размер кэша *в байтах* для движка хранения `vinyl`.

Данные:

* Тип: *int*
* Значение по умолчанию: `134217728` (128 МБ)

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

### instance.pg.ssl {: #instance_pg_ssl }

Признак использования протокола SSL при подключении к Pgproto.

Если для признака указано значение `true`, [в рабочей директории
инстанса](cli.md#run_data_dir) `<DATA_DIR>` должны находиться необходимые
SSL-сертификаты:

* `server.crt`
* `server.key`

Данные:

* Тип: *bool*
* Значение по умолчанию: `false`
