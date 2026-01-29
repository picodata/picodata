# Файл конфигурации

Файл конфигурации содержит параметры кластера и инстанса для
[запуска Picodata](../tutorial/run.md).

## Назначение файла конфигурации {: #config_file_purpose }

Использование файла конфигурации является способом задания
первоначальных параметров кластера и инстанса при запуске Picodata.
Другие способы предполагают ввод опций команды [`picodata
run`](cli.md#run) в консоли, а также использование переменных окружения.

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

<!-- Описание соответствует версии Picodata `25.2.0-1504-ga77a09131`. -->

Для начала работы с файлом конфигурации создайте его шаблон, выполнив
команду `picodata config default -o picodata.yaml`. Файл конфигурации
содержит параметры в формате YAML на следующих уровнях вложения:

- `cluster` — расположенные на этом уровне параметры относятся ко всему кластеру
- `tier` — параметры отдельных тиров
- `instance` — параметры текущего инстанса

Таким образом, различающиеся настройки разных инстансов на уровне
`instance` следует разделить по разным файлам конфигурации. Настройки
более высоких уровней (`cluster` и `tier`) будут использованы только
один раз при первоначальном создании кластера и их последующее изменение
в файле конфигурации будет проигнорировано.

Структура файла конфигурации:

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
  instance_dir: . # (15)!
  backup_dir: ./backup # (42)!
  name: null # (23)!
  replicaset_name: null # (29)!
  tier: default # (30)!
  failure_domain: {} # (12)!
  peer: # (24)!
  - 127.0.0.1:3301
  iproto_listen: 127.0.0.1:3301 # (16)!
  iproto_advertise: 127.0.0.1:3301 # (9)!
  http_listen: null # (13)!
  https:
    enabled: true (14)!
  admin_socket: ./admin.sock # (8)!
  share_dir: null # (28)!
  audit: null # (10)!
  log:
    level: info # (20)!
    destination: null # (18)!
    format: plain # (19)!
  memtx:
    memory: 64M # (22)!
    max_tuple_size: 1M # (21)!
  vinyl:
    memory: 128M # (32)!
    cache: 128M # (31)!
    bloom_fpr: 0.05 # (33)!
    max_tuple_size: 1M # (34)!
    page_size: 8K # (35)!
    range_size: 1G # (36)!
    run_count_per_level: 2 # (37)!
    run_size_ratio: 3.5 # (38)!
    read_threads: 1 # (39)!
    write_threads: 4 # (40)!
    timeout: 60.0 # (41)!
  pg:
    listen: 127.0.0.1:4327 # (26)!
    advertise: 127.0.0.1:4327 # (25)!
    ssl: false # (27)!
  iproto_tls:
    enabled: false # (17)!
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
14. [instance.https](#instance_https)
15. [instance.instance_dir](#instance_instance_dir)
16. [instance.iproto_listen](#instance_iproto_listen)
17. [instance.iproto_tls](#instance_iproto_tls)
18. [instance.log.destination](#instance_log_destination)
19. [instance.log.format](#instance_log_format)
20. [instance.log.level](#instance_log_level)
21. [instance.memtx.max_tuple_size](#instance_memtx_max_tuple_size)
22. [instance.memtx.memory](#instance_memtx_memory)
23. [instance.name](#instance_name)
24. [instance.peer](#instance_peer)
25. [instance.pg.advertise](#instance_pg_advertise)
26. [instance.pg.listen](#instance_pg_listen)
27. [instance.pg.ssl](#instance_pg_ssl)
28. [instance.share_dir](#instance_share_dir)
29. [instance.replicaset_name](#instance_replicaset_name)
30. [instance.tier](#instance_tier)
31. [instance.vinyl.cache](#instance_vinyl_cache)
32. [instance.vinyl.memory](#instance_vinyl_memory)
33. [instance.vinyl.bloom_fpr](#instance_vinyl_bloom_fpr)
34. [instance.vinyl.max_tuple_size](#instance_vinyl_max_tuple_size)
35. [instance.vinyl.page_size](#instance_vinyl_page_size)
36. [instance.vinyl.range_size](#instance_vinyl_range_size)
37. [instance.vinyl.run_count_per_level](#instance_vinyl_run_count_per_level)
38. [instance.vinyl.run_size_ratio](#instance_vinyl_run_size_ratio)
39. [instance.vinyl.read_threads](#instance_vinyl_read_threads)
40. [instance.vinyl.write_threads](#instance_vinyl_write_threads)
41. [instance.vinyl.timeout](#instance_vinyl_timeout)
42. [instance.backup_dir](#instance_backup_dir)

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
Задание параметра в командной строке: [`picodata run --init-replication-factor`]

[`picodata run --init-replication-factor`]: cli.md#run_init_replication_factor

### cluster.name {: #cluster_name }

Имя кластера. Инстанс не сможет присоединиться к кластеру с другим именем.

Данные:

* Тип: *str*
* Значение по умолчанию: `demo`

Аналогичная переменная окружения: `PICODATA_CLUSTER_NAME`<br>
Задание параметра в командной строке: [`picodata run --cluster-name`]

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
Задание параметра в командной строке: [`picodata run --shredding`]

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
команды `picodata admin`. В отличие от [пользовательской консоли],
коммуникация осуществляется в виде обычного текста и всегда происходит
под учетной записью администратора.

Данные:

* Тип: *str*
* Значение по умолчанию: `./admin.sock`

Аналогичная переменная окружения: `PICODATA_ADMIN_SOCK`<br>
Задание параметра в командной строке: [`picodata run --admin-sock`]

[`picodata run --admin-sock`]: cli.md#run_admin_sock
[пользовательской консоли]: ../tutorial/connecting.md#postgresql

### instance.audit {: #instance_audit }

Конфигурация журнала аудита. Доступны следующие варианты:

* `file:<FILE>` или просто `<FILE>` — запись в файл
* `pipe:<COMMAND>` или `| <COMMAND>` — перенаправление вывода в подпроцесс
* `syslog:` — перенаправление вывода в службу `syslog` защищенной ОС

Данные:

* Тип: *str*
* Значение по умолчанию: `null`

Аналогичная переменная окружения: `PICODATA_AUDIT_LOG`<br>
Задание параметра в командной строке: [`picodata run --audit`]

[`picodata run --audit`]: cli.md#run_audit

### instance.backup_dir {: #instance_backup_dir }

Директория для хранения резервных копий, создаваемых командой [`BACKUP`](sql/backup.md).
Каждый инстанс сохраняет данные в поддиректории внутри `backup-dir` в файле, имя
которого формируется в формате `YYYYMMDDThhmmss`.

Данные:

* Тип: *str*
* Значение по умолчанию: `<instance-dir>/backup`

Аналогичная переменная окружения: `PICODATA_BACKUP_DIR`<br>
Задание параметра в командной строке: [`picodata run --backup-dir`]

[`picodata run --backup-dir`]: cli.md#run_backup_dir


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

Список пар ключ-значение, разделенных запятыми, определяющий [домен
отказа] инстанса. Набор параметров домена отказа позволяет указать
расположение сервера, на котором запущен инстанс, в стойке, датацентре,
регионе и т.д. Набор ключей может быть произвольным. Picodata не будет
объединять два инстанса в один репликасет, если у них совпадают значения
хотя бы в одном ключе — вместо этого будет создан новый репликасет.
Репликасеты формируются из инстансов с разными доменами отказа до тех
пор, пока не будет достигнут желаемый [фактор репликации].

[домен отказа]: ../tutorial/deploy.md#failure_domains
[фактор репликации]: ../overview/glossary.md#replication_factor

Данные:

<!-- https://yaml.org/spec/1.2.2/#822-block-mappings -->
* Тип: Block Mappings of *{ str: str }*
* Значение по умолчанию: `{}`

Аналогичная переменная окружения: `PICODATA_FAILURE_DOMAIN`<br>
Задание параметра в командной строке: [`picodata run --failure-domain`]

Синтаксис значений домена отказа различается при работе c файлом
конфигурации (массив данных) и при использовании в командной строке
(список пар `ключ=значение`). Примеры показаны ниже.

Использование в файле конфигурации:

```yaml
failure_domain: { "region":"['us']","zone":"us-west-1" }
```

```yaml
failure_domain: { "rack":"['12-90']","server":"srv_007", "vm":"rhel8" }
```

Использование в командной строке:

```bash
picodata run --failure-domain=region=us,zone=us-west-1
```

```bash
picodata run --failure-domain=rack=12-90,server=srv_007,vm=rhel8
```

Использование в переменной:

```shell
export PICODATA_FAILURE_DOMAIN=region=us,zone=us-west-1
```

```shell
export PICODATA_FAILURE_DOMAIN=rack=12-90,server=srv_007,vm=rhel8
```

Значения домена отказа во всех случаях не зависят от регистра.

[`picodata run --failure-domain`]: cli.md#run_failure_domain

### instance.http_listen {: #instance_http_listen }

Адрес HTTP-сервера.

Данные:

* Тип: *str*
* Значение по умолчанию: `null`

Аналогичная переменная окружения: `PICODATA_HTTP_LISTEN`<br>
Задание параметра в командной строке: [`picodata run --http-listen`]

[`picodata run --http-listen`]: cli.md#run_http_listen

### instance.https {: #instance_https }

Конфигурация защищенного режима для работы с кластером по протоколу
HTTPS. Используется для получения метрик и доступа к веб-интерфейсу.
Основной параметр `instance.https.enabled` отвечает за
включение/отключение режима шифрования.

* Тип: *bool*
* Значение по умолчанию: `false`

При установке значения `true` требуется использовать 2 дополнительных параметра:

- `cert_file` (*str*) — путь к файлу сертификата
- `key_file` (*str*) — путь к файлу с закрытым ключом

а также, опционально, укажите путь к файлу с паролем, если он был задан
в настройках сертификата (пароль требуется для расшифровки закрытого
ключа):

- `password_file` (*str*)

При включенном HTTPS и использовании пароля, блок
настроек файла конфигурации будет иметь следующий вид:

```yaml
https:
    enabled: true
    cert_file: cert.pem
    key_file: key.pem
    password_file: pass.txt
```

Задание параметра в командной строке: [`picodata run --config-parameter`]. Пример:

```shell
picodata run -c instance.https.enabled=true -c instance.https.cert_file=https/cert.pem -c instance.https.key_file=https/key.pem -c instance.https.password_file=https/pass.txt
```

### instance.instance_dir {: #instance_instance_dir }

Рабочая директория инстанса. Здесь Picodata хранит все данные.

Данные:

* Тип: *str*
* Значение по умолчанию: `.`

Аналогичная переменная окружения: `PICODATA_INSTANCE_DIR`<br>
Задание параметра в командной строке: [`picodata run --instance-dir`]

[`picodata run --instance-dir`]: cli.md#run_instance_dir

### instance.iproto_advertise {: #instance_iproto_advertise }

Публичный сетевой адрес инстанса. Анонсируется кластеру при запуске
инстанса и используется для подключения к нему других инстансов.

Данные:

* Тип: *str*
* Значение по умолчанию: `127.0.0.1:3301`

Аналогичная переменная окружения: `PICODATA_IPROTO_ADVERTISE`<br>
Задание параметра в командной строке: [`picodata run --iproto-advertise`]

[`picodata run --iproto-advertise`]: cli.md#run_iproto_advertise

### instance.iproto_listen {: #instance_iproto_listen }

Сетевой адрес инстанса.

Данные:

* Тип: *str*
* Значение по умолчанию: `127.0.0.1:3301`

Аналогичная переменная окружения: `PICODATA_IPROTO_LISTEN`<br>
Задание параметра в командной строке: [`picodata run --iproto-listen`]

[`picodata run --iproto-listen`]: cli.md#run_iproto_listen

### instance.iproto_tls {: #instance_iproto_tls }

Конфигурация защищенного режима для внутренней коммуникации между узлами
кластера по протоколу Iproto. Основной параметр
`instance.iproto_tls.enabled` отвечает за включение/отключение режима
шифрования mutual TLS (mTLS).

* Тип: *bool*
* Значение по умолчанию: `false`

При установке значения `true` требуется использовать 3 дополнительных параметра:

- `instance.iproto_tls.cert_file` (*str*) — путь к файлу сертификата
- `instance.iproto_tls.key_file` (*str*) — путь к файлу с закрытым ключом
- `instance.iproto_tls.ca_file` (*str*) — путь к файлу корневого сертификата

При включенном mTLS блок настроек файла конфигурации будет иметь следующий вид:

```yaml
  iproto_tls:
    enabled: true
    cert_file: tls/server.crt
    key_file: tls/server.key
    ca_file: tls/ca.crt
```

Задание параметра в командной строке: [`picodata run --config-parameter`]. Пример:

```shell
picodata run -c instance.iproto_tls.enabled=true -c instance.iproto_tls.cert_file=tls/server.crt -c instance.iproto_tls.key_file=tls/server.key -c instance.iproto_tls.ca_file=tls/ca.crt
```

Режим mTLS настраивается глобально во всем кластере. Для параметров
`instance.iproto_tls.cert_file` и `instance.iproto_tls.key_file`
содержимое файлов должно быть идентичным на каждом инстансе.

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
Задание параметра в командной строке: [`picodata run --log`]

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
Задание параметра в командной строке: [`picodata run --log-level`]

[`picodata run --log-level`]: cli.md#run_log_level

### instance.memtx.max_tuple_size {: #instance_memtx_max_tuple_size }

Максимальный размер кортежа *в байтах* для движка хранения `memtx`.

Данные:

* Тип: *int*
* Значение по умолчанию: `1M` (1048576 Б)

Для удобства при указании значения можно использовать суффиксы (`K` (Kilobytes), `M`
(Megabytes), `G` (Gigabytes), `T` (Terabytes), `1K` = 1024).

Аналогичная команда — [`picodata run --config-parameter`]. Пример:

```bash
picodata run -c instance.memtx.max_tuple_size=2M
```

Аналогичная переменная окружения: `PICODATA_MEMTX_MAX_TUPLE_SIZE`<br>
Задание параметра в командной строке: [`picodata run --memtx-max-tuple-size`]

[`picodata run --memtx-max-tuple-size`]: cli.md#run_memtx_max_tuple_size

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

Для удобства при указании значения можно использовать суффиксы (`K` (Kilobytes), `M`
(Megabytes), `G` (Gigabytes), `T` (Terabytes), `1K` = 1024).

Пример:

```bash
picodata run -c instance.memtx.memory=128M
```

Аналогичная переменная окружения: `PICODATA_MEMTX_MEMORY`<br>
Задание параметра в командной строке: [`picodata run --memtx-memory`]

[`picodata run --memtx-memory`]: cli.md#run_memtx_memory

### instance.memtx.system_memory {: #instance_memtx_system_memory }
<!-- https://www.tarantool.io/en/doc/2.11/reference/configuration/#cfg-storage-memtx-memory -->

Объем памяти *в байтах*, выделяемый для хранения кортежей и индексов системных таблиц. Когда
достигается лимит использования памяти, запросы команд [INSERT](./sql/insert.md)
и [UPDATE](./sql/update.md) начинают отклоняться с ошибкой *ER_MEMORY_ISSUE*.
Сервер хранит в выделяемом объеме памяти только кортежи — для хранения индексов
и информации о соединениях используется дополнительная память.

Минимальное значение — 33,554,432 байтов (32 МБ)

Данные:

* Тип: *int*
* Значение по умолчанию: `256M` (268435456 Б)

Для удобства при указании значения можно использовать суффиксы (`K` (Kilobytes), `M`
(Megabytes), `G` (Gigabytes), `T` (Terabytes), `1K` = 1024).

Пример:

```bash
picodata run -c instance.memtx.system_memory=128M
```

Аналогичная переменная окружения: `PICODATA_MEMTX_SYSTEM_MEMORY`<br>
Задание параметра в командной строке: [`picodata run --memtx-system-memory`]

[`picodata run --memtx-system-memory`]: cli.md#run_memtx_system_memory

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
Задание параметра в командной строке: [`picodata run --instance-name`]

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
Задание параметра в командной строке: [`picodata run --peer`]

[`picodata run --peer`]: cli.md#run_peer

### instance.pg.advertise {: #instance_pg_advertise }

Публичный адрес сервера для подключения по протоколу
[PostgreSQL](../tutorial/connecting.md#postgresql).
Анонсируется кластеру при запуске инстанса.

Данные:

* Тип: *str*
* Значение по умолчанию: `127.0.0.1:4327`

Аналогичная переменная окружения: `PICODATA_PG_ADVERTISE`<br>
Задание параметра в командной строке: [`picodata run --pg-advertise`]

[`picodata run --pg-advertise`]: cli.md#run_pg_advertise

### instance.pg.listen {: #instance_pg_listen }

Адрес сервера для подключения по протоколу
[PostgreSQL](../tutorial/connecting.md#postgresql).

Данные:

* Тип: *str*
* Значение по умолчанию: `127.0.0.1:4327`

Аналогичная переменная окружения: `PICODATA_PG_LISTEN`<br>
Задание параметра в командной строке: [`picodata run --pg-listen`]

[`picodata run --pg-listen`]: cli.md#run_pg_listen

### instance.pg.ssl {: #instance_pg_ssl }

Признак использования протокола TLS/SSL или mTLS при подключении по протоколу PostgreSQL.

Если для признака указано значение `true`, [в рабочей директории
инстанса](cli.md#run_instance_dir) `<INSTANCE_DIR>` должны находиться
TLS-сертификат и закрытый ключ:

* `server.crt`
* `server.key`

Для двусторонней проверки подлинности (mTLS) требуется разместить рядом
файл корневого сертификата:

* `ca.crt`

Данные:

* Тип: *bool*
* Значение по умолчанию: `false`

Размещение файлов сертификатов и закрытого ключа можно переопределить,
используя следующие 3 дополнительных параметра:

- `instance.pg.cert_file` (*str*) — путь к файлу сертификата
- `instance.pg.key_file` (*str*) — путь к файлу с закрытым ключом
- `instance.pg.ca_file` (*str*) — путь к файлу корневого сертификата

При включенном mTLS блок настроек файла конфигурации будет иметь следующий вид:

```yaml
  pg:
    listen: <URI>
    advertise: <URI>
    ssl: true
    cert_file: tls/server.crt
    key_file: tls/server.key
    ca_file: tls/ca.crt
```

Аналогичная команда — [`picodata run --config-parameter`]. Пример:

```shell
picodata run -c instance.pg.ssl=true -c instance.pg.cert_file=tls/server.crt -c instance.pg.key_file=tls/server.key -c instance.pg.ca_file=tls/ca.crt
```

Режим mTLS настраивается глобально во всем кластере. Для параметров
`instance.iproto_tls.cert_file` и `instance.iproto_tls.key_file`
содержимое файлов должно быть идентичным на каждом инстансе.

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
Задание параметра в командной строке: [`picodata run --replicaset-name`]

[`picodata run --replicaset-name`]: cli.md#run_replicaset_name

### instance.share_dir {: #instance_share_dir }

Путь к директории, содержащей файлы плагинов.

Данные:

* Тип: *str*
* Значение по умолчанию: `null`

Аналогичная переменная окружения: `PICODATA_SHARE_DIR`<br>
Задание параметра в командной строке: [`picodata run --share-dir`]

[`picodata run --share-dir`]: cli.md#run_share_dir

### instance.tier {: #instance_tier }

Имя [тира], которому будет принадлежать инстанс. Используется при
инициализации кластера и присоединении инстанса к уже существующему
кластеру.

Данные:

* Тип: *str*
* Значение по умолчанию: `default`

Аналогичная переменная окружения: `PICODATA_INSTANCE_TIER`<br>
Задание параметра в командной строке: [`picodata run --tier`]

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

Для удобства при указании значения можно использовать суффиксы (`K` (Kilobytes), `M`
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

Для удобства при указании значения можно использовать суффиксы (`K` (Kilobytes), `M`
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

Для удобства при указании значения можно использовать суффиксы (`K` (Kilobytes), `M`
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

Для удобства при указании значения можно использовать суффиксы (`K` (Kilobytes), `M`
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

Для удобства при указании значения можно использовать суффиксы (`K` (Kilobytes), `M`
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
