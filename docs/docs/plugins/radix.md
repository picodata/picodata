# Radix

В данном разделе приведены сведения о Radix, плагине для СУБД Picodata.

!!! tip "Picodata Enterprise"
    Функциональность плагина доступна только в коммерческой версии Picodata.

## Общие сведения {: #intro }

Radix — реализация [Redis](https://ru.wikipedia.org/wiki/Redis) на базе
Picodata, предназначенная для замены существующих инсталляций Redis.

Плагин Radix состоит из одноимённого сервиса (`radix`), реализующего
Redis на базе СУБД Picodata. Каждый экземпляр Radix открывает дополнительный
порт для подключения.

При использовании Picodata c плагином Radix нет необходимости в
отдельной инфраструктуре Redis Sentinel, так как каждый узел Picodata
выполняет роль прокси ко всем данным Redis.

## Соответствие версий Picodata и Radix {: #picodata_radix_versions }

Версии плагина Radix требуют определённых версий СУБД Picodata. Ниже
показана таблица совместимости версий:

| Radix | Picodata | ФСТЭК-сертификат |
| ------ | ------ | :-----: |
| 0.9.0 | 25.2.2 | :white_check_mark: |
| 0.14.1 | 25.5.7 (25.5.*) | |
| 1.0.4 | 26.1.2 (26.1.*) | |

См. также:

- [Страница загрузки Picodata](https://picodata.io/download/)

## Установка {: #install }

### Развёртывание с помощью Ansible {: #radix_deploy_ansible }

При работе с Picodata в промышленной среде удобно использовать [роль
Picodata для Ansible]. С её помощью вы можете развернуть кластер СУБД
Picodata на нескольких узлах, в том числе, с поддержкой плагинов.

Для этого потребуется:

- [установить роль](../admin/deploy_ansible.md#install_role)
- [подготовить плейбук](../admin/deploy_ansible.md#create_playbook)
- [подготовить инвентарный
  файл](../admin/deploy_ansible.md#create_inventory_file), включив в
  него блок параметров для установки Radix
- [установить кластер](../admin/deploy_ansible.md#install_cluster) с Radix

#### Конфигурация Radix для Ansible {: #radix_inventory_config }

Начиная с версии Picodata 26.1, плагин можно настроить, указав его
параметры для тиров (блок `tiers`), которые, в свою очередь, привязаны к
сервису плагина. Добавьте в инвентарный файл блок параметров,
относящийся к Radix:

```yaml

plugins:
  radix:                                                          # плагин
    path: '../files/radix_1.0.4-centos_el8.tar.gz'                # путь до пакета с Radix
    config: '../files/radix-config.yml'                           # путь до файла с настройками Radix
    services:
      radix:
        tiers:                                                    # список тиров, в которые устанавливается сервис radix
          - default:                                              # указано значение по умолчанию (default)
              listener:
                enabled: true
                listen: "0.0.0.0:73<INSTANCE_NUM>"
                advertise: "<INSTANCE_ADDR>:73<INSTANCE_NUM>"
                tls:
                  enabled: false

    migration_context:                                            # параметры миграции для Radix
      tier_for_db_0: 'default'
      tier_for_db_1: 'default'
      tier_for_db_2: 'default'
      tier_for_db_3: 'default'
      tier_for_db_4: 'default'
      tier_for_db_5: 'default'
      tier_for_db_6: 'default'
      tier_for_db_7: 'default'
      tier_for_db_8: 'default'
      tier_for_db_9: 'default'
      tier_for_db_10: 'default'
      tier_for_db_11: 'default'
      tier_for_db_12: 'default'
      tier_for_db_13: 'default'
      tier_for_db_14: 'default'
      tier_for_db_15: 'default'
      unlogged: 'UNLOGGED'
```

При настройке плагина можно использовать литералы, которые будут подставлены при разворачивании кластера:

- `<INSTANCE_ADDR>` — адрес сервера, на котором будет работать плагин
- `<INSTANCE_NUM>` — номер инстанса на сервере в двухзначном формате (01, 02, 03 ...)
- `<INSTANCE_NUM3>` — номер инстанса на сервере в трехзначном формате (001, 002, 003 ...)

#### Пример инвентарного файла {: #inventory_example }

??? example "Пример инвентарного файла cluster.yml с указанием плагина Radix"
    ```yaml
    all:
      vars:
        ansible_user: vagrant      # пользователь для ssh-доступа к серверам

        repo: 'https://download.picodata.io'  # репозиторий, откуда инсталлировать пакет Picodata

        cluster_name: 'demo'           # имя кластера
        admin_password: '123asdZXV'    # пароль пользователя admin

        default_bucket_count: 16384    # количество бакетов в каждом тире (для Radix требуется значение 16384)

        audit: false                   # состояние аудита (отключён)
        log_level: 'info'              # уровень отладки
        log_to: 'file'                 # вывод журнала в файлы (а не в journald)

        conf_dir: '/etc/picodata'         # директория для хранения конфигурационных файлов
        data_dir: '/var/lib/picodata'     # директория для хранения данных
        run_dir: '/var/run/picodata'      # директория для хранения sock-файлов
        log_dir: '/var/log/picodata'      # директория для журналов и файлов аудита
        share_dir: '/usr/share/picodata'  # директория для размещения служебных данных (плагинов)

        listen_address: '{{ ansible_fqdn }}'     # адрес, который будет слушать инстанс. Для IP указать {{ansible_default_ipv4.address}}
        pg_address: '{{ listen_address }}'       # адрес, по которому инстанс принимает подключения по PostgreSQL-протоколу

        first_bin_port: 13301     # начальный бинарный порт для первого инстанса
        first_http_port: 18001    # начальный http-порт для первого инстанса для веб-интерфейса
        first_pg_port: 15001      # начальный номер порта для PostgreSQL-протокола инстансов кластера

        tiers:                         # описание тиров
          arbiter:                     # имя тира
            replicaset_count: 1        # количество репликасетов
            replication_factor: 1      # фактор репликации
            config:
              memtx:
                memory: 64M            # количество памяти, выделяемое каждому инстансу тира
            host_groups:
              - ARBITERS               # целевая группа серверов для установки инстанса

          default:                     # имя тира
            replicaset_count: 3        # количество репликасетов
            replication_factor: 3      # фактор репликации
            bucket_count: 16384        # количество бакетов в тире
            config:
              memtx:
                memory: 71M            # количество памяти, выделяемое каждому инстансу тира
            host_groups:
              - STORAGES               # целевая группа серверов для установки инстанса

        db_config:                     # параметры конфигурации кластера (см. https://docs.picodata.io/picodata/stable/reference/db_config)
          governor_auto_offline_timeout: 30
          iproto_net_msg_max: 500
          memtx_checkpoint_count: 1
          memtx_checkpoint_interval: 7200

        plugins:
          radix:                                                        # плагин
            path: '../plugins/radix_1.0.4-1-ubuntu_noble.tar.gz'        # путь до пакета с Radix
            config: '../plugins/radix-config.yml'                       # путь до файла с настройками Radix
            services:
              radix:
                tiers:                                                  # список тиров, в которые устанавливается сервис radix
                  - default                                             # указано значение по умолчанию (default)
                listener:
                  enabled: true
                  listen: "0.0.0.0:73<INSTANCE_NUM>"
                  advertise: "<INSTANCE_ADDR>:73<INSTANCE_NUM>"
                  tls:
                    enabled: false
            migration_context:                                          # параметры миграции для Radix
              tier_for_db_0: 'default'
              tier_for_db_1: 'default'
              tier_for_db_2: 'default'
              tier_for_db_3: 'default'
              tier_for_db_4: 'default'
              tier_for_db_5: 'default'
              tier_for_db_6: 'default'
              tier_for_db_7: 'default'
              tier_for_db_8: 'default'
              tier_for_db_9: 'default'
              tier_for_db_10: 'default'
              tier_for_db_11: 'default'
              tier_for_db_12: 'default'
              tier_for_db_13: 'default'
              tier_for_db_14: 'default'
              tier_for_db_15: 'default'
              unlogged: 'UNLOGGED'
    DC1:                                # имя датацентра (failure_domain)
      hosts:                            # серверы в датацентре
        server-1-1:                     # имя сервера в инвентарном файле
          ansible_host: '192.168.19.21' # IP-адрес или fqdn если не совпадает с предыдущей строкой
          host_group: 'STORAGES'        # определение целевой группы серверов для установки инстансов

        server-1-2:                     # имя сервера в инвентарном файле
          ansible_host: '192.168.19.22' # IP-адрес или fqdn если не совпадает с предыдущей строкой
          host_group: 'ARBITERS'        # определение целевой группы серверов для установки инстансов

    DC2:                                # имя датацентра (failure_domain)
      hosts:                            # серверы в датацентре
        server-2-1:                     # имя сервера в инвентарном файле
          ansible_host: '192.168.20.21' # IP-адрес или fqdn если не совпадает с предыдущей строкой
          host_group: 'STORAGES'        # определение целевой группы серверов для установки инстансов

    DC3:                                # имя датацентра (failure_domain)
      hosts:                            # серверы в датацентре
        server-3-1:                     # имя сервера в инвентарном файле
          ansible_host: '192.168.21.21' # IP-адрес или fqdn если не совпадает с предыдущей строкой
          host_group: 'STORAGES'        # определение целевой группы серверов для установки инстансов
    ```

[роль Picodata для Ansible]: ../admin/deploy_ansible.md#plugin_management

#### Установка плагина в кластер {: #run_playbook }

Установите кластер Picodata с Radix, указав инвентарный файл и файл плейбука:

```shell
ansible-playbook -i hosts/cluster.yml playbooks/picodata.yml
```

См. также:

- [Развёртывание кластера через Ansible](../admin/deploy_ansible.md)

### Развёртывание вручную {: #radix_deploy_manual }

#### Порядок действий {: #deploy_manual_steps }

Установка плагина Radix вручную имеет ряд особенностей. Процедура установки включает:

- создание файла конфигурации для инстанса Picodata, где в разделе
  `plugins` будут указаны параметры Radix ([пример](#picodata_config)).
- установку у [тиров][tier], на которые предполагается развернуть
  плагин, 16384 [бакетов]. См. описание [bucket_count] и
  [default_bucket_count], а также [пример](#picodata_config) конфигурации
- запуск инстанса Picodata с поддержкой плагинов (параметр [`--share-dir`])
- распаковку архива Radix в директорию, указанную на предыдущем шаге
- подключение к [административной консоли][admin_console] инстанса
- выполнение SQL-команд для регистрации плагина, привязки его сервиса к
  [тиру][tier], выполнения миграции, включения плагина в кластере.
  Данные шаги более подробно описаны ниже.

[`--share-dir`]: ../reference/cli.md#run_share_dir
[admin_console]: ../tutorial/connecting.md#admin_console
[tier]: ../overview/glossary.md#tier
[бакетов]: ../overview/glossary.md#bucket
[bucket_count]: ../reference/config.md#cluster_tier_tier_bucket_count
[default_bucket_count]: ../reference/config.md#cluster_default_bucket_count

См. также:

- [Установка плагинов](../architecture/plugins.md#plugin_install)
- [Управления плагинами](../dev/plugin_mgmt.md)

#### Файл конфигурации Picodata для Radix {: #picodata_config }

Заполните файл конфигурации для инстанса Picodata, указав параметры
плагина Radix. Используйте следующий шаблон:

???+ example "cluster_config.yml: минимальная конфигурация кластера для запуска Radix"
      ```yml
      cluster:
        name: "demo_radix"
        tier:
          default:
            can_vote: true
            bucket_count: 16384
      instance:
        instance_dir: data
        memtx:
          memory: 2000000000
        share_dir: plugins-files
        pgproto:
          enabled: true
          listen: "0.0.0.0:7000"
        iproto:
          enabled: true
          listen: "0.0.0.0:8000"
        http:
          enabled: true
          listen: "0.0.0.0:9000"
        log:
          level: info
          format: plain
          destination: null
        plugin:
          radix:
            service:
              radix:
                listener:
                  enabled: true
                  listen: "0.0.0.0:7379"
                  advertise: "localhost:7379"
                  tls:
                    enabled: false
      ```

Запустите кластер с данным файлом конфигурации:

```shell
picodata run --config=cluster_config.yml
```

[Подключитесь](..//tutorial/connecting.md) к кластеру и перейдите к следующим шагам.

#### Добавление плагина в кластере {: #plugin_add }

Radix поддерживает 16 баз данных, каждую из которых можно расположить на
отдельном тире. На одном тире можно разместить несколько баз данных.
Ниже будут примеры для одного и двух тиров.

Для регистрации плагина в кластере выполните следующую SQL-команду
в административной консоли Picodata:

```sql
CREATE PLUGIN radix 1.0.4;
```

Выполните указанные ниже шаги для того, чтобы включить плагин.

#### Добавление сервиса и установка параметров {: #plugin_enable_details }

На данном этапе выполните следующие шаги:

- назначьте сервис плагина существующим тирам
- задайте значения для 16 параметров `migration_context.tier_for_db_N` (по числу баз данных в Radix)
- задайте значение для параметра `unlogged`

**Пример для одного тира (default)**

```sql
ALTER PLUGIN radix 1.0.4 ADD SERVICE radix TO TIER default;
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_0='default';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_1='default';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_2='default';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_3='default';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_4='default';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_5='default';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_6='default';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_7='default';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_8='default';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_9='default';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_10='default';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_11='default';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_12='default';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_13='default';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_14='default';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_15='default';
```

**Пример для двух тиров (hot/cold)**


```sql
ALTER PLUGIN radix 1.0.4 ADD SERVICE radix TO TIER hot;
ALTER PLUGIN radix 1.0.4 ADD SERVICE radix TO TIER cold;
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_0='hot';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_1='hot';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_2='hot';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_3='hot';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_4='cold';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_5='cold';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_6='cold';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_7='cold';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_8='cold';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_9='cold';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_10='cold';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_11='cold';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_12='cold';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_13='cold';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_14='cold';
ALTER PLUGIN radix 1.0.4 SET migration_context.tier_for_db_15='cold';
```

Параметр `unlogged` может принимать только два значения: `''` (пустая строка) для включенного WAL (режим Radix до версии 1.0.0):

```sql
ALTER PLUGIN radix 1.0.4 SET migration_context.unlogged='' OPTION(TIMEOUT=1200);
```

или `UNLOGGED` для отключённого:

```sql
ALTER PLUGIN radix 1.0.4 SET migration_context.unlogged='UNLOGGED' OPTION(TIMEOUT=1200);
```

#### Запуск миграции {: #plugin_migrate }

Для выполнения миграции выполните команду:

```sql
ALTER PLUGIN radix MIGRATE TO 1.0.4 OPTION(TIMEOUT=300);
```

<!--
!!! note "Примечание"
    При обновлении кластера с более старой версии возможна ошибка:
    ```
    unknown migration files found in manifest migrations (mismatched hash checksum for migrations/0001_dbs.sql, was 0f720bee6e85b3b83e697b1554a79687, became 529c0e80bcb67ede4aa509448550724a)
    ```
    В этом случае следует отключить проверку контрольных сумм на время обновления:
    ```sql
    ALTER SYSTEM SET plugin_check_migration_hash = 'false';
    ```
    После этого провести миграцию и включить проверку обратно:
    ```sql
    ALTER SYSTEM SET plugin_check_migration_hash = 'true';
    ```
 -->

#### Включение плагина {: #plugin_enable }

Для включения плагина в кластере:

```sql
ALTER PLUGIN radix 1.0.4 ENABLE OPTION(TIMEOUT=30);
```

Чтобы убедиться в том, что плагин успешно добавлен и запущен, выполните запрос:

```sql
SELECT * FROM _pico_plugin;
```

В строке, соответствующей плагину Radix, в колонке `enabled` должно быть
значение `true`.

### Перенос данных между кластерами {: #data_transfer }

#### Обновление с предыдущей версии Radix {: #plugin_upgrade }

Миграция с версий Radix, ниже чем 1.0.0, невозможна. Обновление Radix
1.x.x на более новую версию поддерживается и происходит аналогично
[установке](#install) Radix с той разницей, что если в кластере ранее
была включена предыдущая версия плагина, то её следует сначала отключить
и лишь затем включить новую версию. Пример:

```sql
ALTER PLUGIN radix 1.0.0 DISABLE OPTION(TIMEOUT=30);
ALTER PLUGIN radix 1.0.4 ENABLE OPTION(TIMEOUT=30);
```

#### Миграция с помощью Riot {: #migrate_with_riot }

Данный способ подходит только для переноса данных из существующих
инсталляций Redis в Radix и предполагает использование команды `riot
replicate`.

Для начала установите `riot`:

```shell
brew install redis/tap/riot
```

Команда `riot replicate` читает данные из одного экземпляра Redis и
переносит их в другой. Совместимость Radix с Redis на уровне протокола и
команд позволяет использовать этот инструмент для миграции данных из
Redis в Radix.

Операция репликации состоит из двух частей:

- непосредственно переноса данных
- верификации (аналогично команде `compare`)

Базовый синтаксис команды:

```shell
riot replicate [OPTIONS] <src_host>:<src_port> <dest_host>:<dest_port>
```

!!! note "Примечание"
    Иногда требуется перенести логическую БД отличную от 0 — в таком
    случае следует воспользоваться Redis URI с указанием номера БД. Например,
    `riot replicate redis://<src_host>:<src_port>/1 redis://<dest_host>:<dest_port>/2`
    перенесет БД 1 в приемник в БД 2

Пример:

```shell title="Перенос данных из Redis в Radix"
riot replicate --mode=scan --struct --source-cluster --target-cluster <src_host>:<src_port> <dest_host>:<dest_port>
```

После переноса запустите верификацию ключей и их значений, используя следующий синтаксис:

```shell
riot compare [OPTIONS] <src_host>:<src_port> <dest_host>:<dest_port>
```

Более подробная информация по использованию `riot` предоставляется с
документацией, входящей в поставку плагина Radix.

#### Миграция с помощью radix-cli {: #migrate_with_radix-cli }

Данный способ подходит для переноса данных как из Redis в Radix, так и
между двумя кластерами Radix. Это наш собственный инструмент миграции
данных, который доступен через утилиту `radix-cli`, поставляемую вместе
с плагином. Инструмент учитывает особенности некоторых старых версий
Radix.

Минимальный пример использования утилиты:

```shell
$ <picodata_share_dir>/radix/1.0.4/radix-cli migrate redis://localhost:7379/1 redis://localhost:7301/15
⠏ Подключено к источнику Radix 0.11.8 cluster (Picodata 25.3.8)
⠏ Подключено к приёмнику Radix 1.0.4 cluster (Picodata 26.1.3-0-gc21b37df2)
[00:00:00] [========================================] 800/800 ключей
Перенесено 800 ключей за 329мс
Пропущено (существующих): 0
Пропущено (по TTL):       0
Не удалось: 0
```

## Настройка плагина {: #configuration }

Для настройки плагина используйте файл конфигурации, который можно
применить к плагину с помощью [Picodata Pike] или [инвентарного файла
Ansible].

Пример файла конфигурации:

```yaml
clients: # ограничения клиентских соединений
  max_clients: 10000
  max_input_buffer_size: 1073741824
  max_output_buffer_size: 1073741824
redis_compatibility: # совместимость с разными версиями Редиса
  enabled_deprecated_commands: []
  enforce_one_slot_transactions: false
  push_result_includes_popped_items: true
  disable_scatter_gather: true
cluster_mode: true
sentinel_enabled: false
max_defer_actions_per_iteration: 100
authorization_mode: # авторизация
  state: disabled
eviction: # вытеснение ключей
  policy: volatile-ttl
  max_samples: 5
  lfu_decay_time: 60
  lfu_log_factor: 10
  watermark: 0.81
  tenacity: 10
```

[Picodata Pike]: ../dev/plugin_create.md#pike_plugin_config_apply
[инвентарного файла Ansible]: ../admin/deploy_ansible.md#plugin_management

### Настройка адресов и TLS {: #tls_settings }

Начиная с версии 1.0.0 настройка адресов производится в [конфигурационном файле] инстанса Picodata:

```yaml
instance:
  plugin:
    radix:
      service:
        radix:
          listener:
            enabled: true
            listen: "0.0.0.0:7379"                 # Radix откроет сокет по указанному адресу и будет его слушать
            advertise: "localhost:7379"            #  Radix будет использовать этот адрес в кластерных и sentinel-командах
            tls:
              enabled: false
```

!!! warning "Внимание!"
    Убедитесь, что на каждом узле кластера для слушающего сокета Radix
    указаны разные значения. Это необходимо для того, чтобы избежать конфликтов
    портов.

Пример с включённым TLS:

```yaml
instance:
  plugin:
    radix:
      service:
        radix:
          listener:
            enabled: true
            listen: "0.0.0.0:7379"
            advertise: "localhost:7379"
            tls:
              enabled: true
              cert_file: tls/server.crt
              key_file: tls/server.key
              ca_file: tls/ca.crt
```
[конфигурационном файле]: ../reference/config.md

Для генерации сертификатов воспользуйтесь [инструкцией]. По умолчанию будет включён mTLS, то есть клиенту необходимо предоставлять клиентские сертификаты тоже:

```shell
redis-cli --tls --cert tls/client.crt --key tls/client.key --cacert tls/ca.crt -p 7379 incr asdf2
```

[инструкцией]: ../admin/ssl.md/#create_certs_and_keys

Если вы хотите просто использовать TLS без клиентских сертификатов, то следует _НЕ указывать_ `ca_file` в файле конфигурации Picodata и _УКАЗЫВАТЬ_ его в конфигурации клиента.

```yaml
instance:
  plugin:
    radix:
      service:
        radix:
          listener:
            enabled: true
            listen: "0.0.0.0:7379"
            advertise: "localhost:7379"
            tls:
              enabled: true
              cert_file: tls/server.crt
              key_file: tls/server.key
              # ca_file: tls/ca.crt
```

```shell
redis-cli --tls --cacert tls/ca.crt -p 7379 incr asdf2
```

### clients

#### max_clients

Максимальное количество клиентов, которые могут подключиться к одному
узлу Radix. При достижении максимального числа `max_clients` новые
соединения будут отклоняться, пока количество клиентов не станет снова
меньше `max_clients`. Отклонённые соединения увеличивают счётчик метрики
`rejected_connections` (пока доступна только через `INFO STATS`).

#### max_input_buffer_size

Максимальный размер входящего буфера. Если данный параметр у соединения
будет превышен, то соединение будет закрыто. Закрытые по этой причине
соединения увеличивают счётчик метрики
`client_query_buffer_limit_disconnections` (пока доступна только через
`INFO STATS`).

#### max_output_buffer_size

Максимальный размер исходящего буфера. Если данный параметр у соединения
будет превышен, то соединение будет закрыто. Закрытые по этой причине
соединения увеличивают счётчик метрики
`client_output_buffer_limit_disconnections` (пока доступна только через
`INFO STATS`).

### redis_compatibility

#### enabled_deprecated_commands

Список устаревших команд Redis через запятую, которые будут доступны при
работе с Radix. Устаревшие команды выключены по умолчанию. Для
включения, добавьте их в [файл конфигурации](#configuration) в формате
`["command", "other"]`, где `command` и `other` — названия желаемых
команд в нижнем регистре.

#### enforce_one_slot_transactions

Включает режим, в котором в транзакциях могут участвовать ключи только
из одного слота (как и в Redis Cluster). Если он выключен — в
транзакциях могут участвовать ключи из всего репликасета (между
слотами), как в Redis Standalone. В этом случае вы можете эмулировать
Redis Standalone, развернув Radix в составе одного репликасета.

Значение по умолчанию: `false` (выключено).

#### push_result_includes_popped_items

Включает режим, в котором элементы списков и сортированных множеств,
отправившиеся в блокирующие команды, учитываются в выходных значениях
команд [LPUSH](#lpush), [RPUSH](#rpush), [ZADD](#zadd),
[ZUNIONSTORE](#zunionstore), [ZRANGESTORE](#zrangestore),
[ZDIFFSTORE](#zdiffstore) и [ZINTERSTORE](#zinterstore).

Согласно документации Redis, команды, добавляющие элементы в списки и
множества, должны возвращать количество, соответственно вставленных
записей. Однако, если список/множество не содержит элементов, и при этом
есть клиенты, которые ожидают вставки в него через блокирующие команды
([BLPOP](#blpop), например), то часть записей вставлены не будут, а
вместо этого возвратятся этим клиентам — Radix вернёт только количество
действительно вставленных элементов. Если нужно поведение как в Redis
(то есть, необходимо вернуть и количество элементов, улетевших в `BLPOP`
и аналогичные команды), выставите эту настройку в `true`.

### cluster_mode

Данная настройка влияет только на вывод команды `info cluster`.

### sentinel_enabled

Включает режим совместимости с [Redis Sentinel](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/).

В приложении требуется прописать адреса Radix в качестве адресов
Sentinel. В качестве `service_name` укажите имена репликасетов, на
которых развёрнут Radix (например, если в кластере 1 репликасет на
тире `default`, то по умолчанию имя репликасета будет `default_1`).
В качестве адреса Sentinel укажите адрес любого инстанса Radix.

Для примера, если в кластере нескольких тиров, а Radix развёрнут на тирах `hot` с двумя
репликасетами и `cold` — с четырьмя, то получится такая топология:

```yaml
# вариант топологии Radix
hot:
  hot_1:
  - hot_1_1
  - hot_1_2
  hot_2:
  - hot_2_1
  - hot_2_2
cold:
  cold_1:
  - cold_1_1
  - cold_1_2
  cold_2:
  - cold_2_1
  - cold_2_2
  cold_3:
  - cold_3_1
  - cold_3_2
  cold_4:
  - cold_4_1
  - cold_4_2
```

Следует указывать названия `service_name` и адреса из одного тира. Если
использовать в качестве адреса для Sentinel адрес реплики из другого
тира, то такая конфигурация не будет работать.

### max_defer_actions_per_iteration

Максимальное количество действий, которые модуль обработки TTL будет
выполнять за одну итерацию. По умолчанию — 100.

### authorization_mode {: #auth_mode }

Управляет состоянием авторизации в Radix. Доступные поля и их значения:

- **state** — параметр, отвечающий за режим авторизации. Возможные
  значения:
    - `disabled` — авторизация выключена (значение по умолчанию). Любой
      подключившийся имеет доступ ко всем командам, ключам и
      PUBSUB-каналам. Команда `AUTH` всегда возвращает `OK`
    - `enabled` — авторизация включена. Для доступа нужно авторизоваться
      с помощью команды `AUTH`
- **default_user_name** — имя пользователя по умолчанию. Вариант `{
  "state": "enabled", "default_user_name": "<str>" }` используется для
  клиентов, которые подключаются с помощью `AUTH password` без имени
  пользователя. По умолчанию значение не задано
- **aclfile** — Опциональное поле, указывающее расположение файла для команд
  `ACL SAVE` и `ACL LOAD`. Если поле не указано, то вызов этих команд
  завершиться с ошибкой. Путь до файла считается относительно директории, в
  которой был запущен процесс `picodata` (не от [instance-dir]). Для удобства
  рекомендуется использовать полный путь до файла. В отличие от Redis Cluster, в
  Radix достаточно иметь данный файл на одном узле, а загруженные права хранятся
  в кластере в одном экземпляре (загрузив `aclfile` на одном узле, пользователи
  и их права будут одинаковы на всем кластере).

[instance-dir]: ../reference/config.md#instance_instance_dir

### eviction

Управляет механизмом вытеснения ключей.

По умолчанию не задано, то есть выключено.

#### policy

Возможные стратегии вытеснения:

- `noeviction` — вытеснение выключено
- `volatile-*` — могут быть вытеснены только ключи с TTL:
    - `volatile-lru` — вытесняются наиболее давно использованные
    - `volatile-lfu` — вытесняются наименее часто используемые
    - `volatile-random` — вытесняются случайные
    - `volatile-ttl` — вытесняются с наименьшим TTL
- `allkeys-*` — могут быть вытеснены любые ключи:
    - `allkeys-lru` — вытесняются наиболее давно использованные
    - `allkeys-lfu` — вытесняются наименее часто используемые
    - `allkeys-random` — вытесняются случайные

#### max_samples

Количество ключей в каждой базе данных, участвующих в выборе лучшего
кандидата для вытеснения. Чем больше значение параметра, тем медленнее и
точнее работает алгоритм.

По умолчанию равен `5`.

#### lfu_decay_time

Период затухания счётчика использования ключа в минутах. Чем больше
значение параметра, тем медленнее уменьшается счётчик.

По умолчанию равен `60`, то есть каждый час счётчик использования
уменьшается на единицу.

#### lfu_log_factor

Определяет, насколько медленно будет расти счётчик использования. Чем
больше значение параметра, тем медленнее увеличивается счётчик.

По умолчанию равен `10`, то есть счётчик использования увеличивается на
единицу каждые 10 доступов к ключу.

#### watermark

Уровень квоты памяти, при достижении которого начинает работать
вытеснение. Например, если
[`memtx.memory`](../reference/config.md#instance_memtx_memory) выставлен в
100 МБ и `watermark` в 0.75, то вытеснение работает, когда данные
занимают более 75 МБ.

По умолчанию параметр равен `0.81` (если у Picodata `arena_used_ratio` >
0.9 и `items_used_ratio` > 0.9, то она находится в состоянии нехватки
памяти).

#### tenacity

"Упорство" алгоритма — целое число от `0` до `100` включительно,
определяющее, насколько долго может работать вытеснение перед
исполнением команды. Чем больше значение параметра, тем больше таймаут.
Если время исполнения превышает таймаут, алгоритм переходит в работу в
фоновом режиме и освобождение места после исполнения команды не
гарантируется.

В таблице ниже приведены некоторые значения параметров и соответствующая
длина таймаута.

| Значение `tenacity` | Поведение                                     |
| ------------------- | --------------------------------------------- |
| 0                   | Вытеснение происходит только в фоне           |
| 10                  | Переход в работу в фоне после 500 миллисекунд |
| 50                  | Переход в работу в фоне после 134 секунд      |
| 70                  | Переход в работу в фоне после 36 минут        |
| 100                 | Вытеснение происходит синхронно               |

Формула расчета:
$$
f(t) =
\begin{cases}
  50t, & \text{если } 0 \le t \le 10, \\\\
  500 \times 1.15^{\,t-10}, & \text{если } 11 \le t \le 99, \\\\
  +\infty, & \text{если } t = 100.
\end{cases}
$$

#### Предустановленные роли {: #auth_roles }

- Глобальные:
    - `radix_reader` — доступ на чтение ко всем данным,
    - `radix_writer` — доступ на запись ко всем данным.
- Локальные для каждой БД:
    - `radix_reader_0` … `radix_reader_15`
    - `radix_writer_0` … `radix_writer_15`

#### Примеры использования {: #auth_examples }

##### Миграция с кластера с директивой `requirepass` {: #requirepass }

Для миграции на Radix необходимо:

1. В Picodata создать пользователя по умолчанию, с правами `radix_reader` и `radix_writer`
2. Через клиент Redis настроить ACL на доступ ко всему
3. Включить авторизацию через смену конфигурации Radix

```sql title="Создание пользователя в Picodata и наделение его правами"
ALTER PLUGIN radix 1.0.4 SET radix.authorization_mode = '{ "state": "enabled", "default_user_name": "default_radix_user" }';
CREATE USER default_radix_user WITH PASSWORD 'S0m1Str2ngP3ssword';
GRANT radix_reader TO default_radix_user;
GRANT radix_writer TO default_radix_user;
```

```shell title="Выдача прав пользователю в Radix"
$ redis-cli -p 7301
127.0.0.1:7301> acl setuser default_radix_user ~* &* +@all
OK
```

```sql title="Изменение конфигурации Radix через SQL-запросы в Picodata"
ALTER PLUGIN radix 1.0.4 SET radix.authorization_mode='{"state": "enabled", "default_user_name": "default_radix_user"}';
ALTER PLUGIN SET
```

```shell title="Проверка аутентификации в Radix"
$ redis-cli -p 7301
127.0.0.1:7301> GET abc
(error) NOAUTH Authentication required
127.0.0.1:7301> AUTH S0m1Str2ngP3ssword
OK
127.0.0.1:7301> SET abc 123
OK
```

##### Использование LDAP {: #ldapuser }

```sql
ALTER PLUGIN radix 1.0.4 SET radix.authorization_mode = '{ "state": "enabled", "default_user_name": "default_radix_user" }';
CREATE USER default_radix_user USING ldap;
GRANT radix_reader TO default_radix_user;
GRANT radix_writer TO default_radix_user;
```

##### Использование Argus для синхронизации пользователей {: #argus }

```yaml
argus:
  searches:
    - role: "radix_reader"
      base: "dc=example,dc=org"
      filter: "<filter>"
      attr: "cn"
    - role: "radix_writer"
      base: "dc=example,dc=org"
      filter: "<filter>"
      attr: "cn"
```

##### Разделение доступов по БД {: #access_separation }

```sql
ALTER PLUGIN radix 1.0.4 SET radix.authorization_mode = '{ "state": "enabled" }';

CREATE USER app_1_user WITH PASSWORD 'pwd1';
GRANT radix_reader_0 TO app_1_user;
GRANT radix_writer_0 TO app_1_user;

CREATE USER app_2_user WITH PASSWORD 'pwd2';
GRANT radix_reader_0 TO app_2_user;
GRANT radix_writer_2 TO app_2_user;

CREATE USER app_3_user WITH PASSWORD 'pwd3';
GRANT radix_reader_0 TO app_3_user;
GRANT radix_writer_5 TO app_3_user;
```

## Использование {: #usage }

Для работы с Radix используйте клиентскую программу `redis-cli`.
Подключение осуществляется по адресу, заданному параметром `advertise`
(или `listen` если публичный адрес не указан) для секции `radix` в
[файле конфигурации](#picodata_config).

```shell
redis-cli -p 7301
```

## Поддерживаемые команды {: #supported_commands }

<style>

.tag {
    line-height: 1em;
    margin-left: 0.5em;
    width: 87em;
    padding: 0.3em 0.7em;
    border-radius: 1em;
    font-family: monospace;
    font-size: 10pt;
    background-color: #d9ead3;
}

.admin {
    background-color: #000000;
    color: white;
}

.blocking {
    background-color: #a8cb48;
}

.dangerous {
    background-color: #d56000;
    color: white;
}

.connection {
    background-color: #b959f5;
    color: white;
}

.fast {
    background-color: #153cff;
    color: white;
}

.hash {
    background-color: #fff04a;
}

.keyspace {
    background-color: #84ffb3;
}

.list {
    background-color: #28904e;
    color: white;
}

.pico {
    background-color: #f13737;
    color: white;
}

.pubsub {
    background-color: #814444;
    color: white;
}

.read {
    background-color: #ffb5e2;
}

.set {
    background-color: #3af5ff;
}

.scripting {
    background-color: #ffe7bd;
}

.slow {
    background-color: #acacac;
    color: white;
}

.sortedset {
    background-color: #95aac7;
}

.string {
    background-color: #b0deff;
}

.transaction {
    background-color: #ffbf87;
}

.write {
    background-color: #6d106f;
    color: white;
}
</style>


### Управление доступом {: #cluster_acl }

Управление доступом в Radix реализовано с помощью списков контроля
доступа (access control lists, ACL). С их помощью администратор может
ограничить доступ пользователей к определённым ключам и их значениям.
Команды сгруппированы в категории, что позволяет назначать пользователям
сразу нужные наборы прав доступа.

Реализованные в Radix ACL-команды и их категории описаны ниже.

#### Категории ACL {: #acl_categories }

В Radix используются следующие ACL-категории для команд:

- <span class="tag admin">admin</span> — административные команды. Пользователям, работающим с данными БД, они обычно не нужны
- <span class="tag blocking">blocking</span> — команды, блокирующие выполнение других команд
- <span class="tag dangerous">dangerous</span></span> — потенциально опасные команды (с точки зрения сохранности данных)
- <span class="tag connection">connection</span> — команды, имеющие отношение к управлению соединениями
- <span class="tag fast">fast</span> — команды быстрого выполнения, на скорость которых не влияет количество элементов, хранящихся в целевом ключе
- <span class="tag hash">hash</span> — все команды, имеющие отношение к работе с хэшем
- <span class="tag keyspace">keyspace</span> — команды, работающие со значениями ключей
- <span class="tag list">list</span> — команды для работы со списками
- <span class="tag pico">pico</span> — команды, специфичные для Picodata
- <span class="tag pubsub">pubsub</span> — все команды, имеющие отношение к pubsub
- <span class="tag read">read</span> — команды, читающие данные из ключей
- <span class="tag set">set</span> — команды, устанавливающие значения
- <span class="tag scripting">scripting</span> — команды для работы со скриптами
- <span class="tag slow">slow</span> — команды медленного выполнения
- <span class="tag sortedset">sortedset</span> — команды, имеющие отношение к сортированным множествам
- <span class="tag string">string</span> — команды, работающие со строковыми данными
- <span class="tag transaction">transaction</span> — команды для работы с транзакциями
- <span class="tag write">write</span> —  команды, записывающие данные в ключи

#### acl cat {: #acl_cat }

```sql
ACL CAT [category]
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag slow">slow</span>

При использовании без дополнительных аргументов, данная команда выводит
список доступных категорий. При указании в качестве аргумента конкретной
категории, команда выведет список команд, входящих в неё.

#### acl dryrun {: #acl_dryrun }

```sql
ACL DRYRUN username command [arg [arg ...]]
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Симулирует выполнение команды пользователем. `DRYRUN` удобен для
проверки того, что у пользователя достаточно прав на выполнение
указанной команды.

Пример:

```sql
> ACL SETUSER VIRGINIA +SET ~*
"OK"
> ACL DRYRUN VIRGINIA SET foo bar
"OK"
> ACL DRYRUN VIRGINIA GET foo
"User VIRGINIA has no permissions to run the 'get' comman
```

#### acl getuser {: #acl_getuser }

```sql
ACL GETUSER username
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Возвращает все доступные ACL-данные для указанного пользователя (флаги,
хэши паролей, разрешённые команды и т.д.).

#### acl list {: #acl_list }

```sql
ACL LIST
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Возвращает список всех пользователей (кроме служебных
`guest`/`admin`/`pico-service`) с сопоставленными им правилами. Если для
пользователя ACL не заданы, то используются правила по умолчанию
(запрещены все команды/ключи/каналы).

#### acl load {: #acl_load }


```sql
ACL LOAD
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Загружает на сервер Radix набор ACL-правил, определённых в файле
`aclfile` (задаётся в разделе [authorization_mode](#auth_mode)
конфигурации Radix). Существующие правила ACL переписываются (сначала
удаляются все заданные правила ACL, а потом загружаются из файла).

#### acl log {: #acl_log }

```sql
ACL LOG [count | RESET]
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Выводит журнал последних событий, связанных с доступом к данным, включая:

- ошибки авторизации (например, при [AUTH](#auth))
- ошибки выполнения команд из-за недостатка прав

Недавние записи находятся в начале списка. Параметр `count` позволяет
указать количество выводимых записей (по умолчанию `10`). Параметр
`RESET` позволяет очистить журнал.

#### acl save {: #acl_save }

```sql
ACL SAVE
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Записывает текущие ACL-правила на сервере Radix в `aclfile`. Для работы
этой команды необходимо настроить расположение файла `aclfile` задаётся
в разделе [authorization_mode](#auth_mode) конфигурации Radix).

#### acl setuser {: #acl_setuser }

```sql
ACL SETUSER username [rule [rule ...]]
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Задаёт набор правил ACL для существующего пользователя Picodata.
Если пользователю ранее были заданы правила ACL, то данная
команда добавит новые правила к существующему списку, не затирая его.

Пример:

```sql
ACL SETUSER virginia on allkeys +set
ACL SETUSER virginia +get
> ACL LIST
1) "user virginia on -@allkeys +set +get"
```

Список доступных правил для работы с данными:

- `~<pattern>` — добавляет указанный шаблон ключа в список шаблонов
  ключей, доступных пользователю. Это предоставляет права как на чтение,
  так и на запись для ключей, соответствующих данному шаблону. Можно
  добавить несколько шаблонов ключей для одного и того же пользователя.
  Пример: `~objects:*`
- `%R~<pattern>` — добавляет указанный шаблон ключа для чтения. Он
  работает аналогично обычному шаблону ключа, но предоставляет права
  только на чтение из ключей, соответствующих данному шаблону
- `%W~<pattern>` — добавляет указанный шаблон записи ключей. Он работает
  аналогично обычному шаблону ключей, но предоставляет разрешение на
  запись только в те ключи, которые соответствуют данному шаблону
- `%RW~<pattern>` — аналогичен `~<pattern>`
- `allkeys` — предоставляет доступ ко всем ключам, аналогичен `~*`
- `resetkeys` — очищает все шаблоны ключей, ранее назначенные пользователю
- `&<pattern>` — добавляет указанный шаблон в стиле _glob_ в список
  шаблонов каналов Pub/Sub, доступных пользователю ([подробнее](#keys))
- `allchannels` — предоставляет доступ ко всем каналам Pub/Sub, аналогичен `&*`
- `resetchannels` — очищает все шаблоны каналов, ранее назначенные пользователю
- `+<command>` —  добавляет команду в список команд, которые
  пользователь может выполнять. Может использоваться с символом `|` для
  разрешения подкоманд (например, `+config|get`)
- `+@<category>` — добавляет категорию команд в список команд, которые
  пользователь может выполнять (напримр, `+@string`). Список категорий доступен
  по команде [`ACL CAT`](#acl_cat)
- `allcommands` — добавляет все команды, имеющиеся на сервере, включая
  будущие команды, загружаемые через модули, для выполнения этим
  пользователем. Аналогичен `+@all`
- `-<command>` — удаляет команду из списка команд, которые
  пользователь может выполнять. Может использоваться с символом `|` для
  блокирования подкоманд (например, `-config|set`)
- `-@<category>` — действует противоположно `+<category>`, то есть,
  удаляет все команды категории из списка команд, которые
  пользователь может выполнять.
- `nocommands` — удаляет все права пользователя, лишая его возможности
  что-либо выполнять. Аналогичен `-@all`
- `(<rule list>)` — создаёт новый селектор для сопоставления правил.
  Селекторы применяются после прав пользователя и в том порядке, в
  котором они перечислены. Если команда соответствует либо правам
  пользователя, либо любому селектору, она разрешена
- `clearselectors` — удаляет все селекторы, привязанные к пользователю
- `reset` — удаляет все  правила работы с данными у пользователя. Они
  устанавливаются в состояние «выключено»: без паролей, без возможности
  выполнять какие-либо команды и без доступа к каким-либо ключам

#### acl users {: #acl_users }

```sql
ACL USERS
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Выводит список пользователей и их ACL.

#### acl whoami {: #acl_whoami }

```sql
ACL WHOAMI
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag slow">slow</span>

Выводит список текущего (прошедшего авторизацию) пользователя.

### Управление кластером {: #cluster_management }

#### cluster getkeysinslot {: #cluster_getkeysinslot }

```sql
CLUSTER GETKEYSINSLOT slot count
```
<span class="tag">поддерживается с версии 0.4.0</span>
<span class="tag slow">slow</span>

Возвращает набор ключей, которые, в соответствии со своими хэш-суммами,
относятся к указанному слоту. Второй аргумент ограничивает максимальное
количество возвращаемых ключей.

#### cluster info {: #cluster_info }

```sql
CLUSTER INFO
```
<span class="tag">поддерживается с версии 0.12.0</span>
<span class="tag slow">slow</span>

Возвращает основной набор параметров кластера. Пример:

```sql
127.0.0.1:7301> cluster info
cluster_state:ok
cluster_slots_assigned:16384
cluster_slots_ok:16384
cluster_slots_pfail:0
cluster_slots_fail:0
cluster_known_nodes:8
cluster_size:4
cluster_current_epoch:2
cluster_my_epoch:2
cluster_stats_messages_ping_sent:0
cluster_stats_messages_pong_sent:0
cluster_stats_messages_sent:0
cluster_stats_messages_ping_received:0
cluster_stats_messages_pong_received:0
cluster_stats_messages_meet_received:0
cluster_stats_messages_fail_received:0
cluster_stats_messages_received:0
```

#### cluster keyslot {: #cluster_keyslot }

```sql
CLUSTER KEYSLOT key
```
<span class="tag">поддерживается с версии 0.4.0</span>
<span class="tag slow">slow</span>

Позволяет узнать, к какому хэш-слоту относится указанный в команде ключ.

#### cluster myid {: #cluster_myid }

```sql
CLUSTER MYID
```
<span class="tag">поддерживается с версии 0.4.0</span>
<span class="tag slow">slow</span>

Возвращает идентификатор текущего узла кластера (INSTANCE UUID).

#### cluster myshardid {: #cluster_myshardid }

```sql
CLUSTER MYSHARDID
```
<span class="tag">поддерживается с версии 0.4.0</span>
<span class="tag slow">slow</span>

Возвращает идентификатор текущего репликасета, в который входит текущий
узел кластера (REPLICASET UUID).

#### cluster nodes {: #cluster_nodes }

```sql
CLUSTER NODES
```
<span class="tag">поддерживается с версии 0.5.0</span>
<span class="tag slow">slow</span>

Возвращает информацию о текущем составе и конфигурации узлов кластера,
включая номера бакетов, относящихся к узлам.

#### cluster replicas {: #cluster_replicas }

```sql
CLUSTER REPLICAS node-id
```
<span class="tag">поддерживается с версии 0.5.0</span>
<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Возвращает состав реплицированных узлов (т.е. состав репликасета)

#### cluster shards {: #cluster_shards }

```sql
CLUSTER SHARDS
```
<span class="tag">поддерживается с версии 0.5.0</span>
<span class="tag slow">slow</span>

Возвращает подробную информацию о шардах кластера.

#### cluster slots {: #cluster_slots }

```sql
CLUSTER SLOTS
```
<span class="tag">поддерживается с версии 0.5.0</span>
<span class="tag slow">slow</span>

Возвращает информацию о соответствии слотов инстансам кластера.

#### echo {: #cluster_echo }

```sql
ECHO message
```
<span class="tag">поддерживается с версии 0.11.0</span>
<span class="tag connection">connection</span>
<span class="tag fast">fast</span>

Возвращает сообщение (`message`).

#### ping

```sql
PING [message]
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag connection">connection</span>
<span class="tag fast">fast</span>

Возвращает `PONG`, если аргумент не указан, в противном случае
возвращает строкой аргумент, который пришёл. Эта команда полезна для:

- проверки того, живо ли ещё соединение
- проверки способности сервера обслуживать данные — ошибка возвращается,
  если это не так (например, при загрузке из постоянного хранилища или
  обращении к устаревшей реплике)
- измерения задержки

#### quit

```sql
QUIT
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag connection">connection</span>
<span class="tag fast">fast</span>

Отправляет серверу сигнал на закрытие соединения. Сервер исполнит запрос
после того как будут отправлены все ответы на уже обработанные запросы.
Данная команда относится к числу устаревших и не рекомендуется к
использованию — более правильно разрывать соединение на стороне клиента
когда оно больше не требуется.

??? warning "Примечание"
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN radix 1.0.4 SET radix.redis_compatibility = '{ "enforce_one_slot_transactions": true, "push_result_includes_popped_items": true, "disable_scatter_gather": true, "enabled_deprecated_commands": ["quit" ] }';
    ```

#### readonly {: #cluster_readonly }

```sql
READONLY
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag connection">connection</span>
<span class="tag fast">fast</span>

Переводит сессию в режим,  в котором получение данных производится не с
лидеров репликасетов, а с резервных реплик (при факторе репликации ≥ 2).

### Управление соединениями {: #connection_management }

#### auth  {: #auth }

```sql
auth password
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag connection">connection</span>
<span class="tag fast">fast</span>

Выполняет аутентификацию пользователя по умолчанию. Имя пользователя должно быть задано
в конфигурации плагина в параметре `default_user_name`.

```sql
auth username password
```

Выполняет аутентификацию выбранного пользователя.

#### reset

```sql
reset
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag connection">connection</span>
<span class="tag fast">fast</span>

Сбрасывает соединение в состояние по умолчанию:

- откатывается текущая транзакция, если она была открыта,
- сбрасываются наблюдения за ключами, которые раньше были установлены командой WATCH,
- если были открыты курсоры командами SCAN/HSCAN, то они закрываются,
- сбрасывается авторизация, потребуется её пройти заново.

#### select

```sql
SELECT index
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag connection">connection</span>
<span class="tag fast">fast</span>

Получение логической базы данных Redis с указанным нулевым числовым
индексом. Новые соединения всегда используют базу данных 0.

### Общие команды {: #general }

#### dbsize

```sql
DBSIZE
```
<span class="tag">поддерживается с версии 0.5.0</span>
<span class="tag fast">fast</span>
<span class="tag keyspace">keyspace</span>
<span class="tag read">read</span>

Возвращает количество ключей в базе данных

#### del

```sql
DEL key [key ...]
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag keyspace">keyspace</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Удаляет указанные ключи. Несуществующие ключи игнорируются.

#### exists

```sql
EXISTS key [key ...]
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag fast">fast</span>
<span class="tag keyspace">keyspace</span>
<span class="tag read">read</span>

Проверяет, существует ли указанный ключ `key` и возвращает число совпадений.
Например, запрос `EXISTS somekey somekey` вернёт `2`.

#### expire

```sql
EXPIRE key seconds [NX | XX | GT | LT]
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag fast">fast</span>
<span class="tag keyspace">keyspace</span>
<span class="tag write">write</span>

Устанавливает срок жизни (таймаут) для ключа `key` в секундах (TTL, time
to live). По истечении таймаута ключ будет автоматически удален. В
терминологии Redis ключ с установленным тайм-аутом часто называют
_волатильным_.

Тайм-аут будет сброшен только командами, которые удаляют или
перезаписывают содержимое ключа, включая DEL, SET и GET/SET. Это
означает, что все операции, которые концептуально изменяют значение,
хранящееся в ключе, не заменяя его новым, оставляют таймаут нетронутым.

#### expireat

```sql
EXPIREAT key unix-time-seconds [NX | XX | GT | LT]
```
<span class="tag">поддерживается с версии 0.7.0</span>
<span class="tag fast">fast</span>
<span class="tag keyspace">keyspace</span>
<span class="tag write">write</span>

Устанавливает срок жизни (таймаут) для ключа `key` подобно
[EXPIRE](#expire), но вместо оставшегося числа секунд (TTL, time to
live) использует абсолютное время Unix timestamp — число секунд,
прошедших с 01.01.1970. Если максимальное число секунд превышено (т.е.
дата отсчёта находится ранее 01.01.1970), то ключ будет автоматически
удален.

Дополнительные параметры `EXPIREAT`:

- `NX` — установить срок жизни только если он не был ранее установлен
- `XX` — установить срок жизни только если ключ уже имеет ранее установленный срок
- `GT` — установить срок жизни только если он превышает ранее установленный срок
- `LT` — установить срок жизни только если он меньше ранее установленного срока

#### expiretime

```sql
EXPIRETIME key
```
<span class="tag">поддерживается с версии 0.7.0</span>
<span class="tag fast">fast</span>
<span class="tag keyspace">keyspace</span>
<span class="tag read">read</span>

Возвращает срок жизни (таймаут) ключа `key` в секундах согласно формату
Unix timestamp.

#### keys

```sql
KEYS pattern
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag dangerous">dangerous</span>
<span class="tag keyspace">keyspace</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>

Возвращает все ключи, соответствующие шаблону.

Поддерживаются шаблоны в стиле _glob_:

- `h?llo` соответствует hello, hallo и hxllo
- `h*llo` соответствует hllo и heeeello
- `h[ae]llo` соответствует hello и hallo, но не hillo
- `h[^e]llo` соответствует hallo, hbllo, ... но не hello
- `h[a-b]llo` соответствует hallo и hbllo

#### persist

```sql
PERSIST key
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag fast">fast</span>
<span class="tag keyspace">keyspace</span>
<span class="tag write">write</span>

Удаляет существующий таймаут для ключа `key`, превращая его из непостоянного
(ключ с установленным сроком действия) в постоянный (ключ, срок действия
которого никогда не истечёт, поскольку таймаут для него не установлен).

#### pexpire

```sql
PEXPIRE key milliseconds [NX | XX | GT | LT]
```
<span class="tag">поддерживается с версии 0.7.0</span>
<span class="tag fast">fast</span>
<span class="tag keyspace">keyspace</span>
<span class="tag write">write</span>

Устанавливает срок жизни (таймаут) для ключа `key` подобно
[EXPIRE](#expire), но в миллисекундах.

#### pexpireat

```sql
PEXPIREAT key unix-time-milliseconds [NX | XX | GT | LT]
```
<span class="tag">поддерживается с версии 0.7.0</span>
<span class="tag fast">fast</span>
<span class="tag keyspace">keyspace</span>
<span class="tag write">write</span>

Устанавливает срок жизни (таймаут) для ключа `key` подобно
[EXPIREAT](#expireat), но в миллисекундах.

#### pexpiretime

```sql
PEXPIRETIME key
```
<span class="tag">поддерживается с версии 0.7.0</span>
<span class="tag fast">fast</span>
<span class="tag keyspace">keyspace</span>
<span class="tag read">read</span>

Возвращает срок жизни (таймаут) ключа `key` подобно
[EXPIRETIME](#expiretime), но в миллисекундах.

#### pttl

```sql
PTTL key
```
<span class="tag">поддерживается с версии 0.7.0</span>
<span class="tag fast">fast</span>
<span class="tag keyspace">keyspace</span>
<span class="tag read">read</span>

Возвращает оставшееся время жизни ключа `key` подобно [TTL](#ttl), но в
миллисекундах.

#### scan

```sql
SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag keyspace">keyspace</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>

Команда `SCAN` используется для инкрементного итерационного просмотра
коллекции элементов в выбранной в данный момент базе данных Redis.

#### ttl

```sql
TTL key
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag fast">fast</span>
<span class="tag keyspace">keyspace</span>
<span class="tag read">read</span>

Возвращает оставшееся время жизни ключа `key`, для которого установлен
таймаут. Эта возможность интроспекции позволяет клиенту Redis проверить,
сколько секунд данный ключ будет оставаться частью набора данных.

Команда возвращает `-2`, если ключ не существует.

Команда возвращает `-1`, если ключ существует, но не имеет связанного с
ним истечения срока действия.

#### type

```sql
TYPE key
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag fast">fast</span>
<span class="tag keyspace">keyspace</span>
<span class="tag read">read</span>

Возвращает строковое представление типа значения, хранящегося по адресу
ключа `key`. Могут быть возвращены следующие типы:

- `string`
- `list`
- `set`
- `zset`
- `hash`
- `stream`

#### unlink

```sql
UNLINK key [key ...]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag keyspace">keyspace</span>
<span class="tag write">write</span>

Выполняет асинхронное удаление ключей. Работает точно также, как и `DEL`,
за исключением того, что фактическое удаление данных происходит в фоне.

Можно использовать для повышения отзывчивости приложения.

### Хэш-команды {: #hash }

#### hdel

```sql
HDEL key field [field ...]
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag fast">fast</span>
<span class="tag hash">hash</span>
<span class="tag write">write</span>

Удаляет указанные поля из хэша, хранящегося по адресу ключа `key`.
Указанные поля, которые не существуют в этом хэше, игнорируются. Удаляет
хэш, если в нем не осталось полей. Если `key` не существует, он
рассматривается как пустой хэш, и эта команда возвращает `0`.

#### hexists

```sql
HEXISTS key field
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag fast">fast</span>
<span class="tag hash">hash</span>
<span class="tag read">read</span>

Возвращает, является ли поле `field` существующим полем в хэше, хранящемся по
адресу ключа `key`.

#### hget

```sql
HGET key field
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag fast">fast</span>
<span class="tag hash">hash</span>
<span class="tag read">read</span>

Возвращает значение, связанное с полем `field` в хэше, хранящемся по
адресу ключа `key`.

#### hgetall

```sql
HGETALL key
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag hash">hash</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>

Возвращает все поля и значения хэша, хранящегося по адресу ключа `key`. В
возвращаемом значении за именем каждого поля следует его значение,
поэтому длина ответа будет в два раза больше размера хэша.

#### hincrby

```sql
HINCRBY key field increment
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag fast">fast</span>
<span class="tag hash">hash</span>
<span class="tag write">write</span>

Увеличивает число, хранящееся в поле `field`, в хэше, хранящемся в ключе
`key`, на инкремент. Если ключ не существует, создаётся новый ключ,
содержащий хэш. Если поле не существует, то перед выполнением операции
его значение устанавливается в `0`.

Диапазон значений, поддерживаемых `HINCRBY`, ограничен 64-битными
знаковыми целыми числами.

#### hkeys

```sql
HKEYS key
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag hash">hash</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>

Возвращает все имена полей в хэше, хранящемся по адресу ключа `key`.

#### hlen

```sql
HLEN key
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag fast">fast</span>
<span class="tag hash">hash</span>
<span class="tag read">read</span>

Возвращает количество полей, содержащихся в хэше, хранящемся по адресу
ключа `key`.

#### hmget

```sql
HMGET key field [field ...]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag hash">hash</span>
<span class="tag read">read</span>

Возвращает значения указанных полей из хэша.

#### hmset

```sql
HMSET key field value [field value ...]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag hash">hash</span>
<span class="tag write">write</span>

Выставляет значения указанным полям для заданного хэша.

??? warning "Примечание"
    Вместо этой команды необходимо использовать команду `HSET`
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN radix 1.0.4 SET radix.redis_compatibility = '{ "enforce_one_slot_transactions": true, "push_result_includes_popped_items": true, "disable_scatter_gather": true, "enabled_deprecated_commands": ["hmset" ] }';
    ```

#### hscan

```sql
HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag hash">hash</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>

Работает подобно [SCAN](#scan), но с некоторым отличием: `HSCAN`
выполняет итерацию полей типа Hash и связанных с ними значений.

#### hset

```sql
HSET key field value [field value ...]
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag fast">fast</span>
<span class="tag hash">hash</span>
<span class="tag write">write</span>

Устанавливает указанные поля в соответствующие им значения в хэше,
хранящемся по адресу ключа `key`.

Эта команда перезаписывает значения указанных полей, которые существуют
в хэше. Если ключ не существует, создаётся новый ключ, содержащий хэш.

#### hvals

```sql
HVALS key
```
<span class="tag">поддерживается с версии 0.7.0</span>
<span class="tag hash">hash</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>

Возвращает значения всех полей в хэше, хранящиеся по адресу ключа `key`.

### Команды для множеств {: #set_commands }

#### sadd

```sql
SADD key member [member ...]
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag fast">fast</span>
<span class="tag set">set</span>
<span class="tag write">write</span>

Добавляет указанные элементы (`member`) к множеству, хранящемуся по
ключу `key`. Если такие элементы уже есть, они будут проигнорированы.
Если указанного ключа `key` нет, он будет создан, а элементы — добавлены
в новое множество. Если ключ `key` существует, но хранящееся в нем
значение не является множеством, команда вернёт ошибку.

#### scard

```sql
SCARD key
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag fast">fast</span>
<span class="tag read">read</span>
<span class="tag set">set</span>

Возвращает мощность множества (количество элементов), хранящегося по
ключу `key`.

#### sdiff

```sql
SDIFF key [key ...]
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag read">read</span>
<span class="tag set">set</span>
<span class="tag slow">slow</span>

Работает аналогично [SDIFFSTORE](#sdiffstore), но вместо записи
результирующего множества выводит его клиенту.

#### sdiffstore

```sql
SDIFFSTORE destination key [key ...]
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag set">set</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Вычисляет разницу между первым и последующими множествами (хранящимися в
соответствующих ключах `key`) и записывает его в `destination`. Команда
выводит количество элементов в результирующем множестве. Несуществующий
ключ обрабатывается как ключ, содержащий пустое множество. Если целевое
множество в `destination` уже существует, оно будет перезаписано.

Примеры:

```sql
127.0.0.1:7379> SADD key1 "a"
(integer) 1
127.0.0.1:7379> SADD key1 "b"
(integer) 1
127.0.0.1:7379> SADD key1 "c"
(integer) 1
127.0.0.1:7379> SADD key2 "c"
(integer) 1
127.0.0.1:7379> SADD key2 "d"
(integer) 1
127.0.0.1:7379> SADD key2 "e"
(integer) 1
127.0.0.1:7379> SDIFFSTORE key key1 key2
(integer) 2
127.0.0.1:7379> SMEMBERS key
1) "a"
2) "b"
127.0.0.1:7379>
```

#### sinter

```sql
SINTER key [key ...]
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag read">read</span>
<span class="tag set">set</span>
<span class="tag slow">slow</span>

Работает аналогично [SINTERSTORE](#sinterstore), но вместо записи
результирующего множества выводит его клиенту.

#### sintercard

```sql
SINTERCARD numkeys key [key ...] [LIMIT limit]
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag read">read</span>
<span class="tag set">set</span>
<span class="tag slow">slow</span>

Работает аналогично [SINTER](#sinter), но выводит клиенту не само
множество, а только его мощность. Если указан несуществующий ключ `key`,
то он будет обработан как пустое множество. Если из нескольких указанных
ключей хотя бы один будет содержать пустое множество, то и
результирующее пересечение также будет пустым.

Дополнительный параметр `LIMIT` позволяет ограничить показатель мощности
явно заданным числом. По умолчанию, ограничение не используется (`LIMIT`
равен 0).

#### sinterstore

```sql
SINTERSTORE destination key [key ...]
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag set">set</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Вычисляет пересечение элементов из двух или более множеств,
хранящихся по указанным ключам (`key`) в виде нового
множества и записывает его в `destination`. Команда
выводит количество элементов в результирующем множестве.

Пример:

```sql
127.0.0.1:7379> SADD key1 "a"
(integer) 1
127.0.0.1:7379> SADD key1 "b"
(integer) 1
127.0.0.1:7379> SADD key1 "c"
(integer) 1
127.0.0.1:7379> SADD key2 "c"
(integer) 1
127.0.0.1:7379> SADD key2 "d"
(integer) 1
127.0.0.1:7379> SADD key2 "e"
(integer) 1
127.0.0.1:7379> SINTERSTORE key key1 key2
(integer) 1
127.0.0.1:7379> SMEMBERS key
1) "c"
127.0.0.1:7379>
```

#### sismember

```sql
SISMEMBER key member
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag fast">fast</span>
<span class="tag read">read</span>
<span class="tag set">set</span>

Возвращает признак присутствия элемента в указанном множестве. В выводе
будет `1` или `0`, соответственно.

#### smembers

```sql
SMEMBERS key
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag read">read</span>
<span class="tag set">set</span>
<span class="tag slow">slow</span>

Возвращает список всех элементов, хранящихся в указанном множестве.

#### smove

```sql
SMOVE source destination member
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag fast">fast</span>
<span class="tag set">set</span>
<span class="tag write">write</span>

Перемещает элемент из исходного множества (`source`) в целевое
(`destination`). Операция является атомарной. В любой момент времени
элемент будет отображаться как элемент источника или назначения для
других клиентов. Если исходное множествоне существует или не содержит
указанный элемент, операция не выполняется и возвращается значение 0. В
противном случае элемент удаляется из исходного множества и добавляется
в целевое. Если указанный элемент уже существует в целевом множестве, он
удаляется из исходного множестве.

#### spop

```sql
SPOP key [count]
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag fast">fast</span>
<span class="tag set">set</span>
<span class="tag write">write</span>

Извлекает один или несколько элементов (согласно числу, указанному в
`count`), хранящихся в множестве по указанному ключу. Если `count` не
указан, то по умолчанию команда извлечёт один элемент.

#### srandmember

```sql
SRANDMEMBER key [count]
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag read">read</span>
<span class="tag set">set</span>
<span class="tag slow">slow</span>

Возвращает случайный элемент из множества, хранящегося по ключу `key`.
Дополнительный параметр `count` позволяет указать количество выводимых
элементов.
Если `count` положителен, команда возвращает массив различных элементов.
Длина массива равна `count` или мощности множества ([SCARD](#scard)), в
зависимости от того, какое из этих значений меньше.
При вызове с отрицательным значением `count` поведение меняется, и
команда может возвращать один и тот же элемент несколько раз. В этом
случае количество возвращаемых элементов равно абсолютному значению
указанного `count`.

#### srem

```sql
SREM key member [member ...]
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag fast">fast</span>
<span class="tag set">set</span>
<span class="tag write">write</span>

Удаляет указанные элементы из множества, хранящегося по ключу `key`.
Если указанный элемент отсутствует в множестве, то такой элемент
игнорируется. Если ключ `key` существует, но хранящееся в нем значение
не является множеством, команда вернёт ошибку.

#### sscan

```sql
SSCAN key cursor [MATCH pattern] [COUNT count]
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag read">read</span>
<span class="tag set">set</span>
<span class="tag slow">slow</span>

См. [SCAN](#scan)

#### sunion

```sql
SUNION key [key ...]
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag read">read</span>
<span class="tag set">set</span>
<span class="tag slow">slow</span>

Работает аналогично [SUNIONSTORE](#sunionstore), но вместо записи
результирующего множества выводит его клиенту.

#### sunionstore

```sql
SUNIONSTORE destination key [key ...]
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag set">set</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Вычисляет пересечение элементов из двух или более множеств,
хранящихся по указанным ключам (`key`) в виде нового
множества и записывает его в `destination`. Команда
выводит количество элементов в результирующем множестве. Если множество
в `destination` уже существует, оно будет перезаписано.

Пример:

```sql
127.0.0.1:7379> SADD key1 "a"
(integer) 1
127.0.0.1:7379> SADD key1 "b"
(integer) 1
127.0.0.1:7379> SADD key1 "c"
(integer) 1
127.0.0.1:7379> SADD key2 "c"
(integer) 1
127.0.0.1:7379> SADD key2 "d"
(integer) 1
127.0.0.1:7379> SADD key2 "e"
(integer) 1
127.0.0.1:7379> SUNIONSTORE key key1 key2
(integer) 5
127.0.0.1:7379> SMEMBERS key
1) "a"
2) "b"
3) "c"
4) "d"
5) "e"
```

### Команды для сортированных множеств {: #ordered_sets }

#### bzmpop

```sql
BZMPOP timeout numkeys key [key ...] <MIN | MAX> [COUNT count]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag blocking">blocking</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Вариант команды [ZMPOP](#zmpop) с блокировкой. Ведёт себя аналогично
[ZMPOP](#zmpop) в ситуации:

- когда хотя бы в одном из сортированных множеств, хранящихся по
  указанным ключам (`key`), есть элементы
- при использовании внутри блока [MULTI](#multi) или [EXEC](#exec).

Если все сортированные множества пусты, то Radix заблокирует соединение до
тех пор, пока другой клиент не добавит значение хотя бы к одному множеству в
указанных ключах `key`, либо не истечёт время таймаута `timeout`. Если
таймаут установить в `0`, то блокировка будет бесконечной.

#### bzpopmax

```sql
BZPOPMAX key [key ...] timeout
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag blocking">blocking</span>
<span class="tag fast">fast</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Вариант команды [ZPOPMAX](#zpopmax) с блокировкой. Ведёт себя так же как
`ZPOPMAX`, но при отсутствии элементов во всех cортированных множествах,
хранящихся по ключам `key`, блокирует соединение. В остальных случаях
возвращает один элемент с наивысшей оценкой из первого непустого ключа
из переданных в команду. Блокировка истекает после таймаута `timeout`.
Если таймаут установить в `0`, то блокировка будет бесконечной.

#### bzpopmin

```sql
BZPOPMIN key [key ...] timeout
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag blocking">blocking</span>
<span class="tag fast">fast</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Вариант команды [ZPOPMIN](#zpopmin) с блокировкой. Ведёт себя так же как
`ZPOPMIN`, но при отсутствии элементов во всех cортированных множествах,
хранящихся по ключам `key`, блокирует соединение. Возвращает один
элемент с наименьшей оценкой из первого непустого ключа из переданных в
команду. Блокировка истекает после таймаута `timeout`. Если таймаут
установить в `0`, то блокировка будет бесконечной.

#### zadd

```sql
ZADD key [NX | XX] [GT | LT] [CH] [INCR] score member [score member ...]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Добавляет указанные элементы (`member`) и соответствующие им оценки
(`score`) к сортированному множеству, хранящемуся по ключу `key`. Если
указанного ключа `key` нет, он будет создан, а элементы — добавлены в
новое сортированное множество. Если ключ `key` существует, но в нем нет
сортированного множества, команда вернёт ошибку. Если указанный элемент
уже есть в сортированном множестве, то он будет вставлен повторно на ту
же позицию с обновленной оценкой.

Дополнительные параметры:

- `NX` — только добавить новые элементы (существующие не обновлять)
- `XX` — только обновить существующие элементы (новые не добавлять)
- `GT` — обновить существующие элементы только если их новые оценки
  выше, а также добавить новые элементы (если указаны)
- `LT` — обновить существующие элементы только если их новые оценки
  ниже, а также добавить новые элементы (если указаны)
- `CH` — учитывать в выводе не только новые элементы, но и измененные. В
  таком случае команда вернёт число, отражающее сумму новых элементов и
  тех существующих элементов, для которых была обновлена оценка. Если
  указать в команде существующие элементы с их текущей оценкой, то они
  не будут учтены.
- `INCR` — заставляет `ZADD` вести себя как [ZINCRBY](#zincrby). В этом
  режиме можно указать только одну пару оценка/элемент.

??? warning "Примечание"
    Параметры `GT`,`LT` и `NX` можно использовать
    только по отдельности, не сочетая друг с другом.

#### zcard

```sql
ZCARD key
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag read">read</span>
<span class="tag sortedset">sortedset</span>

Возвращает мощность множества (количество элементов в сортированном
множестве), хранящегося по ключу `key`.

#### zcount

```sql
ZCOUNT key min max
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag read">read</span>
<span class="tag sortedset">sortedset</span>

Возвращает мощность множества (количество элементов в сортированном
множестве), хранящегося по ключу `key`, с оценкой в диапазоне от `min` до
`max`. Поведение аргументов `min` и `max` такое же, как в
[ZREMRANGEBYSCORE](#zremrangebyscore).

#### zdiff

```sql
ZDIFF numkeys key [key ...] [WITHSCORES]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>

Работает аналогично [ZDIFFSTORE](#zdiffstore), но вместо записи
результирующего сортированного множества выводит его клиенту.

#### zdiffstore

```sql
ZDIFFSTORE destination numkeys key [key ...]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Вычисляет разницу между первым и последующими сортированными множествами
(хранящимися в соответствующих ключах `key`) и записывает его в
`destination`. Перед списком ключей необходимо указать их количество
(`numkeys`). Команда выводит количество элементов в результирующем
множестве. Несуществующий ключ обрабатывается как ключ, содержащий пустое
сортированное множество. Если целевое множество в `destination` уже
существует, оно будет перезаписано.

Примеры:

```sql
127.0.0.1:7379> ZADD zset1 1 "one"
(integer) 1
127.0.0.1:7379> ZADD zset1 2 "two"
(integer) 1
127.0.0.1:7379> ZADD zset1 3 "three"
(integer) 1
127.0.0.1:7379> ZADD zset2 1 "one"
(integer) 1
127.0.0.1:7379> ZADD zset2 2 "two"
(integer) 1
127.0.0.1:7379> ZDIFFSTORE out 2 zset1 zset2
(integer) 1
127.0.0.1:7379> ZRANGE out 0 -1 WITHSCORES
1) "three"
2) "3"
```

#### zincrby

```sql
ZINCRBY key increment member
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Увеличивает оценку элемента `member` в сортированном множестве,
хранящемся по ключу `key`, на величину `increment`. Если указанный
элемент в множестве отсутствует, то он будет создан с оценкой, равной
`increment`. Если указанного ключа `key` нет, он будет создан, и элемент
добавлен в новое сортированное множество. Если ключ `key` существует, но
в нем нет сортированного множества, команда вернёт ошибку. Величина
`increment` может быть отрицательной (в таком случае оценка будет
понижена).

#### zinter

```sql
ZINTER numkeys key [key ...] [WEIGHTS weight [weight ...]]
  [AGGREGATE <SUM | MIN | MAX>] [WITHSCORES]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>

Работает аналогично [ZINTERSTORE](#zinterstore), но вместо записи
результирующего сортированного множества выводит его клиенту.

#### zintercard

```sql
ZINTERCARD numkeys key [key ...] [LIMIT limit]
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>

Работает аналогично [ZINTER](#zinter), но вместо результирующего
сортированного множества выводит только его мощность.

#### zinterstore

```sql
ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight
  [weight ...]] [AGGREGATE <SUM | MIN | MAX>]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Вычисляет пересечение элементов из двух или более сортированных множеств,
хранящихся по указанным ключам (`key`) в виде нового сортированного
множества и записывает его в `destination`. Перед
списком ключей необходимо указать их количество (`numkeys`). Команда
выводит количество элементов в результирующем множестве.

По умолчанию, результирующая оценка элемента является суммой оценок
этого элемента во всех исходных множествах, где он присутствует.

Дополнительные параметры `WEIGHTS` и `AGGREGATE` ведут себя так же, как
в команде [ZUNIONSTORE](#zunionstore).

Пример:

```sql
127.0.0.1:7379> ZADD zset1 1 "one"
(integer) 1
127.0.0.1:7379> ZADD zset1 2 "two"
(integer) 1
127.0.0.1:7379> ZADD zset2 1 "one"
(integer) 1
127.0.0.1:7379> ZADD zset2 2 "two"
(integer) 1
127.0.0.1:7379> ZADD zset2 3 "three"
(integer) 1
127.0.0.1:7379> ZINTERSTORE out 2 zset1 zset2 WEIGHTS 2 3
(integer) 2
127.0.0.1:7379> ZRANGE out 0 -1 WITHSCORES
1) "one"
2) "5"
3) "two"
4) "10"
```

#### zlexcount

```sql
ZLEXCOUNT key min max
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag read">read</span>
<span class="tag sortedset">sortedset</span>

Возвращает количество всех элементов из сортированного множества,
хранящегося по ключу `key`, в лексикографическом диапазоне от `min` до
`max`. Аргументы `min` и `max` применяются так же, как в команде
[ZRANGEBYLEX](#zrangebylex).

#### zmpop

```sql
ZMPOP numkeys key [key ...] <MIN | MAX> [COUNT count]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Извлекает один или несколько элементов, составляющих пары
оценка/элемент, из первого непустого сортированного множества на основании
указанного наборы ключей `key`.

Модификатор `MIN` позволяет выводить элементы с наименьшей оценкой,
`MAX` — с наивысшей. Параметр `COUNT` ограничивает число элементов (по
умолчанию — 1).

#### zmscore

```sql
ZMSCORE key member [member ...]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag read">read</span>
<span class="tag sortedset">sortedset</span>

Возвращает оценки указанных элементов (`member`) сортированных множеств,
хранящихся по указанному ключу `key`. Если элемент отсутствует в
множестве, то для него будет выведена оценка `nil`.

#### zpopmax

```sql
ZPOPMAX key [count]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Извлекает указанное в `count` число элементов с наивысшей оценкой из
сортированного множества, хранящегося по указанному ключу. По умолчанию
`count` равен 1. Если указанный `count` больше мощности множества, то
ошибки не будет. Команда выводит элементы с сортировкой по убыванию
оценки.

#### zpopmin

```sql
ZPOPMIN key [count]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Извлекает указанное в `count` число элементов с наименьшей оценкой из
сортированного множества, хранящегося по указанному ключу. По умолчанию
`count` равен 1. Если указанный `count` больше мощности множества, то
ошибки не будет. Команда выводит элементы с сортировкой по возрастанию
оценки.

#### zrandmember

```sql
ZRANDMEMBER key [count [WITHSCORES]]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>

Возвращает случайный элемент из сортированного множества, хранящегося по
ключу `key`.

Дополнительный параметр `count` позволяет указать количество выводимых
элементов. Если добавить `WITHSCORES`, то в вывод будут включены оценки
элементов. Если указанный `count` положителен, то будет выведен массив
элементов размером либо с `count`, либо мощность множества (смотря какое
значение ниже). Если указанный `count` отрицателен, то поведение команды
меняется: один и тот же элемент может быть возвращен несколько раз.
Размер итогового массива при этом будет равняться абсолютному значению
`count`.

#### zrange

```sql
ZRANGE key start stop [BYSCORE | BYLEX] [REV] [LIMIT offset count]
  [WITHSCORES]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>

Возвращает указанный набор элементов из сортированного множества,
хранящегося по ключу `key`. Команда может выполнять разные типы запросов,
выводя наборы элементов: по индексу, по оценки, в лексикографическом
порядке.

Следующие параметры меняют поведение команды:

- `BYSCORE` — сортировка элементов по возрастанию их оценок. Элементы с
  одинаковыми оценками сортируются лексикографически
- `BYLEX` — лексикографическая сортировка элементов с одинаковой оценкой
- `REV` — оценка элементов множества в обратном порядке

Дополнительный параметр `LIMIT` позволяет ограничить результат явно
заданными рамками (`offset` — смещение, `count` — число элементов).
Отрицательное значение `count` выведет все элементы после `offset`.
Дополнительный параметр `WITHSCORES` позволяет включить в вывод оценки
элементов.

**Диапазоны индексов**

По умолчанию команда выполняет запрос на основе индексов. Отрезок от
`start` до `stop` позволяет ограничить вывод элементов и обрабатывается
включительно (`0` соответствует первому элементу). Например, команда
`ZRANGE myzset 0 1 ` выведет только первый и второй элемент из множества
в ключе `myzset`. Отрицательные значения обозначают позицию относительно
конца множества (`-1` — последний элемент). Индексы, выходящие за
пределы диапазона, не вызывают ошибку. Если `start` больше конечного
индекса сортированного множества или `stop`, возвращается пустой список.
Если `stop` больше конечного индекса сортированного множества, команда
будет использовать последний элемент сортированного множества.

**Диапазоны оценок**

Если указан параметр `BYSCORE`, команда ведёт себя как
[ZRANGEBYSCORE](#zrangebyscore) и возвращает диапазон элементов из
сортированного множества, имеющих оценки, равные или лежащие между
`start` и `stop`.

`start` и `stop` могут быть _-inf_ и _+inf_, обозначая отрицательную и
положительную бесконечность соответственно. Это означает, что вам не
нужно знать наивысшую или наименьшую оценку в сортированном множестве,
чтобы получить все элементы с определённой оценкой или выше.

По умолчанию интервалы оценок, указанные с помощью `start` и `stop`,
являются замкнутыми. Можно указать открытый интервал, добавив перед
оценкой символ (.

Например:

```sql title="элементы с оценкой > 1 и <= 5"
ZRANGE zset (1 5 BYSCORE
```

```sql title="элементы с оценкой > 5 и < 10"
ZRANGE zset (5 (10 BYSCORE
```

**Обратные диапазоны**

Использование параметра `REV` обращает сортированное множество, при этом
индекс `0` будет относиться к элементу с наивысшей оценкой.

По умолчанию, чтобы вернуть какие-либо результаты, значение `start`
должно быть меньше или равно `stop`. Однако, если использован параметр
`BYSCORE` или `BYLEX`, значение `start` является наивысшей оценкой,
которую следует учитывать, а `stop` — наименьшей. Поэтому, чтобы вернуть
какие-либо результаты, значение `start` должно быть больше или равно
`stop`.

Например:

```sql title="элементы между индексами 5 и 10 в обратном порядке"
ZRANGE zset 5 10 REV
```

```sql title="элементы с оценками меньше 10 и больше 5"
ZRANGE zset 10 5 REV BYSCORE
```

**Лексикографические диапазоны**

При использовании параметра `BYLEX` команда ведёт себя как
[ZRANGEBYLEX](#zrangebylex) и возвращает диапазон элементов из
сортированного множества между лексикографическими закрытыми интервалами
диапазона `start` и `stop`.

Обратите внимание, что лексикографическая сортировка ожидает, что оценки
у всех элементов множества будут одинаковыми. Если элементы имеют разные
оценки, то ответ может быть любым.

Допустимые значения `start` и `stop` должны начинаться с ( или [, чтобы
указать, является ли интервал диапазона открытым или замкнутым,
соответственно.

Специальные значения `+` или `-` для `start` и `stop` означают
положительные и отрицательные бесконечные строки, соответственно,
поэтому, например, команда `ZRANGE myzset - + BYLEX` гарантированно
возвращает все элементы в сортированном множестве (при условии, что все
элементы имеют одинаковую оценку).

Параметр `REV` меняет порядок элементов `start` и `stop`, где `start`
должен быть лексикографически больше `stop`, чтобы получить непустой
результат.

**Лексикографическое сравнение строковых значений**

Строки сравниваются как двоичный массив байтов. В случае с набором
символов ASCII сравнение происходит обычным словарным способом.

Приложение сохраняет регистр символов, но не учитывает его при
сравнении. Для сравнения используются строки, приведенные к нижнему
регистру, для вывода результата — исходные значения.

Двоичная природа сравнения позволяет использовать сортированные
множества в качестве индекса общего назначения, например, первая часть
элемента может быть 64-разрядным числом в формате big-endian. Поскольку
в числах big-endian наиболее значимые байты находятся в начальных
позициях, двоичное сравнение будет соответствовать числовому сравнению
чисел. Это можно использовать для реализации запросов по диапазону на
64-разрядных значениях. Как показано в примере ниже, после первых 8 байт
мы можем хранить значение индексируемого элемента.

Пример:

```sql
> ZADD myzset 1 "one" 2 "two" 3 "three"
(integer) 3
> ZRANGE myzset 0 -1
1) "one"
2) "two"
3) "three"
> ZRANGE myzset 2 3
1) "three"
> ZRANGE myzset -2 -1
1) "two"
2) "three"
```

Дополнительные примеры:

```sql
127.0.0.1:7379> ZADD myzset 1 "one" 2 "two" 3 "three"
(integer) 3
127.0.0.1:7379> ZRANGE myzset 0 -1
1) "one"
2) "two"
3) "three"
127.0.0.1:7379> ZRANGE myzset 0 3 BYSCORE
1) "one"
2) "two"
3) "three"
127.0.0.1:7379> ZRANGE myzset 0 3 REV BYSCORE
1) "three"
2) "two"
3) "one"
127.0.0.1:7379> ZRANGE myzset 0 3 BYLEX
(empty array)
127.0.0.1:7379> ZRANGE myzset 0 3 BYSCORE LIMIT 1 1
1) "two"
2) "three"
```

#### zrangebylex

```sql
ZRANGEBYLEX key min max [LIMIT offset count]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>

Работает аналогично [ZRANGE](#zrange) c параметром вывода `BYLEX`.

??? warning "Примечание"
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN radix 1.0.4 SET radix.redis_compatibility = '{ "enforce_one_slot_transactions": true, "push_result_includes_popped_items": true, "disable_scatter_gather": true, "enabled_deprecated_commands": ["zrangebylex" ] }';
    ```

#### zrangebyscore

```sql
ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>

Работает аналогично [ZRANGE](#zrange) c параметром вывода `BYSCORE`.

??? warning "Примечание"
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN radix 1.0.4 SET radix.redis_compatibility = '{ "enforce_one_slot_transactions": true, "push_result_includes_popped_items": true, "disable_scatter_gather": true, "enabled_deprecated_commands": ["zrangebyscore" ] }';
    ```

#### zrangestore

```sql
ZRANGESTORE destination src min max [BYSCORE | BYLEX] [REV] [LIMIT offset count]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Работает аналогично [ZRANGE](#zrange), но вместо вывода
результирующего сортированного множества записывает его в `destination`.

#### zrank

```sql
ZRANK key member [WITHSCORE]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag read">read</span>
<span class="tag sortedset">sortedset</span>

Возвращает позиции элемента (`member`) в сортированном множестве,
хранящемся по указанному ключу `key`, с сортировкой по возрастанию
оценки. Отсчёт начинается с `0`.
Дополнительный параметр `WITHSCORE` добавляет в вывод команды сами оценки.

Для вывода позиций элементов по возрастанию оценки (включая в вывод сами
оценки) используйте [ZREVRANK](#zrevrank).

#### zrem

```sql
ZREM key member [member ...]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Удаляет указанные элементы из сортированного множества, хранящегося по
ключу `key`. Если указанный элемент отсутствует в множестве, то такой
элемент игнорируется. Если ключ `key` существует, но в нем нет
сортированного множества, команда вернёт ошибку.

#### zremrangebylex

```sql
ZREMRANGEBYLEX key min max
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Удаляет все элементы из сортированного множества, хранящегося по ключу
`key`, в лексикографическом диапазоне от `min` до `max`. Аргументы `min`
и `max` применяются так же, как в команде [ZRANGEBYLEX](#zrangebylex).


#### zremrangebyrank

```sql
ZREMRANGEBYRANK key start stop
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Удаляет все элементы из сортированного множества, хранящегося по
ключу `key`, с позиции в диапазоне от `start` до `stop`. Значение `0 `—
наиболее низкая позиция, `-1` — наивысшая позиция, `-2` — вторая после
наивысшей и т.д.

#### zremrangebyscore

```sql
ZREMRANGEBYSCORE key min max
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Удаляет все элементы из сортированного множества, хранящегося по
ключу `key`, с оценкой в диапазоне от `min` до `max` включительно.

#### zrevrange

```sql
ZREVRANGE key start stop [WITHSCORES]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>

Работает аналогично [ZRANGE](#zrange) c параметром вывода `REV`.

??? warning "Примечание"
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN radix 1.0.4 SET radix.redis_compatibility = '{ "enforce_one_slot_transactions": true, "push_result_includes_popped_items": true, "disable_scatter_gather": true, "enabled_deprecated_commands": ["zrevrange" ] }';
    ```

#### zrevrangebylex

```sql
ZREVRANGEBYLEX key max min [LIMIT offset count]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>

Работает аналогично [ZRANGE](#zrange) c параметрами вывода `REV` и `BYLEX`.

??? warning "Примечание"
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN radix 1.0.4 SET radix.redis_compatibility = '{ "enforce_one_slot_transactions": true, "push_result_includes_popped_items": true, "disable_scatter_gather": true, "enabled_deprecated_commands": ["zrevrangebylex" ] }';
    ```

#### zrevrangebyscore

```sql
ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>

Работает аналогично [ZRANGE](#zrange) c параметрами вывода `REV` и `BYSCORE`.

??? warning "Примечание"
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN radix 1.0.4 SET radix.redis_compatibility = '{ "enforce_one_slot_transactions": true, "push_result_includes_popped_items": true, "disable_scatter_gather": true, "enabled_deprecated_commands": ["zrevrangebyscore" ] }';
    ```

#### zrevrank

```sql
ZREVRANK key member [WITHSCORE]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag read">read</span>
<span class="tag sortedset">sortedset</span>

Возвращает позиции элемента (`member`) в сортированном множестве,
хранящемся по указанному ключу `key`, с сортировкой по убыванию оценки.
Отсчёт начинается с `0`.
Дополнительный параметр `WITHSCORE` добавляет в
вывод команды сами оценки.

Для вывода позиций элементов по возрастанию оценки (включая в вывод сами
оценки) используйте [ZRANK](#zrank).

#### zscan

```sql
ZSCAN key cursor [MATCH pattern] [COUNT count]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>

См. [SCAN](#scan)

#### zscore

```sql
ZSCORE key member
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag read">read</span>
<span class="tag sortedset">sortedset</span>

Возвращает оценку элемента `member` в сортированном множестве, хранящемся
по ключу `key`.

#### zunion

```sql
ZUNION numkeys key [key ...] [WEIGHTS weight [weight ...]]
  [AGGREGATE <SUM | MIN | MAX>] [WITHSCORES]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>

Работает аналогично [ZUNIONSTORE](#zunionstore), но вместо записи
результирующего сортированного множества выводит его клиенту.

#### zunionstore

```sql
ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight
  [weight ...]] [AGGREGATE <SUM | MIN | MAX>]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Объединяет элементы двух или более сортированных множеств, хранящихся по
указанным ключам (`key`) в новое сортированное множество и записывает его в
`destination`. Объединение происходит на основе оценок элементов,
которые встречаются в исходных множествах. Перед списком ключей необходимо
указать их количество (`numkeys`). Команда выводит количество элементов
в результирующем множестве.

Дополнительный параметр `WEIGHTS` позволяет указать "вес" для каждого
исходного множества. Это число будет использовано как мультипликатор для
оценок в множестве.

Дополнительный параметр `AGGREGATE` позволяет указать способ объединения.
По умолчанию, это суммирование (`SUM`), однако можно указать запись
минимальной (`MIN`) или максимальной (`MAX`) оценки элемента из всех
исходных множеств, где он встречается.

Примеры:

```sql
127.0.0.1:7379> ZADD zset1 1 "one"
(integer) 1
127.0.0.1:7379> ZADD zset1 2 "two"
(integer) 1
127.0.0.1:7379> ZADD zset2 2 "two"
(integer) 1
127.0.0.1:7379> ZADD zset2 3 "three"
(integer) 1
127.0.0.1:7379> ZUNIONSTORE zsetout11 2 zset1 zset2 WEIGHTS 1 1
(integer) 3
127.0.0.1:7379> ZUNIONSTORE zsetout23 2 zset1 zset2 WEIGHTS 2 3
(integer) 3
127.0.0.1:7379> ZUNIONSTORE zsetout34 2 zset1 zset2 WEIGHTS 2 3 AGGREGATE MIN
(integer) 3
127.0.0.1:7379> ZRANGE zsetout11 0 1000 WITHSCORES
1) "one"
2) "1"
3) "three"
4) "3"
5) "two"
6) "4"
127.0.0.1:7379> ZRANGE zsetout23 0 1000 WITHSCORES
1) "one"
2) "2"
3) "three"
4) "9"
5) "two"
6) "10"
127.0.0.1:7379> ZRANGE zsetout34 0 1000 WITHSCORES
1) "one"
2) "2"
3) "two"
4) "4"
5) "three"
6) "9"
```

### Команды для списков {: #lists }

#### blmove

```sql
BLMOVE source destination <LEFT | RIGHT> <LEFT | RIGHT> timeout
```
<span class="tag">поддерживается с версии 0.4.0</span>
<span class="tag blocking">blocking</span>
<span class="tag list">list</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Работает аналогично [LMPOP](#lmpop), но с использованием блокировки.
Если исходный список (`source`) пуст, то команда будет ждать наполнения
списка в течение указанного в `timeout` времени (в секундах), и в случае
неудачи вернёт ошибку. Если таймаут установить в `0`, то блокировка
будет бесконечной.

#### blmpop

```sql
BLMPOP timeout numkeys key [key ...] <LEFT | RIGHT> [COUNT count]
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag blocking">blocking</span>
<span class="tag list">list</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Работает аналогично [LMOVE](#lmove), но с использованием блокировки.
Если все указанные списки ключей пусты, то команда будет ждать
наполнения любого из них в течение указанного в `timeout` времени (в
секундах), и в случае неудачи вернёт ошибку. Если таймаут установить в
`0`, то блокировка будет бесконечной.

#### blpop

```sql
BLPOP key [key ...] timeout
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag blocking">blocking</span>
<span class="tag list">list</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Работает аналогично [LPOP](#lpop), но с использованием блокировки.
Если указанный список пуст, то команда будет ждать его наполнения
в течение указанного в `timeout` времени (в секундах), и в случае
неудачи вернёт ошибку. Если таймаут установить в `0`, то блокировка
будет бесконечной.

#### brpoplpush

```sql
BRPOPLPUSH source destination timeout
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag blocking">blocking</span>
<span class="tag list">list</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Работает аналогично [RPOPLPUSH](#rpoplpush), но с использованием
блокировки. При использовании внутри блока [MULTI](#multi) или
[EXEC](#exec) данная команда ведёт себя полностью идентично
[RPOPLPUSH](#rpoplpush).

??? warning "Примечание"
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN radix 1.0.4 SET radix.redis_compatibility = '{ "enforce_one_slot_transactions": true, "push_result_includes_popped_items": true, "disable_scatter_gather": true, "enabled_deprecated_commands": ["brpoplpush" ] }';
    ```

#### brpop

```sql
BRPOP key [key ...] timeout
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag blocking">blocking</span>
<span class="tag list">list</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Работает аналогично [RPOP](#lpop), но с использованием блокировки.
Поведение механизма блокировки аналогично таковому для [BLPOP](#blpop).

#### lindex

```sql
LINDEX key index
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag list">list</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>

Возвращает элемент с указанным индексом (`index`) из списка, хранящегося
по указанному ключу `key`. Индекс `0` означает первый элемент списка,
`-1` — последний и т.д.

Примеры:

```sql
127.0.0.1:7379> LPUSH mylist "World"
(integer) 1
127.0.0.1:7379> LPUSH mylist "Hello"
(integer) 2
127.0.0.1:7379> LINDEX mylist 0
"Hello"
127.0.0.1:7379> LINDEX mylist -1
"World"
127.0.0.1:7379> LINDEX mylist 3
(nil)
127.0.0.1:7379>
```

#### linsert

```sql
LINSERT key <BEFORE | AFTER> pivot element
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag list">list</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Вставляет в список, хранящийся по ключу `key`, элемент (`element`) до
(`BEFORE`) или после (`AFTER`) указанного другого элемента (`pivot`).
Если указан несуществующий ключ, то команда ничего не сделает. Если по
указанному ключу нет списка, то команда вернёт ошибку.

Примеры:

```sql
127.0.0.1:7379> RPUSH mylist "Hello"
(integer) 1
127.0.0.1:7379> RPUSH mylist "World"
(integer) 2
127.0.0.1:7379> LINSERT mylist BEFORE "World" "There"
(integer) 3
127.0.0.1:7379> LRANGE mylist 0 -1
1) "Hello"
2) "There"
3) "World"
127.0.0.1:7379>
```

#### llen

```sql
LLEN key
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag fast">fast</span>
<span class="tag list">list</span>
<span class="tag read">read</span>

Возвращает длину (количество элементов) списка, хранящегося по ключу
`key`. Если указан несуществующий ключ, то команда вернёт `0`. Если по
указанному ключу нет списка, то команда вернёт ошибку.

Примеры:

```sql
127.0.0.1:7379> LPUSH mylist "World"
(integer) 1
127.0.0.1:7379> LPUSH mylist "Hello"
(integer) 2
127.0.0.1:7379> LLEN mylist
(integer) 2
```

#### lmove

```sql
LMOVE source destination <LEFT | RIGHT> <LEFT | RIGHT>
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag list">list</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Перемещает первый/последний элемент первого списка (`source`) в
начало/конец второго списка (`destination`).

Примеры:

```sql
127.0.0.1:7379> RPUSH mylist "one"
(integer) 1
127.0.0.1:7379> RPUSH mylist "two"
(integer) 2
127.0.0.1:7379> RPUSH mylist "three"
(integer) 3
127.0.0.1:7379> LMOVE mylist myotherlist RIGHT LEFT
"three"
127.0.0.1:7379> LMOVE mylist myotherlist LEFT RIGHT
"one"
127.0.0.1:7379> LRANGE mylist 0 -1
1) "two"
127.0.0.1:7379> LRANGE myotherlist 0 -1
1) "three"
2) "one"
127.0.0.1:7379>
```

#### lmpop

```sql
LMPOP numkeys key [key ...] <LEFT | RIGHT> [COUNT count]
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag list">list</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Извлекает (и удаляет) один или несколько (`count`) элементов в начале
(`LEFT`) или в конце (`REFT`) из первого непустого
списка ключей (`key`) в перечне списков ключей.

Примеры:

```sql
127.0.0.1:7379> LMPOP 2 non1 non2 LEFT COUNT 10
(nil)
127.0.0.1:7379> LPUSH mylist "one" "two" "three" "four" "five"
(integer) 5
127.0.0.1:7379> LMPOP 1 mylist LEFT
1) "mylist"
2) 1) "five"
127.0.0.1:7379> LRANGE mylist 0 -1
1) "four"
2) "three"
3) "two"
4) "one"
127.0.0.1:7379> LMPOP 1 mylist RIGHT COUNT 10
1) "mylist"
2) 1) "one"
   2) "two"
   3) "three"
   4) "four"
127.0.0.1:7379> LPUSH mylist "one" "two" "three" "four" "five"
(integer) 5
127.0.0.1:7379> LPUSH mylist2 "a" "b" "c" "d" "e"
(integer) 5
127.0.0.1:7379> LMPOP 2 mylist mylist2 right count 3
1) "mylist"
2) 1) "one"
   2) "two"
   3) "three"
127.0.0.1:7379> LRANGE mylist 0 -1
1) "five"
2) "four"
127.0.0.1:7379> LMPOP 2 mylist mylist2 right count 5
1) "mylist"
2) 1) "four"
   2) "five"
127.0.0.1:7379> LMPOP 2 mylist mylist2 right count 10
1) "mylist2"
2) 1) "a"
   2) "b"
   3) "c"
   4) "d"
   5) "e"
127.0.0.1:7379> EXISTS mylist mylist2
(integer) 0
127.0.0.1:7379>
```

#### lpop

```sql
LPOP key [count]
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag fast">fast</span>
<span class="tag list">list</span>
<span class="tag write">write</span>

Извлекает (и удаляет) указанное число (`count`) первых элементов,
хранящихся в списке по адресу ключа `key`. Без аргумента `count` команда
извлекает один первый элемент в начале списка.

Примеры:

```sql
127.0.0.1:7379> RPUSH mylist "one" "two" "three" "four" "five"
(integer) 5
127.0.0.1:7379> LPOP mylist
"one"
127.0.0.1:7379> LPOP mylist 2
1) "two"
2) "three"
127.0.0.1:7379> LRANGE mylist 0 -1
1) "four"
2) "five"
```

#### lpos

```sql
LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag list">list</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>

Возвращает индекс найденного в списке, хранящегося по ключу `key`,
элемента (`element`). Без дополнительных аргументов эта команда
просканирует список слева направо и вернёт индекс первого найденного
элемента. Нумерация элементов начинается с `0`.

Пример:

```sql
> RPUSH mylist a b c 1 2 3 c c
> LPOS mylist c
2
```

Параметр `RANK` позволяет вывести другой (по счёту `rank`) найденный
элемент в случае, если их несколько. Отрицательное значение `rank`
означает, что нумерация результата будет вестись справа налево.

Примеры:

```sql
> LPOS mylist c RANK 2
6
> LPOS mylist c RANK -1
7
```

Параметр `COUNT` позволяет вывести позиции всех (по счёту `num-matches`)
найденных элементов.

Пример:

```sql
> LPOS mylist c COUNT 2
[2,6]
```

При совместном использовании `COUNT` и `RANK` можно изменить точку
отсчёта, с которой будет производиться поиск совпадений.

Пример:

```sql
> LPOS mylist c RANK -1 COUNT 2
[7,6]
```

#### lpush

```sql
LPUSH key element [element ...]
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag fast">fast</span>
<span class="tag list">list</span>
<span class="tag write">write</span>

Вставляет указанные элементы в начала списка, хранящегося по ключу
`key`.

Примеры:

```sql
127.0.0.1:7379> LPUSH mylist "world"
(integer) 1
127.0.0.1:7379> LPUSH mylist "hello"
(integer) 2
127.0.0.1:7379> LRANGE mylist 0 -1
1) "hello"
2) "world"
```

#### lpushx

```sql
LPUSHX key element [element ...]
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag fast">fast</span>
<span class="tag list">list</span>
<span class="tag write">write</span>

Работает аналогично [LPUSH](#lpush), но проверяет, что указанный ключ
`key` существует. В противном случае команда ничего не делает (в отличие
от `LPUSH`).

#### lrange

```sql
LRANGE key start stop
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag list">list</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>

Возвращает диапазон элементов списка, хранящегося по ключу `key`.
Позиция `start` обозначает начало диапазона, `stop` — его конец. При
указании отрицательных значений можно использовать диапазон, отсчитанный
справа налево. Нумерация элементов списка начинается с нуля.
Некорректный диапазон будет воспринят либо как пустой список (если
`start` превышает максимальный номер элемента), либо как корректный с
отсечением пустой части (если `stop` превышает максимальный номер
элемента). Следует учитывать, что при прямом отсчёте элементов слева
направо значение `stop` будет включено в состав элементов. То есть,
диапазон `LRANGE list 0 10` будет содержать 11 элементов.

Примеры:

```sql
127.0.0.1:7379> RPUSH mylist "one"
(integer) 1
127.0.0.1:7379> RPUSH mylist "two"
(integer) 2
127.0.0.1:7379> RPUSH mylist "three"
(integer) 3
127.0.0.1:7379> LRANGE mylist 0 0
1) "one"
127.0.0.1:7379> LRANGE mylist -3 2
1) "one"
2) "two"
3) "three"
127.0.0.1:7379> LRANGE mylist -100 100
1) "one"
2) "two"
3) "three"
127.0.0.1:7379> LRANGE mylist 5 10
(empty array)
```

#### lrem

```sql
LREM key count element
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag list">list</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Удаляет из списка, хранящегося по ключу `key`, указанное количество
(`count`) найденных элементов (`element`). Положительное значение `count`
означает поиск слева направо, отрицательное — справа налево. При
значении `0` будут удалены все найденные элементы.

Примеры:

```sql
127.0.0.1:7379> RPUSH mylist "hello"
(integer) 1
127.0.0.1:7379> RPUSH mylist "hello"
(integer) 2
127.0.0.1:7379> RPUSH mylist "foo"
(integer) 3
127.0.0.1:7379> RPUSH mylist "hello"
(integer) 4
127.0.0.1:7379> LREM mylist -2 "hello"
(integer) 2
127.0.0.1:7379> LRANGE mylist 0 -1
1) "hello"
2) "foo"
127.0.0.1:7379>
```

#### lset

```sql
LSET key index element
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag list">list</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Устанавливает индекс (`index`) для добавляемого элемента (`element`).
Таким образом можно затереть один элемент списка и заменить его новым
значением.

Примеры:

```sql
127.0.0.1:7379> RPUSH mylist "one"
(integer) 1
127.0.0.1:7379> RPUSH mylist "two"
(integer) 2
127.0.0.1:7379> RPUSH mylist "three"
(integer) 3
127.0.0.1:7379> LSET mylist 0 "four"
"OK"
127.0.0.1:7379> LSET mylist -2 "five"
"OK"
127.0.0.1:7379> LRANGE mylist 0 -1
1) "four"
2) "five"
3) "three"
127.0.0.1:7379>
```

#### ltrim

```sql
LTRIM key start stop
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag list">list</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Обрезает список, хранящийся по ключу `key`, задавая его размер с помощью
диапазона. При указании отрицательных значений можно использовать
диапазон, отсчитанный справа налево. Нумерация элементов списка
начинается с нуля. Некорректный диапазон будет воспринят либо как пустой
список (если `start` превышает максимальный номер элемента) c удалением
ключей, либо как корректный с увеличением границ списка (если `stop`
превышает максимальный номер элемента).

Типичное применение `LTRIM`:

```sql
LPUSH mylist someelement
LTRIM mylist 0 99
```

Эти команды добавят в список значение `someelement` и при этом установят
емкость списка равной 100 элементам.

Дополнительные примеры:

```sql
127.0.0.1:7379> RPUSH mylist "one"
(integer) 1
127.0.0.1:7379> RPUSH mylist "two"
(integer) 2
127.0.0.1:7379> RPUSH mylist "three"
(integer) 3
127.0.0.1:7379> LTRIM mylist 1 -1
"OK"
127.0.0.1:7379> LRANGE mylist 0 -1
1) "two"
2) "three"
127.0.0.1:7379>
```

#### rpop

```sql
RPOP key [count]
```
<span class="tag">поддерживается с версии 0.4.0</span>
<span class="tag fast">fast</span>
<span class="tag list">list</span>
<span class="tag write">write</span>

Извлекает (и удаляет) указанное число (`count`) последних элементов,
хранящихся в списке по адресу ключа `key`. Без аргумента `count` команда
извлекает один первый элемент в начале списка.

Примеры:

```sql
127.0.0.1:7379> RPUSH mylist "one" "two" "three" "four" "five"
(integer) 5
127.0.0.1:7379> RPOP mylist
"five"
127.0.0.1:7379> RPOP mylist 2
1) "four"
2) "three"
127.0.0.1:7379> LRANGE mylist 0 -1
1) "one"
2) "two"
```

#### rpoplpush

```sql
RPOPLPUSH source destination
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag list">list</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Извлекает один последний элемент из множества, хранящегося в `source` и
добавляет его в начало множества, хранящегося в `destination`. Если
`source` не существует, команда вернёт `nil` и ничего не переместит.
Если в качестве `source` и `destination` указать одно и то же множество,
то команда переместит элемент из его конца в его начало.

??? warning "Примечание"
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN radix 1.0.4 SET radix.redis_compatibility = '{ "enforce_one_slot_transactions": true, "push_result_includes_popped_items": true, "disable_scatter_gather": true, "enabled_deprecated_commands": ["rpoplpush" ] }';
    ```

#### rpush

```sql
RPUSH key element [element ...]
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag fast">fast</span>
<span class="tag list">list</span>
<span class="tag write">write</span>

Работает аналогично [LPUSH](#lpush), но добавляет элементы в конец
списка.

#### rpushx

```sql
RPUSHX key element [element ...]
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag fast">fast</span>
<span class="tag list">list</span>
<span class="tag write">write</span>

Работает аналогично [RPUSH](#rpush), но проверяет существование ключа
`key` и то, что этот ключ содержит список. В противном случае команда
ничего не делает.

### Команды управления подпиской (Pub/Sub) {: #pubsub }

Pub/Sub — механизм для отправки сообщений между клиентами через каналы.

#### psubscribe

```sql
PSUBSCRIBE pattern [pattern ...]
```
<span class="tag">поддерживается с версии 0.2</span>
<span class="tag pubsub">pubsub</span>
<span class="tag slow">slow</span>

Подписывает клиента на получение данных согласно указанному шаблону (`pattern`). Примеры шаблонов:

- `h?llo` подписывает на _hello_, _hallo_ и _hxllo_
- `h*llo` подписывает на _hllo_ и _heeeello_
- `h[ae]llo`подписывает на _hello_ и _hallo_, но не _hillo_

#### publish

```sql
PUBLISH channel message
```
<span class="tag">поддерживается с версии 0.2</span>
<span class="tag fast">fast</span>
<span class="tag pubsub">pubsub</span>

Размещает сообщение (`message`) в указанном канале (`channel`).
Сообщение будет доступно клиентам вне зависимости от того, к какому узлу
кластера они подключены.

#### pubsub channels {: #pubsub_channels }

```sql
PUBSUB CHANNELS [pattern]
```
<span class="tag">поддерживается с версии 0.2</span>
<span class="tag pubsub">pubsub</span>
<span class="tag slow">slow</span>

Выводит список активных каналов. Канал считается активным, если на него
есть хотя бы один подписчик (подписка на шаблоны (`pattern`) не
считается). Если в команде не указан шаблон (`pattern`), то будут
выведены все активные каналы. В противном случае будут выведены только
те активные каналы, которые соответствуют шаблону.

#### pubsub numpat {: #pubsub_numpat }

```sql
PUBSUB NUMPAT
```
<span class="tag">поддерживается с версии 0.2</span>
<span class="tag pubsub">pubsub</span>
<span class="tag slow">slow</span>

Выводит список уникальных шаблонов, на которые были произведены подписки
со стороны клиентов (с помощью команды [PSUBSCRIBE](#psubscribe)). Не
следует путать вывод этой команды с общим числом клиентов.

#### pubsub numsub {: #pubsub_numsub }

```sql
PUBSUB NUMSUB [channel [channel ...]]
```
<span class="tag">поддерживается с версии 0.2</span>
<span class="tag pubsub">pubsub</span>
<span class="tag slow">slow</span>

Выводит список всех подписчиков указанных каналов. Подписчики на шаблоны
(`pattern`) не считаются.

#### pubsub shardchannels {: #pubsub_shardchannels}

```sql
PUBSUB SHARDCHANNELS [pattern]
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag pubsub">pubsub</span>
<span class="tag slow">slow</span>

Выводит список активных шард-каналов (каналов Redis, работающих в рамках
отдельных репликасетов). Параметр `pattern` позволяет отфильтровать
список, указав необходимый шаблон.

#### pubsub shardnumsub {: #pubsub_shardnumsub}

```sql
PUBSUB SHARDNUMSUB [shardchannel [shardchannel ...]]
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag pubsub">pubsub</span>
<span class="tag slow">slow</span>

Выводит количество подписчиков для указанных шард-каналов (каналов
Redis, работающих в рамках отдельных репликасетов).

#### punsubscribe

```sql
PUNSUBSCRIBE [pattern [pattern ...]]
```
<span class="tag">поддерживается с версии 0.2</span>
<span class="tag pubsub">pubsub</span>
<span class="tag slow">slow</span>

Отписывает клиента от указанных шаблонов. Если ни один канал (`pattern`)
не указан, то клиент будет отписан от всех шаблонов.

#### spublish

```sql
SPUBLISH shardchannel message
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag fast">fast</span>
<span class="tag pubsub">pubsub</span>

Размещает сообщение (`message`) в указанном шард-канале
(`shardchannel`). Сообщение будет доступно клиентам на всех репликах,
входящих в состав репликасета, на котором создан шард-канал.

#### ssubscribe

```sql
SSUBSCRIBE shardchannel [shardchannel ...]
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag pubsub">pubsub</span>
<span class="tag slow">slow</span>

Подписывает клиента на получение данных из указанных шард-каналов
(`shardchannel`). Все шард-каналы, указанные в команде, должны
относиться к одному репликасету. Cм. также [PSUBSCRIBE](#psubscribe).


#### subscribe

```sql
SUBSCRIBE channel [channel ...]
```
<span class="tag">поддерживается с версии 0.2</span>
<span class="tag pubsub">pubsub</span>
<span class="tag slow">slow</span>

Подписывает клиента на получение данных из указанных каналов
(`channel`). Cм. также [PSUBSCRIBE](#psubscribe).

#### sunsubscribe

```sql
SUNSUBSCRIBE [shardchannel [shardchannel ...]]
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag pubsub">pubsub</span>
<span class="tag slow">slow</span>

Отписывает клиента от указанных шард-каналов. Если ни один шард-канал
(`shardchannel`) не указан, то клиент будет отписан от всех
шард-каналов.

#### unsubscribe

```sql
UNSUBSCRIBE [channel [channel ...]]
```
<span class="tag">поддерживается с версии 0.2</span>
<span class="tag pubsub">pubsub</span>
<span class="tag slow">slow</span>

Отписывает клиента от указанных каналов. Если ни один канал (`channel`)
не указан, то клиент будет отписан от всех каналов.

### Команды для строк {: #string }

#### append

```sql
APPEND key value
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag fast">fast</span>
<span class="tag string">string</span>
<span class="tag write">write</span>

Добавляет значение `value` к строке, хранящейся по ключу `key`. Если
указанный ключ не существует, то он будет создан, и тогда данная команда
сработает аналогично [SET](#set).

Пример:

```sql
> APPEND mykey "Hello"
(integer) 5
> APPEND mykey " World"
(integer) 11
> GET mykey
"Hello World"
```

#### get

```sql
GET key
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag fast">fast</span>
<span class="tag read">read</span>
<span class="tag string">string</span>

Получает значение ключа `key`. Если ключ не существует, возвращается
специальное значение `nil`. Если значение, хранящееся в ключе, не
является строкой, возвращается ошибка, поскольку `GET` работает только
со строковыми значениями.

#### getdel

```sql
GETDEL key
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag fast">fast</span>
<span class="tag string">string</span>
<span class="tag write">write</span>

Получает значение ключа `key` и удаляет его. Команда действует подобно
[GET](#get), но удаляет ключ только в том случае, если в нём
действительно хранятся строковые значения.

#### getrange

```sql
GETRANGE key start end
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>
<span class="tag string">string</span>

Возвращает подстроку из значения, хранящегося по указанному ключу.
Границы подстроки определяют аргументами `start` и `end`.

#### incr

```sql
INCR key
```
<span class="tag">поддерживается с версии 0.4.3</span>
<span class="tag fast">fast</span>
<span class="tag string">string</span>
<span class="tag write">write</span>

Увеличивает значение, хранящееся по указанному ключу, на `1.` Если
указанный ключ не существует, то его значение
принимается за `0`.

#### incrby

```sql
INCRBY key increment
```
<span class="tag">поддерживается с версии 0.4.3</span>
<span class="tag fast">fast</span>
<span class="tag string">string</span>
<span class="tag write">write</span>

Увеличивает значение, хранящееся по указанному ключу, на величину
`increment`. Если указанный ключ не существует, то его значение
принимается за `0`.

#### incrbyfloat

```sql
INCRBYFLOAT key increment
```
<span class="tag">поддерживается с версии 0.4.3</span>
<span class="tag fast">fast</span>
<span class="tag string">string</span>
<span class="tag write">write</span>

Увеличивает значение, хранящееся по указанному ключу, на величину
`increment`, но при этом поддерживает дробные и отрицательные значения.
Если указанный ключ не существует, то его значение принимается за `0`.

#### mget

```sql
MGET key [key ...]
```
<span class="tag">поддерживается с версии 0.12.0</span>
<span class="tag fast">fast</span>
<span class="tag read">read</span>
<span class="tag string">string</span>

Возвращает значения всех указанных ключей. Если ключ не существует, или
не содержит значения, то для него команда вернёт `nil`. Благодаря этому,
команда никогда не возвращает ошибку.

#### mset

```sql
MSET key value [key value ...]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag slow">slow</span>
<span class="tag string">string</span>
<span class="tag write">write</span>

Сохраняет строковые значения в ключах в соответствующих парах.
Существующие значения при этом перезаписываются (аналогично
[SET](#set)). Команда работает атомарно, устанавливая все значения за один
проход, без возможности отследить, какие ключи были изменены, а какие
нет.

#### psetex

```sql
PSETEX key milliseconds value
```
<span class="tag">поддерживается с версии 0.7.0</span>
<span class="tag slow">slow</span>
<span class="tag string">string</span>
<span class="tag write">write</span>

Устанавливает значение и срок жизни (таймаут) для ключа `key` подобно
[SETEX](#setex), но в миллисекундах.

??? warning "Примечание"
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN radix 1.0.4 SET radix.redis_compatibility = '{ "enforce_one_slot_transactions": true, "push_result_includes_popped_items": true, "disable_scatter_gather": true, "enabled_deprecated_commands": ["psetex" ] }';
    ```

#### set

```sql
SET key value [NX | XX] [GET] [EX seconds | PX milliseconds |
  EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag slow">slow</span>
<span class="tag string">string</span>
<span class="tag write">write</span>

Сохраняет строковое значение в ключе. Если ключ уже содержит значение,
оно будет перезаписано, независимо от его типа. Любое предыдущее
ограничение таймаута, связанное с ключом, отменяется при успешном
выполнении операции `SET`.

Параметры:

- `EX` — установка указанного времени истечения срока действия в
  секундах (целое положительное число)
- `PX` — установка указанного времени истечения в миллисекундах (целое
  положительное число)
- `EXAT` — установка указанного времени Unix, в которое истекает срок
  действия ключа, в секундах (целое положительное число)
- `PXAT` — установка указанного времени Unix, по истечении которого срок
  действия ключа истечёт, в миллисекундах (целое положительное число)
- `NX` — установка значения ключа только в том случае, если он ещё
  не существует
- `XX` — установка значения ключа только в том случае, если он уже
  существует
- `KEEPTTL` — сохранить время жизни, связанное с ключом
- `GET` — возвращает старую строку, хранящуюся по адресу ключа, или
  `nil`, если ключ не существовал. Возвращается ошибка и `SET`
  прерывается, если значение, хранящееся по адресу ключа `key`, не
  является строкой.

#### setex

```sql
SETEX key seconds value
```
<span class="tag">поддерживается с версии 0.7.0</span>
<span class="tag slow">slow</span>
<span class="tag string">string</span>
<span class="tag write">write</span>

Устанавливает для ключа `key` значение `value` и срок жизни (таймаут) в секундах.
Аналогичный результат достигается так:

```sql
SET key value EX seconds
```

??? warning "Примечание"
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN radix 1.0.4 SET radix.redis_compatibility = '{ "enforce_one_slot_transactions": true, "push_result_includes_popped_items": true, "disable_scatter_gather": true, "enabled_deprecated_commands": ["setex" ] }';
    ```

Установка некорректного значения вернёт ошибку.

#### setnx

```sql
SETNX key value
```
<span class="tag">поддерживается с версии 0.7.0</span>
<span class="tag fast">fast</span>
<span class="tag string">string</span>
<span class="tag write">write</span>

Устанавливает для ключа `key` значение `value` только если такого ключа
ранее не было.

??? warning "Примечание"
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN radix 1.0.4 SET radix.redis_compatibility = '{ "enforce_one_slot_transactions": true, "push_result_includes_popped_items": true, "disable_scatter_gather": true, "enabled_deprecated_commands": ["setnx" ] }';
    ```

#### strlen

```sql
STRLEN key
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag fast">fast</span>
<span class="tag read">read</span>
<span class="tag string">string</span>

Возвращает длину текстового значения, хранящегося по
указанному ключу.

### Команды для получения информации о Sentinel {: #sentinel }

Radix поддерживает необходимый минимум команд для того, чтобы приложения
могли получать адреса серверов Picodata, если эти приложения написаны
с поддержкой Sentinel.

По умолчанию перечисленные ниже команды отключены. Для того, чтобы их
включить, используйте запрос:

```sql
ALTER PLUGIN radix 1.0.4 SET radix.sentinel_enabled = 'true';
```
<span class="tag">поддерживается с версии 0.10.0</span>

#### sentinel get-master-addr-by-name {: #sentinel-get-master-addr-by-name }

```sql
SENTINEL GET-MASTER-ADDR-BY-NAME <replicaset name>
```
<span class="tag">поддерживается с версии 0.10.0</span>

Возвращает адрес Radix для заданного репликасета.

#### sentinel master {: #sentinel-master }

```sql
SENTINEL MASTER <replicaset name>
```
<span class="tag">поддерживается с версии 0.10.0</span>

Выводит мастера для заданного репликасета. Radix возвращает мастера
репликасета с соответствующим именем.

#### sentinel masters {: #sentinel-masters }

```sql
SENTINEL MASTERS
```
<span class="tag">поддерживается с версии 0.10.0</span>

Возвращает список репликасетов, которые есть в системе.

#### sentinel myid {: #sentinel-myid }

```sql
SENTINEL MYID
```
<span class="tag">поддерживается с версии 0.10.0</span>

Возвращает id текущего инстанса

#### sentinel replicas {: #sentinel-replicas }

```sql
SENTINEL REPLICAS <replicaset name>
```
<span class="tag">поддерживается с версии 0.10.0</span>

Показывает список реплик для заданного репликасета.

#### sentinel sentinels {: #sentinel-sentinels }

```sql
SENTINEL SENTINELS <replicaset name>
```
<span class="tag">поддерживается с версии 0.10.0</span>

Показывает список сентинелей для заданного репликасета. Radix
возвращает мастера репликасета с соответствующим именем.

### Команды для скриптов {: #scripting }

Radix поддерживает следующие команды для работы с Lua-скриптами:

#### eval

```sql
EVAL script numkeys [key [key ...]] [arg [arg ...]]
```
<span class="tag">поддерживается с версии 0.5.0</span>
<span class="tag scripting">scripting</span>
<span class="tag slow">slow</span>

Вызывает Lua-скрипт. Первый аргумент — исходный код Lua-скрипта
(`script`). Следующий за ним аргумент — количество передаваемых ключей
(`numkeys`) и далее сами ключи и их аргументы.

Пример:

```sql
> EVAL "return ARGV[1]" 0 hello
"hello"
```

Поддерживаемые скриптовые функции для `EVAL`:

- `redis.call(command) `— вызов команды Redis и вывод её результата (при его наличии)
- `redis.pcall(command)` — аналог `redis.call()`, но с гарантированным возвратом ответа, что удобно для анализа ошибок команд
- `redis.log(level, message)` — запись в журнал инстанса сообщения с указанием уровня важности. Например, `redis.log(redis.LOG_WARNING, 'Something is terribly wrong')`
- `redis.sha1hex(x)` — возврат шестнадцатеричного SHA1-хэша для указанного числа _x_
- `redis.status_reply(x)` — возврат статуса состояния _x_ в виде строки
- `redis.error_reply` — возврат состояния _x_ в виде строки
- `redis.REDIS_VERSION` — возврат текущей версии Redis в виде строки в формате Lua
- `redis.REDIS_VERSION_NUM `— возврат текущей версии Redis в виде номера

#### evalro

```sql
EVAL_RO script numkeys [key [key ...]] [arg [arg ...]]
```
<span class="tag">поддерживается с версии 0.5.0</span>
<span class="tag scripting">scripting</span>
<span class="tag slow">slow</span>

Вызывает Lua-скрипт аналогично [eval](#eval), но в режиме "только
чтение", т.е. без модификации данных БД.

#### evalsha

```sql
EVALSHA sha1 numkeys [key [key ...]] [arg [arg ...]]
```
<span class="tag">поддерживается с версии 0.5.0</span>

Вызывает Lua-скрипт аналогично [eval](#eval), но в качестве аргумента
принимает не сам скрипт, а его SHA1-хэш из кэша скриптов.

#### evalsharo

```sql
EVALSHA_RO sha1 numkeys [key [key ...]] [arg [arg ...]]
```
<span class="tag">поддерживается с версии 0.5.0</span>
<span class="tag scripting">scripting</span>
<span class="tag slow">slow</span>

Вызывает Lua-скрипт аналогично [evalsha](#evalsha), но в режиме "только
чтение", т.е. без модификации данных БД.

#### script exists {: #script_exists }

```sql
SCRIPT EXISTS sha1 [sha1 ...]
```
<span class="tag">поддерживается с версии 0.6.0</span>
<span class="tag scripting">scripting</span>
<span class="tag slow">slow</span>

Возвращает информацию о существовании скрипта с указанным хэшем SHA1 в
кэше скриптов.

#### script flush {: #script_flush }

```sql
SCRIPT FLUSH [ASYNC | SYNC]
```
<span class="tag">поддерживается с версии 0.11.0</span>
<span class="tag scripting">scripting</span>
<span class="tag slow">slow</span>

Очищает кэш Lua-скриптов. По умолчанию, операция производится в
синхронном режиме. Пользователь может указать режим явно:

- `ASYNC` — очистить кэш асинхронно
- `SYNC` — очистить кэш синхронно

#### script load {: #script_load }

```sql
SCRIPT LOAD script
```
<span class="tag">поддерживается с версии 0.6.0</span>
<span class="tag scripting">scripting</span>
<span class="tag slow">slow</span>

Загружает скрипт в кэш скриптов. Работает идемпотентно (т.е.
подразумевая, что такой скрипт уже есть в хранилище).

### Команды для транзакций {: #transactions }

#### discard

```sql
DISCARD
```
<span class="tag">поддерживается с версии 0.6.0</span>
<span class="tag fast">fast</span>
<span class="tag transaction">transaction</span>

Удаляет все команды из очереди исполнения

#### exec

```sql
EXEC
```
<span class="tag">поддерживается с версии 0.6.0</span>
<span class="tag slow">slow</span>
<span class="tag transaction">transaction</span>

Исполняет все команды в очереди в рамках единой транзакции.

#### multi

```sql
MULTI
```
<span class="tag">поддерживается с версии 0.6.0</span>
<span class="tag fast">fast</span>
<span class="tag transaction">transaction</span>

Обозначает момент блокировки транзакции. Последующие команды будут
исполняться одна за другой при помощи [exec](#exec).

#### unwatch

```sql
UNWATCH key [key ...]
```
<span class="tag">поддерживается с версии 0.6.0</span>
<span class="tag fast">fast</span>
<span class="tag transaction">transaction</span>

Удаляет все ключи из списка наблюдения [watch](#watch).

#### watch

```sql
WATCH key [key ...]
```
<span class="tag">поддерживается с версии 0.6.0</span>
<span class="tag fast">fast</span>
<span class="tag transaction">transaction</span>

Включает проверку значений указанных ключей для последующих транзакций.

### Команды управления и диагностики {: #server }

#### flushall

```sql
FLUSHALL [ASYNC | SYNC]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag dangerous">dangerous</span>
<span class="tag keyspace">keyspace</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Очищает все базы данных.

- `SYNC`: синхронно, т.е. команда вернёт управление только после полной очистки БД.
- `ASYNC`: асинхронно, команда вернёт управление быстрее, данные очистятся в фоне.

Если ни одна из опций не указана, используется режим SYNC

#### flushdb

```sql
FLUSHDB [ASYNC | SYNC]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag dangerous">dangerous</span>
<span class="tag keyspace">keyspace</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Очищает текущую базу данных.

- `SYNC`: синхронно — команда вернёт управление только после полной очистки БД
- `ASYNC`: асинхронно — команда вернёт управление быстрее, данные очистятся в фоне

Если ни одна из опций не указана, используется режим SYNC

#### info

```sql
INFO [section [section ...]]
```
<span class="tag">поддерживается с версии 0.4.0</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Возвращает информацию о сервере, подключенных клиентах, нагрузке на
текущий узел и прочую статистику. Параметр `section` позволяет уточнить
запрос, ограничив его нужной секцией. Доступные секции:

- `server`
- `clients`
- `memory`
- `persistence`
- `stats`
- `replication`
- `cpu`
- `modules`
- `errorstats`
- `cluster`
- `keyspace`
- `commandstats`
- `latencystats`
- `sentinel`

??? example "Образец вывода полного набора сведений"
    ```
    127.0.0.1:7379> info
    # Server
    radix_version:1.0.4
    picodata_version:26.1.2-0-g6db4aba0a
    picodata_cluster_name:demo_radix
    picodata_cluster_uuid:f5cec208-cbc2-4e8f-86f2-5b57d5ed8ef6
    picodata_instance_name:default_1_1
    picodata_instance_uuid:0af5bbe6-d1bd-42c4-8ea5-cdfd6ef3a93f
    redis_version:8.0.0
    redis_git_sha1:7fcd79c0f16eb27b302f962e027d0e8916f7ea90
    redis_git_dirty:1
    redis_build_id:
    redis_mode:standalone
    os:Fedora Linux 6.12.0-160000.29-default x86_64
    arch_bits:64
    monotonic_clock:POSIX clock_gettime with CLOCK_MONOTONIC
    multiplexing_api:epoll
    atomicvar_api:c11-builtin
    gcc_version:rustc 1.95.0 (59807616e 2026-04-14)
    process_id:175
    process_supervised:no
    run_id:39d461f545b54c2d852cbc37d7fc9b63
    tcp_port:7379
    server_time_usec:1778589327392186000
    uptime_in_seconds:111
    uptime_in_days:0
    hz:800
    configured_hz:0
    lru_clock:0
    executable:/usr/bin/picodata
    config_file:
    io_threads_active:1

    # Clients
    connected_clients:1
    cluster_connections:0
    maxclients:10000
    client_recent_max_input_buffer:8192
    client_recent_max_output_buffer:8192
    blocked_clients:0
    tracking_clients:0
    pubsub_clients:0
    watching_clients:0
    clients_in_timeout_table:0
    total_watched_keys:0
    total_blocking_keys:0
    total_blocking_keys_on_nokey:0

    # Memory
    used_memory:92274688
    used_memory_human:88.00M
    used_memory_rss:151195648
    used_memory_rss_human:144.19M
    used_memory_peak:92274688
    used_memory_peak_human:88.00M
    used_memory_peak_perc:100.00
    used_memory_overhead:92274688
    used_memory_startup:92274688
    used_memory_dataset:0
    used_memory_dataset_perc:0.00
    allocator_allocated:92274688
    allocator_active:92274688
    allocator_resident:151195648
    total_system_memory:16618340352
    total_system_memory_human:15.48G
    used_memory_lua:14041813
    used_memory_vm_eval:14041813
    used_memory_lua_human:13.39M
    used_memory_scripts_eval:0
    number_of_cached_scripts:0
    number_of_functions:0
    number_of_libraries:0
    used_memory_vm_functions:0
    used_memory_vm_total:14041813
    used_memory_vm_total_human:13.39M
    used_memory_functions:0
    used_memory_scripts:0
    used_memory_scripts_human:0B
    maxmemory:2000000000
    maxmemory_human:1.86G
    maxmemory_policy:noeviction
    allocator_frag_ratio:0.00
    allocator_frag_bytes:0
    allocator_muzzy:0
    allocator_rss_ratio:NaN
    allocator_rss_bytes:0
    mem_not_counted_for_evict:0
    mem_replication_backlog:0
    mem_total_replication_buffers:0
    mem_fragmentation_ratio:NaN
    mem_fragmentation_bytes:0
    mem_clients_normal:24576
    mem_allocator:slab
    active_defrag_running:0
    lazyfree_pending_objects:0
    lazyfreed_objects:0
    slab_info_items_size:0
    slab_info_items_used:0
    slab_info_items_used_ratio:0
    slab_info_quota_size:2000000000
    slab_info_quota_used:0
    slab_info_quota_used_ratio:0
    slab_info_arena_size:0
    slab_info_arena_used:0
    slab_info_arena_used_ratio:0

    # Persistence
    loading:0
    async_loading:0

    # Stats
    total_connections_received:2
    total_commands_processed:6
    instantaneous_ops_per_sec:0
    total_net_input_bytes:154
    total_net_output_bytes:1939
    total_net_repl_input_bytes:0
    total_net_repl_output_bytes:0
    instantaneous_input_kbps:0.00
    instantaneous_output_kbps:0.00
    instantaneous_input_repl_kbps:0.00
    instantaneous_output_repl_kbps:0.00
    rejected_connections:0
    sync_full:0
    sync_partial_ok:0
    sync_partial_err:0
    expired_keys:0
    evicted_keys:0
    total_eviction_exceeded_time:0
    current_eviction_exceeded_time:0
    keyspace_hits:0
    keyspace_misses:0
    pubsub_channels:0
    pubsub_patterns:0
    latest_fork_usec:0
    migrate_cached_sockets:0
    unexpected_error_replies:0
    total_error_replies:4
    total_reads_processed:7
    total_writes_processed:6
    client_query_buffer_limit_disconnections:0
    client_output_buffer_limit_disconnections:0
    reply_buffer_expands:0
    reply_buffer_shrinks:0
    request_buffer_expands:0
    request_buffer_shrinks:0
    acl_access_denied_auth:0
    acl_access_denied_cmd:0
    acl_access_denied_key:0
    acl_access_denied_channel:0
    watching_clients:0
    clients_in_timeout_table:0
    total_watched_keys:0

    # Replication
    role:master
    connected_slaves:0
    master_failover_state:no-failover
    master_replid:0af5bbe6-d1bd-42c4-8ea5-cdfd6ef3a93f
    master_replid2:0af5bbe6-d1bd-42c4-8ea5-cdfd6ef3a93f
    master_repl_offset:34836
    second_repl_offset:34836
    repl_backlog_active:0
    repl_backlog_size:0
    repl_backlog_first_byte_offset: 0
    repl_backlog_histlen:0

    # CPU
    used_cpu_sys:1.062690
    used_cpu_user:7.042313
    used_cpu_sys_children:0.000000
    used_cpu_user_children:0.000000
    used_cpu_sys_main_thread:1.057354
    used_cpu_user_main_thread:7.038022

    # Modules

    # Errorstats
    errorstat_UNKNOWN_COMMAND:count=4
    # Cluster
    cluster_enabled:1

    # Keyspace
    db0:keys=0,expires=0,avg_ttl=0,subexpiry=0
    # Commandstats
    cmdstat_info:calls=2,usec=106,usec_per_call=53,rejected_calls=0,failed_calls=0
    # Latencystats
    latency_percentiles_usec_info:p50.0=85,p99.0=85,p99.9=85
    # Sentinel
    sentinel_masters:1
    sentinel_tilt:0
    sentinel_tilt_since_seconds:0
    sentinel_running_scripts:0
    sentinel_scripts_queue_length:0
    sentinel_simulate_failure_flags:0
    ```

#### memory usage {: #memory_usage }

```sql
MEMORY USAGE key [SAMPLES count]
```
<span class="tag">поддерживается с версии 0.6.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>

Показывает объём ОЗУ, занимаемый указанным ключом `key`. Параметр
`SAMPLES` позволяет указать число дочерних элементов ключа (если такие
имеются), объём которых также будет учтён. По умолчанию, значение
`SAMPLES` равно 5. Для учёта всех дочерних элементов следует указать
`SAMPLES 0`.

#### object encoding {: #object_encoding }

```sql
OBJECT ENCODING key
```
<span class="tag">_поддерживается с версии 0.14.0_</span>
<span class="tag keyspace">keyspace</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>

Возвращает способ кодирования объекта, хранящегося по ключу `key`.
Варианты кодирования:

- `raw` — стандартное кодирование текстовых строк
- `quicklist` — способ кодирования списков, совместимый с типами `linkedlist`, `ziplist` и `listpack` в Redis
- `hashtable` — стандартное кодирование множеств
- `skiplist` — стандартное кодирование сортированных множеств

### Команды для отладки {: #pico_debug }

Команды с префиксом `pico` предназначены для отладки и диагностики
кластера в том случае, если отсутствует возможность подключиться к
Picodata другим способом.

!!! warning "Внимание!"
    Никаких гарантий на форматы вывода нет; любые
    попытки использования данных команд в работе приложения не
    поддерживаются!

#### pico status  {: #pico_status }

<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag pico">pico</span>

Отображает состояние команд для отладки (включено/выключено).

#### pico enable {: #pico_enable }

<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag pico">pico</span>

Включает использование команд для отладки.

#### pico disable {: #pico_disable }

<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag pico">pico</span>

Выключает использование команд для отладки.

#### pico sql {: #pico_sql }

<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag pico">pico</span>

Позволяет выполнить SQL-запрос в Redis-консоли.

Пример:

```sql
127.0.0.1:7379> pico sql "SELECT * FROM _pico_user WHERE schema_version=4"
1) "+----+------+----------------+------------------------------------------------+-------+------+"
2) "| id | name | schema_version | auth                                           | owner | type |"
3) "+============================================================================================+"
4) "| 33 | andy | 4              | ['md5', 'md59ee8b0076cf18219f9b2f585f57d4d0c'] | 1     | user |"
5) "+----+------+----------------+------------------------------------------------+-------+------+"
6) "(1 rows)"
```

#### pico lua {: #pico_lua}

<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag pico">pico</span>

Позволяет выполнить Lua-запрос в Redis-консоли.

Пример:

```sql
127.0.0.1:7379> pico lua "return box.cfg.memtx_dir"
"data"
```

## Журнал изменений {: #changelog }

### 1.0.4 — 2026-05-27 {: #01.0.4 }

**Новая функциональность (CLI)**

- Добавлена миграция данных в radix-cli

**Исправления**

- Исправлен неверный подсчет значения курсора в SCAN
- Исправлен лишний пробел в выводе master в cluster shards

**Документация**

- Добавлена документация к radix-cli
- Добавлен АДР о совместной работе AUTH и подсистемы ACL
- Добавлен раздел о работе с merge request'ами

**Прочее**

- Упаковка radix-cli по аналогии с ouroboros-cli
- Разрешена лицензия BSL-1.0
- Исправлен python → python3 в lint.just, обновлены метаданные сертификации
- Возвращена явная сборка radix-cli перед упаковкой плагина
- Добавлен CI для radix-cli
- Обновлены cliff конфиги под radix-cli
- Добавлена джоба для очистки веток
- Джоба с тикетом на документацию переведена на glab
- Автоматизировано создание веток поддержки
- Замёржили авторизацию гитлаба и основного репозитория для докера
- Перевели CI с событий на ветках на события на MR'ах

### 1.0.2 — 2026-05-13 {: #01.0.2 }

**Прочее**

- В test-unlogged добавлен -failfast
- Мигрировали сборку Docker-образов с Kaniko на BuildKit rootless

**Build**

- Выделили версию плагина в Dockerfile
- Исправили детект версии для теста даунгрейда


### 1.0.1 — 2026-05-13 {: #01.0.1 }

**Исправления**

- Команды PING и INCRBYFLOAT приведены в соответствие с Redis
- Поправлено превращение структур из Lua в RESP2
- Исправлена интерполяция переменных в ошибках в migration_controller
- Исправлено использование стореджей в транзакциях
- Исправлен учет шард подписок с реплики
- Исправлено обновление статистики по ключам при обращениях к виртуальной БД
- Исправлено ожидание уже завершенных файберов при дропе листенера
- Поправлен вывод списков в командах EVAL

**Документация**

- Добавлены полные скрипты для примеров в документации
- Добавлена информация по поводу несовместимости старых конфигурационных файлов в пользовательской документации

**Внутренние улучшения**

- Обновлена логика Lua-скриптов

**Тестирование**

- Добавлены тесты на статистику spubsub

**Прочее**

- Добавили детектор сломанных записей в поставку
- :construction_worker: добавили standalone-докер образ для быстрого тестирования

**Build**

- :arrow_up: добавили детектор коррапченных данных

**Lint**

- Исправили заголовки лицензий в детекторе

### 1.0.0 — 2026-04-30 {: #01.0.0 }

**Новая функциональность**


- Добавлена генерация и валидация ACl-категорий
- Добавлена статика eviction
- Убрано разделение на acl и auth, теперь включение авторизации подразумевает включение ACL
- Добавили сборку докер-образа на релизе
- Поправили работу выключенного вытеснения, теперь это noop
- Реализована проверка доступа по ACL
- Используем машинерию Picodata для tcp
- Добавлена проверка локальности кода и данных сервиса Radix
- Добавлена возможность читать метаданные о сервисах плагинов
- Добавлен валидатор контекста миграции и настройка журналируемости системных таблиц
- Добавлена настройка scatter_gather
- Убрано двойное чтение таблицы types при вытеснении
- Добавлено обращение к LFU кешу при вытеснении
- На TTL индексах включена exclude_null настройка
- Улучшен алгоритм выбора БД для вытеснения
- Реализовано вытеснение с мягким ограничением по памяти
- Добавлена инфраструктура для вытеснения устаревших данных
- Закеширован список баз данных с признаком локальности
- Добавлена no-op реализация CLIENT
- Добавлена поддержка команд getdel и append
- Добавлена проверка на несовместимые изменения через git-cliff для тестов на даунгрейд
- Добавлена совместимость с будущими версиями Picodata
- Добавлена статистика ACL
- 📈 добавлены метрики request_buffer_expands и request_buffer_shrinks

**Ломающие изменения**

- Внесли bucket_id в PK во всех таблицах

**Исправления**

⚡ команда INFO ускорена
Не выходить из listener без отмены файбера
Исправлены тесты после включения флага push_result_includes_popped_items
Исправлен запуск теста с tls downgrade
Реализован правильный учет активных подписок
Unsubscribe без аргументов отписывает от всех существующих подписок
Исправили баг в декременте счётчика ключа при вытеснения
Исправили возвращаемое значение hmset
Исправлено сохранение правил ACL
Исправлена паника при использовании ref cell в транзакциях
Идентификаторы соединений теперь являются целочисленными и соответствуют Redis
Исправлено удаление нескольких ключей через unlink
Незнакомые команды теперь не отображаются в статистике
Подкоманды в статистике форматируются через символ |
Исправлена некорректная работа счетчиков в статистике
Usec и usec_per_call теперь считаются корректно
Исправлен пустой вывод в INFO commandstats
Исправлена неверная конвертация в секунды в INFO
Улучшена совместимость с будущими версиями Picodata
Исправлена конфигурация кластеров разработчиков
Исправил некорректный вывод TYPE для zset
🔒 обновлена библиотека bytes
Исправлена запись нулевых данных cluster info
Изменена сериализация ответов GET/SET/TYPE в lua
Исправлено хранение строк
Исправлена некорректная десериализация из lua-скрипта
Убрано состояние Reader из публичного контракта
🧵 добавлен RefCell к Crc16Hasher
🩹 изменён алгоритм расширения входящего буфера
🩹 изменён алгоритм определения активности бакета
Исправлен алгоритм уменьшения входящего буфера

**Ломающие изменения**

- 💥 исправлен алгоритм вычисления RedisBucketId по ключу

**Производительность**

- Применение ACL при миграции больше не ожидается глобально
- ⚡ удалён реестр отложенных действий
- Исправлена отправка ответов на пакетный запрос

**Ломающие изменения**
- Добавлена оптимизация списков при помощи использования дробных чисел для индексов

**Документация**

- Добавлена инструкция по настройке адресов и TLS
- Добавили описание ситуации с пустыми паролями
- Актуализирована пользовательская документация
- Добавлена инструкция по экспорту метрик через redis_exporter и Prometheus
- Добавлена инструкция по настройке Grafana с Redis Data Source
- Добавлена статья по переносу данных с помощью RIOT
- Изменена формулировка о неприменимости ACL к команде SELECT
- Дополнен перечень поддерживаемых ACL команд
- Исправлено описание вычисления актуальных прав
- Исправлена опечатка
- Описана разница в доступе к базам Redis vs Radix
- Написан ADR для ACL

**Внутренние улучшения**

- Удален channel-индекс на шард-подписки
- Упрощена логика очистки подписок при обрыве соединения
- Переписан макрос retry! с дефолтами
- Pubsub-ы теперь используют глобальные таблицы
- Убрали ручную сериализацию LockType и Type
- Переименовали методы декодирования данных из Picodata
- Переименовали в коде oset в zset
- Заменили префикс N на суффикс Record
- Привели имена методов к Rust naming convention
- ToEntityKey удалён
- Добавили отдельную структуру для первичного ключа типов
- Добавили отдельную структуру для первичного ключа Lua-скриптов
- Добавили отдельную структуру для первичного ключа строк
- Добавили отдельную структуру для первичного ключа каналов
- Добавили отдельную структуру для первичного ключа подписчиков по шаблону
- Добавили отдельную структуру для первичного ключа подписчиков
- Добавили отдельные структуры для первичных ключей блокировок сортированных множеств
- Добавили отдельные структуры для первичных ключей блокировок списков
- Добавили отдельную структуру для первичного ключа списков
- Добавили отдельную структуру для первичного ключа упорядоченных множеств
- Добавили отдельную структуру для первичного ключа множеств
- Добавили отдельную структуру для первичного ключа хешей
- Вернули Display вместо Debug для наших сущностей
- Изменили сериализацию типа сущности или блокировки на u8
- Реализовано zero-copy чтение строковых полей из Tuple
- Унифицировали десериализацию сущностей
- Реализовали ToEntityKey для всех сущностей
- Реализована возможность ленивой десериализации сортированных множеств
- Реализована возможность ленивой десериализации множеств
- Реализована возможность ленивой десериализации списков
- Сортированные множества переведены на трейты с отдельных типов
- Множества переведены на трейты с отдельных типов
- Списки переведены на трейты с отдельных типов
- Реализована возможность ленивой десериализации блокировок упорядоченных множеств
- Реализована возможность ленивой десериализации блокировок списков
- Реализована возможность ленивой десериализации хешей
- Реализована возможность ленивой десериализации каналов
- Реализована возможность ленивой десериализации lua скриптов
- Реализована возможность ленивой десериализации подписок по шаблону
- Реализована возможность ленивой десериализации подписок
- Реализована возможность ленивой десериализации типов
- Реализована возможность ленивой десериализации строк
- rtype вынесен в отдельный модуль
- Снизим уровень логирования в вытеснении
- В крейте Radix-picodata код разделён на мелкие модули
- Код в крейте Radix разделён на несколько модулей
- Использовали inspect_err вместо map_err
- Удалены устаревшие аргументы в локах
- Добавлена роль pico_metadata_reader
- Миграции упрощены к релизу 1.0
- Мутабельные ссылки трейта Cmd изменены на иммутабельные
- calculate_sha1 унифицирован
- Удалены абстракции над BytesMut
- Удалены пробелы в конце строк в sql запросах
- Убран избыточный pattern-matching
- Добавлена реализация Drop для Reader и Writer
- 🎨 упрощён код обработки входящих запросов
- 🎨 упрощён код Shrinker
- ♻️ добавлена обработку смены состояния бакетов
- 🚚 lua вынесен из строковых констант в *.lua файлы
- 💥 переписан алгоритм определения репликасета по бакету
- ♻️ закешировано id соединения в функции его обработки
- ⚡ закешированы инстансы Crc16Hasher
- ⚡ изменён алгоритм вычисления репликасета по bucket_id
- Переписан основной цикл обработки команд
- ⬆️ крейт redis-protocol обновлён до 6.0.0
- ⚗️ параметр соединения tcp_no_delay явно выставляется в true

**Тестирование**

- Добавлены тесты на багфиксы
- Добавлены тесты на удаление множеств и отсортированных множеств в FLUSH-командах
- Обновлены тесты в связи с новым поведением authorization_mode и default пользователя
- Убрали использование устаревшей настройки addr
- Понизили watermark для более стабильной работы тестов
- Добавлены тесты на вытеснение на кластерном режиме
- Обновлена конфигурация тестовых стендов согласно новой валидации
- Добавлены интеграционные тесты на eviction
- Sentinel-тесты теперь сами накатывают конфиг, в дефолтном конфиге sentinel_enabled выставлен в false
- Исправлено чтение ответа редиса для кастомных команд в тестах
- Добавлен тест на status reply
- Добавлен тест на eval целого числа

**Прочее**

- 👷 исправлена сборка релизного докер-образа
- Добавлен тест на невалидный сценарий tls
- Выставили push_result_includes_popped_items по умолчанию в true
- Уменьшили количество повторяемого кода в пайплайне
- Обновили образа для тестов миграции данных
- Изменим версию Pikeа в бенчмарках на 5.0.0
- Обновили rust-edition до 2024
- Обновили pike до 5.0.0
- Обновим Picodata в CI до 26.1.2
- Обновили зависимости для включения последних исправлений
- Перевели bench джобу на unlogged таблицы
- Обновили rust до 1.95
- Обновили rand до 0.10.1
- Поправим версию крейта eviction
- Запуск всех тестов с tls
- Исправлен запуск бенчмарков
- Обновлён крейт unicode-segmentation до 1.13.2
- Test-migration сохраняет артефакты после запуска
- Все тесты в рамках CI работают с -failfast
- Sdk обновлен до 26.1.1
- Внесены небольшие исправления в документацию к observability
- В гайд по observability добавлены Dockerfile и docker-compose.yml
- Переструктурирована документация по observability
- Увеличен интервал сбора метрик до 1 минуты
- Удален полуживой compose в корне
- Добавлены dev-контейнеры для сбора метрик в Redis и Radix
- Добавлен контейнер для локальной работы с Grafana
- Обновлён Rust до 1.94, исправлены замечания clippy
- Обновили версии зависимостей в SBOM-файлах
- Обновили Picodata до 25.5.9 и Pike до 3.2.1
- Добавлена возможность запуска deploy-certified-build для ручных пайплайнов
- Добавлена генерация SBOM-файлов для ФСТЭК-сертификации
- 📌 зависимости обновлены до последних версий
- Исправлен запуск бенчмарков на main
- Улучшено использования gitlab cache для cargo
- Добавлены контроли для ФСТЭК-сертификации
- Добавлен пайплайн для проверки переноса данных с помощью RIOT
- Обновлен docker-compose.yml для Redis
- Обновлены Dockerfile в репозитории с учётом новых образов
- Убраны лишние операции при обработке клиентского буфера
- Явно указан идентификатор лицензии крейта colog
- Добавлена проверка cargo deny для ФСТЭК-сертификации
- Добавлен тир arbiter в кластер по умолчанию
- Исключено время записи ошибки через логгер в бенчмарках
- Добавлена сортировка результатов по имени команды в бенчмарках
- 👷 добавлена команда lint::fix
- 📄 обновлены лицензионные заголовки в связи с новым годом
- Исправлен CI для ветки main
- 📄 обновлены лицензионные заголовки в связи с новым годом

**Ломающие изменения**

- Удалена очень старая настройка addr

**Build**

- Исправили детектор ломающих изменений для релиза
- Добавлена проверка на ломающие изменения в тестовые скрипты
- Заменили луашный костыль ожидания ребаланса на Pike
- 📦 добавлена поддержка fedora:43, alt:p11 и osnova onyx:3
- 📦 удалена поддержка fedora:41 в связи с её EOL
- Исправлен скрипт, ожидающий конца ребаланса
- 📦 добавлена поддержка redos:8.0
- Добавлен простенький детектор завершения ребаланса стораджей

### 0.14.1 — 2026-02-18 {: #0.14.01 }

**Исправления**

- Добавлена совместимость с будущими версиями для системных таблиц

**Тестирование**

- Из TCP-сокета в случае массива вычитываются все данные

**Прочее**

- Обновлены Dockerfile в репозитории с учётом новых образов
- Обновил лицензионные заголовки в связи с новым годом

**Build**

- добавлена поддержка fedora:43, alt:p11 и osnova onyx:3
- удалена поддержка fedora:41 в связи с её EOL

### 0.14.0 — 2025-12-23 {: #0.14.0 }

**Новая функциональность**

- Добавлена статистика использования памяти

**Исправления**

- Исправлено поведение флага `push_result_includes_popped_items` — теперь он влияет только на вывод
- Исправлен вывод результата команды `pico lua` — теперь он включает в себя все элементы таблиц
- Исправлен вывод ошибки `pico sql` — теперь выводятся все строчки, вместо только первой

**Внутренние улучшения**

- Поправили заголовки лицензий в файлах и добавили в утилиту

**Тестирование**

- Исправлен вывод ошибок теста на даунгрейд плагина

**Прочее**

- Бенчи надо для удобства катать и на мейне
- добавим по тесткейсу на каждый процент перформанса
- выставим `RUST_LIB_BACKTRACE` в 0
- добавил утилиту для генерации ключей в каждый слот

**Build**

- вернул команды для обновления ченжлога
- перевели сборку Radix на just

### 0.13.0 — 2025-11-20 {: #0.13.0 }

**Новая функциональность**

- Добавлена команда QUIT
- Добавлены команды pico для дебага
- Добавлена секция latency в info
- Добавлена поддержка шардированного пабсаб
- Добавлена поддержка rpoplpush и brpoplpush
- Добавлены команды со структурой данных set

**Исправления**

- Добавлена повторная попытка отправить запрос обновления подписчика паттерна через cas
- Exec всегда помечает соединение как нетранзакционное
- Исправлено поведение флага PUSH_RESULT_INCLUDES_POPPED_ITEMS: теперь он считает элементы, даже если они все улетели в блокирующую команду
- Убрана паника при отсутствии аргументов pubsub shardnumsub
- Исправлены команды zremrangeby*: теперь они удаляют ключ, если были удалены все его элементы
- Исправлены команды z*store: теперь они заменяют значение ключа вне зависимости от типа
- Исправлены команды s*store: теперь они заменяют значение ключа вне зависимости от типа
- Исправлено удаление patsubscriber
- Исправлена команда mset — теперь она перезаписывает значение ключа вне зависимости от типа
- Исправлен расчёт бакета для z*store команд
- Исправлено поведение флага ch команды zadd
- Исправлена передача аргументов начала и конца поиска в командах zrevrange*
- Исправлено сообщение об ошибке значения первого и последнего индексов поиска команды zrange
- Исправлены команды eval и eval_ro — теперь они не сохраняют скрипт
- Исправлено поведение команды lrange с отрицательными индексами
- Добавлена сортировка значений в команде lpos и правильная обработка аргумента COUNT
- Исправлен порядок элементов в блокирующих командах над списками при удалении справа
- Исправлена команда hincrby — теперь она создаёт тип, если ключа не было в базе данных
- Исправлены команды ttl и pttl — теперь их результаты округляются вверх
- Исправлены команды set, psetex, setex, setnx — теперь они перезаписывают ключ вне зависимости от типа
- Инплейс проверки для defer модуля
- Детализация ошибок записи в Picodata
- Игнорирование expell узлов при определении режима работы
- Запрещены команды в контексте пабсаб кроме пабсаб команд
- Добавлена передача адреса при возврате moved ошибки
- Исправлено поведение zrandmember в соответствии с документацией redis

**Документация**

- детализировал описание настройки enforce_one_slot_transactions
- добавили описание особенностей некоторых команд
- добавили таймаут в 20 минут на все команды из README

**Внутренние улучшения**

- исправили предупреждения rust 1.91

**Тестирование**

- Добавлен тест команды quit на редисе
- Обновлены ключи в тесте TestRedisPExpireBusyMainLoop — теперь они уникальны между клиентами
- Добавили реакцию на PICODATA_WRITE_ERROR — переподключение
- Обновили зависимости в тестах
- Добавлен запуск интеграционных тестов на Redis Cluster, Redis Standalone, Radix Standalone и Radix в режиме совместимости с Redis
- Добавил вывод отчёта о бенчмарках в гитлаб
- Добавлена проверка одновременной работы sentinel и deprecated команд
- Добавлен режим тестирования на реплике для чтения
- Тест на downgrade плагина

**Прочее**

- исправил сообщения на колбеках: info при успехе, warn при ошибке
- добавлено логирование на плагин хуках
- сделаем отдельный образ для тестов редиса
- унифицировали установку го и версию Picodata для тестов
- обновлён темплейт issue — теперь правильно выставляется лейбл бага

**Сhore**

- Удалено разделение команд на сабкоманды

### 0.12.0 — 2025-10-29 {: #0.12.0 }

**Новая функциональность**

- Implement cluster info
- Добавлена команда mget

**Исправления**

- Вывод значения настройки max_clients в info (498d8e9)
- В вывод команды info server добавлены имя и uuid инстанса Picodata (417d62b)

**Документация**

📝 обновлена документация о процессе разработки (4713cfe)

**Внутренние улучшения**

Упорядочено чтение настроек на старте Radix (ec23336)

**Тестирование**

- Добавлен тест вывода параметра maxclients (761fcf8)
- Передача пути к Picodata в установке пароля в Makefile (8e86a40)

**Прочее**

- Обновили SDK до 25.4.3 (8d9514e)
- 👷 исправлен CI для ветки main (a5ff0e1)
- Bump picodata version to 25.4.1 (cd5cd2d)

### 0.11.0 — 2025-09-25 {: #0.11.0 }

**Новая функциональность**

- Implement master cond
- Echo
- Script flush

**Исправления**

- Add zset commands into eval_ro ban
- Script flush and echo comments/panic messages
- Set log regarding any fails (adb41a0)
- Make the "unknown command" error message match redis'

**Документация**

- 📝 улучшим конфиг для генерации ченжлога на публику

**Тестирование**

- Add script flush async test (24980d7)

**Прочее**

- Curlf.sh теперь в $PATH
- 👷 бенчмарки должны запускаться автоматически
- 👷 добавим таймстемпы в логи CI
- 👷 не надо бенчи на тегах запускать
- 👷 улучшим вывод сообщение о релизе в телеграм
- Benchmark some commands in ci
- Update the benchmarks
- Add ci benchmarks
- Remove debug logging

### 0.10.0 — 2025-09-02 {: #0.10.0 }

**Новая функциональность**

- FLUSHDB должен очищать и ordered set спейсы
- By default auth should be disabled
- Remove chrono
- Implement auth method
- Implement mset
- Add sentinel section to info
- Introduce sentinel
- Introduce ordered sets
- Implement flush && flushall
- Split RADIX_ADDR into listen/advertise
- Add hmget/hmset, readonly, reset, unlink cmds

**Исправления**

- Явно прописываем WAIT APPLIED LOCALLY для асинхронного FLUSHDB
- Pubsub should work with su
- Обрабатывать ошибку "доступа нет" от ядра
- Remove key type if the last list element was popped
- Do not block in scripts
- Rename timeout func
- Reschedule list lock remove action if there are pending actions
- Use defer
- Remove locks if command went into a timeout (only blpop rn)
- Do not reverse values and scores for zscan, add tests for new behaviour
- Zscan
- Fail on decode bucket in \`get_buckets\`
- Final fixes in auth method
- Create type on zdiffstore
- Use instance name for replica
- Use the original redis error for evalsha
- Close conn on client handle return
- Use box.info.replication to check replication status in INFO REPLICATION
- Use cas for patsub dml ops

**Производительность**

- :zap: move debug logs to debug and raise default level to info

**Документация**

- Авторизация, описание и примеры использования
- Исправим документацию по сентинелю
- Добавим документацию конфигурацию клиента sentinel
- Добавим скрипт для автоматического обновления списка поддерживаемых команд

**Внутренние улучшения**

- Переименуем миграции, чтобы была нумерация последовательная
- Stringify the field name of a stat for a subtraction warning

**Тестирование**

- Перепишем тесты с шелла и пайпов на psql

**Прочее**

- Remove docs from gamayun scan
- Обновим список поддерживаемых команд
- Review fix
- Добавим лицензию в файлы
- Удалим docker-compose.yml из анализа гамаюна
- License update
- Remove unneeded dirs for gamayun
- :page_facing_up: опечатку исправим
- Add license-check job
- :page_facing_up: добавил лицензию на плагин
- Do not build in ci pipeline, block tests, until linting is done

**Build**

- Remove PIKE_DATA_DIR
- Remove TARGET_ROOT, remove unused parameters in the cluster config
- Pack old migrations, use new migrations locally
- Update dependencies
- Update the makefile to use new pike features

**Deps**

- Обновим Picodata до 25.3.2

### 0.9.0 — 2025-06-25 {: #0.9.0 }

**Новая функциональность**

- Watch empty keys too
- Add picodata's cluster_name and cluster_uuid to server info
- Add a config option to enforce same-slot transactions

**Исправления**

- Handle all commands in transactions, even if they have no bucket_id
- TYPE должна возвращать "none" на ключах, которых нет
- Don't panic on empty del cmd call
- Conn dead lock while receive on drop

**Производительность**

- Implement partial list deserialization

**Внутренние улучшения**

- :arrow_up: запускаю cargo update для обновления токио
- :arrow_up: обновляю picodata-plugin до 25.2.2
- :rotating_light: будем в бенчмарке использовать crypto/rand вместо math/rand

**Тестирование**

- :adhesive_bandage: грязный трюк с прогоном теста на тире с 1 репликасетом
- Увеличиваем таймаут в тесте пабсаба
- :adhesive_bandage: добавить небольшой таймаут после старта кластера, чтобы он закончил с ребалансом

**Прочее**

- Fetch tags for gamayun
- Останавливаю кластер перед тем, как забрать артефакты
- Пробуем запустить тесты ещё и на альте
- Передадим прошлую версию в Гамаюн
- :heavy_minus_sign: удаляю неиспользуемые dev-dependencies
- Wait for quality gate
- Подставим версию 3 в Cargo.lock, ничего не сломается.
- Удалим пароль для админского юзера из топологии
- :coffin: удалим старые неиспользуемые луашки
- :construction_worker: добавляю Гамаюна

### 0.8.0 — 2025-06-04 {: #0.8.0 }

**Новая функциональность**

- Add more script telemetry

**Исправления**

- Don't panic when downstream is not in the follow state or upstream's fiber is invalid

**Документация**

- Add redis_compatibility user documentation

### 0.7.0 — 2025-05-28 {: #0.7.0 }

**Новая функциональность**

- Add deprecated set commands
- Use non-blocking variants of commands in transactions
- Implement more expire commands
- Add expiretime
- Make scripts transactional
- Implement hvals

**Исправления**

- Correctly close client connections on listener drop
- :bug: используем правильный запрос для получения информации о репликасете
- Rust 1.87.0
- Pop crash

**Документация**

- Создаём тикет на обновление доков Radix, а не Argus
- Обновим пользовательскую документацию

**Прочее**

- :arrow_up: обновим плагин для Picodata до 25.2.1
- 🔨 обновим редис до 8.0 в кластере для тестов
- :construction_worker: при релизе создаём тикет в Picodata на обновление документации
- :bug: исправим пути файлов в релизе и приложим файл от бендера всегда

**Build**

- :arrow_up: picodata 25.1.2

  0.6.1 — 2025-04-28 0.6.1

**Документация**

- :memo: обновим документацию

**Прочее**

- :construction_worker: вернём redos

### 0.6.0 — 2025-04-18 {: #0.6.0 }

**Новая функциональность**

- ✨ add functions to the redis object in lua scripts, add SCRIPT LOAD, SCRIPT EXISTS commands
- Use custom radix string type
- Use negative indexing for lists
- Introduce transactions

**Исправления**

- :bug: удалим падающую миграцию
- Command processed metric
- Deadlocks for blocking commands on same bucket
- :bug: бакеты из другого тира всегда удаленные
- :technologist: исправим упаковку релиза после мержа пики
- :technologist: исправим сообщение
- Mem stat
- 🚑 fix the plugin file layout for pike
- :ambulance: provide replication_factor setting in picodata.yaml

**Производительность**

- Try to optimize list op

**Документация**

- Опишем конфигурацию миграций в пользовательской документации

**Внутренние улучшения**

- :loud_sound: поправим сообщение для лога, в случае ошибки

**Прочее**

- :fire: удалить лишние файлы
- Fix path to cargo2junit
- Fix clippy format warnings
- :rotating_light: rust 1.86.0
- :construction_worker: используем новые образа для упаковки
- :construction_worker: поправим бендера в мейне
- :construction_worker: добавим новые ОС в процесс сборки
- :hammer: положим редис-кластер в репу с командой для запуска
- 🩹 match the topology with the main branch's, move env variables to topology.toml

**Build**

- Используем Pike 2.1.0 для билда
- :adhesive_bandage: сделал по два репликасета на каждый тир как и в оригинальном кластере
- :arrow_up: introduce pike 2.0.0

### 0.5.2 — 2025-03-19 {: #0.5.2 }

**Прочее**

- :technologist: добавим отлаженного Бендера

### 0.5.1 — 2025-03-13 {: #0.5.1 }

**Исправления**

- :adhesive_bandage: проверяем на андерфлоу при вычитании на статистике

**Производительность**

- :zap: если бакет локальный, то не ходить по рпц

**Документация**

- :memo: исправим разметку в readme

**Тестирование**

- :construction_worker: исправим \`make test_ci\`, чтобы совпадало с реальностью
- :white_check_mark: переведём бенч на кластерный клиент

**Прочее**

- :construction_worker: отсылаем нотификацию о релизе в спецчат в телеге

### 0.5.0 — 2025-03-06 {: #0.5.0 }

**Новая функциональность**

- :sparkles: реализуем новую команду \`dbsize\` для проверки состояния кластера
- :building_construction: используем CRC16/XMODEM для сегментирования
- :construction_worker: теперь паники будут в файловых логах
- Allow multitier mode
- Eval

**Исправления**

- Deadlock on single mode for blocking ops
- :bug: cluster getkeysinslot исправлена
- :bug: используем UUID узла и репликасета в ответе на myid, myshardid
- :card_file_box: fix migrations
- Tests data cleanup
- Eval ptr propagation
- Eval ptr propagation
- Parse timeout arguments to f64 not i64

**Документация**

- :memo: обновим документацию для пользователя
- :memo: ADR для мультитирного (многорядного?) Radix

**Внутренние улучшения**

- :recycle: зафиксируем, что работаем только с 16384 бакетами
- :recycle: вынесем \`RedisBucketId\` и \`PicodataBucketId\` в отдельные файлы
- :recycle: режим выполнения команды не отделим от бакета выполнения команды
- :recycle: типизируем айдишники бакетов
- :recycle: переименовываем ID в Name, потому что мы использовали имена
- :recycle: адаптировал запуск к запуску в нескольких тирах

**Структура кода**

- :rotating_light: отформатировал код
- :recycle: поправил комменты и ошибку к методу insert_patsubscriber

**Тестирование**

- :white_check_mark: исправил тест на cluster nodes

**Прочее**

- :construction_worker: переедем на образ с явно выставленным стабильным растом
- :green_heart: укажем полный путь до cargo2junit, пока его нет в базовом образе
- Add warn log for attempting sub from 0 value to stat macro
- :construction_worker: поправим пути к карго
- :construction_worker: попробуем новый базовый образ для Picodata
- Fix lints for rust 1.85
- Rename radix nodes migration in manifest
- Add deploy to EE repo (pdg)
- :construction_worker: временно разрешим тестам падать
- :white_check_mark: запускаем тесты в ci теперь
- :technologist: делаем удобный запуск кластера Picodata
- Rename replace_patsubscriber to insert_patsubscriber

**Bench**

- List

**Build**

- \`make pico_radix_release\` для запуска релизного Radix
- :arrow_up: обновимся до Picodata 25.1
- На \`pico_stop\` убиваем Picodata из \`PICODATA_BINARY_PATH\`, а не просто \`picodata\`
- :construction_worker: можно запускать тесты как на CI, но локально

### 0.4.4 — 2025-01-13 {: #0.4.4 }

**Исправления**

- Lpop and rpop are used to panic

**Прочее**

- Bump version
- Fix lints for rust 1.84
- Reduce unsafe usage
- More benches
- Stress test

**Bench**

- Add benches for hash commands

### 0.4.3 — 2024-12-24 {: #0.4.3 }

**Новая функциональность**

- Implement incrs

### 0.4.1 — 2024-12-18 {: #0.4.1 }

**Новая функциональность**

- Implement writeln_crlf
- Support expire for hash and list

**Исправления**

- :bug: исправим всё-таки 62, надо возвращать в протоколе правильно ошибку
- Declare dummy RUSAGE_THREAD for macos

**Структура кода**

- Melformed -> malformed

**Build**

- Добавим возможность запустить вторую копию

### 0.4.0 — 2024-12-10 {: #0.4.0 }

**Новая функциональность**

- :loud_sound: фильтрация логов для бедных
- Добавил версию Picodata в вывод \`info server\`
- Implements cluster ids commands
- Implement getkeysinslot
- Implement cluster keyslot
- Reduce size of persistence section
- Use migrations for redis db creation
- Implement keyspace info section
- Implement replication info section
- Change Astralinux from Orel to 1.7, 1.8 version added
- Collect memory stat
- Add cmdstat to info
- Add client configuration. support input/output buffer shrinks, preallocate and reuse client output buffer
- Add redis insight support
- Use subkey in bucket calculation
- Implement info stub
- Implement missing list commands
- Implement (b)lmpop
- Implement (b)lmove
- Implement rpush
- Implement lpop and rpop
- Implement basic list commands
- Use borrowed string in a stored type

**Исправления**

- RESP is using CRLF as line ending
- Delete type on del call

**Производительность**

- Мелкая оптимизация при создании хешсета
- Fetch replicasets only for broadcasted commands
- Increase performance for hset command

**Документация**

- :memo: актуализируем документацию

**Внутренние улучшения**

- :recycle: сделали более явным клонирование
- :art: разбил либу инфо на более мелкие и локализованные файлы
- :art: перенес отдельные части \`info\` на уровень модуля этой команды
- :rotating_light: удовлетворил требования нового стабильного раста

**Прочее**

- :bookmark: нарежем 0.3.0 релиз
- Change type error message
- Log improvements
- :construction_worker: используем шаблонный CI
- Add perf results in commands docs

**Build**

- :construction_worker: поправил докерфайлы для установки всегда новой Picodata
- :heavy_plus_sign: переводим плагин на picodata-plugin сдк

### 0.2.0 — 2024-10-04 {: #0.2.0 }

**Новая функциональность**

- Nonblocking gather executor
- Write to pubsub locally if possible
- Implement pubsub commands
- Reuse user connection buffer in command decode

**Исправления**

- :adhesive_bandage: добавил скрипт по умолчанию для Picodata

**Производительность**

- :alembic: add scripts for running performance tests

**Документация**

- :speech_balloon: save supported commands into docs
- :hammer: good enough Readme

**Структура кода**

- :art: добавил символ конца строки в конец моих файлов
- :art: add checks module to make code readable

**Прочее**

- :sparkles: Заливаем артефакты в нексус.
- Init python tests
- Rename redisproto to radix
- Change name of package
- :hammer: set up docker compose for every artifact

### 0.1.1 — 2024-09-13 {: #0.1.1 }

**Новая функциональность**

- Support single node mode

**Исправления**

- Execute set on master
- Replicaset decode
- Connection fibers leak

**Документация**

- :hammer: good enough Readme

**Прочее**

- Add pack for all supported by picodata OS

### 0.1.0 — 2024-09-09 {: #0.1.0 }

**Новая функциональность**

- Support eval command without enabling it
- Use bytes crate to use views in original clients buffer
- Implement scan and hscan command
- Redisproto

**Исправления**

- Decode tests

**Внутренние улучшения**

- Fix rust toolchain to stable
- Clippy warnings

**Прочее**

- Proper layout again, let's hope, it's final version.
- Fix folder layout of artifacts.
- Fix artifacts collection
- Skip everything on main branch, because we use fast-forward only.
- Main should build artifacts always
- Init
- Lints
- Remove useless clusters dir
- Basic Makefile
