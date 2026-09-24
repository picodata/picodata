# Установка {: #install }

## Развёртывание с помощью Ansible {: #radix_deploy_ansible }

При работе с Picodata в промышленной среде удобно использовать [роль
Picodata для Ansible]. С её помощью вы можете развернуть кластер СУБД
Picodata на нескольких узлах, в том числе, с поддержкой плагинов.

Для этого потребуется:

- [установить роль](../../admin/deploy_ansible.md#install_role)
- [подготовить плейбук](../../admin/deploy_ansible.md#create_playbook)
- [подготовить инвентарный
  файл](../../admin/deploy_ansible.md#create_inventory_file), включив в
  него блок параметров для установки Radix
- [установить кластер](../../admin/deploy_ansible.md#install_cluster) с Radix

### Конфигурация Radix для Ansible {: #radix_inventory_config }

Начиная с версии Picodata 26.1, плагин можно настроить, указав его
параметры для тиров (блок `tiers`), которые, в свою очередь, привязаны к
сервису плагина. Добавьте в инвентарный файл блок параметров,
относящийся к Radix:

```yaml

plugins:
  radix:                                                          # плагин
    path: '../files/radix_1.1.1-centos_el8.tar.gz'                # путь до пакета с Radix
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
      unlogged: ''
```

При настройке плагина можно использовать литералы, которые будут подставлены при разворачивании кластера:

- `<INSTANCE_ADDR>` — адрес сервера, на котором будет работать плагин
- `<INSTANCE_NUM>` — номер инстанса на сервере в двухзначном формате (01, 02, 03 ...)
- `<INSTANCE_NUM3>` — номер инстанса на сервере в трехзначном формате (001, 002, 003 ...)

### Пример инвентарного файла {: #inventory_example }

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
            path: '../plugins/radix_1.1.1-1-ubuntu_noble.tar.gz'        # путь до пакета с Radix
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
              unlogged: ''
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

[роль Picodata для Ansible]: ../../admin/deploy_ansible.md#plugin_management

### Установка плагина в кластер {: #run_playbook }

Установите кластер Picodata с Radix, указав инвентарный файл и файл плейбука:

```shell
ansible-playbook -i hosts/cluster.yml playbooks/picodata.yml
```

См. также:

- [Развёртывание кластера через Ansible](../../admin/deploy_ansible.md)

## Развёртывание с помощью Docker {: #radix_deploy_docker }

Доступны два образа:

- `<registry>/radix:1.1.1` — полноценный образ, готовый для
  использования в Kubernetes. Не имеет преднастроек.
- `<registry>/radix:1.1.1-standalone` — образ с одиночным инстансом
  Picodata и предустановленным плагином Radix для быстрого ознакомления.
  Не предназначен для использования в производственной среде.

!!! note "Примечание"
    Доступ к репозиторию с образами Picodata и Radix предоставляется по запросу для клиентов Picodata.

Запуск одиночного образа:

```shell
docker pull <registry>/radix:1.1.1-standalone
docker run --rm -p 7379:7379 -p 4327:4327 -p 5327:5327 <registry>/radix:1.1.1-standalone
```

После старта плагин готов к работе:

```shell
redis-cli -p 7379 ping
```

Открытые порты:

| Порт | Протокол      |
| ---- | ------------- |
| 7379 | Redis (RESP2) |
| 4327 | PostgreSQL    |
| 5327 | HTTP          |

## Развёртывание вручную {: #radix_deploy_manual }

### Порядок действий {: #deploy_manual_steps }

Установка плагина Radix вручную имеет ряд особенностей. Процедура установки включает:

- создание файла конфигурации для инстанса Picodata, где в разделе
  `plugins` будут указаны параметры Radix ([пример](#picodata_config)).
- установку у [тиров][tier], на которые предполагается развернуть
  плагин, 16384 [бакетов]. См. описание [bucket_count] и
  [default_bucket_count], а также [пример](#picodata_config) конфигурации
- запуск инстанса Picodata с поддержкой плагинов (параметр [`--share-dir`])
- распаковку архива Radix в директорию, указанную на предыдущем шаге
- подключение к [административной консоли][admin_console] инстанса
- выполнение SQL-команд для:
    - регистрации плагина
    - привязки его сервиса к [тиру][tier]
    - миграции
    - создания пользователя, который будет использоваться в Radix по умолчанию
    - включения плагина в кластере

Данные шаги более подробно описаны ниже.

[`--share-dir`]: ../../reference/cli.md#run_share_dir
[admin_console]: ../../tutorial/connecting.md#admin_console
[tier]: ../../overview/glossary.md#tier
[бакетов]: ../../overview/glossary.md#bucket
[bucket_count]: ../../reference/config.md#cluster_tier_tier_bucket_count
[default_bucket_count]: ../../reference/config.md#cluster_default_bucket_count

См. также:

- [Установка плагинов](../../architecture/plugins.md#plugin_install)
- [Управление плагинами](../../dev/plugin_mgmt.md)

### Файл конфигурации Picodata для Radix {: #picodata_config }

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

[Подключитесь](../../tutorial/connecting.md) к кластеру и перейдите к следующим шагам.

### Добавление плагина в кластере {: #plugin_add }

Radix поддерживает 16 баз данных, каждую из которых можно расположить на
отдельном тире. На одном тире можно разместить несколько баз данных.
Ниже будут примеры для одного и двух тиров.

Для регистрации плагина в кластере выполните следующую SQL-команду
в административной консоли Picodata:

```sql
CREATE PLUGIN radix 1.1.1;
```

Выполните указанные ниже шаги для того, чтобы включить плагин.

### Добавление сервиса и установка параметров {: #plugin_enable_details }

На данном этапе выполните следующие шаги:

- назначьте сервис плагина существующим тирам
- задайте значения для 16 параметров `migration_context.tier_for_db_N` (по числу баз данных в Radix)
- задайте значение для параметра `unlogged`

**Пример для одного тира (default)**

```sql
ALTER PLUGIN radix 1.1.1 ADD SERVICE radix TO TIER default;
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_0='default';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_1='default';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_2='default';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_3='default';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_4='default';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_5='default';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_6='default';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_7='default';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_8='default';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_9='default';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_10='default';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_11='default';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_12='default';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_13='default';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_14='default';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_15='default';
```

**Пример для двух тиров (hot/cold)**


```sql
ALTER PLUGIN radix 1.1.1 ADD SERVICE radix TO TIER hot;
ALTER PLUGIN radix 1.1.1 ADD SERVICE radix TO TIER cold;
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_0='hot';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_1='hot';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_2='hot';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_3='hot';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_4='cold';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_5='cold';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_6='cold';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_7='cold';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_8='cold';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_9='cold';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_10='cold';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_11='cold';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_12='cold';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_13='cold';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_14='cold';
ALTER PLUGIN radix 1.1.1 SET migration_context.tier_for_db_15='cold';
```

Параметр `unlogged` может принимать только два значения: `''` (пустая строка) для включённого WAL (режим Radix до версии 1.0.0):

```sql
ALTER PLUGIN radix 1.1.1 SET migration_context.unlogged='' OPTION(TIMEOUT=1200);
```

или `UNLOGGED` для отключённого:

```sql
ALTER PLUGIN radix 1.1.1 SET migration_context.unlogged='UNLOGGED' OPTION(TIMEOUT=1200);
```

!!! warning "Внимание!"
    При установке значения `UNLOGGED` данные на диск не пишутся, и
    сохраняются только в ОЗУ на [лидере репликасета]. Это означает,
    что перезапуск лидера приведёт к потере данных, которые на нём
    хранились. См. [подробнее](../../reference/sql/create_table.md#params) о
    режиме `UNLOGGED` при создании таблицы.

[лидере репликасета]: ../../overview/glossary.md#leader

### Запуск миграции {: #plugin_migrate }

Для выполнения миграции выполните команду:

```sql
ALTER PLUGIN radix MIGRATE TO 1.1.1 OPTION(TIMEOUT=300);
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

### Включение плагина {: #plugin_enable }

Перед включением плагина убедитесь, что в кластере создан пользователь,
указанный в параметре `default_user_name` секции
[authorization_mode](radix_settings.md#auth_mode) файла конфигурации плагина (по умолчанию это
`default`). При необходимости создайте этого пользователя в Picodata:

```sql
CREATE USER default WITH PASSWORD 'замените на ваш пароль' USING md5;
```

Данному пользователю также следует выдать роли `radix_reader` и
`radix_writer` (появляются в Picodata после выполнения
[миграций](#plugin_migrate) Radix) для работы с объектами БД:

```sql
GRANT radix_reader TO default;
GRANT radix_writer TO default;
```

После этого включите плагин:

```sql
ALTER PLUGIN radix 1.1.1 ENABLE OPTION(TIMEOUT=30);
```

Чтобы убедиться в том, что плагин успешно добавлен и запущен, выполните запрос:

```sql
SELECT * FROM _pico_plugin;
```

В строке, соответствующей плагину Radix, в колонке `enabled` должно быть
значение `true`.
