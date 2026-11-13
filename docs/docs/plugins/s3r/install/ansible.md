# Установка ролью Ansible

Страница описывает, что добавить в инвентарный файл роли Ansible для Picodata,
чтобы она установила S3R. Она нужна администратору, который разворачивает
кластер этой ролью. Роль выполняет те же действия, что описаны на странице
[Пошаговая установка](manual.md): распаковывает архив плагина на [узлы](../reference/glossary.md#node),
выполняет команды SQL [шага 5](manual.md#plugin_enable), применяет файл
настроек плагина и включает плагин.

## Перед началом {: #before_you_begin }

- Узлы соответствуют [требованиям](requirements.md#requirements), топология
  выбрана по странице [Планирование топологии](planning.md).
- Файлы сертификатов TLS находятся на узлах до установки плагина.
- Пользователь Picodata, которому выдаётся ключ доступа S3, создаётся до
  применения настроек плагина (см. [шаг 3](#plugin_config_file)).

## Шаг 1. Блок `plugins` {: #plugins_block }

Добавьте в инвентарный файл блок `plugins`:

```yaml
plugins:
  s3r:
    path: '../files/s3r_2.2.0-ubuntu_22.04.tar.gz'
    config: '../files/s3r-config.yml'
    services:
      s3r_storage:
        tiers:
          - default:
              listener:
                enabled: true
                listen: "0.0.0.0:87<INSTANCE_NUM>"
                advertise: "<INSTANCE_ADDR>:87<INSTANCE_NUM>"
      s3r_meta:
        tiers:
          - default
      s3r_api:
        tiers:
          - default:
              listener:
                enabled: true
                listen: "0.0.0.0:84<INSTANCE_NUM>"
    migration_context:
      storage_tier: 'default'
      extent_size: '1GiB'
      engine: 'memtx'
      unlogged: ''
```

| Ключ | Что задаёт |
|---|---|
| `path` | архив плагина |
| `config` | файл настроек плагина (шаг 3) |
| `services` | [тиры](../../../overview/glossary.md#tier) Picodata и [слушатели](../reference/glossary.md#listener) сервисов |
| `migration_context` | контекст миграции, как на [шаге 5](manual.md#plugin_enable) ручной установки |

Литералы `<INSTANCE_ADDR>` и `<INSTANCE_NUM>` роль заменяет адресом узла и
двузначным номером [инстанса](../reference/glossary.md#instance) на узле. Разделы `tls` слушателей задаются так же,
как на [шаге 3](manual.md#configure_instances) ручной установки.

## Шаг 2. Диски и пул рабочих потоков {: #disks_and_threads }

Задайте диски и размер пула рабочих потоков переменными окружения в параметре
`extra_vars` тира:

```yaml
tiers:
  default:
    extra_vars:
      S3R_DISKS: '/data/s3r/i<INSTANCE_NUM>d0:hba=a,/data/s3r/i<INSTANCE_NUM>d1:hba=b'
      S3R_WORKER_THREADS: '8'
```

В каждую директорию примера смонтирована отдельная файловая система, поэтому
ёмкость дисков не задаётся (см.
[Ёмкость диска](../concepts/disks.md#disk_capacity)). Правила для списка дисков
приведены в разделе [Список дисков](manual.md#disk_list).

## Шаг 3. Файл настроек плагина {: #plugin_config_file }

Создайте файл настроек плагина `s3r-config.yml`. Он содержит параметры
сервисов, в том числе
[схему избыточности](../reference/glossary.md#erasure_shape) каждого
[яруса](../reference/glossary.md#storage_tier) с дисками:

```yaml
s3r_api:
  credentials:
    - access_key: AKIAEXAMPLE000000001
      secret_key: "..."
      pico_user: alice
s3r_storage:
  erasure:
    - k: 2
      m: 2
```

!!! warning "Пользователь из `pico_user` создаётся заранее"
    Пользователь, указанный в `pico_user`, должен существовать до применения
    настроек. Его создание выполняется скриптом, заданным параметром роли
    `sql_file_pre_plugins`.

## Шаг 4. Домены отказа и параметры тира {: #tier_settings }

[Домены отказа инстансов](../reference/glossary.md#instance_failure_domain) роль задаёт по группам серверов инвентаря. Они должны
соответствовать правилам страницы
[Планирование топологии](planning.md#site_rules):

- компонент `dc` только при нескольких [площадках](../reference/glossary.md#site);
- при одной площадке с несколькими инстансами домен, различающий узлы.

Для тира, на котором работает плагин, рекомендуется установить следующие
параметры:

| Параметр | Что даёт |
|---|---|
| `replication_mode: sync` | Синхронная репликация внутри [репликасета](../reference/glossary.md#replicaset): запись подтверждается только после того, как её принял кворум реплик |
| `wal_mode: fsync` | Запись журнала с синхронизацией. Задаётся вместе с синхронной репликацией. Picodata синхронизирует записи журнала [Raft](../../../overview/glossary.md#raft) на диске независимо от этого параметра, но он защищает данные репликасета. При асинхронной репликации параметр пользы не даёт, потому что запись на реплику всё равно выполняется асинхронно |

### Тир арбитров {: #arbiter_tier }

!!! note "Примечание"
    Отдельный тир [арбитров](../reference/glossary.md#arbiter) для S3R, как правило, не нужен и только добавляет
    административную нагрузку. Он оправдан, когда данные размещаются в двух
    датацентрах, а в третьем, например во внешнем облаке, можно разместить
    инстанс арбитра: тогда кластер переживёт отказ любого одного датацентра.

## Шаг 5. Проверка после работы роли {: #post_install_checks }

После работы роли выполните проверки ручной установки. Адреса в них берите из своего инвентарного
файла: в примере ниже эндпоинт S3 инстанса слушает порт `84<номер инстанса>`, а обмен [фрагментами](../reference/glossary.md#chunk) —
порт `87<номер инстанса>`, тогда как в примерах ручной установки это 8443 и 8700.

1. [Шаг 6. Проверка готовности](manual.md#endpoint_check).
2. [Шаг 7. Проверка дисков и групп размещения](manual.md#disks_check).
3. [Шаг 10. Проверка работы](manual.md#checkup).

Если [начальное формирование](../reference/glossary.md#formation) не завершается, причины разбираются по разделу
[Если установка не завершается](troubleshooting.md#install_troubleshooting).

## Пример инвентарного файла {: #inventory_example }

Инвентарный файл кластера из трёх серверов с плагином S3R:

!!! warning "Схема избыточности должна соответствовать числу доменов отказа дисков"
    В примере три сервера, на каждом два инстанса с одним диском без [поддомена](../reference/glossary.md#sub_domain): [доменов отказа](../reference/glossary.md#failure_domain)
    дисков три. Схема в файле `s3r-config.yml` должна в них укладываться: `k + m` не больше 3.
    Схема 2+2 из примера ручной установки для такого инвентаря не подходит: начальное
    формирование не завершится с причиной `not_enough_domains`.

```yaml
all:
  vars:
    ansible_user: root

    repo: 'https://download.picodata.io'  # репозиторий пакетов Picodata
    cluster_name: 'demo'
    admin_password: '<пароль администратора>'

    audit: false
    log_level: 'info'
    log_to: 'file'

    conf_dir: '/etc/picodata'
    data_dir: '/var/lib/picodata'
    run_dir: '/var/run/picodata'
    log_dir: '/var/log/picodata'
    share_dir: '/usr/share/picodata'  # директория плагинов

    listen_address: '{{ ansible_fqdn }}'
    first_bin_port: 13301
    first_http_port: 18001
    first_pg_port: 15001

    sql_file_pre_plugins: files/sql_start.sql   # создание пользователей до установки плагина
    sql_file_post_plugins: files/sql_finish.sql # действия после установки плагина

    tiers:
      default:
        replicaset_count: 2
        replication_factor: 3
        can_vote: true
        replication_mode: sync
        wal_mode: fsync
        extra_vars:
          S3R_API_LISTEN: '0.0.0.0:84<INSTANCE_NUM>'
          S3R_DISKS: '/data/s3r/i<INSTANCE_NUM>'
          S3R_WORKER_THREADS: '8'
        config:
          memtx:
            memory: 16G         # по разделу «Расчёт ресурсов»

    plugins:
      s3r:
        path: '../files/s3r_2.2.0-ubuntu_22.04.tar.gz'
        config: '../files/s3r-config.yml'  # со схемой избыточности яруса
        services:
          s3r_storage:
            tiers:
              - default:
                  listener:
                    enabled: true
                    listen: "0.0.0.0:87<INSTANCE_NUM>"
                    advertise: "<INSTANCE_ADDR>:87<INSTANCE_NUM>"
          s3r_meta:
            tiers:
              - default
          s3r_api:
            tiers:
              - default
        migration_context:
          storage_tier: default
          extent_size: 1GiB
          engine: memtx
          unlogged: ""

GROUP1:                       # группа серверов задаёт домен отказа
  hosts:
    server-1-1:
      ansible_host: 'node1.example.net'
      host_group: 'STORAGES'

GROUP2:
  hosts:
    server-2-1:
      ansible_host: 'node2.example.net'
      host_group: 'STORAGES'

GROUP3:
  hosts:
    server-3-1:
      ansible_host: 'node3.example.net'
      host_group: 'STORAGES'
```

Пояснения к примеру:

| Параметр | Пояснение |
|---|---|
| `S3R_API_LISTEN` | задаёт адрес эндпоинта S3. Раздел `listener` сервиса `s3r_api` задаёт тот же адрес и, кроме того, позволяет включить TLS; без TLS оба способа дают одинаковый результат |
| `S3R_DISKS` | путь включает номер инстанса, потому что на одном сервере работает несколько инстансов, а одна и та же директория диска не может принадлежать двум инстансам |
| `memtx.memory` | рассчитывается по разделу [Расчёт ресурсов](requirements.md#resources_allocation) |

Если в директорию инстанса не смонтирована отдельная файловая система, у диска
задаётся ёмкость, например `/data/s3r/i<INSTANCE_NUM>:1TiB` (см.
[Ёмкость диска](../concepts/disks.md#disk_capacity)).
