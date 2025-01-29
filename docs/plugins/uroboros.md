
# Uroboros

В данном разделе приведены сведения о
[Uroboros](https://git.picodata.io/picodata/plugin/uroboros), плагине для
СУБД Picodata.

!!! tip "Picodata Enterprise"
    Функциональность плагина доступна только в коммерческой версии Picodata.

## Общие сведения {: #intro }

Плагин Uroboros используется для однонаправленной асинхронной репликации
данных между двумя кластерами, созданных при помощи фреймворка Vshard.

Основные задачи Uroboros:

- перенос кластера на новую площадку без простоя
- обеспечение отказоустойчивости путем репликации одного кластера в другой

<!--
## Сборка и подготовка файлов плагина {: #local_build }

Склонируйте репозиторий с исходным кодом Uroboros:

```bash
git clone https://git.picodata.io/picodata/plugin/uroboros.git
```

Соберите разделяемые библиотеки Uroboros (требуется актуальные версии Rust, Cargo, GNU Make):

```bash
cd uroboros
make build
```

Результатом сборки будут следующие библиотеки в директории `target/debug`:

- liburoboros_config.so
- liburoboros.so
- liburoboros_utils.so

-->

## Состав плагина {: #plugin_files }

<!--
Загрузите плагин для нужной операционной системы по адресу:

[https://git.picodata.io/picodata/plugin/argus/-/releases/permalink/latest](https://git.picodata.io/picodata/plugin/argus/-/releases/permalink/latest)
 -->

Внутри архива с плагином находится структура вложенных директорий,
включающая имя и версию плагина, а также его файлы:

```
└── argus
    └── 1.0.0
        ├── liburoboros.so
        ├── manifest.yaml
        └── migrations
```

Основная логика плагина обеспечивается разделяемой библиотекой
`liburoboros.so`. Исходная конфигурация плагина задается в файле манифеста
(`manifest.yaml`). Директория `migrations` зарезервирована для файлов
[миграций].

[миграций]: ../overview/glossary.md#migration

<!--  ## Проверка плагина вручную {: #manual_test }

Скопируйте библиотеки Uruboros, файл манифеста и файлы миграций в директорию
плагина. Выполните следующий набор команд из основной директории
репозитория:

```bash
mkdir -p plugins/uroboros
cp ./target/debug/liburoboros*.so ./plugins/uroboros/<версия>/
cp manifest.yml ./plugins/uroboros/<версия>/manifest.yaml
cp -r migrations/* plugins/uroboros/<версия>
```

Запустите плагин согласно руководству [Управление плагинами](../tutorial/plugins.md)

## Проверка в локальном тестовом окружении {: #docker_test }

Локальное тестовое окружение использует Docker-образ с ОС Rocky Linux 8
и предоставляет два кластера (источник и назначение) для проверки работы
плагина Uroboros. Файлы тестового окружения находятся в директории
`local-dev-clusters`.

Для запуска локального тестового окружения требуется запущенная
системная служба Docker. Выполните следующий набор команд из основной
директории репозитория:

```bash title="Сборка образа для поднятия тестовых кластеров"
make build_docker_cluster
```

```bash title="Запуск образов"
make run_docker_clusters
```

Перед запуском тестов убедитесь, что в системе установлены Python-модули
`pytest` и `tarantool`. После этого выполните команду:

```bash title="Запуск интеграционных тестов"
TEST_RUN_CLUSTERS=docker make test
```
-->

## Подключение плагина {: #plugin_enable }

Содержимое архива с плагином следует распаковать в любую удобную
директорию, которую после этого нужно будет указать как `PLUGIN_DIR` для
инстанса Picodata.

При запуске одного инстанса из [командной строки] директорию плагина
можно указать с помощью параметра:

```bash
picodata run --plugin-dir=<PLUGIN-DIR> ...
```

Однако, для полноценной использования плагина рекомендуется запустить кластер с помощью [роли Ansible].

[командной строки]: ../reference/cli.md
[роли Ansible]: ../tutorial/deploy_ansible.md

После запуска Picodata с поддержкой плагинов в заданной директории подключитесь к [административной
консоли] инстанса Picodata.

Установите плагин, добавьте его к тиру и включите его с помощью
следующих SQL-команд:

```sql
CREATE PLUGIN uroboros 1.0.0;
ALTER PLUGIN uroboros 1.0.0 ADD SERVICE argus TO TIER default;
ALTER PLUGIN uroboros 1.0.0 ENABLE;
```

[административной консоли]: ../tutorial/connecting.md#admin_console

## Проверка с помощью Ansible {: #ansible_test }

### Подготовка {: #preparation }

1. Изучите [документацию по развертыванию кластера пикодаты](https://docs.picodata.io/picodata/24.6/tutorial/deploy_ansible/). Выполнить инструкции по установке роли.
2. Скачайте нужную версию плагина `uroboros` и положить пакет в рабочий каталог.
3. Проверьте наличие конфигурационного файла для плагина `uroboros-config.yml`, проверьте настройки в нем (см. ниже).

!!! note "Примечание"
    На сервере, с которого будет происходить установка,
    необходим Ansible и доступ на серверы кластера с повышением привилегий.


### Установка окружения {: #setting_env }

Создайте файл с описанием кластера согласно [руководству по
развертыванию кластера](../tutorial/deploy_ansible.md). Например,
`uroboros.yml`.

```yaml
---
all:
  vars:
    user: username # имя пользователя, под которым будут запущены процессы picodata
    group: groupname # группа пользователя, под которой будут запущены процессы picodata
    password: "<password>"
    cluster_name: uroboros
    audit: false
    log_level: warn
    log_to: file

    conf_dir: "/opt/picodata/etc"
    data_dir: "/opt/picodata/data"
    run_dir: "/var/run/picodata"
    log_dir: "/opt/picodata/logs"

    fd_uniq_per_instance: true

    purge: true # при очистке кластера удалять в том числе все данные и логи с сервера

    listen_ip: "{{ ansible_default_ipv4.address }}" # ip-адрес, который будет слушать инстанс, по умолчанию ansible_default_ipv4.address

    first_bin_port: 13301 # начальный бинарный порт для первого инстанса (он же main_peer)
    first_http_port: 18001
    first_pg_port: 15001

    init_system: "supervisord"
    rootless: true

    plugins:
      uroboros:
        path: "uroboros_0.3.0.tar.gz"
        tiers:
          - default
        config: "uroboros-config.yml"
    tiers:
      default:
        instances_per_server: 5
        replication_factor: 15
        config:
          memtx:
            memory: 1G
          iproto:
            max_concurrent_messages: 1500
    admin_password: "<password>"
    property:
      auto_offline_timeout: 30
DC1: # Датацентр (failure_domain)
  hosts:
    hostname1:
      ansible_host: ip1
    hostname2:
      ansible_host: ip2
    hostname3:
      ansible_host: ip3
```

Создайте файл с конфигурацией. Пример:

```yaml
uroboros:
  producer: # настройки кластера-источника
    space_info_url: "http://<source_address>/uroboros/api/v1/space"
    user_url: "http://<source_address>/uroboros/api/v1/user"
    topology_url: "http://<source_address>/uroboros/api/v1/topology"
  consumer: # настройки приемника
    type: "tarantool" # может быть tarantool, или, в будущем kafka
    attributes:
      space_info_url: "http://<destination_address>/uroboros/api/v1/space"
      user_url: "http://<destination_address>/uroboros/api/v1/user"
      topology_url: "http://<destination_address>/uroboros/api/v1/topology"
  enabled_groups: # vshard-группы, которые следует реплицировать
    - index
    - kafka
    - storage
  disabled_spaces: # спейсы из указанных выше vshard-group, которые реплицировать НЕ следует
    - notify_storage_vinyl
    - distributed_index_queue_vinyl
    - distributed_index_queue_memtx
    - _repair_queue_v2
    - _tmp_event_storage
    - notify_storage_memtx
    - event_storage
  buckets_per_writer: 300 # степень параллелизации обработки. Не стоит изменять без консультации с разработчиками.
  reconnect_delay: 10 # задержка перед восстановлением коннекта к источнику
```

Подготовьте плейбук `picodata.yml`:

```yaml
---
- name: Deploy picodata cluster
  hosts: all
  become: true

  tasks:
    - name: Import picodata-ansible role
      ansible.builtin.import_role:
        name: picodata-ansible
```

В результате в рабочем каталоге должно быть 4 файла:

- uroboros.yml
- picodata.yml
- uroboros_config.yml
- uroboros_xxxxx.tar.gz

Запустите раскатку Uroboros:

```bash
ansible-playbook -i uroboros.yml picodata.yml
```
