
# Ouroboros

В данном разделе приведены сведения о
[Ouroboros](https://git.picodata.io/picodata/plugin/ouroboros), плагине для
СУБД Picodata.

!!! tip "Picodata Enterprise"
    Функциональность плагина доступна только в коммерческой версии Picodata.

## Общие сведения {: #intro }

Плагин Ouroboros используется для однонаправленной асинхронной логической
[репликации](../overview/glossary.md#replication) данных между двумя
кластерами Picodata.

Основные задачи Ouroboros:

- перенос кластера на новую площадку без простоя
- обеспечение отказоустойчивости путем репликации одного кластера в другой

<!--
## Ограничения {: #limitations }

1. Источником может быть только кластер Tarantool Cartridge, где
   развернуты [роли-сайдкары], включенные в поставку.
2. Роли надо развернуть на всех Vshard-группах, которые планируется
   реплицировать.
3. Источник должен быть запущен на форке Tarantool от компании Picodata,
   для которого должно быть включено расширение для
   [WAL](../overview/glossary.md#wal):

```lua
local ok, err = cartridge.cfg({}, {
    wal_ext = { new_old = true },
})
```

[роли-сайдкары]: https://git.picodata.io/picodata/plugin/uroboros-sidecars/cartridge-sidecar
-->

## Принцип работы {: #details }

Для использования Ouroboros требуются два кластера:
кластер источник и кластер-приемник. Далее необходимо [развернуть кластер]
Picodata с включенным плагином Ouroboros. В этом кластере может быть как
один инстанс, так и несколько — число инстансов определяет то, сколько
исполнителей (процессов копирования) будет выполняться одновременно.

При условии [корректной настройки](#config) подключения к кластерам Ouroboros
будет переносить данные из кластера-источника в кластер-приемник с помощью
логической репликации (копирования журналов изменений). Этот способ
предоставляет необходимую гибкость (позволяет работать с отдельными таблицами) и
учитывает семантику данных.

[развернуть кластер]: ../tutorial/deploy.md
[файберов]: ../overview/glossary.md#fiber

Схема работы плагина показана ниже:

![Ouroboros replication](../images/uroboros_replication.svg)

Плагин учитывает топологию кластеров, с которыми он работает. Каждый
исполнитель плагина Ouroboros реплицирует один или более репликасетов из
кластера-источника. На исполнителе создается набор [файберов], каждый
из которых обрабатывает свою связку <репликасет источник + диапазон
бакетов>. Таким образом, получается паралелльная отправка в
кластер-приемник записей из различных бакетов. При этом сохраняется
упорядоченность записей и изменений в рамках каждого бакета (не
возникает "состояния гонки").

## Состав плагина {: #plugin_files }

Внутри архива с плагином находится структура вложенных директорий,
включающая имя и версию плагина, а также его файлы:

```
└── ouroboros
    └── 1.0.0
        ├── liburoboros.so
        ├── manifest.yaml
        └── migrations
            ├── 0001_state.db
            └── 0002_ouroboros_state.db
```

Основная логика плагина обеспечивается разделяемой библиотекой
`libouroboros.so`. Исходная [конфигурация](#config) плагина задается в файле манифеста
(`manifest.yaml`). Директория `migrations` зарезервирована для файлов
[миграций].

[миграций]: ../overview/glossary.md#migration

## Конфигурация плагина {: #config }

Основные параметры плагина включают в себя:

- имя отдельного пользователя (должно совпадать на кластере-источнике и
  кластере-приемнике), под которым Ouroboros будет подключаться
- группа параметров для подключения к кластеру-источнику (producer) и
  кластеру-приемнику (consumer)
- группа параметров по настройке репликации (группы шардирования, исключаемые
  спейсы (таблицы), настройки параллелизма, переподключения и т.д.)

Исходная конфигурация плагина определяется файлом-манифестом:

??? example "manifest.yaml"
    ```yaml
    description: Plugin tnt clusters replication
    name: ouroboros
    version: 0.4.2
    services:
      - name: ouroboros
        description: ouroboros descr
        default_configuration:
          password: password
          producer:
            user_url: http://localhost:9001/uroboros/api/v1/user
            topology_url: http://localhost:9001/uroboros/api/v1/topology
            space_info_url: http://localhost:9001/uroboros/api/v1/space
          consumer:
            type: tarantool
            attributes:
              space_info_url: http://localhost:9002/uroboros/api/v1/space
              user_url: http://localhost:9002/uroboros/api/v1/user
              topology_url: http://localhost:9002/uroboros/api/v1/topology
          enabled_groups: ["default"]
          disabled_spaces: []
          buckets_per_writer: 1000
          reconnect_delay: 10
    migration:
      - migrations/0001_state.db
      - migrations/0002_uroboros_state.db
    ```

Пользовательская конфигурация плагина определяется отдельным
конфигурационным файлом для сервиса плагина:


```yaml
default_configuration:
  producer: # настройки кластера-источника
    space_info_url: "http://<source_address>/uroboros/api/v1/space"
    user_url: "http://<source_address>/uroboros/api/v1/user"
    topology_url: "http://<source_address>/uroboros/api/v1/topology"
  consumer: # настройки кластера-приемника
    type: "tarantool" # может быть Tarantool, или, в будущем — Kafka или Picodata
    attributes:
      space_info_url: "http://<destination_address>/uroboros/api/v1/space"
      user_url: "http://<destination_address>/uroboros/api/v1/user"
      topology_url: "http://<destination_address>/uroboros/api/v1/topology"
  enabled_groups: # группы шардирования, которые следует реплицировать
    - storage
    - group_1
    - group_2
  disabled_spaces: # таблицы из указанных выше групп шардирования, которые реплицировать НЕ следует
    - ignored_space_1
    - ignored_space_2
  buckets_per_writer: 1000 # степень параллелизации обработки. Не стоит изменять без консультации с разработчиками.
  reconnect_delay: 10 # задержка перед восстановлением коннекта к источнику
```

Для изменения настроек уже запущенного плагина используйте SQL-команду `ALTER PLUGIN ...`

См. также:

- [Конфигурация плагинов](../architecture/plugins.md#plugin_config)

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
[роли Ansible]: ../admin/deploy_ansible.md

После запуска Picodata с поддержкой плагинов в заданной директории подключитесь к [административной
консоли] инстанса Picodata.

Установите плагин, добавьте его к тиру и включите его с помощью
следующих SQL-команд:

```sql
CREATE PLUGIN ouroboros 0.4.1;
ALTER PLUGIN ouroboros MIGRATE TO 0.4.1;
ALTER PLUGIN ouroboros 0.4.1 ADD SERVICE ouroboros TO TIER default;
ALTER PLUGIN ouroboros 0.4.1 ENABLE;
```

[административной консоли]: ../tutorial/connecting.md#admin_console

## Проверка с помощью Ansible {: #ansible_test }

### Подготовка {: #preparation }

1. Изучите [документацию по развертыванию кластера Picodata](https://docs.picodata.io/picodata/stable/tutorial/deploy_ansible/). Выполнить инструкции по установке роли.
2. Скачайте нужную версию плагина `ouroboros` и положите пакет в рабочую директорию.
3. Проверьте наличие конфигурационного файла для плагина `ouroboros-config.yml`, проверьте настройки в нем (см. ниже).

!!! note "Примечание"
    На сервере, с которого будет происходить установка,
    необходим Ansible и доступ на серверы кластера с повышением привилегий.

### Установка окружения {: #setting_env }

Создайте файл с описанием кластера согласно [руководству по
развертыванию кластера](../admin/deploy_ansible.md). Например,
`ouroboros.yml`.

```yaml
---
all:
  vars:
    user: username # имя пользователя, под которым будут запущены процессы picodata
    group: groupname # группа пользователя, под которой будут запущены процессы picodata
    password: "<password>"
    cluster_name: ouroboros
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
      ouroboros:
        path: "ouroboros_0.3.0.tar.gz"
        tiers:
          - default
        config: "ouroboros-config.yml"
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

Создайте файл с [конфигурацией](#config).

Подготовьте плейбук `picodata.yml`:

```yaml
---
- name: Deploy Picodata cluster
  hosts: all
  become: true

  tasks:
    - name: Import picodata-ansible role
      ansible.builtin.import_role:
        name: picodata-ansible
```

В результате в рабочем каталоге должно быть 4 файла:

- ouroboros.yml
- picodata.yml
- ouroboros_config.yml
- ouroboros_xxxxx.tar.gz

Запустите раскатку Ouroboros:

```bash
ansible-playbook -i ouroboros.yml picodata.yml
```

## Мониторинг показателей плагина {: #grafana_board}

Процесс работы плагина Ouroboros можно удобно отслеживать в графическом
интерфейсе [Grafana], для которого Picodata поставляет файл
конфигурации (dashboard).

Для настройки мониторинга импортируйте dashboard с данными Ouroboros. Для
этого понадобится файл [dashboard.grafana.json], который следует
добавить в меню `Dashboards` > `New` > `Import`:

![Import Ouroboros dashboard](../images/grafana/import_dashboard.png)

Панель dashboard для плагина Ouroboros отображает ряд графиков, на
которых отражена пропускная способность кластера с Ouroboros, статистика
по скопированным пакетам, а также данные по количеству записей в
кластере-источнике и кластере-приемнике.

Внешний вид dashboard для Ouroboros показан ниже:

![Ouroboros dashboard](../images/grafana/uroboros.png)

Для настройки dashboard используйте параметры в верхней части экрана:

- **Источник данных** — по умолчанию `Prometheus`
- **Кластер уробороса** — имя кластера с Ouroboros
- **Нода уробороса** — данные с каких узлов кластера с Ouroboros следует отображать (по умолчанию `All`, т.е. со всех узлов)
- **Кластер источник** — имя кластера, с которого Ouroboros будет забирать данные (значение по умолчанию — `pustoe`)
- **Кластер-приемник** — имя кластера, в который Ouroboros будет записывать данные (значение по умолчанию — `porognee`)
- **Игнорируемые спейсы** — список спейсов, данные которых не будут скопированы

[Grafana]: ../admin/monitoring.md#grafana
[dashboard.grafana.json]: https://git.picodata.io/picodata/plugin/uroboros/-/raw/master/observability/dashboard.grafana.json

См. также:

- [Управление плагинами](../tutorial/plugins.md)

<!--
## Сборка и подготовка файлов плагина {: #local_build }

Склонируйте репозиторий с исходным кодом Ouroboros:

```bash
git clone https://git.picodata.io/picodata/plugin/uroboros.git
```

Соберите разделяемые библиотеки Ouroboros (требуется актуальные версии Rust, Cargo, GNU Make):

```bash
cd ouroboros
make build
```

Результатом сборки будут следующие библиотеки в директории `target/debug`:

- liburoboros_config.so
- liburoboros.so
- liburoboros_utils.so

-->

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
плагина Ouroboros. Файлы тестового окружения находятся в директории
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
