# Argus

В данном разделе приведены сведения о
[Argus](https://git.picodata.io/picodata/plugin/argus), плагине для СУБД
Picodata.

!!! tip "Picodata Enterprise"
    Функциональность плагина доступна только в
    коммерческой версии Picodata.

## Общие сведения {: #intro }

Плагин Argus используется для синхронизации учётных данных между
сервером LDAP/LDAPS и Picodata. Синхронизация происходит однонаправленно и
позволяет импортировать данные пользователей и групп с сервера LDAP в
Picodata, для которой перед этим была настроена авторизация с помощью
[LDAP/LDAPS](../admin/ldap.md).

Начиная с версии 3.0.0, синхронизацию в кластере выполняет только один
инстанс — текущий [raft-лидер]. На остальных инстансах, где установлен
[сервис] плагина, он находится в спящем режиме: не подключается к серверу
LDAP и не выполняет синхронизацию. При смене raft-лидера прежний лидер
останавливает синхронизацию, а новый — её запускает. Во время сбоев в кластере
синхронизацию могут кратковременно выполнять несколько инстансов одновременно;
в штатном состоянии кластера она всегда выполняется на одном инстансе.

!!! warning "Внимание!"
    В версиях Argus до 3.0.0 синхронизация выполнялась на мастер-репликах
    всех репликасетов, в которых был установлен сервис плагина.

[raft-лидер]: ../overview/glossary.md#raft_leader

## Соответствие версий Picodata и Argus {: #picodata_argus_versions }

Для разных версий плагина Argus требуются определённые версии Picodata.
Ниже приведена таблица совместимости:

| Argus | Picodata        |  ФСТЭК-сертификат  |
|-------|-----------------|:------------------:|
| 2.1.2 | 25.2.2          | :white_check_mark: |
| 2.2.5 | 25.5.7 (25.5.*) |                    |
| 2.3.1 | 26.1.*          | :white_check_mark: |
| 3.0.0 | 26.2.*          |                    |

См. также:

- [Страница загрузки Picodata](https://picodata.io/download/)

## Состав плагина {: #plugin_files }

<!--
Загрузите плагин для нужной операционной системы по адресу:

[https://git.picodata.io/picodata/plugin/argus/-/releases/permalink/latest](https://git.picodata.io/picodata/plugin/argus/-/releases/permalink/latest)
 -->

Внутри архива с плагином находится структура вложенных директорий,
включающая имя и версию плагина, а также его файлы:

```
└── argus
    └── 3.0.0
        ├── libargus.so
        └── manifest.yaml
```

Основная логика плагина обеспечивается разделяемой библиотекой
`libargus.so`. Исходная конфигурация плагина задаётся в файле манифеста
(`manifest.yaml`).

## Предварительные настройки в Picodata {: #configure_picodata }

Перед использованием плагина Argus убедитесь, что на стороне Picodata:

- созданы [роли] (соответствующие группам пользователей в каталоге
  LDAP), и ролям выданы нужные [привилегии]
- настроено [подключение] к серверу LDAP

[роли]: ../admin/access_control.md#roles
[привилегии]: ../admin/access_control.md#privileges
[подключение]: ../admin/ldap.md

## Подключение плагина {: #plugin_enable }

Содержимое архива с плагином следует распаковать в любую удобную
директорию, которую после этого нужно будет указать как `SHARE_DIR` для
инстанса Picodata.

При запуске одного инстанса из [командной строки] директорию плагина
можно указать с помощью параметра:

```shell
picodata run --share-dir=<SHARE-DIR> ...
```

Однако для полноценного использования плагина рекомендуется запускать кластер с помощью [роли Ansible].

[командной строки]: ../reference/cli.md
[роли Ansible]: ../admin/deploy_ansible.md

После запуска Picodata с поддержкой плагинов в заданной директории подключитесь к [административной
консоли] инстанса Picodata.

Установите плагин, добавьте его [сервис] в [тир] и затем включите плагин с помощью
следующих SQL-команд:

```sql
CREATE PLUGIN argus 3.0.0;
ALTER PLUGIN argus 3.0.0 ADD SERVICE argus TO TIER default;
ALTER PLUGIN argus 3.0.0 ENABLE;
```

Сервис плагина следует добавлять только в тиры, инстансы которых
участвуют в голосовании на выборах raft-лидера, то есть в тиры с
параметром [`can_vote`], равным `true`. Если в кластере используется
отдельный тир-арбитр, рекомендуется добавлять сервис в него.

!!! warning "Внимание!"
    При запуске сервис плагина проверяет значение `can_vote` своего тира
    в системной таблице [`_pico_tier`]. Если сервис добавлен в тир с
    `can_vote: false`, его запуск завершится ошибкой и включение плагина
    будет отменено во всём кластере.

!!! note "Примечание"
    После запуска плагин Argus выполняет синхронизацию только на
    raft-лидере кластера. На остальных инстансах сервис плагина
    находится в спящем режиме.

[`can_vote`]: ../reference/config.md#cluster_tier_tier_can_vote
[`_pico_tier`]: ../architecture/system_tables.md#_pico_tier
[тир]: ../overview/glossary.md#tier
[сервис]: ../dev/plugin_create.md#service

Для диагностики работы плагина обратитесь к [отладочному журналу] инстанса Picodata.

[отладочному журналу]: ../admin/local_monitoring.md#reading_log
[административной консоли]: ../tutorial/connecting.md#admin_console

## Проверка с помощью Ansible {: #ansible_test }

### Подготовка {: #preparation }

1. Изучите [документацию по развёртыванию кластера Picodata](../admin/deploy_ansible.md). Выполните инструкции по установке роли.
2. Скачайте нужную версию плагина `argus` и положите пакет в рабочую директорию.
3. Проверьте наличие конфигурационного файла для плагина `argus_config.yml` и [настройки](#setting_env) в нём.

!!! note "Примечание"
    На сервере, с которого будет происходить установка,
    необходим Ansible и доступ на серверы кластера с повышением привилегий.

### Установка окружения {: #setting_env }

Создайте файл с описанием кластера согласно [руководству по
развёртыванию кластера](../admin/deploy_ansible.md).
Ниже приведён пример для четырёх серверов, расположенных в трёх группах (DC1,
DC2 и DC3). Группа — отдельный [домен отказа].

[домен отказа]: ../overview/glossary.md#failure_domain

```yaml title="argus.yml"
all:
  vars:
    ansible_user: vagrant      # пользователь для SSH-доступа к серверам

    repo: 'https://download.picodata.io'  # репозиторий, откуда инсталлировать пакет picodata

    cluster_name: 'demo'           # имя кластера
    admin_password: '123asdZXV'    # пароль пользователя admin

    default_bucket_count: 23100    # количество бакетов в каждом тире (по умолчанию 30000)

    audit: false                   # отключение аудита
    log_level: 'info'              # уровень отладки
    log_to: 'file'                 # вывод журналов в файлы, а не в journald

    conf_dir: '/etc/picodata'         # директория для хранения конфигурационных файлов
    data_dir: '/var/lib/picodata'     # директория для хранения данных
    run_dir: '/var/run/picodata'      # директория для хранения sock-файлов
    log_dir: '/var/log/picodata'      # директория для журналов и файлов аудита
    share_dir: '/usr/share/picodata'  # директория для хранения служебных данных (плагинов)

    listen_address: '{{ ansible_fqdn }}'     # адрес прослушивания инстанса. Для IP укажите {{ansible_default_ipv4.address}}
    pg_address: '{{ listen_address }}'       # адрес, по которому инстанс будет принимать подключения по PostgreSQL-протоколу

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

    db_config:                     # параметры конфигурации кластера https://docs.picodata.io/picodata/stable/reference/db_config/
      governor_auto_offline_timeout: 30
      iproto_net_msg_max: 500
      memtx_checkpoint_count: 1
      memtx_checkpoint_interval: 7200

    plugins:
      argus:                                    # плагин
        path: '../plugins/argus_3.0.0.tar.gz'   # путь до пакета плагина
        config: '../plugins/argus_config.yml'   # путь до файла с настройками плагина
        services:
          argus:
            tiers:                              # список тиров, в которые устанавливается сервис плагина. Только тиры с can_vote: true
              - default                         # по умолчанию — default

    GROUP1:                             # Группа серверов (failure_domain)
      hosts:                            # серверы в группе
        server-1-1:                     # имя сервера в инвентарном файле
          ansible_host: '192.168.19.21' # IP-адрес или fqdn если не совпадает с предыдущей строкой
          host_group: 'STORAGES'        # определение целевой группы серверов для установки инстансов

        server-1-2:                     # имя сервера в инвентарном файле
          ansible_host: '192.168.19.22' # IP-адрес или fqdn если не совпадает с предыдущей строкой
          host_group: 'ARBITERS'        # определение целевой группы серверов для установки инстансов

    GROUP2:                             # Группа серверов (failure_domain)
      hosts:                            # серверы в группе
        server-2-1:                     # имя сервера в инвентарном файле
          ansible_host: '192.168.20.21' # IP-адрес или fqdn если не совпадает с предыдущей строкой
          host_group: 'STORAGES'        # определение целевой группы серверов для установки инстансов

    GROUP3:                             # Группа серверов (failure_domain)
      hosts:                            # серверы в группе
        server-3-1:                     # имя сервера в инвентарном файле
          ansible_host: '192.168.21.21' # IP-адрес или fqdn если не совпадает с предыдущей строкой
          host_group: 'STORAGES'        # определение целевой группы серверов для установки инстансов
```

Создайте файл с конфигурацией. Пример:

```yaml title="argus_config.yml"
argus:
    interval_secs: 60                         # Как часто опрашивать LDAP для синхронизации пользователей и ролей. Значение в секундах
    ldap:                                     # Настройки подключения к LDAP:
      bind_dn: "cn=admin,dc=example,dc=org"   # Учётная запись, под которой Argus будет подключаться к LDAP-серверу
      bind_password: "admin"                  # Пароль этого пользователя
      url: "ldap://localhost:389"             # Ссылка на LDAP-сервер
      disabled_attr: "employeeType"           # Название атрибута, который используется для отключения, но не удаления пользователей. Если значение этого атрибута для пользователя равно `true`, Argus отключит его и в Picodata
      tries: 1                                # Число попыток подключения к LDAP-серверу. Для небольших интервалов рекомендуем оставить `1`

      searches:                               # Поисковые запросы и их соответствие ролям:
          - role: "reader"                    # Название роли в Picodata, которая синхронизируется данным запросом
            base: "dc=example,dc=org"         # База поиска (объект вашего LDAP-каталога, с которого начнётся поиск)
            filter: "(&(objectClass=inetOrgPerson)(businessCategory=reader))" # Фильтр для поиска
            attr: "cn"                        # Атрибут, в котором находится имя пользователя, которое будет использоваться в Picodata
```

Подготовьте плейбук:

```yaml title="picodata.yml"
---
- name: Deploy Picodata cluster
  hosts: all
  become: true

  tasks:
    - name: Import picodata-ansible role
      ansible.builtin.import_role:
        name: picodata-ansible
```

В результате в рабочей директории должно быть 4 файла:

- argus.yml
- picodata.yml
- argus_config.yml
- argus_xxxxx.tar.gz

Запустите развёртывание Argus:

```shell
ansible-playbook -i argus.yml picodata.yml
```

См. также:

- [Управление плагинами](../architecture/plugins.md)

<!--  Информация о сборке из исходного кода:

## Сборка плагина {: #plugin_build }

Для установки плагина понадобятся Rust и Cargo версии 1.85 или новее, а
также Git, Curl и заголовочные файлы OpenSSL.

Склонируйте репозиторий с исходным кодом плагина:

```shell
git clone https://git.picodata.io/picodata/plugin/argus.git
```

Соберите плагин:

```shell
cd argus
cargo build --release
```

Результатом сборки будет разделяемая библиотека `libargus.so` в
директории `target/release`.

-->

## Журнал изменений {: #changelog }

### 3.0.0 - 2026-10-07 {: #3.0.0 }

#### Новая функциональность {: #3.0.0-novaia-funktsional-nost }

- Ограничены инстансы, на которых работает Argus

#### Исправления {: #3.0.0-ispravleniia }

- Улучшено выключение файбера при disable/enable
- Pike обновлён до 5.7.0

#### Зависимости {: #3.0.0-zavisimosti }

- Обновлены зависимости и Picodata до 26.2.1
- Обновлены зависимости и Picodata до 26.1.6

#### Прочее {: #3.0.0-prochee }

- Пайплайн унифицирован с остальными плагинами
- Добавили регулярную проверку зависимостей
- Перенесены наработки по CI из Radix
- Ограничиваем список ОС для джобы deploy-certified-build

#### Lint {: #3.0.0-lint }

- Исправлены предупреждения 1.97

### 2.3.1 - 2026-05-22 {: #2.3.1 }

#### Зависимости {: #2.3.1-zavisimosti }

- Picodata обновлена до 26.1.3

### 2.3.0 - 2026-04-01 {: #2.3.0 }

#### Новая функциональность {: #2.3.0-novaia-funktsional-nost }

- :construction_worker: добавлена проверка SBOM-метаданных

#### Тестирование {: #2.3.0-testirovanie }

- Исправлено условие проверки в тесте

#### Зависимости {: #2.3.0-zavisimosti }

- Обновлена Picodata до 26.1.1

#### Прочее {: #2.3.0-prochee }

- Заводим тесты на всех поддерживаемых ОС
- Исправлено название команды `pico-stop`
- Добавлена возможность запуска deploy-certified-build для ручных пайплайнов
- Добавлен сбор артефактов с джобы `sbom-checker`
- Проверка SBOM встроена в gitlab
- :lock: добавлены метаданные для sbom-checker
- Добавлена генерация SBOM-файлов
- :arrow_up: обновим все зависимости перед сертификацией
- Fix sonarqube job do not use old gamayun token
- Fix branch name for sonarqube report download

#### Build {: #2.3.0-build }

- Добавлена debian:trixie
- Убрана сборка ubuntu focal, эта ОС больше нами не поддерживается
- :construction_worker: переменные окружения для сборки вынесены в файл .env

### 2.2.5 - 2026-02-17 {: #2.2.5 }

#### Прочее {: #2.2.5-prochee }

- Добавлено ожидание полной проверки sonarqube'ом

#### Build {: #2.2.5-build }

- 📦 добавлена поддержка alt:p11, пропущенная ранее

### 2.2.4 - 2026-02-16 {: #2.2.4 }

#### Исправления {: #2.2.4-ispravleniia }

- 🔒 обновлена библиотека bytes

#### Внутренние улучшения {: #2.2.4-vnutrennie-uluchsheniia }

- Линты разбиты на отдельные
- :white_check_mark: удалён неиспользуемый параметр, переименован тест

#### Прочее {: #2.2.4-prochee }

- Add pack-sources and deploy-certified-build jobs
- Исправлены ошибки в условиях запуска джобов
- :construction_worker: make заменён на just
- Replace gamayun with sonarqube
- Обновлены заголовки лицензий

#### Build {: #2.2.4-build }

- 📦 добавлена поддержка fedora:43, alt:p11 и osnova onyx:3
- 📦 удалена поддержка fedora:41 в связи с её EOL

#### Lint {: #2.2.4-lint }

- Добавлен конфиг для `cargo audit`
- Добавлен конфиг для `cargo deny`

### 2.2.3 - 2025-12-29 {: #2.2.3 }

#### Исправления {: #2.2.3-ispravleniia }

- :package: обновил механизм определения пути плагина

#### Документация {: #2.2.3-dokumentatsiia }

- :memo: обновлена документация о процессе разработки

#### Зависимости {: #2.2.3-zavisimosti }

- Обновил пикодату до 25.5.2

### 2.2.2 - 2025-10-14 {: #2.2.2 }

#### Внутренние улучшения {: #2.2.2-vnutrennie-uluchsheniia }

- :rotating_light: poetry deps fix

#### Зависимости {: #2.2.2-zavisimosti }

- Picodata 25.4.2

#### Прочее {: #2.2.2-prochee }

- Добавим тег docker бендеру

### 2.2.1 - 2025-09-29 {: #2.2.1 }

#### Исправления {: #2.2.1-ispravleniia }

- :bug: убраны проверки имён пользователей в Argus

#### Прочее {: #2.2.1-prochee }

- Улучшен вывод сообщения о релизе в Телеграм

### 2.2.0 - 2025-09-11 {: #2.2.0 }

#### Новая функциональность {: #2.2.0-novaia-funktsional-nost }

- Use psycopg to login via LDAP

#### Исправления {: #2.2.0-ispravleniia }

- Разрешим использовать однобуквенные имена
- :bug: исправим обработку имён пользователей

#### Тестирование {: #2.2.0-testirovanie }

- :recycle: получаем версию плагина из пикодаты во время тестов

#### Зависимости {: #2.2.0-zavisimosti }

- Picodata 25.3.4

#### Прочее {: #2.2.0-prochee }

- Remove PIKE_DATA_DIR from build

### 2.1.4 - 2025-08-18 {: #2.1.4 }

#### Прочее {: #2.1.4-prochee }

- Bump picodata to 25.3.2
- Add ldaps testing

### 2.1.3 - 2025-07-30 {: #2.1.3 }

#### Документация {: #2.1.3-dokumentatsiia }

- Исправим ссылку на настройку LDAP

#### Внутренние улучшения {: #2.1.3-vnutrennie-uluchsheniia }

- ⬆️ обновляю picodata-plugin до 25.3.1

#### Тестирование {: #2.1.3-testirovanie }

- :arrow_down: наши тестовые образы содержат только Python 3.11

#### Прочее {: #2.1.3-prochee }

- Добавил проверку копирайтов в заголовках файлов
- Добавил лицензию и копирайты

### 2.1.2 - 2025-06-25 {: #2.1.2 }

#### Внутренние улучшения {: #2.1.2-vnutrennie-uluchsheniia }

- :arrow_up: запускаю cargo update для обновления версии tokio
- :arrow_up: обновляю picodata-plugin до 25.2.2

#### Прочее {: #2.1.2-prochee }

- Останавливаем кластер перед выключением тестов
- Удалим локальный docker-compose из гамаюна из-за false positive

#### Build {: #2.1.2-build }

- :construction_worker: разрешаю ставить плагин в неправильную версию пикодаты

### 2.1.1 - 2025-06-23 {: #2.1.1 }

#### Прочее {: #2.1.1-prochee }

- Передаём предыдущую версию в Гамаюн.
- :rotating_light: поправим некоторые замечания Гамаюна
- Внедряю Гамаюн, как и в Radix

### 2.1.0 - 2025-06-04 {: #2.1.0 }

#### Новая функциональность {: #2.1.0-novaia-funktsional-nost }

- Теперь Argus работает только на мастерах
- :arrow_up: closes #17, обновляю picodata plugin до 25.2.1

#### Исправления {: #2.1.0-ispravleniia }

- Заполняем версию в тикет в кору

#### Документация {: #2.1.0-dokumentatsiia }

- Closes #14, переношу документацию пользовательскую в docs/user

#### Внутренние улучшения {: #2.1.0-vnutrennie-uluchsheniia }

- :rotating_light: поправлю замечания linter'а

#### Структура кода {: #2.1.0-struktura-koda }

- Добавляю питоновую типизацию

### 2.0.3 - 2025-05-06 {: #2.0.3 }

#### Исправления {: #2.0.3-ispravleniia }

- 🐛 исправим экранировку

### 2.0.2 - 2025-05-06 {: #2.0.2 }

#### Исправления {: #2.0.2-ispravleniia }

- :bug: исправим многострочный скрипт, пропущено "\"

### 2.0.1 - 2025-05-06 {: #2.0.1 }

#### Прочее {: #2.0.1-prochee }

- :construction_worker: добавим создание тикетов по факту релиза
- Move bender to MarkdownV2

### web.1.0.0.4.g486a5ae - 2025-04-30 {: #web.1.0.0.4.g486a5ae }

#### Документация {: #web.1.0.0.4.g486a5ae-dokumentatsiia }

- :memo: написал инструкцию для пользователя и для программиста

#### Прочее {: #web.1.0.0.4.g486a5ae-prochee }

- :construction_worker: перенесём все зависимости в базовый образ
- Deploy packages to PDG repo (EE)

#### Build {: #web.1.0.0.4.g486a5ae-build }

- Move to picodata 25.1 and pike

### 1.0.0 - 2024-12-28 {: #1.0.0 }

#### Новая функциональность {: #1.0.0-novaia-funktsional-nost }

- Make it plugin
- Ci-cd
- Add enable disable functionality
- Add roles to users
- Add username validation
- Add staff for demo
- Add dockerfiles and readme
- Add ldap-picodata sync

#### Исправления {: #1.0.0-ispravleniia }

- :bug: исправлен запрос проверки, отключён юзер или нет
- :bug: не пытаемся синхронизировать pico_service

#### Документация {: #1.0.0-dokumentatsiia }

- Add docs

#### Внутренние улучшения {: #1.0.0-vnutrennie-uluchsheniia }

- Удалим отдельный cbus_endpoint.
- Remove old tests
- :loud_sound: добавил логов и убрал лишние кавычки из запросов
- :building_construction: переписано на plugin sdk, исправлены явные паники

#### Тестирование {: #1.0.0-testirovanie }

- :white_check_mark: вернули полный тест сьют, поправили баги
- :test_tube: написал два простых теста на подключение к пикодате
- :arrow_up: обновил зависимости
- :card_file_box: добавили пользователей в тестовый контейнер
- :test_tube: подготовим почву для написания тестов
- :white_check_mark: добавим заглушку для тестов

#### Прочее {: #1.0.0-prochee }

- :construction_worker: picodata, screen & ldap
- :hammer: добавим direnv layout для питоновых тестов
- :heavy_plus_sign: ссылаемся на релизный СДК плагинов пикодаты
- :arrow_up: используем новую версию CI
- Up rust version
- Up rust version
- Fix docker image

#### Build {: #1.0.0-build }

- :recycle: зачистим .dockerignore
- :pushpin: будем держать cargo.lock в репе
