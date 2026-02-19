# Argus

В данном разделе приведены сведения о
[Argus](https://git.picodata.io/picodata/plugin/argus), плагине для СУБД
Picodata.

!!! tip "Picodata Enterprise"
    Функциональность плагина доступна только в
    коммерческой версии Picodata.

## Общие сведения {: #intro }

Плагин Argus используется для синхронизации учетных данных между
сервером LDAP/LDAPS и Picodata. Синхронизация происходит однонаправленно и
позволяет импортировать данные пользователей и групп с сервера LDAP в
Picodata, для которой перед этим была настроена авторизация с помощью
[LDAP/LDAPS](../admin/ldap.md).

## Соответствие версий Picodata и Argus {: #picodata_argus_versions }

Версии плагина Argus требуют определённых версий СУБД Picodata. Ниже
показана таблица совместимости версий:

| Argus | Picodata | ФСТЭК-сертификат |
| ------ | ------ | :-----: |
| 2.1.2 | 25.2.2 | :white_check_mark: |
| 2.2.5 | 25.5.7 (25.5.*) | |

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
    └── 2.2.5
        ├── libargus.so
        └── manifest.yaml
```

Основная логика плагина обеспечивается разделяемой библиотекой
`libargus.so`. Исходная конфигурация плагина задается в файле манифеста
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

Установите плагин, добавьте его сервис к тиру и включите его с помощью
следующих SQL-команд:

```sql
CREATE PLUGIN argus 2.2.5;
ALTER PLUGIN argus 2.2.5 ADD SERVICE argus TO TIER default;
ALTER PLUGIN argus 2.2.5 ENABLE;
```

!!! note "Примечание"
	  После запуска плагин Argus будет работать только на мастер-репликах кластера

Для диагностики работы плагина обратитесь к [отладочному журналу] инстанса Picodata.

[отладочному журналу]: ../admin/monitoring.md#reading_log
[административной консоли]: ../tutorial/connecting.md#admin_console

## Проверка с помощью Ansible {: #ansible_test }

### Подготовка {: #preparation }

1. Изучите [документацию по развёртыванию кластера Picodata](https://docs.picodata.io/picodata/stable/tutorial/deploy_ansible/). Выполнить инструкции по установке роли.
2. Скачайте нужную версию плагина `argus` и положите пакет в рабочую директорию.
3. Проверьте наличие конфигурационного файла для плагина `argus-config.yml`, проверьте настройки в нем (см. ниже).

!!! note "Примечание"
    На сервере, с которого будет происходить установка,
    необходим Ansible и доступ на серверы кластера с повышением привилегий.

### Установка окружения {: #setting_env }

Создайте файл с описанием кластера согласно [руководству по
развёртыванию кластера](../admin/deploy_ansible.md).
Ниже показан пример для 4-х серверов, расположенных в 3-х группах (DC1,
DC2 и DC3). Группа — отдельный [домен отказа].

[домен отказа]: ../overview/glossary.md#failure_domain

```yaml title="argus.yml"
all:
  vars:
    ansible_user: vagrant      # пользователь для ssh-доступа к серверам

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
    share_dir: '/usr/share/picodata'  # директория для хранения размещения служебных данных (плагинов)

    listen_address: '{{ ansible_fqdn }}'     # адрес, который будет слушать инстанс. Для IP указать {{ansible_default_ipv4.address}}
    pg_address: '{{ listen_address }}'       # адрес, который будет слушать PostgreSQL-протокола инстанса

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
        path: '../plugins/argus_2.2.5.tar.gz'   # путь до пакета плагина
        config: '../plugins/argus-config.yml'   # путь до файла с настройками плагина
        services:
          argus:
            tiers:                              # список тиров, в которые устанавливается служба плагина
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
    interval_secs: 60                         # Как часто опрашивать LDAP на предмет пользователей и ролей. Число, в секундах
    ldap:                                     # Настройки подключения к LDAP:
      bind_dn: "cn=admin,dc=example,dc=org"   # Имя пользователя, под которой Argus будет подключаться к вашему LDAP-серверу
      bind_password: "admin"                  # Пароль этого пользователя
      url: "ldap://localhost:389"             # Ссылка на LDAP-сервер
      disabled_attr: "employeeType"           # Название атрибута, который вы используете для отключения, но не удаления пользователей. Если значение этого атрибута у пользователя `true`, Argus отключит его и в Picodata
      tries: 1                                # Число попыток подключения к LDAP-серверу. Для маленьких интервалов рекомендуем оставить `1`

      searches:                               # Поисковые запросы и их соответствие ролям:
          - role: "reader"                    # Название роли в Picodata, которая синхронизируется данным запросом
            base: "dc=example,dc=org"         # База поиска (объект вашего LDAP-каталога, с которого начнется поиск)
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

Запустите раскатку Argus:

```bash
ansible-playbook -i argus.yml picodata.yml
```

См. также:

- [Управление плагинами](../architecture/plugins.md)

<!--  Информация о сборке из исходного кода:

## Сборка плагина {: #plugin_build }

Для установки плагина понадобятся Rust и Cargo версии 1.85 или новее, а
также Git, Curl и заголовочные файлы OpenSSL.

Склонируйте репозиторий с исходным кодом плагина:

```bash
git clone https://git.picodata.io/picodata/plugin/argus.git
```

Соберите плагин:

```bash
cd argus
cargo build --release
```

Результатом сборки будет разделяемая библиотека `libargus.so` в
директории `target/release`.

-->
