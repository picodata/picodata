# Argus

В данном разделе приведены сведения о
[Argus](https://git.picodata.io/picodata/plugin/argus), плагине для СУБД
Picodata.

!!! tip "Picodata Enterprise"
    Функциональность плагина доступна только в
    коммерческой версии Picodata.

## Общие сведения {: #intro }

Плагин Argus используется для синхронизации учетных данных между
сервером LDAP и Picodata. Синхронизация происходит однонаправленно и
позволяет импортировать данные пользователей и групп с сервера LDAP в
Picodata, для которой перед этим была настроена авторизация с помощью
[LDAP](../admin/ldap.md).

## Состав плагина {: #plugin_files }

<!--
Загрузите плагин для нужной операционной системы по адресу:

[https://git.picodata.io/picodata/plugin/argus/-/releases/permalink/latest](https://git.picodata.io/picodata/plugin/argus/-/releases/permalink/latest)
 -->

Внутри архива с плагином находится структура вложенных директорий,
включающая имя и версию плагина, а также его файлы:

```
└── argus
    └── 2.1.3
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
CREATE PLUGIN argus 2.1.3;
ALTER PLUGIN argus 2.1.3 ADD SERVICE argus TO TIER default;
ALTER PLUGIN argus 2.1.3 ENABLE;
```

!!! note "Примечание"
	  После запуска плагин Argus будет работать только на мастер-репликах кластераs

Для диагностики работы плагина обратитесь к [отладочному журналу] инстанса Picodata.

[отладочному журналу]: ../admin/monitoring.md#reading_log
[административной консоли]: ../tutorial/connecting.md#admin_console

## Проверка с помощью Ansible {: #ansible_test }

### Подготовка {: #preparation }

1. Изучите [документацию по развертыванию кластера Picodata](https://docs.picodata.io/picodata/stable/tutorial/deploy_ansible/). Выполнить инструкции по установке роли.
2. Скачайте нужную версию плагина `argus` и положите пакет в рабочую директорию.
3. Проверьте наличие конфигурационного файла для плагина `argus-config.yml`, проверьте настройки в нем (см. ниже).

!!! note "Примечание"
    На сервере, с которого будет происходить установка,
    необходим Ansible и доступ на серверы кластера с повышением привилегий.

### Установка окружения {: #setting_env }

Создайте файл с описанием кластера согласно [руководству по
развертыванию кластера](../admin/deploy_ansible.md). Например,
`argus.yml`.

```yaml
---
all:
  vars:
    user: username # имя пользователя, под которым будут запущены процессы picodata
    group: groupname # группа пользователя, под которой будут запущены процессы picodata
    password: "<password>"
    cluster_name: argus
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
      argus:
        path: "argus_2.1.3.tar.gz"
        tiers:
          - default
        config: "argus-config.yml"
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
