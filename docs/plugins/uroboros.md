
# Uroboros

В данном разделе приведены сведения о
[Uroboros](https://git.picodata.io/picodata/plugin/uroboros), плагине для
СУБД Picodata.

!!! tip "Picodata Enterprise"
    Функциональность плагина доступна только в коммерческой версии Picodata.

## Общие сведения {: #intro }

Плагин Uroboros используется для одонаправленной асинхронной репликации
данных между двумя кластерами одного приложения, созданного при помощи
фреймворка Vshard.

Основное применение Uroboros — миграция кластера между различными
системами без простоя.

## Установка {: #install }

### Подготовка {: #preparation }

> На сервере, с которого будет происходить установка необходим Ansible и доступ на серверы кластера с повышением привилегий.

> Имя пользователя и пароль для скачивания нужно запросить у сопровождающих проекта.

1. Изучить [документацию по развёртыванию кластера пикодаты](https://docs.picodata.io/picodata/24.6/tutorial/deploy_ansible/). Выполнить инструкции по установке роли.
2. Скачать нужную версию плагина `uroboros` и положить пакет в рабочий каталог.
3. Проверить наличие конфигурационного файла для плагина `uroboros-config.yml`, проконтролировать настройки в нём (см. ниже).

### Установка окружения {: #setting_env }

Создать файл с описанием кластера согласно документации по развёртыванию кластера. Например, `uroboros.yml`.

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

Создать файл с конфигурацией. Пример:

```yaml
uroboros:
  producer: # настройки кластера-источника
    space_info_url: "http://<source_address>/uroboros/api/v1/space"
    user_url: "http://<source_address>/uroboros/api/v1/user"
    topology_url: "http://<source_address>/uroboros/api/v1/topology"
  consumer: # настройки приёмника
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

Создать плейбук `picodata.yml`

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

Запустить раскатку Uroboros:

```bash
ansible-playbook -i uroboros.yml picodata.yml
```
