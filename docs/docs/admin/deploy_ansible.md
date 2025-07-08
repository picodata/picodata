# Развертывание кластера через Ansible {: #ansible }

В данном разделе приведена информация по развертыванию кластера Picodata
из нескольких инстансов, запущенных на разных серверах посредством роли
[picodata-ansible].

[picodata-ansible]: https://git.picodata.io/core/picodata-ansible

## Требования к оборудованию и ПО {: #requirements }

Для промышленной эксплуатации кластера следует учитывать базовые
требования к оборудованию и программному обеспечению:

- наличие подготовленных серверов с поддерживаемыми ОС на базе Linux (список см.
https://picodata.io/download/)
- минимальный расчет ресурсов на 1 инстанс (без учета ресурсов для ОС): 1 ядро ЦП, 64
МБ ОЗУ, 512 МБ дискового пространства

## Установка роли {: #install_role }

Установите роль из репозитория через `ansible-galaxy`:

```shell
ansible-galaxy install -f git+https://git.picodata.io/core/picodata-ansible.git
```

или создайте файл `requirements.yml` со следующим содержимым:

```yml
- src: https://git.picodata.io/core/picodata-ansible.git
  scm: git
```

и затем выполните команду:

```bash
ansible-galaxy install -fr requirements.yml
```

## Создание директорий {: #make_dirs }

В текущей директории создайте поддиректории — это нужно для отделения
инвентарных файлов от сценариев (плейбуков):

```shell
mkdir {hosts,playbooks}
```

## Создание инвентарного файла {: #create_inventory_file }

Рассмотрим два сценария: простой кластер с единственным [тиром] и кластер
из нескольких тиров. Пусть в распоряжении есть два сервера: `192.168.0.1` и
`192.168.0.2`.

Создайте инвентарный файл `hosts/cluster.yml` и наполните его
в зависимости от нужного сценария содержанием ниже.

[тиром]: ../overview/glossary.md#tier

### Простой кластер на нескольких серверах {: #simple_cluster }

???+ example "cluster.yml"
    ```yaml
    all:
      vars:
        install_packages: true                           # для установки пакета picodata из репозитория
        cluster_name: simple_cluster                     # имя кластера
        first_bin_port: 13301                            # начальный бинарный порт для первого инстанса (он же main_peer)
        first_http_port: 18001                           # начальный http-порт для первого инстанса для веб-интерфейса

        tiers:                                           # описание тиров
          default:                                       # имя тира (default)
            instances_per_server: 2                      # сколько инстансов запустить на каждом сервере этого тира

    DC1:                                                 # имя датацентра (используется для failure_domain)
      hosts:                                             # далее перечисляем серверы в датацентре
        server-1:                                        # имя сервера в инвентарном файле (используется для failure_domain)
          ansible_host: 192.168.0.1                      # IP адрес или fqdn если не совпадает с предыдущей строкой
        server-2:                                        # имя сервера в инвентарном файле (используется для failure_domain)
          ansible_host: 192.168.0.2                      # IP адрес или fqdn если не совпадает с предыдущей строкой
    ```

### Кластер из двух тиров на нескольких серверах {: #multi_tier_cluster }

Пример инвентарного файла для 4-х серверов, расположенных в 3-х датацентрах (DC1, DC2 и DC3):

???+ example "cluster.yml"
    ```yml
    all:
      vars:
        ansible_user: vagrant      # пользователь для ssh-доступа к серверам

        repo: 'https://download.picodata.io'  # репозиторий, откуда инсталлировать пакет picodata

        cluster_name: 'demo'           # имя кластера
        admin_password: '123asdZXV'    # пароль пользователя admin

        default_bucket_count: 23100    # количество бакетов в каждом тире (по умолчанию 30000)

        audit: false                   # состояние аудита (отключен)
        log_level: 'info'              # уровень отладки
        log_to: 'file'                 # вывод журнала в файлы (а не в journald)

        conf_dir: '/etc/picodata'         # директория для хранения конфигурационных файлов
        data_dir: '/var/lib/picodata'     # директория для хранения данных
        run_dir: '/var/run/picodata'      # директория для хранения sock-файлов
        log_dir: '/var/log/picodata'      # директория для журналов и файлов аудита
        share_dir: '/usr/share/picodata'  # директория для размещения служебных данных (плагинов)

        listen_address: '{{ ansible_fqdn }}'     # адрес, который будет слушать инстанс. Для IP указать {{ansible_default_ipv4.address}}
        pg_address: '{{ listen_address }}'       # адрес, который будет слушать PostgreSQL-протокол инстанса

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
          example:                                                  # плагин
            path: '../plugins/weather_0.1.0-ubuntu-focal.tar.gz'    # путь до пакета плагина
            config: '../plugins/weather-config.yml'                 # путь до файла с настройками плагина
            services:
              weather_service:
                tiers:                                                  # список тиров, в которые устанавливается сервис плагина
                  - default                                             # указано значение по умолчанию (default)

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

## Изменение параметров роли {: #change_defaults }

Параметры роли, используемые по умолчанию, располагаются в файле
`defaults/main.yml`, их также можно переопределять в инвентарном файле.

Помимо параметров по умолчанию используется словарь `tiers` с указанием внутри:

- имени тира (например `router`)
- количества репликасетов (`replicaset_count`) или количества инстансов на каждом сервере (`instances_per_server`)
- фактора репликации (`replication_factor`) для тира (по умолчанию равен 1)

Пример словаря tiers:

```yaml
tiers:
  default:                     # имя тира default
    replicaset_count: 2        # количество репликасетов
    replication_factor: 3      # фактор репликации
    bucket_count: 16384        # количество бакетов в тире
    config:
      memtx:
        memory: 128M           # количество памяти, предоставляемое непосредственно на хранение данных
```

См также:

- [Переменные, используемые в роли Ansible](../reference/ansible_variables.md)


## Расчет количества инстансов/репликасетов {: #estimation }

В инвентарном файле для тиров можно указывать количество как
репликасетов, так и инстансов на каждом сервере. При этом, в первом
случае значение `instances_per_server` будет рассчитано автоматически.
Если при расчете будет получено дробное значение, то роль остановится с
ошибкой.

Если указаны оба параметра, то приоритет у `instances_per_server`

### Количество репликасетов в кластере {: #replicasets_estimation }

```
replicaset_count = instances_per_server * SERVER_COUNT / replication_factor
```

Если при расчете вы получаете дробное число, значит количество серверов
или фактор репликации подобраны некорректно и это необходимо исправить.

### Количество инстансов на одном сервере {: #instances_estimation }

```
instances_per_server = replicaset_count * replication_factor / SERVER_COUNT
```

Аналогично, если при расчете вы получаете дробное число, значит количество серверов
или фактор репликации подобраны некорректно и это необходимо исправить.

## Создание плейбука {: #create_playbook }

Для подключения роли создайте плейбук `playbooks/picodata.yml`,
добавив в него следующее содержание:

???+ example "picodata.yml"
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

## Установка кластера {: #install_cluster }

Выполните установку кластера командой:

```shell
ansible-playbook -i hosts/cluster.yml playbooks/picodata.yml
```

При успешном окончании выполнения плейбука будет создан yaml-файл
`report.yml` с перечислением всех инстансов и портов кластера.

Более подробно о доступных переменных в инвентарном файле можно узнать в
[git-репозитории
роли](https://git.picodata.io/core/picodata-ansible/-/blob/main/docs/variables.md).

См. также:

- [Настройка Systemd](systemd.md)
- [Подключение и работа в консоли](../tutorial/connecting.md)
- [Создание кластера в ручном режиме](../tutorial/deploy.md)

## Управление кластером {: #manage_cluster }

Для управления кластером используйте теги роли, указав их после параметра `-t`.

Пример команды для удаления кластера:

```shell
ansible-playbook -i hosts.yml picodata.yml -t remove
```

Пример команды для создания резервной копии кластера:

```shell
ansible-playbook -i hosts.yml picodata.yml -t backup
```

Пример команды для восстановления кластера:

```shell
ansible-playbook -i hosts.yml picodata.yml -t restore
```

??? example "Список поддерживаемых тегов"
    | Имя | Описание |
    | ---      | ---      |
    | deploy | Установка кластера. Используется по умолчанию, если не указаны другие тэги |
    | remove | Удаление кластера |
    | install_pkgs | Установка пакета Picodata. Также используется при установке кластера |
    | backup | Создание резервной копии кластера |
    | restore | Восстановление из резервной копии кластера |
    | restore_full | Установка кластера и восстановление из резервной копии |
    | genin | Получить список инстансов в разбивке для каждого сервера (используется в других сценариях ) |
    | restart | Перезапуск всех инстансов на всех серверах |
    | plugins | Установка и конфигурация плагинов |
    | crash_dump | Сбор файлов необходимых для анализа разработчиками в случае сбоя кластера  |
    | deploy_become, remove_become | Используются для установки/удаления кластера в режиме rootless |

## Управление плагинами {: #plugin_management }

C помощью роли [picodata-ansible] можно также добавлять в кластер
плагины и их конфигурации. Для этого модифицируйте инвентарный файл
`hosts/cluster.yml`, добавив в него блок `plugins`. Пример инвентарного
файла, поддерживающего установку в кластер c одним [тиром] тестового
плагина _weather_:

???+ example "cluster.yml"
    ```yaml
    all:
      vars:
        install_packages: true                           # для установки пакета picodata из репозитория
        cluster_name: simple_cluster                     # имя кластера
        first_bin_port: 13301                            # начальный бинарный порт для первого инстанса (он же main_peer)
        first_http_port: 18001                           # начальный http-порт для первого инстанса для веб-интерфейса

        tiers:                                           # описание тиров
          default:                                       # имя тира (default)
            bucket_count: 20000                          # сколько бакетов будет размещено на данном тире
            instances_per_server: 2                      # сколько инстансов запустить на каждом сервере этого тира

        plugins:                                         # описание плагинов
          example:                                       # имя плагина в Ansible (может не совпадать с именем в Picodata)
            path: '../plugins/weather_0.1.0.tar.gz'      # путь к архиву с плагином
            config: '../plugins/weather-config.yml'      # путь к файлу конфигурации плагина
            tiers:                                       # список тиров, на которых будет запущен плагин
              - default                                  # имя тира (default)


    DC1:                                                 # имя датацентра (используется для failure_domain)
      hosts:                                             # далее перечисляем серверы в датацентре
        server-1:                                        # имя сервера в инвентарном файле (используется для failure_domain)
          ansible_host: 192.168.0.1                      # IP адрес или fqdn если не совпадает с предыдущей строкой
        server-2:                                        # имя сервера в инвентарном файле (используется для failure_domain)
          ansible_host: 192.168.0.2                      # IP адрес или fqdn если не совпадает с предыдущей строкой
    ```

Для подключения плагина потребуется сформировать архив из его файлов, а
также подготовить файл конфигурации плагина.

См. также:

- [Управление плагинами](../tutorial/plugins.md)

Модифицировать файл плейбука не требуется.

Выполните установку плагинов в кластер командой:

```shell
ansible-playbook -i hosts/cluster.yml playbooks/picodata.yml -t plugins
```

!!! note "Примечание"
    С помощью роли Ansible можно только добавлять
    плагины в кластер. Удаление производится
    [вручную](../tutorial/plugins.md#drop_plugin).
