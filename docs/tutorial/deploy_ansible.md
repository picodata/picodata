# Развертывание кластера через Ansible {: #ansible }

В данном разделе приведена информация по развертыванию кластера Picodata
из нескольких инстансов, запущенных на разных серверах посредством роли
[picodata-ansible].

[picodata-ansible]: https://git.picodata.io/picodata/picodata/picodata-ansible

## Установка роли {: #install_role }

Установите роль из репозитория через `ansible-galaxy`:

```shell
ansible-galaxy install -f git+https://git.picodata.io/picodata/picodata/picodata-ansible.git
```

## Создание директорий {: #make_dirs }

В текущей директории создайте поддиректории — это нужно для отделения
инвентарных файлов от сценариев (плэйбуков):

```shell
mkdir {hosts,playbooks}
```

## Создание инвентарного файла {: #create_inventory_file }

Рассмотрим два сценария: простой кластер с единственным тиром и кластер
из нескольких тиров. Пусть в распоряжении есть два сервера: `192.168.0.1` и
`192.168.0.2`.

Создайте инвентарный файл `hosts/cluster.yml` и наполните его
в зависимости от нужного сценария содержанием ниже.

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
          default:                                       # имя тира default
            instances_per_server: 2                      # сколько инстансов запустить на каждом сервере

    DC1:                                                 # Имя датацентра (используется для failure_domain)
      hosts:                                             # далее перечисляем серверы в датацентре
        server-1:                                        # имя сервера в инвентарном файле (используется для failure_domain)
          ansible_host: 192.168.0.1                      # IP адрес или fqdn если не совпадает с предыдущей строкой
        server-2:                                        # имя сервера в инвентарном файле (используется для failure_domain)
          ansible_host: 192.168.0.2                      # IP адрес или fqdn если не совпадает с предыдущей строкой
    ```

### Кластер из двух тиров на нескольких серверах {: #multi_tier_cluster }

???+ example "cluster.yml"
    ```yaml
    all:
      vars:
        install_packages: true                           # для установки пакета picodata из репозитория
        cluster_name: multi_tier_cluster                 # имя кластера
        first_bin_port: 13301                            # начальный бинарный порт для первого инстанса (он же main_peer)
        first_http_port: 18001                           # начальный http-порт для первого инстанса для веб-интерфейса

        tiers:                                           # описание тиров
          compute:
            instances_per_server: 1
            replication_factor: 1
            host_groups:
              - computes

          storage:
            instances_per_server: 1
            replication_factor: 2
            host_groups:
              - storages

    DC1:                                                 # Имя датацентра (используется для failure_domain)
      hosts:                                             # далее перечисляем серверы в датацентре
        server-1:                                        # имя сервера в инвентарном файле (используется для failure_domain)
          ansible_host: 192.168.0.1                      # IP адрес или fqdn если не совпадает с предыдущей строкой
          host_group: 'storages'
        server-2:                                        # имя сервера в инвентарном файле (используется для failure_domain)
          ansible_host: 192.168.0.2                      # IP адрес или fqdn если не совпадает с предыдущей строкой
          host_group: 'storages'
        server-3:                                        # имя сервера в инвентарном файле (используется для failure_domain)
          ansible_host: 192.168.0.3                      # IP адрес или fqdn если не совпадает с предыдущей строкой
          host_group: 'computes'
    ```

## Создание плейбука {: #create_playbook }

Для подключения роли создайте плэйбук `playbooks/picodata.yml`,
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

Более подробно о доступных переменных в инвентарном файле можно узнать в
[git-репозитории
роли](https://git.picodata.io/picodata/picodata/picodata-ansible/-/blob/main/docs/variables.md?ref_type=heads).

См. также:

- [Подключение и работа в консоли](connecting.md)
- [Создание кластера в ручном режиме](deploy.md)

## Управление плагинами {: #plugin_management }

C помощью роли [picodata-ansible] можно также добавлять в кластер
плагины и их конфигурации. Для этого модифицируйте инвентарный файл
`hosts/cluster.yml`, добавив в него блок `plugins`. Пример инвентарного
файла, поддерживающего установку в кластер c одним тиром тестового
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
          default:                                       # имя тира default
            instances_per_server: 2                      # сколько инстансов запустить на каждом сервере

        plugins:                                         # описание плагинов
          example:                                       # имя плагина в Ansible (может не совпадать с именем в Picodata)
            path: '../plugins/weather_0.1.0.tar.gz'      # путь к архиву с плагином
            config: '../plugins/weather-config.yml'      # путь к файлу конфигурации плагина
            tiers:                                       # список тиров, на которых будет запущен плагин
              - default                                  # имя тира default


    DC1:                                                 # Имя датацентра (используется для failure_domain)
      hosts:                                             # далее перечисляем серверы в датацентре
        server-1:                                        # имя сервера в инвентарном файле (используется для failure_domain)
          ansible_host: 192.168.0.1                      # IP адрес или fqdn если не совпадает с предыдущей строкой
        server-2:                                        # имя сервера в инвентарном файле (используется для failure_domain)
          ansible_host: 192.168.0.2                      # IP адрес или fqdn если не совпадает с предыдущей строкой
    ```

Для подключения плагина потребуется сформировать архив из его файлов, а
также подготовить файл конфигурации плагина.

См. также:

- [Управление плагинами](plugins.md)

Модифицировать файл плейбука не требуется.

Выполните установку плагинов в кластер командой:

```shell
ansible-playbook -i hosts/cluster.yml playbooks/picodata.yml -t plugins
```

!!! note "Примечание"
    С помощью роли Ansible можно только добавлять
    плагины в кластер. Удаление производится
    [вручную](plugins.md#drop_plugin).

