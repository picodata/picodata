# Развертывание кластера через Ansible {: #ansible }

В данном разделе приведена информация по развертыванию кластера Picodata
из нескольких инстансов, запущенных на разных серверах посредством роли
[picodata-ansible](https://git.picodata.io/picodata/picodata/picodata-ansible).

## Кластер на нескольких серверах {: #distributed_cluster }

Разворачивание кластера на нескольких серверах отражает сценарии
полноценного использования Picodata в распределенной среде.

Предположим, что таких серверов два: `192.168.0.1` и
`192.168.0.2`. Порядок действий будет следующим:

### Установка роли {: #install_role }

Установите роль из репозитория через `ansible-galaxy`:

```shell
ansible-galaxy install git+https://git.picodata.io/picodata/picodata/picodata-ansible.git
```

### Создание каталогов {: #make_dirs }

В текущем каталоге создайте подкаталоги — это нужно для отделения
инвентарных файлов от сценариев (плэйбуков):

```shell
mkdir {hosts,playbooks}
```

### Создание файла инвентаря {: #create_inventory_file }

Создайте инвентарный файл `hosts/demo.yml` следующего содержания:

```yaml
all:
  vars:
    ansible_user: vagrant                            # пользователь для ssh-доступа к серверам
    install_packages: true                           # для установки пакета picodata из репозитория
    cluster_id: demo                                 # имя кластера
    first_bin_port: 13301                            # начальный бинарный порт для первого инстанса (он же main_peer)
    first_http_port: 18001                           # начальный http-порт для первого инстанса для веб-интерфейса

    tiers:                                           # описание тиров (тиры пока нигде не используются, поэтому нет смсыла сосздавать дополнительные тиры)
      default:                                       # имя тира default
        instances_per_server: 2                      # сколько инстансов запустить на каждом сервере

DC1:                                                 # Имя датацентра (используется для failure_domain)
  hosts:                                             # далее перечисляем серверы в датацентре
    server-1:                                        # имя сервера в инвентарном файле (используется для failure_domain)
      ansible_host: 192.168.0.1                      # IP адрес или fqdn если не совпадает с предыдущей строкой
    server-2:                                        # имя сервера в инвентарном файле (используется для failure_domain)
      ansible_host: 192.168.0.2                      # IP адрес или fqdn если не совпадает с предыдущей строкой
```

### Создание плейбука {: #create_playbook }

Создайте плэйбук `playbooks/cluster_up.yml`, который подключает роль
следующего содержания:

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

### Установка кластера {: #install_cluster }

Выполните установку кластера командой:

```shell
ansible-playbook -i hosts/demo.yml playbooks/cluster_up.yml
```

Более подробно о доступным переменных в инвентарном файле можно узнать в
[git-репозитории
роли](https://git.picodata.io/picodata/picodata/picodata-ansible).

См. также:

- [Подключение и работа в консоли](connecting.md)
- [Создание кластера в ручном режиме](deploy.md)
