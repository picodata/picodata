# Настройка Systemd

В случае выбора системы управления процессами Systemd вы можете
произвести дополнительные настройки Systemd-службы Picodata, например
увеличить лимит на количество открытых файлов, поменять таймаут ожидания
запуска службы и т.п. Рекомендуемый способ переопределения подобных
настроек — изменение инвентарного файла Picodata для Ansible.

См. также:

- [Развертывание кластера через Ansible](../tutorial/deploy_ansible.md)

Настройки при этом будут применяться для каждого инстанса кластера.

## Настройка лимитов в инвентарном файле {: #inventory_limits }

По умолчанию для Systemd-службы Picodata выставлены следующие параметры:

```yaml
  LimitNOFILE: 65535        # Increase fd limit for Vinyl
  LimitCORE: infinity       # Unlimited coredump filesize
  TimeoutStartSec: 86400s   # Systemd waits until all xlogs are recovered
  TimeoutStopSec: 20s       # Give a reasonable amount of time to close xlogs
```

Вы можете переопределить их в инвентарном файле через словарь
`systemd_params`, либо в этом же словаре выставить любые другие
параметры, которые используются в Systemd-юнитах.

Пример фрагмента инвентарного файла с определением словаря
`systemd_params`:

```yaml
all:
  vars:
    systemd_params:
      LimitNOFILE: 900000
      TimeoutStartSec: '30m'
```

## Системные ограничения {: #system_constraints }

Некоторые параметры не могут иметь значение выше установленного в
операционной системе, поэтому для их выставления предварительно нужно
увеличить системные параметры через `sysctl` или `limits.conf`.

Например, параметр `LimitNOFILE` не может превышать системное значение
`fs.nr_open`— если вы хотите выставить большее значение, то
предварительно нужно изменить значение в системе:

Узнать текущее значение:

```shell
sysctl fs.nr_open
```

Отредактируйте файл `/etc/sysctl.conf` (при отсутствии создайте его),
выставив в нем увеличенное значение параметра `fs.nr_open`. Например:

```shell
sudo -i
echo "fs.nr_open=999999" >> /etc/sysctl.conf
```

Примените измененные значения:

```shell
sudo sysctl -p
```

Читайте далее:

- [The Linux Kernel
  Documentation](https://www.kernel.org/doc/Documentation/sysctl/)


## Проверка лимитов {: #lookup_limits }

Лимиты, выставленные для процесса, можно получить командой:

```bash
cat /proc/<MainPID>/limits
```

Значение `MainPID` вы можете получить из вывода команды:

```shell
systemctl show --property=MainPID picodata
```

где `picodata` — имя службы инстанса.
