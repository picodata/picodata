# Использование журнала аудита

## Общие сведения {: #intro }

На каждом инстансе Picodata можно включить регистрацию событий и
запись соответствующей информации в журнал аудита.

Журнал аудита позволяет администратору отслеживать все важные события,
которые влияют на безопасность кластера Picodata, включая, например,
создание новых учетных записей, изменение их паролей и привилегий, и
т.д.

См. также:

- [Журнал аудита в защищенной ОС](../security/audit_log.md)
- [Регистрируемые события безопасности](../reference/audit_events.md)
- [Команда AUDIT POLICY](../reference/sql/audit_policy.md)

## Способы ведения журнала {: #audit_log_config }

Каждый узел кластера ведет свой собственный журнал, но настройка
параметров журнала производится централизованно. В зависимости от
настройки, журнал может вестись следующими способами:

- `file` — в виде текстового файла, по строчке на событие. В таком
случае смотреть его можно средствами защищенной ОС, а также при помощи
API, предоставляемых самой СУБД.
- `pipe` — с использованием внешнего процесса-коллектора, который СУБД должна
будет запустить при старте узла. В этом случае журнал будет поступать в
текстовом виде на вход процессу-коллектору (через `stdin`). Вся дальнейшая
логика работы целиком зависит от запущенного процесса; например, он
может отправлять журнал в централизованное внешнее хранилище.
- `syslog` — с использованием общесистемного журналирования и встроенных
средств защищенной ОС. Подобная схема решает вопросы архивации, ротации
и оповещения администратора. В качестве реализации
[Syslog](https://ru.wikipedia.org/wiki/Syslog) на
современных дистрибутивах Linux часто используется `journald` (компонент
системного менеджера
[Systemd](https://ru.wikipedia.org/wiki/Systemd)),
который имеет множество настроек и даже позволяет отправлять журналы на
внешние хосты.

По умолчанию, запись событий не ведется.

Способ вывода журнала задается при запуске инстанса. После
первоначального бутстрапа инстанса эту настройку можно изменить в
дальнейшем, перезапустив инстанс с новым значением параметра.

## Включение журнала {: #enable_audit_log }

Задайте способ ведения журнала при запуске инстанса. Набор действий
зависит от того, каким способом вы запускаете инстансы кластера.

### В командной строке {: #enable_in_cli }

Используйте параметр [`picodata run --audit`] в командной
строке:

Вывод журнала в текстовый файл:

```bash
picodata run --audit=/tmp/audit.log
```

Вывод журнала в syslog:

```bash
picodata run --audit=syslog:
```

[`picodata run --audit`]: ../reference/cli.md#run_audit

### В файле конфигурации {: #enable_in_config }

Используйте параметр [instance.audit] в [файле конфигурации]:

1. Сгенерируйте шаблон файла конфигурации со значениями по умолчанию:
```shell
picodata config default -o config.yaml
```

1. Отредактируйте полученный файл `config.yaml`, задав нужные значения у
   параметров. В том числе, измените строку `audit: null`, указав
   требуемый способ вывода журнала.<br><br>
   Вывод журнала в текстовый файл:
   ```yaml
   audit: file:/tmp/audit.log
   ```
   Вывод журнала в syslog:
   ```yaml
   audit: syslog:
   ```

1. Запустите инстанс, используя параметры из файла конфигурации:
```shell
picodata run --config config.yaml
```

[instance.audit]: ../reference/config.md#instance_audit
[файле конфигурации]: ../reference/config.md

### С помощью переменной окружения {: #enable_in_env }

Задайте нужное значение для переменной `PICODATA_AUDIT_LOG`.

Вывод журнала в текстовый файл:

```shell
PICODATA_AUDIT_LOG=audit.log picodata run
```

Вывод журнала в syslog:

```shell
PICODATA_AUDIT_LOG=syslog: picodata run
```

Необходимый набор переменных иногда удобнее задать в shell-скрипте.
Например:

```shell
#!/bin/bash
export PICODATA_AUDIT_LOG=syslog:
picodata run
```

### С помощью Ansible {: #enable_in_ansible }

Используйте [переменные] `audit`, `audit_to` и `audit_pipe_command` при настройке роли
Picodata для Ansible:

- `audit` — включение аудита событий в кластере. Может принимать значения `true` и `false`
- `audit_to` — способ ведения журнала аудита. Может принимать значения `syslog` (по умолчанию), `file` и `pipe`
- `audit_pipe_command` — команда для перенаправления сообщений аудит в
  подпроцесс в том случае, если в `audit_to` задано значение `pipe`

Порядок действий для использования переменных:

1. Ознакомьтесь с инструкцией по [Развертыванию кластера с помощью Ansible](deploy_ansible.md)
1. Создайте инвентарный файл `hosts/cluster.yml` и добавьте в него нужный набор параметров аудита.

    Вывод журнала в текстовый файл (файлы аудита будут размещаться в `log_dir` и начинаться с `audit-`):

    ```yaml
        all:
        vars:
            log_dir: /var/log/picodata
            audit: true
            audit_to: file
    ```

    Вывод журнала в syslog:

    ```yaml
        all:
        vars:
            audit: true
            audit_to: syslog
    ```

1. Создайте плейбук `playbooks/picodata.yml` (см. [подробнее](deploy_ansible.md#create_playbook))
1. Установите кластер:
    ```shell
    ansible-playbook -i hosts/cluster.yml playbooks/picodata.yml
    ```

[переменные]: ../reference/ansible_variables.md#logs_and_audit

## Получение доступа к журналу {: #access_audit_log }

Если журнал выводится в отдельный текстовый файл, то отслеживать
изменения в нем можно следующей командой:

```bash
tail -f /tmp/audit.log
```

Вывод журнала в syslog в современных дистрибутивах Linux предполагает
доступ к данным с помощью
[journalctl](https://www.man7.org/linux/man-pages/man1/journalctl.1.html)
(компонента Systemd). Для изоляции событий, относящихся только к
инстансу Picodata, используйте такую команду:

```bash
journalctl -f | grep tarantool
```

Если запись в `syslog` происходит через скрипт-обработчик
([пример](#desktop_notifications)), то команда для доступа к журналу
будет немного отличаться:

```bash
journalctl -f | grep AUDIT
```

## Включение журнала для DML-операций {: #enable_dml_audit_log }

По умолчанию, DML-операции (`INSERT`/`UPDATE`/`DELETE`) не записываются в журнал аудита.
Эту функциональность можно включить через SQL-запрос для конкретных пользователей:

- `AUDIT POLICY dml_default BY dbuser` — включить регистрацию DML-операций для пользователя `dbuser`
- `AUDIT POLICY dml_default EXCEPT dbuser` — отключить регистрацию DML-операций для пользователя `dbuser`

См. также:

- [AUDIT POLICY](../reference/sql/audit_policy.md).

!!! note "Примечание"
    Включение журнала для DML-операций влияет на
    производительность системы (на число выполняемых запросов в единицу времени).
    Согласно данным бенчмарка `pgbench`, снижение составляет ~5%.

## Оповещения о событиях безопасности {: #notifications }

### Поддерживаемые способы доставки оповещений {: #supported_notifications }

В Picodata существует возможность получать оповещения о событиях,
зафиксированных в журнале аудита. Поддерживаются следующие способы
доставки оповещений:

- уведомления для графического рабочего стола
- уведомления по электронной почте

### Оповещения для рабочего стола {: #desktop_notifications }

Для доставки оповещений на рабочий стол используется программа
`notify-send`, имеющаяся в составе защищенной ОС. Для того, чтобы
использовать ее совместно с Picodata, потребуется создать файл-обработчик
следующего содержания:

```python
#!/usr/bin/env python

import json
import subprocess
import sys
from systemd import journal

for line in sys.stdin:
    if line:
        journal.send(f"AUDIT {line}")
        entry = json.loads(line)
        if entry['severity']:
            subprocess.call(['notify-send', entry['message']])

```

Сохраните этот код в файл `audit-sink.py` в директорию, из которой будет
запускаться `picodata`, сделайте его исполняемым (`chmod +x
audit-sink.py`) и затем запустите инстанс:

```shell
picodata run --admin-sock ./i1.sock --audit='|/tmp/audit-sink.py'
```
В результате, сообщения из журнала аудита будут сохраняться в `syslog` и
дублироваться в виде уведомлений на рабочем столе.

<!--
Установка среды рабочего стола в защищенной ОС Альт СП Релиз 10 выполняется из
репозитория [c10f1]:

```bash
su -
cat << EOF | sudo tee /etc/apt/sources.list.d/c10f1
rpm http://ftp.altlinux.org/pub/distributions/ALTLinux/ c10f1/branch/x86_64 classic
rpm http://ftp.altlinux.org/pub/distributions/ALTLinux/ c10f1/branch/noarch classic
EOF
apt-repo
apt-get update
```

[c10f1]: https://packages.altlinux.org/ru/c10f1/about/
-->

### Оповещения по электронной почте {: #email_notifications }

Для получения оповещений по электронной почте потребуются:

- программа `sendmail`, имеющаяся в составе защищенной ОС
- настройка `sendmail` для корректной работы с почтовым сервером
- файл-обработчик для дублирования событий журнала аудит в виде сообщений для `sendmail`

Для настройки `sendmail` нужно отредактировать файл настроек
`/etc/mail/sendmail.mc` следующим образом:

```
define(`SMART_HOST', `your.smtp.server')dnl
define(`confAUTH_MECHANISMS', `EXTERNAL GSSAPI DIGEST-MD5 CRAM-MD5 LOGIN PLAIN')dnl
FEATURE(`authinfo',`hash -o /etc/mail/authinfo.db')dnl
```

где `your.smtp.server` — адрес SMTP-сервера для отправки писем

Далее следует настроить аутентификацию, отредактировав файл `/etc/mail/authinfo`:

```
AuthInfo:your.smtp.server "U:your_username" "P:your_password" "M:PLAIN"
```

где:

- `your.smtp.server` — адрес SMTP-сервера для отправки писем
- `your_username` — имя пользователя
- `your_password` — пароль пользователя
- `PLAIN` — тип аутентификации

Следующим шагом нужно сгенерировать базу данных для аутентификации:

```shell
sudo makemap hash /etc/mail/authinfo < /etc/mail/authinfo
```

сгенерировать конфигурацию `sendmail`:

```shell
sudo make -C /etc/mail
```

включить и запустить `sendmail`:

```shell
sudo systemctl enable --now sendmail.service
```

Теперь можно использовать модифицированный скрипт `audit-sink.py` со следующим содержанием:

```python
#!/usr/bin/env python

import json
import subprocess
import sys
from systemd import journal

for line in sys.stdin:
    if line:
        journal.send(f"AUDIT {line}")
        entry = json.loads(line)
        if entry['severity']:
            subprocess.call(['sendmail user@example.com <', entry['message']])

```

В результате, сообщения из журнала аудита будут попадать в `syslog` от
имени процесса `python` и дублироваться в виде писем на указанный вместо
`user@example.com` почтовый ящик.
