# Использование журнала аудита

## Общие сведения  {: #intro }

На каждом инстансе Picodata можно включить регистрацию событий и
запись соответствующей информации в журнал аудита.

Журнал аудита позволяет администратору отслеживать все важные события,
которые влияют на безопасность кластера Picodata, включая, например,
создание новых учетных записей, изменение их паролей и привилегий, и
т.д.

См. также:

- [Журнал аудита в защищенной ОС](../security/audit_log.md)
- [Регистрируемые события безопасности](../reference/audit_events.md)

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
- `syslog` — с использованием общесистемного логирования и встроенных
средств защищенной ОС. Подобная схема решает вопросы архивации, ротации
и оповещения администратора. В качестве реализации
[Syslog](https://ru.wikipedia.org/wiki/Syslog) на
современных дистрибутивах Linux часто используется `journald` (компонент
системного менеджера
[Systemd](https://ru.wikipedia.org/wiki/Systemd)),
который имеет множество настроек и даже позволяет отправлять журналы на
внешние хосты.

## Включение журнала {: #enable_audit_log }

По умолчанию, запись событий не ведется. Включить журнал можно при
запуске инстанса, указав параметр [picodata run --audit].
<!-- - с помощью API-функции [pico.audit()](../reference/api.md#picoaudit) -->

Для примера, задействуем файл журнала при запуске инстанса:

Вывод журнала в текстовый файл:

```bash
picodata run --audit=/tmp/audit.log
```

Вывод журнала в syslog:

```bash
picodata run --audit=syslog:
```

[picodata run --audit]: ../reference/cli.md#run_audit

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
