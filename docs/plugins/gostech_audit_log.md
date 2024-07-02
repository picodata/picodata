# Внешний модуль аудита

В данном разделе приведена информация о модуле `gostech-audit-log`,
который предназначен для экспорта событий журнала аудита в Logstash.

!!! tip "Picodata Enterprise"
    Функциональность модуля `gostech-audit-log` доступна только в
    коммерческой версии Picodata.

### Схема взаимодействия с Picodata {: #outline }

Схема взаимодействия Picodata и модуля `gostech-audit-log` показана
ниже.

![](../images/gostech_audit_log.svg)

Задача модуля `gostech-audit-log` — разбор [журнала
аудита](../tutorial/audit_log.md) Picodata и отправка его на указанный
пользователем адрес.

### Запуск модуля {: #run }

Внешний Модуль `gostech-audit-log` запускается как еще один способ
вывода журнала.

Пример команды запуска:

```shell
picodata run --audit='| gostech-audit-log --url https://example.com'
```

В качестве `url` следует указать адрес внешнего сервера, принимающего
данные аудита.
