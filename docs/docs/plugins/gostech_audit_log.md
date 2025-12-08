# Внешний аудит

В данном разделе приведена информация о плагине `gostech-audit-log`,
который предназначен для экспорта событий журнала аудита в Logstash.

!!! tip "Picodata Enterprise"
    Функциональность плагина `gostech-audit-log` доступна только в
    коммерческой версии Picodata.

### Схема взаимодействия с Picodata {: #outline }

Схема взаимодействия Picodata и плагина `gostech-audit-log` показана
ниже.

![](../images/gostech_audit_log.svg)

Задача плагина `gostech-audit-log` — разбор [журнала
аудита](../admin/audit_log.md) Picodata и отправка его на указанный
пользователем адрес.

### Запуск плагина {: #run }

Плагин `gostech-audit-log` запускается как еще один способ
вывода журнала.

Пример команды запуска:

```shell
picodata run --audit='| gostech-audit-log --url https://example.com'
```

В качестве `url` следует указать адрес внешнего сервера, принимающего
данные аудита.
