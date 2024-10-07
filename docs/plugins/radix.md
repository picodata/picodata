# Radix

В данном разделе приведены сведения о Radix, плагине для СУБД Picodata.

!!! tip "Picodata Enterprise"
    Функциональность плагина доступна только в коммерческой версии Picodata.

## Общие сведения {: #intro }

Radix — реализация [Redis](https://ru.wikipedia.org/wiki/Redis) на базе
Picodata, предназначенная для замены существующих инсталляций Redis.

Плагин Radix состоит из одноименного сервиса (`rаdix`), который
поднимает отдельный порт и на нем сервер, имплементирующий бинарный
протокол Redis. Проще говоря, этот сервис просто позволяет обращаться к
Picodata как к Redis. В кластерном развертывании порт и сервер Redis
поднимаются на каждом узле кластера, при этом каждый узел предоставляет
доступ ко всем данным кластера. Возможен как вариант развертывания со
100% сохранностью данных, репликацией и георезервированием, так и
развертывание без журналирования, в режиме кэша.

При использовании Picodata c плагином Radix нет необходимости в
отдельной инфраструктуре Redis Sentinel, так как каждый узел Picodata
выполняет роль прокси ко всем данным Redis.

## Установка {: #install }

### Предварительные действия {: #prerequisites }

Установка плагина Radix соответствует общей процедуре установки плагинов в Picodata.
<!-- вставить ссылку на туториал по плагинам, когда он будет залит -->

В частности, набор шагов включает:

- установку переменной окружения `RADIX_ADDR` для адреса Redis-сервера,
  например `RADIX_ADDR=0.0.0.0:7379`
- запуск инстанса Picodata с поддержкой плагинов (параметр [--plugin-dir])
- распаковка архива Radix в директорию, указанную на предыдущем шаге
- подключение к [административной консоли][admin_console] инстанса
- выполнение SQL-команд для регистрации плагина, привязки его сервиса к [тиру],
  выполнения миграции, включения плагина в кластере

[--plugin-dir]: ../reference/cli.md#run_plugin_dir
[admin_console]: ../tutorial/connecting.md#admin_console
[тиру]: ../overview/glossary.md#tier

### Подключение плагина {: #plugin_enable }

Выполните следующие SQL-команды в административной консоли Picodata:

```sql
CREATE PLUGIN radix 0.2.0;
ALTER PLUGIN radix 0.2.0 ADD SERVICE radix TO TIER default;
ALTER PLUGIN radix MIGRATE TO 0.2.0;
ALTER PLUGIN radix 0.2.0 ENABLE;
```

Чтобы убедиться в том, что плагин успешно добавлен и запущен, выполните запрос:

```sql
SELECT * FROM _pico_plugin
```

В строке, соответствующей плагину Radix, в колонке `enabled` должно быть значение `true`.

## Использование {: #usage }

В консоли для общения с Redis используется клиентская программа
`redis-cli.` Для подключения к инстансу Picodata по протоколу Redis
используйте адрес, заданный ранее в переменной `RADIX_ADDR`:

```shell
redis-cli -p 7379
```

## Поддерживаемые команды {: #supported_commands }

Полностью поддерживаемые команды:

    - cluster management:
        - ping
    - connection management:
        - select
    - generic:
        - del
        - exists
        - expire
        - keys
        - persist
        - scan
        - ttl
        - type
    - hash:
        - hdel
        - hexists
        - hget
        - hgetall
        - hincrby
        - hkeys
        - hlen
        - hscan
        - hset
    - string
        - get
        - set

Частично поддерживаемые команды:

    - cluster management:
        - auth (no-op, always returns ok)
    - scripting:
        - eval (can't call redis functions at the moment)
