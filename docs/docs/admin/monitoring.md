# Мониторинг

В данном разделе приведена информация о мониторинге запущенного кластера Picodata.

Мониторинг состояния кластера возможен после
[подключения](../tutorial/connecting.md) к кластеру в консоли. После этого можно
использовать команды, показывающие состояние raft-группы, отдельных
инстансов, собранных из них репликасетов и т.д.

## Получение лидера raft-группы {: #getting_raft_leader }

Узнать лидера raft-группы, а также ID и статус текущего инстанса:

```
pico.raft_status();
```

Пример вывода:

```
---
- term: 2
  leader_id: 1
  raft_state: Leader
  id: 1
...
```

## Получение состава raft-группы {: #getting_raft_group }

Просмотр состава raft-группы и данных инстансов:

```
box.space._pico_instance:fselect();
```

Пример вывода:

```
---
- - ​+-------------+--------------------------------------+-------+---------------+--------------------------------------+-------------+------------+--------------+
  - ​|instance_name|            instance_uuid             |raft_id|replicaset_name|           replicaset_uuid            |current_state|target_state|failure_domain|
  - ​+-------------+--------------------------------------+-------+---------------+--------------------------------------+-------------+--------------+--------------+
  - ​|   "i1"    |"68d4a766-4144-3248-aeb4-e212356716e4"|   1   |     "r1"      |"e0df68c5-e7f9-395f-86b3-30ad9e1b7b07"|["Online",1] |["Online",1]|      {}      |
  - ​|   "i2"    |"24c4ac5f-4981-3441-879c-aee1edb608a6"|   2   |     "r1"      |"e0df68c5-e7f9-395f-86b3-30ad9e1b7b07"|["Online",1] |["Online",1]|      {}      |
  - ​|   "i3"    |"5d7a7353-3e82-30fd-af0d-261436544389"|   3   |     "r2"      |"eff4449e-feb2-3d73-87bc-75807cb23191"|["Online",1] |["Online",1]|      {}      |
  - ​|   "i4"    |"826cbe5e-6979-3191-9e22-e39deef142f0"|   4   |     "r2"      |"eff4449e-feb2-3d73-87bc-75807cb23191"|["Online",1] |["Online",1]|      {}      |
  - ​+-------------+--------------------------------------+-------+---------------+--------------------------------------+-------------+------------+--------------+
...
```

Можно отдельно посмотреть список репликасетов, их UUID и вес:

```
box.space._pico_replicaset:fselect();
```

Пример вывода:

```
---
- — ​+---------------+--------------------------------------+-----------+------+
  - ​|replicaset_name|           replicaset_uuid            |master_name|weight|
  - ​+---------------+--------------------------------------+-----------+------+
  - ​|     "r1"      |"e0df68c5-e7f9-395f-86b3-30ad9e1b7b07"|   "i1"    |  1   |
  - ​|     "r2"      |"eff4449e-feb2-3d73-87bc-75807cb23191"|   "i3"    |  1   |
  - ​+---------------+--------------------------------------+-----------+------+
...
```

Таблицы выше позволяют узнать текущий и целевой уровень ([state](../overview/glossary.md#state))
каждого инстанса, а также вес (`weight`) репликасета. Уровни отражают
конфигурацию остальных инстансов относительно текущего, а вес
репликасета — его наполненность инстансами согласно фактору репликации
(см. [подробнее](../tutorial/deploy.md#failure_domains)). Вес
репликасета определяет его
приоритет при распределении бакетов с данными.

## Получение версии схемы данных {: #getting_data_schema }

Узнать текущую версию схему данных можно с помощью команды:

```
box.space._pico_property:get("current_schema_version");
---
- ['current_schema_version', 1]
...
```

Каждое изменение схемы данных в кластере приводит к
увеличению этого номера.

## Просмотр отладочного журнала {: #reading_log }

Способ просмотра отладочного журнала инстансов зависит от того, как был
запущен кластер Picodata.

Если при запуске инстанса командой `picodata run` в консоли не был явно
указан способ вывода отладочного журнала, то он выводится в ту же
консоль.

Если кластер развернут при помощи [файла конфигурации], то в нем вывод
отладочного журнала может быть настроен в файл или в службу `syslog`.
Этим поведением управляет параметр конфигурации
[`instance.log.destination`].

[`instance.log.destination`]: ../reference/config.md#instance_log_destination
[файла конфигурации]: ../reference/config.md#config_file_description

При развертывании кластера через Ansible отладочный журнал по умолчанию
выводится в службу `syslog`.

Для доступа к отладочному журналу:

- при выводе в файл используйте команду `tail -f` или любой другой
инструмент просмотра файлов
- при выводе журнала в `syslog` используйте команду `journalctl`

При использовании службы `syslog` для просмотра журнала конкретного
инстанса вам понадобится знать имя его сервиса. При развертывании через
Ansible оно по умолчанию формируется как
`<cluster_name>@<instance_name>.service`. Например, для кластера с именем
`test` и инстанса `default-1000` команда для просмотра отладочного
журнала будет такой:

```shell
journalctl -u test@default-1000.service
```

Пример вывода:

```
systemd[1]: Starting Picodata cluster test@default-1000...
systemd[1]: Started Picodata cluster test@default-1000.
picodata[4731]: 'cluster.name': "test"
picodata[4731]: 'cluster.tier': {"default": {"replication_factor": 3, "bucket_count": 3000, "can_vote": true}}
picodata[4731]: 'cluster.default_replication_factor': 1
picodata[4731]: 'instance.instance_dir': "/var/lib/picodata/test/default-1000"
picodata[4731]: 'instance.name': "default-1000"
picodata[4731]: 'instance.iproto_listen': "0.0.0.0:13301"
picodata[4731]: 'instance.http_listen': "0.0.0.0:18001"
picodata[4731]: 'instance.admin_socket': "/var/run/picodata/test/default-1000.sock"
picodata[4731]: [supervisor:4731] running StartDiscover
picodata[4733]: entering discovery phase
...
```

### Журнал raft-лидера {: #reading_raft_leader_log }

Отладочный журнал raft-лидера представляет особый интерес при
диагностике неполадок в кластере т.к. содержит важную информацию об
изменениях, происходящих с конфигурацией и топологией. Raft-лидером в
каждый момент времени является только один инстанс в кластере и именно
он управляет остальными. Для доступа к отладочному журналу raft-лидера
выполните следующие шаги.

Вычислите raft-лидера в кластере. Для этого [подключитесь](../tutorial/connecting.md) к
административной консоли любого инстанса:

```shell
picodata admin ./admin.sock
```

Переключите язык ввода на Lua и узнайте идентификатор текущего
raft-лидера:

```
\lua;
pico.raft_status();
```

Пример вывода:

```
---
- main_loop_status: idle
  leader_id: 2
  id: 1
  term: 30
  raft_state: Follower
...
```

C помощью полученного `leader_id` выясните адрес сервера, на котором
запущен этот инстанс:

```sql
\sql;
SELECT "name", "_pico_instance"."raft_id", "_pico_peer_address"."address"
FROM "_pico_instance" JOIN "_pico_peer_address"
ON "_pico_peer_address"."raft_id"="_pico_instance"."raft_id"
WHERE "_pico_instance"."raft_id" = 2;
```

Пример вывода:

```
+-------------------------+---------+--------------------+
| name                    | raft_id | address            |
+========================================================+
| "default-2000"          | 2       | "192.168.0.2:3301" |
+-------------------------+---------+--------------------+
(1 rows)
```

Дальнейшие шаги зависят от варианта запуска. При развертывании кластера
через Ansible потребуется подключиться к серверу по ssh и открыть
диагностический журнал через `journalctl`, см. [Просмотр отладочного
журнала](#reading_log).

## Метрики инстанса {: #instance_metrics }

Функциональность сбора метрик позволяет получать параметры работы СУБД
из инстанса Picodata и предоставлять доступ к ним для внешних систем в
формате Prometheus.

### Включение сбора метрик {: #enable_metrics }

Модуль сбора метрик автоматически включается при использовании
встроенного HTTP-сервера в Picodata. Для этого нужно при запуске
инстанса использовать параметр `--http-listen` и задать адрес
веб-сервера. Например:

```shell
picodata run --http-listen '127.0.0.1:8081'
```

См. также:

- Параметр конфигурации [instance.http.listen]

[instance.http.listen]: ../reference/config.md#instance_http_listen

### Метрики в консоли {: #curls_metrics }

Метрики инстанса Picodata можно получить в консоли, используя `curl`:

```shell
curl --location 'http://127.0.0.1:8081/metrics'
```

См. также:

- [Справочник метрик](../reference/metrics.md)

### Dashboard в Grafana {: #grafana }

#### Быстрый запуск в Docker {: #docker_compose }

Picodata предоставляет быстрый способ автоматически поднять
веб-интерфейс [Grafana] и получить доступ к дашбоарду, отображающему
метрики кластера. Для этого необходимо иметь в системе запущенную службу
[Docker] и консольную утилиту `docker-compose`.

В дереве исходного кода Picodata перейдите в директорию `monitoring` и
выполните команду:

```shell
docker-compose up -d
```
На локальном узле будет запущен [Prometheus], настроенный на получение
данных с адресов `127.0.0.1:8081`, `127.0.0.1:8082`, `127.0.0.1:8083`
(адреса и прочие настройки Prometheus можно изменить в файле
`monitoring/prometheus/prometheus.yml`).

Также, будет поднят сервер Grafana по адресу `127.0.0.1:3000` (порт
настраивается в файле `docker-compose.yml`). Для авторизации используйте
`admin`/`grafana`.

Импортируйте дашбоард Picodata из файла [Picodata.json].

#### Настройка Prometheus и Grafana вручную {: #manual_setup }

Перед настройкой Grafana сконфигурируйте и запустите систему мониторинга
событий и оповещений [Prometheus], которая является источником данных
для Grafana.

Установите и настройте Prometheus согласно рекомендациям для вашей ОС.
Укажите для Prometheus цели в файле `/etc/prometheus/prometheus.yml`:

```yaml
global:
  scrape_interval: 10s

scrape_configs:
  - job_name: 'prometheus'
    scrape_interval: 5s
    static_configs:
      - targets: ['127.0.0.1:9090']

  - job_name: 'picodata'
    scrape_interval: 5s
    metrics_path: /metrics
    static_configs:
      - targets: ['127.0.0.1:8081']
```

В приведенном примере:

- `127.0.0.1:9090` — адрес, с которого Prometheus будет отдавать метрики
- `127.0.0.1:8081` — адрес, с которого Prometheus собирает метрики
  (должен соответствовать параметру `--http-listen` при запуске инстанса
  Picodata или параметру [instance.http.listen] при использовании [файла конфигурации])

Под каждый инстанс Picodata нужно выделять отдельный адрес сбора метрик.
Например, если локально запустить 4 инстанса Picodata, то файл
конфигурации Prometheus может выглядеть так:

```yaml
global:
  scrape_interval: 10s

scrape_configs:
  - job_name: 'prometheus'
    scrape_interval: 5s
    static_configs:
      - targets: ['127.0.0.1:9090']

  - job_name: 'picodata'
    scrape_interval: 5s
    metrics_path: /metrics
    static_configs:
      - targets: ['127.0.0.1:8081', '127.0.0.1:8082', '127.0.0.1:8083', '127.0.0.1:8084']
```

После того как Prometheus сконфигурирован и запущен, установите и
настройте Grafana согласно рекомендациям для вашей ОС. Удостоверьтесь,
что Grafana работает, перейдя по адресу `127.0.0.1:3000`.

Далее выполните следующие шаги.

1.&nbsp;Убедитесь, что в настройках подключений в Grafana (`Connections` >
   `Data sources`) имеется источник данных Prometheus:

![Prometheus data source](../images/grafana/data_sources.png)

2.&nbsp;Импортируйте dashboard с данными Picodata. Для этого понадобится файл
   [Picodata.json], который следует добавить в меню `Dashboards` > `New` > `Import`:

![Import Picodata dashboard](../images/grafana/import_dashboard.png)

После этого в Grafana можно будет оперативно отслеживать состояние
инстансов, потребляемую память, нагрузку на сеть, изменения в составе
кластера и прочие параметры:

![Picodata dashboard](../images/grafana/dashboard.png)

[Docker]: https://www.docker.com
[Grafana]: https://grafana.com
[Prometheus]: https://prometheus.io
[Picodata.json]: https://git.picodata.io/core/picodata/-/tree/master/monitoring/dashboard
