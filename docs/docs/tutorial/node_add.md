# Добавление узлов

В данном разделе приведена информация о добавлении новых узлов в
существующий кластер Picodata.

## Идентификация кластера {: #cluster_traits }

Для идентификации кластера Picodata используется его [имя]. Данный
параметр задается администратором и используется только при
[первоначальной сборке] кластера. Впоследствии, при добавление к
работающему кластеру новых инстансов, указание имени кластера
игнорируется.

См. также:

- [Файл конфигурации](../reference/config.md#config_file_description)

## Идентификация узлов {: #instance_traits }

Узел — логическая единица кластера, представленная на программном
уровне [инстансом] (экземпляром приложения) Picodata.

Инстанс может быть идентифицирован по:

- [имени][instance_name] — удобочитаемому параметру, который задается
  администраторов при запуске инстанса и в дальнейшем может быть изменен
  (например, при перезапуске инстанса)
- [идентификатору][`_pico_instance`] — уникальному ключу в формате [UUID], который
  автоматически присваивается инстансу в момент присоединения к кластеру
  и в дальнейшем не может быть изменен

Имена и идентификаторы инстансов кластера содержатся в системной таблице
[`_pico_instance`].

[имя]: ../reference/cli.md#run_cluster_name
[инстансом]: ../overview/glossary.md#instance
[UUID]: https://en.wikipedia.org/wiki/Universally_unique_identifier
[`_pico_instance`]: ../architecture/system_tables.md#_pico_instance
[первоначальной сборке]: ../overview/glossary.md#bootstrap

## Обязательные параметры {: #essentials }

Для того, чтобы добавить инстанс к существующему кластеру, необходимо:

- знать [имя кластера] (предполагается, что оно известно администратору)
- знать [адрес] одного из действующих узлов кластера

[имя кластера]: ../reference/cli.md#run_cluster_name
[адрес]: ../reference/cli.md#run_peer

Указанные два параметра являются обязательными: без них подключение к
существующему кластеру невозможно.

## Дополнительные параметры {: #extras }

Дополнительно рекомендуется задать значения некоторых необязательных
параметров:

- [имя][instance_name] добавляемого инстанса
- его [рабочую директорию]
- его [сетевой адрес]

[instance_name]: ../reference/cli.md#run_instance_name
[рабочую директорию]: ../reference/cli.md#run_instance_dir
[сетевой адрес]: ../reference/cli.md#run_iproto_listen

Имя используется для удобства и быстрой идентификации инстанса. Если имя
не дать, то оно будет сгенерировано автоматически в момент добавления в
кластер на основе [`raft_id`] инстанса.

[`raft_id`]: ../architecture/system_tables.md#_pico_instance

Имя инстанса задается один раз и не может быть изменено в
дальнейшем (например, оно постоянно сохраняется в снапшотах инстанса). В
кластере нельзя иметь два инстанса с одинаковым именем — пока инстанс
живой, другой инстанс сразу после запуска получит ошибку при добавлении
в кластер. Тем не менее, имя можно повторно использовать, если
предварительно [исключить](node_expel.md#expel) первый инстанс с таким
именем из кластера.

## Использование набора файлов {: #file_set }

При запуске инстанса можно использовать как непосредственно одну
команду, в которой указаны необходимые параметры, так и набор из [файла
конфигурации](deploy.md#simple_cluster_config) и
[скрипта](deploy.md#simple_cluster_scripts) инстанса. Последний способ
более предпочтителен, так как позволяет удобнее контролировать параметры
запуска. Файл конфигурации удобен для хранения глобальных параметров,
которые впоследствии не будут меняться. Скрипт инстанса, напротив,
используется для хранения динамических параметров, которые можно будет
переопределить.

Предположим, что имеется ранее запущенный кластер с двумя [тирами][tier]
`compute` и `storage`, как описано в разделе [Создание
кластера](deploy.md#multi_tier_cluster). Добавим к нему еще один инстанс
для хранения данных. Для этого подготовим скрипт инстанса со
следующим содержанием:

!!! example "storage_3"
    ```shell
    #!/bin/bash

    export PICODATA_CONFIG_FILE="storage.yml"

    export PICODATA_INSTANCE_NAME="storage_3"
    export PICODATA_INSTANCE_DIR="./data/multi_tier_cluster/storage_3"
    export PICODATA_IPROTO_LISTEN="127.0.0.1:3304"
    export PICODATA_PG_LISTEN="127.0.0.1:5435"

    picodata run
    ```

Далее следует сделать этот скрипт исполняемым и запустить:

```shell
chmod +x storage_3
./storage_3
```

!!! note "Примечание"
    Если указанная в параметре `PICODATA_INSTANCE_DIR` директория не
    существует, она будет создана автоматически.

Если существующий кластер был запущен с использованием файла
конфигурации (например, `storage.yml` в тестовом [кластере из нескольких
тиров](deploy.md#multi_tier_cluster)), то желательно использовать его и
для запуска дополнительных инстансов. Также можно использовать параметры
из файла иным способом: объявив их через переменные окружения в
командной строке или указав в виде аргументов для [`picodata
run`](../reference/cli.md#run).

Минимальный набор параметров состоит из имени кластера и адреса одного
из узлов, например:

???+ example "storage.yml"
    ```yaml
    cluster:
      name: my_cluster

    instance:
      peer:
      - 127.0.0.1:3301
    ```

## Ограничения {: #limitations }

При добавлении узла к существующему кластеру нельзя переопределить
список [тиров][tier] и [фактор репликации]. Данные параметры задаются только
при [создании кластера](deploy.md).

[tier]: ../overview/glossary.md#tier
[фактор репликации]: ../overview/glossary.md#replication_factor

## Проверка присоединения {: #check }

После успешного присоединения к кластеру новый инстанс появится в
таблице [`_pico_instance`] в состоянии `Online`:

```sql
(admin) sql> SELECT "name","current_state" FROM _pico_instance;
+-------------+---------------+
| name        | current_state |
+=============================+
| "compute_1" | ["Online", 1] |
|-------------+---------------|
| "storage_1" | ["Online", 1] |
|-------------+---------------|
| "storage_2" | ["Online", 1] |
|-------------+---------------|
| "storage_3" | ["Online", 1] |
+-------------+---------------+
(4 rows)
```

См. также:

- [Удаление узлов](node_expel.md)
