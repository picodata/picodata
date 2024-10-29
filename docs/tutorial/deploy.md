# Создание кластера

В данном разделе приведена информация по развертыванию кластера Picodata
из нескольких инстансов для разных сценариев.
Описанные способы предназначены в первую очередь
для локального использования при разработке. О промышленной эксплуатации
читайте в разделе [Развертывание кластера через Ansible].

[Развертывание кластера через Ansible]: ../tutorial/deploy_ansible.md

## Простой кластер {: #simple_cluster }

### Файл конфигурации {: #simple_cluster_config }

Для развертывания кластера используйте следующий файл конфигурации:

???+ example "my_cluster.yml"
    ```yaml
    cluster:
      name: my_cluster
      default_replication_factor: 2

    instance:
      peer:
      - 127.0.0.1:3301
      audit: false
      shredding: false
      log:
        level: info
        format: plain
        destination: null
      memtx:
        memory: 64M
        checkpoint_count: 2
        checkpoint_interval: 3600.0
      vinyl:
        memory: 128M
        cache: 128M
    ```

Вы можете скопировать в файл этот образец или сгенерировать свой файл
конфигурации командой [picodata config default]. Полный перечень
возможных параметров конфигурации приведен в разделе [Описание файла
конфигурации].

Для примера мы запустим кластер из 4 инстансов на локальном сетевом
интерфейсе `127.0.0.1`. Приведенный набор параметров явно задает имя
кластера "cluster_name" и фактор репликации 2. В
данном примере файл конфигурации используется для запуска всех
инстансов.

[picodata config default]: ../reference/cli.md#config_default
[Описание файла конфигурации]: ../reference/config.md

### Скрипты инстансов {: #simple_cluster_scripts }

Создайте скрипты запуска для инстансов, указав в них путь к общему файлу
конфигурации (в примере он находится в той же директории, что и сами
скрипты), а также индивидуальные настройки:

???+ example "i1"
    ```shell
    #!/bin/bash

    export PICODATA_CONFIG_FILE="my_cluster.yml"

    export PICODATA_INSTANCE_NAME="i1"
    export PICODATA_DATA_DIR="./data/my_cluster/i1"
    export PICODATA_LISTEN="127.0.0.1:3301"
    export PICODATA_HTTP_LISTEN="127.0.0.1:8080"
    export PICODATA_PG_LISTEN="127.0.0.1:5432"

    picodata run
    ```

??? example "i2"
    ```shell
    #!/bin/bash

    export PICODATA_CONFIG_FILE="my_cluster.yml"

    export PICODATA_INSTANCE_NAME="i2"
    export PICODATA_DATA_DIR="./data/my_cluster/i2"
    export PICODATA_LISTEN="127.0.0.1:3302"

    picodata run
    ```

??? example "i3"
    ```shell
    #!/bin/bash

    export PICODATA_CONFIG_FILE="my_cluster.yml"

    export PICODATA_INSTANCE_NAME="i3"
    export PICODATA_DATA_DIR="./data/my_cluster/i3"
    export PICODATA_LISTEN="127.0.0.1:3303"

    picodata run
    ```

??? example "i4"
    ```shell
    #!/bin/bash

    export PICODATA_CONFIG_FILE="my_cluster.yml"

    export PICODATA_INSTANCE_NAME="i4"
    export PICODATA_DATA_DIR="./data/my_cluster/i4"
    export PICODATA_LISTEN="127.0.0.1:3304"

    picodata run
    ```

Полный перечень возможных параметров запуска и их описание содержатся в
разделе [Аргументы командной строки](../reference/cli.md). Запустите
один за другим инстансы в четырех окнах терминала. Приведенные в примере
параметры приведут к созданию кластера в рабочей директории
`./data/my_cluster` с веб-интерфейсом, доступным по адресу
[127.0.0.1:8080](http://127.0.0.1:8080).

Читайте далее:

- [Подключение и работа в консоли](../tutorial/connecting.md)
- [Развертывание кластера через Ansible](../tutorial/deploy_ansible.md)

<!--
TBD:
## Кластер на нескольких серверах
-->

## Кластер из нескольких тиров {: #multi_tier_cluster }

Тир — это группа инстансов, объединенных по функциональному назначению.

В рамках отдельных тиров данные
[шардируются](../overview/description.md#sharding) независимо друг от
друга. Для каждой шардированной таблицы определена принадлежность
конкретному тиру.

На каждом тире запускаются свои
[сервисы](../tutorial/plugins.md#services) плагинов.

Конфигурация инстансов (выделяемая память и т.д.) и фактор репликации
также настраивается на уровне тиров.

<!--
Круто было бы здесь иллюстрацию иметь.
-->

Набор тиров, равно как и принадлежность инстансов тирам, определяется на
момент [развертывания кластера][cluster_bootstrap] и в дальнейшем не
изменяется. Каждый инстанс принадлежит ровно одному тиру.

[cluster_bootstrap]: ../overview/glossary.md#bootstrap


#### Файлы конфигурации {: #multi_tier_cluster_config }

Следующий пример показывает запуск кластера, состоящий из двух тиров —
compute и storage. Создайте отдельные файлы конфигурации для каждого из
тиров:

???+ example "compute.yml"
    ```yaml
    cluster:
      name: multi_tier_cluster
      tier:
        compute:
          replication_factor: 1
          can_vote: true
        storage:
          replication_factor: 2
          can_vote: false

    instance:
      tier: compute
      peer:
      - 127.0.0.1:3301
      memtx:
        memory: 64M
    ```

??? example "storage.yml"
    ```yaml
    cluster:
      name: multi_tier_cluster
      tier:
        compute:
          replication_factor: 1
          can_vote: true
        storage:
          replication_factor: 2
          can_vote: false

    instance:
      tier: storage
      peer:
      - 127.0.0.1:3301
      memtx:
        memory: 1024M
    ```

#### Скрипты инстансов {: #multi_tier_cluster_scripts }

Создайте скрипты запуска для каждого из инстансов. В данном примере
создается один compute инстанс и 2 storage. Инстансы storage образуют
один репликасет.

???+ example "compute_1"
    ```shell
    #!/bin/bash

    export PICODATA_CONFIG_FILE="compute.yml"

    export PICODATA_INSTANCE_NAME="compute_1"
    export PICODATA_DATA_DIR="./data/multi_tier_cluster/compute_1"
    export PICODATA_LISTEN="127.0.0.1:3301"
    export PICODATA_HTTP_LISTEN="127.0.0.1:8080"
    export PICODATA_PG_LISTEN="127.0.0.1:5432"

    picodata run
    ```

??? example "storage_1"
    ```shell
    #!/bin/bash

    export PICODATA_CONFIG_FILE="storage.yml"

    export PICODATA_INSTANCE_NAME="storage_1"
    export PICODATA_DATA_DIR="./data/multi_tier_cluster/storage_1"
    export PICODATA_LISTEN="127.0.0.1:3302"

    picodata run
    ```

??? example "storage_2"
    ```shell
    #!/bin/bash

    export PICODATA_CONFIG_FILE="storage.yml"

    export PICODATA_INSTANCE_NAME="storage_2"
    export PICODATA_DATA_DIR="./data/multi_tier_cluster/storage_2"
    export PICODATA_LISTEN="127.0.0.1:3303"

    picodata run
    ```

## Зоны доступности (failure domains) {: #failure_domains }

### Использование зон доступности {: #setting_failure_domain}

_Зона доступности_ — дополнительный параметр [instance.failure_domain],
который может быть использован в [скрипте отдельного
инстанса](#multi_tier_cluster_scripts) для того, чтобы не допустить
объединения в репликасет инстансов из одного и того же датацентра.

Параметр [instance.failure_domain] отражает физическое размещение
сервера, на котором выполняется инстанс Picodata. Это может быть как
датацентр, так и какое-либо другое обозначение расположения: регион
(например, `eu-east`), стойка, сервер, или собственное обозначение
(blue, green, yellow).

Для установки зоны доступности добавьте в скрипт инстанса строку,
которая объявит значение переменной `PICODATA_FAILURE_DOMAIN`. Например:

```shell
export PICODATA_FAILURE_DOMAIN="region=us,zone=us-west-1"
```

После запуска инстанса посредством скрипта, содержащего такую строку,
добавление инстанса в репликасет произойдет так:

- если в каком-либо репликасете количество инстансов меньше необходимого
  [фактора репликации][rep_factor], то новый инстанс добавится в него
  при условии, что установленные для них значения
  `PICODATA_FAILURE_DOMAIN` отличаются (регистр символов не учитывается)
- если подходящих репликасетов нет, то Picodata создаст новый
  репликасет

Значение переменной `PICODATA_FAILURE_DOMAIN` играет роль только в
момент добавления инстанса в кластер. Принадлежность инстанса
репликасету впоследствии не меняется. Для изменения зоны доступности
следует остановить инстанс, отредактировать значение переменной
`PICODATA_FAILURE_DOMAIN` в скрипте и затем перезапустить скрипт.

Добавляемый инстанс должен обладать, как минимум, тем же набором
параметров, которые уже есть в кластере. Например, инстанс `dc=msk` не
сможет присоединиться к кластеру с зоной `region=eu/us` и
вернет ошибку.

Как было указано выше, сравнение зон доступности производится без учета
регистра символов, поэтому, к примеру, два инстанса с зонами `region=us`
и `REGION=US` будут относиться к одному региону и, следовательно, не
попадут в один репликасет.

[instance.failure_domain]: ../reference/config.md#instance_failure_domain
[rep_factor]: ../reference/config.md#cluster_default_replication_factor

## Удаление инстанса (expel) {: #expel }

Данная процедура позволяет исключить инстанс из состава кластера.

Если инстанс хранит сегменты шардированных данных, перед его удалением
данные будет автоматически перераспределены.

Для удаления инстанса из кластера потребуется пароль Администратора СУБД
(`admin`), который должен быть заранее установлен в консоли администратора:

```shell
picodata admin ./admin.sock
ALTER USER "admin" WITH PASSWORD 'T0psecret';
```

Для удаления инстанса из кластера используйте команду [picodata
expel](../reference/cli.md#expel):

```shell
picodata expel barsik --peer 192.168.0.1:3301
```

См. также:

- [Подключение и работа в консоли](connecting.md)
