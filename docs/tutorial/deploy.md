# Создание кластера

В данном разделе приведена информация по развертыванию кластера Picodata
из нескольких инстансов. Описанный способ предназначен в первую очередь
для локального использования при разработке. О промышленной эксплуатации
читайте в разделе [Развертывание кластера через Ansible].

[Развертывание кластера через Ansible]: ../tutorial/deploy_ansible.md

## Файл конфигурации {: #config }

Для развертывания кластера используйте следующий файл конфигурации:

???+ example "my_cluster.md"
    ```yaml
    cluster:
      cluster_id: my_cluster
      tier:
        default:
          replication_factor: 2
          can_vote: true

    instance:
      peer:
      - 127.0.0.1:3301
      audit: false
      shredding: false
      tier: default
      log:
        level: info
        format: plain
        destination: null
      memtx:
        memory: 67108864
        checkpoint_count: 2
        checkpoint_interval: 3600.0
      vinyl:
        memory: 134217728
        cache: 134217728
    ```

Вы можете скопировать в файл текст выше или сгенерировать его командой
[picodata config default]. Полный перечень возможных параметров
конфигурации приведен в разделе [Описание файла конфигурации].

Для примера мы запустим кластер из 4 инстансов на локальном сетевом
интерфейсе `127.0.0.1`. Приведенный набор параметров явно задает имя
кластера "cluster_id", имя тира "default" и фактор репликации 2. В
данном примере файл конфигурации используется для запуска всех
инстансов.

[picodata config default]: ../reference/cli.md#config_default
[Описание файла конфигурации]: ../reference/config.md

## Скрипты инстансов {: #instance_scripts }

Создайте скрипты запуска для инстансов, указав в них путь к общему файлу
конфигурации (в примере он находится в той же директории, что и сами
скрипты), а также индивидуальные настройки:

???+ example "i1"
    ```shell
    #!/bin/bash

    export PICODATA_CONFIG_FILE="my_cluster.yml"

    export PICODATA_INSTANCE_ID="i1"
    export PICODATA_DATA_DIR="./data/my_cluster/i1"
    export PICODATA_LISTEN="127.0.0.1:3301"
    export PICODATA_HTTP_LISTEN="127.0.0.1:8080"
    export PICODATA_PG_LISTEN="127.0.0.1:5432"

    picodata run
    ```

??? example "i2"
    ```
    #!/bin/bash

    export PICODATA_CONFIG_FILE="my_cluster.yml"

    export PICODATA_INSTANCE_ID="i2"
    export PICODATA_DATA_DIR="./data/my_cluster/i2"
    export PICODATA_LISTEN="127.0.0.1:3302"

    picodata run
    ```

??? example "i3"
    ```
    #!/bin/bash

    export PICODATA_CONFIG_FILE="my_cluster.yml"

    export PICODATA_INSTANCE_ID="i3"
    export PICODATA_DATA_DIR="./data/my_cluster/i3"
    export PICODATA_LISTEN="127.0.0.1:3303"

    picodata run
    ```

??? example "i4"
    ```
    #!/bin/bash

    export PICODATA_CONFIG_FILE="my_cluster.yml"

    export PICODATA_INSTANCE_ID="i4"
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
## Кластер из нескольких тиров
-->

## Репликация и зоны доступности (failure domains) {: #failure_domains }

### Установка фактора репликации {: #replication_factor}

При создании кластера [количество реплик (инстансов)][rep_factor] в
репликасете определяется параметром запуска [--init-replication-factor]
или переменной `PICODATA_INIT_REPLICATION_FACTOR`. Указанное значение
присваивается всему кластеру при запуске первого инстанса и затем
хранится в виде параметра `replication_factor` в [файле
конфигурации](../reference/config.md).

Изменить фактор репликации для уже созданного
кластера нельзя.

[--init-replication-factor]: ../reference/cli.md#run_init_replication_factor
[--failure-domain]: ../reference/cli.md#run_failure_domain
[--advertise]: ../reference/cli.md#run_advertise
[rep_factor]: ../overview/glossary.md#replication_factor

<!-- Отредактировать фактор репликации, сохраненный в конфигурации кластера, можно командой `picodata set-replication-factor`. Редактирование конфигурации сказывается только на вновь добавляемых инстансах, но не затрагивает уже работающие. -->

### Использование зон доступности {: #setting_failure_domain}

По мере усложнения топологии возникает еще один вопрос — как не
допустить объединения в репликасет инстансов из одного и того же
датацентра. Для этого в Picodata имеется параметр [--failure-domain]
(переменная `PICODATA_FAILURE_DOMAIN`) — _зона доступности_, отражающая
признак физического размещения сервера, на котором выполняется инстанс
Picodata. Это может быть как датацентр, так и какое-либо другое
обозначение расположения: регион (например, `eu-east`), стойка, сервер,
или собственное обозначение (blue, green, yellow). Ниже показан пример
запуска инстанса Picodata в командной строке с указанием зоны
доступности:

```shell
picodata run --init-replication-factor 2 --failure-domain region=us,zone=us-west-1
```

С учетом этого параметра добавление инстанса в репликасет происходит так:

- если в каком-либо репликасете количество инстансов меньше необходимого
  фактора репликации, то новый инстанс добавляется в него при условии,
  что их параметры [--failure-domain] отличаются (регистр символов не
  учитывается)
- если подходящих репликасетов нет, то Picodata создает новый
  репликасет

Параметр [--failure-domain] играет роль только в момент добавления
инстанса в кластер. Принадлежность инстанса репликасету впоследствии не
меняется.

Как и параметр [--advertise], значение параметра [--failure-domain]
каждого инстанса можно редактировать, перезапустив инстанс с новыми
параметрами.

Добавляемый инстанс должен обладать, как минимум, тем же набором
параметров, которые уже есть в кластере. Например, инстанс `dc=msk` не
сможет присоединиться к кластеру с `--failure-domain region=eu/us` и
вернет ошибку.

Как было указано выше, сравнение зон доступности производится без учета
регистра символов, поэтому, к примеру, два инстанса с аргументами
`--failure-domain region=us` и `--failure-domain REGION=US` будут
относиться к одному региону и, следовательно, не попадут в один
репликасет.

## Удаление инстанса (expel) {: #expel }

Данная процедура позволяет исключить инстанс из состава кластера.

Если инстанс хранит сегменты шардированных данных, перед его удалением
данные будет автоматически перераспределены.

Для удаления инстанса из кластера потребуется пароль Администратора СУБД
(`admin`), который должен быть заранее установлен в консоли администратора:

```shell
picodata admin ./admin.sock
ALTER USER "admin" WITH PASSWORD 'T0psecret'
```

Для удаления инстанса из кластера используйте команду [picodata
expel](../reference/cli.md#expel):

```shell
picodata expel barsik --peer 192.168.0.1:3301
```

См. также:

- [Подключение и работа в консоли](connecting.md)
