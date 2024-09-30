# Создание кластера

В данном разделе приведена информация по развертыванию кластера Picodata
из нескольких инстансов для разных сценариев.
Описанные способы предназначены в первую очередь
для локального использования при разработке. О промышленной эксплуатации
читайте в разделе [Развертывание кластера через Ansible].

[Развертывание кластера через Ansible]: ../tutorial/deploy_ansible.md

## Простой кластер {: #simple_cluster }

### Запуск

#### Файл конфигурации {: #config }

Для развертывания простого кластера используйте следующий файл конфигурации:

???+ example "my_cluster.yml"
    ```yaml
    cluster:
      cluster_id: my_cluster
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
        memory: 67108864
        checkpoint_count: 2
        checkpoint_interval: 3600.0
      vinyl:
        memory: 134217728
        cache: 134217728
    ```

Вы можете скопировать в файл этот образец или сгенерировать свой файл
конфигурации командой [picodata config default]. Полный перечень
возможных параметров конфигурации приведен в разделе [Описание файла
конфигурации].

Для примера мы запустим кластер из 4 инстансов на локальном сетевом
интерфейсе `127.0.0.1`. Приведенный набор параметров явно задает имя
кластера "cluster_id" и фактор репликации 2. В
данном примере файл конфигурации используется для запуска всех
инстансов.

[picodata config default]: ../reference/cli.md#config_default
[Описание файла конфигурации]: ../reference/config.md

#### Скрипты инстансов {: #instance_scripts }

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
-->

## Кластер из нескольких тиров {: #tiered_cluster }

Тиры - функциональность, позволяющая управлять
физическим расположением шардированных таблиц.
В части хранения шардированных данных тир
представляет собой отдельную группу хранения.
Другими словами - для каждой шардированной таблицы
определена принадлежность конкретному тиру.
В свою очередь глобальные таблицы создаются на каждом
инстансе. Глобальные на тир таблицы отсутствуют.

<!--
Круто было бы здесь иллюстрацию иметь.
-->

Набор тиров и принадлежность
инстанса тиру определяется на момент деплоя кластера и в дальнейшем
не изменяется. Каждый инстанс принадлежит только одному тиру.


Также полезной может оказаться возможность переопределять некоторые из
глобальных параметров кластера на уровне тира. Например [replication_factor]. 

В этом разделе мы запустим кластер, состоящий из двух тиров
с именами "blue" и "red". Файл конфигурации и скрипты инстансов
отличаются от используемых на предыдущем шаге только в местах
относящимся к тирам.

<!--
Можно отдельным примером добавить пример с не дефолтным can_vote
-->

### Запуск

#### Файл конфигурации {: #tiered_config }

Тиры создаются только один раз на этапе [бутстрапа кластера][cluster_bootstrap] по информации
из [конфигурационного файла][config_file_description] и после этого не изменяются.
Описание тиров содержится в секции "cluster.tier". Подробную информацию
о параметрах настройки тиров можно найти в разделе
[Параметры тиров][config_file_section_tier].

В секции "cluster.tier" конфигурационного файла опишите необходимые тиры.
Для развертывания кластера используйте следующий файл конфигурации:

???+ example "tiered_cluster.md"
    ```yaml
    cluster:
      cluster_id: tiered_cluster
      tier:
        red:
          replication_factor: 2
        blue:
          replication_factor: 1

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
        memory: 67108864
        checkpoint_count: 2
        checkpoint_interval: 3600.0
      vinyl:
        memory: 134217728
        cache: 134217728
    ```


#### Скрипты инстансов {: #instance_scripts }

Создайте скрипты запуска для инстансов, указав в них путь к общему файлу
конфигурации (в примере он находится в той же директории, что и сами
скрипты), а также индивидуальные настройки. С помощью `PICODATA_TIER` нужно указать
к какому тиру будет принадлежать инстанс:

<!--
Криво двигается highlight по разделам (справа) при скроле.
Может связано с именем? (i1), сверху такое же.
-->

???+ example "i1"
    ```shell
    #!/bin/bash

    export PICODATA_CONFIG_FILE="my_cluster.yml"

    export PICODATA_INSTANCE_ID="red_i1"
    export PICODATA_TIER="red"

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

    export PICODATA_INSTANCE_ID="red_i2"
    export PICODATA_TIER="red"

    export PICODATA_DATA_DIR="./data/my_cluster/i2"
    export PICODATA_LISTEN="127.0.0.1:3302"

    picodata run
    ```

??? example "i3"
    ```
    #!/bin/bash

    export PICODATA_CONFIG_FILE="my_cluster.yml"

    export PICODATA_INSTANCE_ID="blue_i1"
    export PICODATA_TIER="blue"

    export PICODATA_DATA_DIR="./data/my_cluster/i3"
    export PICODATA_LISTEN="127.0.0.1:3303"

    picodata run
    ```

Про работу с шардированными таблицами подбробнее в разделе [SQL].

[tier_glossary]: ../overview/glossary.md#tier
[cluster_bootstrap]: ../overview/glossary.md#bootstrap
[config_file_description]: /reference/config/#config_file_description
[config_file_section_tier]: /reference/config/#cluster_tier_tier_can_vote
[local_config]: ../tutorial/deploy.md#config
[instance_scripts]: ../tutorial/deploy.md#instance_scripts


## Зоны доступности (failure domains) {: #failure_domains }

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
