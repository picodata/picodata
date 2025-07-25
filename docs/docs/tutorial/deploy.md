# Создание кластера

В данном разделе приведена информация по развертыванию кластера Picodata
из нескольких инстансов для разных сценариев.
Описанные способы предназначены в первую очередь
для локального использования при разработке. О промышленной эксплуатации
читайте в разделе [Развертывание кластера через Ansible](../admin/deploy_ansible.md).

## Общие сведения {: #basics }

### Имя кластера {: #cluster_name }

Кластер в Picodata может быть идентифицирован по [имени](../reference/cli.md#run_cluster_name), которое задается
администратором при запуске одного или нескольких инстансов. Имя
кластера необходимо инстансам для первоначальной сборки кластера
(bootstrap).

### Постоянные и динамические параметры {: #parameters_persistence }

Параметры кластера и инстансов могут быть заданы несколькими способами:

* с помощью параметров команды [`picodata run`](../reference/cli.md)
* через экспорт соответствующих переменных окружения в командной строке
* с помощью файлов конфигурации, в которых задаются нужные переменные окружения

В данном руководстве используется последний метод, при котором
параметры задаются в файле конфигурации.

См. также:

* [Конфигурирование](../admin/configure.md)

## Простой кластер {: #simple_cluster }

Для примера мы запустим кластер из 3 инстансов на локальном сетевом
интерфейсе `127.0.0.1`. Приведенный набор параметров явно задает имя
кластера "cluster_name" и фактор репликации 3.

### Файл конфигурации {: #simple_cluster_config }

Использование файла конфигурации — более удобный способ управления
настройками инстанса и кластера. Используйте образцы конфигурации для
трех инстансов в кластере, приведенные ниже, или сгенерируйте свой файл
конфигурации командой [`picodata config default`]. Полный перечень
возможных параметров конфигурации приведен в разделе [Описание файла
конфигурации].

[`picodata config default`]: ../reference/cli.md#config_default
[Описание файла конфигурации]: ../reference/config.md

Файл конфигурации для инстанса с именем `i1`:

???+ example "i1.yml"
    ```yaml
    cluster:
      name: my_cluster
      shredding: False

      tier:
        default:
          replication_factor: 3

    instance:
      instance_dir: './i1'
      name: 'i1'
      tier: 'default'
      peer: [ 127.0.0.1:3301 ]

      iproto_listen: '0.0.0.0:3301'
      iproto_advertise: '127.0.0.1:3301'
      http_listen: '0.0.0.0:8081'
      pg:
        listen: '0.0.0.0:4327'
        advertise: '127.0.0.1:4327'

      memtx:
        memory: 64M
    ```

---

Файл конфигурации для инстанса с именем `i2`:

???+ example "i2.yml"
    ```yaml
    cluster:
      name: my_cluster
      shredding: False

      tier:
        default:
          replication_factor: 3

    instance:
      instance_dir: './i2'
      name: 'i2'
      tier: 'default'
      peer: [ 127.0.0.1:3301 ]

      iproto_listen: '0.0.0.0:3302'
      iproto_advertise: '127.0.0.1:3302'
      http_listen: '0.0.0.0:8082'
      pg:
        listen: '0.0.0.0:4328'
        advertise: '127.0.0.1:4328'

      memtx:
        memory: 64M
    ```

---

Файл конфигурации для инстанса с именем `i3`:

???+ example "i3.yml"
    ```yaml
    cluster:
      name: my_cluster
      shredding: False

      tier:
        default:
          replication_factor: 3

    instance:
      instance_dir: './i3'
      name: 'i3'
      tier: 'default'
      peer: [ 127.0.0.1:3301 ]

      iproto_listen: '0.0.0.0:3303'
      iproto_advertise: '127.0.0.1:3303'
      http_listen: '0.0.0.0:8083'
      pg:
        listen: '0.0.0.0:4329'
        advertise: '127.0.0.1:4329'

      memtx:
        memory: 64M
    ```

!!! note "Примечание"
    Параметры в разделе `cluster` (имя кластера и
    фактор репликации) учитываются только при первоначальной сборке
    кластера и в дальнейшем не могут быть изменены. При добавлении новых
    узлов в уже работающий кластер эти параметры игнорируются.


### Запуск кластера {: #simple_cluster_start }

Запустите каждый из инстансов в отдельном окне терминала следующими
командами:

Запуск инстанса `i1`:

```shell
picodata run --config i1.yml
```

Запуск инстанса `i2`:

```shell
picodata run --config i2.yml
```

Запуск инстанса `i3`:

```shell
picodata run --config i3.yml
```

Полный перечень возможных параметров запуска и их описание содержатся в
разделе [Аргументы командной строки](../reference/cli.md). Приведенные в примере
параметры приведут к созданию кластера с веб-интерфейсом, доступным по адресам
[127.0.0.1:8081](http://127.0.0.1:8081), [127.0.0.1:8082](http://127.0.0.1:8082), [127.0.0.1:8083](http://127.0.0.1:8083).

Читайте далее:

- [Подключение и работа в консоли](../tutorial/connecting.md)
- [Развертывание кластера через Ansible](../admin/deploy_ansible.md)


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

Следующий пример показывает запуск кластера, состоящего из двух тиров —
`compute` и `storage`. Создайте отдельные файлы конфигурации для каждого из
тиров:

???+ example "compute-1.yml"
    ```yaml
    cluster:
      name: multi_tier_cluster
      tier:
        compute:
          replication_factor: 1
          bucket_count: 1500
          can_vote: true
        storage:
          replication_factor: 2
          bucket_count: 1500
          can_vote: false

    instance:
      tier: compute
      instance_dir: './compute-1'
      name: 'compute-1'
      peer:
      - 127.0.0.1:3301

      iproto_listen: '0.0.0.0:3301'
      iproto_advertise: '127.0.0.1:3301'
      http_listen: '0.0.0.0:8081'
      pg:
        listen: '0.0.0.0:4327'
        advertise: '127.0.0.1:4327'

      memtx:
        memory: 64M
    ```

??? example "storage-1.yml"
    ```yaml
    cluster:
      name: multi_tier_cluster
      tier:
        compute:
          replication_factor: 1
          bucket_count: 1500
          can_vote: true
        storage:
          replication_factor: 2
          bucket_count: 1500
          can_vote: false

    instance:
      tier: storage
      instance_dir: './storage-1'
      peer:
      - 127.0.0.1:3301

      iproto_listen: '0.0.0.0:3302'
      iproto_advertise: '127.0.0.1:3302'
      http_listen: '0.0.0.0:8082'
      pg:
        listen: '0.0.0.0:4328'
        advertise: '127.0.0.1:4328'

      memtx:
        memory: 1024M
    ```

??? example "storage-2.yml"
    ```yaml
    cluster:
      name: multi_tier_cluster
      tier:
        compute:
          replication_factor: 1
          bucket_count: 1500
          can_vote: true
        storage:
          replication_factor: 2
          bucket_count: 1500
          can_vote: false

    instance:
      tier: storage
      instance_dir: './storage-2'
      peer:
      - 127.0.0.1:3301

      iproto_listen: '0.0.0.0:3303'
      iproto_advertise: '127.0.0.1:3303'
      http_listen: '0.0.0.0:8083'
      pg:
        listen: '0.0.0.0:4329'
        advertise: '127.0.0.1:4329'

      memtx:
        memory: 1024M
    ```

!!! note "Примечание"
    Параметры в разделе `cluster` (имя кластера,
    состав тиров и их факторы репликации) учитываются только при
    первоначальной сборке кластера и в дальнейшем не могут быть
    изменены. При добавлении новых узлов в уже работающий кластер эти
    параметры игнорируются.


### Запуск кластера {: #tiered_cluster_start }

В данном примере создается один инстанс для тира `compute` и два
инстанса для тира `storage`. Инстансы тира `storage` образуют один
репликасет.

Запустите каждый из инстансов в отдельном окне терминала следующими
командами:

Запуск инстанса `compute-1`:

```shell
picodata run --config compute-1.yml
```

Запуск инстанса `storage-1`:

```shell
picodata run --config storage-1.yml
```

Запуск инстанса `storage-2`:

```shell
picodata run --config storage-2.yml
```

## Управление плагинами {: #plugin_management }

### Предварительные действия {: #prerequisites }

Плагины в Picodata могут работать как во всем кластере, так и в отдельных
[тирах]. Для запуска и работы плагина все инстансы тира должны быть запущены с
параметром [`share_dir`] в файле конфигурации.

??? example "Пример файла конфигурации с указанием директории плагинов"
    ```yaml
    cluster:
      name: my_cluster
      shredding: False

      tier:
        default:
          replication_factor: 3

    instance:
      instance_dir: './i1'
      name: 'i1'
      tier: 'default'
      share_dir: extra/plugins
      peer: [ 127.0.0.1:3301 ]

      iproto_listen: '0.0.0.0:3301'
      iproto_advertise: '127.0.0.1:3301'
      http_listen: '0.0.0.0:8081'
      pg:
        listen: '0.0.0.0:4327'
        advertise: '127.0.0.1:4327'

      memtx:
        memory: 64M
    ```

Для каждого инстанса в указанной директории должны
находиться файлы плагина, организованные в определенную иерархическую
структуру, например:

```
share_dir
└── weather_cache                 # название плагина
    └── 0.1.0                     # версия плагина
        ├── libweather_cache.so   # разделяемая библиотека плагина
        └── manifest.yaml         # манифест плагина
```

Плагины для Picodata поставляются сразу с правильной структурой файлов и
директорий: достаточно распаковать архив с плагином в `share_dir`.

См. также:

- [Пробный запуск плагина](create_plugin.md#plugin_test_run)

Если работа плагина требует объявления переменных или установки
адресов/портов, то их нужно указать индивидуально для каждого инстанса.
Если инстансы кластера/тира запускаются в одном пространстве имен (например,
на одном хосте), то эти параметры должны отличаться между собой ([пример]).

[тирах]: ../overview/glossary.md#tier
[`share_dir`]: ../reference/config.md#instance_share_dir
[пример]: ../plugins/radix.md#prerequisites


### Включение плагина {: #plugin_enable }

В общем случае, процедура включения плагина в кластере (или отдельном
тире) сводится к выполнению нескольких типовых действий от лица
администратора СУБД:

```sql title="Создание плагина"
CREATE PLUGIN <plugin_name> <plugin_version>;
```

```sql title="Добавление сервиса плагина к тиру"
ALTER PLUGIN <plugin_name> <plugin_version> ADD SERVICE <service_name> TO TIER <tier_name>;
```

```sql title="Запуск миграции плагина"
ALTER PLUGIN <plugin_name> MIGRATE TO <plugin_version>;
```

```sql title="Включение плагина"
ALTER PLUGIN <plugin_name> <plugin_version> ENABLE;
```

См. также:

- [Управление плагинами](plugins.md)

## Зоны доступности (домены отказа) {: #failure_domains }

### Использование зон доступности {: #setting_failure_domain}

_Зона доступности_ — дополнительный параметр
[`instance.failure_domain`], который может быть использован в файле
конфигурации инстанса для того, чтобы не допустить объединения в
репликасет инстансов из одного и того же датацентра.

Параметр [`instance.failure_domain`] отражает физическое размещение
сервера, на котором выполняется инстанс Picodata. Это может быть как
датацентр, так и какое-либо другое обозначение расположения: регион
(например, `eu-east`), стойка, сервер, или собственное обозначение
(blue, green, yellow).

Для установки зоны доступности добавьте в файл конфигурации инстанса
указанный выше параметр. Например:

```shell
...
instance:
  tier: storage
  instance_dir: './storage-2'
  failure_domain: { "DC":"['DC1']","HOST":"trn1" }
  peer:
  - 127.0.0.1:3301
...
```

[`instance.failure_domain`]: ../reference/config.md#instance_failure_domain

После запуска инстанса с таким файлом конфигурации, добавление инстанса
в репликасет произойдет так:

* если в каком-либо репликасете количество инстансов меньше необходимого
  [фактора
  репликации](../reference/config.md#cluster_default_replication_factor),
  то новый инстанс добавится в него при условии, что установленные для
  них значения [`instance.failure_domain`] отличаются (регистр символов
  не учитывается)
* если подходящих репликасетов нет, то Picodata создаст новый
  репликасет

Значение параметра [`instance.failure_domain`] играет роль только в
момент добавления инстанса в кластер. Принадлежность инстанса
репликасету впоследствии не меняется. Для изменения зоны доступности
следует остановить инстанс, отредактировать его файл конфигурации,
изменив значение [`instance.failure_domain`], и затем перезапустить
инстанс.

Добавляемый инстанс должен обладать, как минимум, тем же набором
параметров, которые уже есть в кластере. Например, инстанс `{ "DC":"['DC1']" }` не
сможет присоединиться к кластеру с зоной `{ "DC":"['DC1']","HOST":"trn1" }` и
вернет ошибку.

Как было указано выше, сравнение зон доступности производится без учета
регистра символов, поэтому, к примеру, два инстанса с зонами `{ "DC":"['DC1']" }`
и `{ "DC":"['dc1']" }` будут относиться к одному региону и, следовательно, не
попадут в один репликасет.

## Проверка работы кластера {: #check_status }

Для проверки работы кластера используйте команду [`picodata status`]. С
ее помощью можно узнать основную информацию о кластере, а также
состояние отдельных инстансов. Пример:

```shell
picodata status
Enter password for pico_service:
 CLUSTER NAME: my_cluster
 CLUSTER UUID: ba894c85-41cf-479f-90ff-114ae370792f
 TIER/DOMAIN: default

 name   state    uuid                                   uri
i1      Online   ce9870c3-e8f1-4ab3-88d4-c2ad10ef25ca   127.0.0.1:3301
i2      Online   6866d9fc-ad48-4ea1-a1e3-f35e2b9e60a9   127.0.0.1:3302
i3      Online   5d153ee3-dae3-4bea-a4de-06ac2cc4be22   127.0.0.1:3303
i4      Online   9560e6ad-445c-4334-a01f-02ee2f1fb6a8   127.0.0.1:3304
```

!!! note "Примечание"
    По умолчанию паролем системного пользователя `pico_service` является
    пустая строка.

[`picodata status`]: ../reference/cli.md#status
