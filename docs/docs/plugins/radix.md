# Radix

В данном разделе приведены сведения о Radix, плагине для СУБД Picodata.

!!! tip "Picodata Enterprise"
    Функциональность плагина доступна только в коммерческой версии Picodata.

## Общие сведения {: #intro }

Radix — реализация [Redis](https://ru.wikipedia.org/wiki/Redis) на базе
Picodata, предназначенная для замены существующих инсталляций Redis.

Плагин Radix состоит из одноименного сервиса (`radix`), реализующего
Redis на базе СУБД Picodata. Каждый экземпляр Radix открывает дополнительный
порт для подключения.

При использовании Picodata c плагином Radix нет необходимости в
отдельной инфраструктуре Redis Sentinel, так как каждый узел Picodata
выполняет роль прокси ко всем данным Redis.

## Установка {: #install }

### Предварительные действия {: #prerequisites }

Установка плагина Radix, в целом, соответствует общей процедуре
установки плагинов в Picodata, но имеет ряд особенностей.
<!-- вставить ссылку на туториал по плагинам, когда он будет залит -->

Процедура установки включает:

- установку адреса, который будет слушать Radix (например, `export RADIX_LISTEN_ADDR=0.0.0.0:7379`).
  Эта настройка также доступна для инвентарного файла Ansible. Если в одном пространстве имен
  (например, на одном хосте) запущено несколько инстансов Picodata, то нужно
  задать для них разные значения `RADIX_LISTEN_ADDR`.
- установку публичного адреса, который Radix будет использовать для кластерных команд
  (например, `export RADIX_ADVERTISE_ADDR=public.hostname.int:7379`). Этот адрес будет
  возвращаться клиентам (например, в `CLUSTER NODES`).
- установку у [тиров][tier], на которые предполагается развернуть
  плагин, 16384 [бакетов]. См. описание [bucket_count] и
  [default_bucket_count].
- запуск инстанса Picodata с поддержкой плагинов (параметр [`--share-dir`])
- распаковку архива Radix в директорию, указанную на предыдущем шаге
- подключение к [административной консоли][admin_console] инстанса
- выполнение SQL-команд для регистрации плагина, привязки его сервиса к [тиру][tier],
  выполнения миграции, включения плагина в кластере

[`--share-dir`]: ../reference/cli.md#run_share_dir
[admin_console]: ../tutorial/connecting.md#admin_console
[tier]: ../overview/glossary.md#tier
[бакетов]: ../overview/glossary.md#bucket
[bucket_count]: ../reference/config.md#cluster_tier_tier_bucket_count
[default_bucket_count]: ../reference/config.md#cluster_default_bucket_count

### Подключение плагина {: #plugin_enable }

Радикс поддерживает 16 баз данных, каждую из которых можно расположить на отдельном тире.
На одном тире можно разместить несколько баз данных. Ниже будут примеры для одного и двух тиров.

Для подключение плагина последовательно выполните следующие SQL-команды
в административной консоли Picodata.

```sql
CREATE PLUGIN radix 0.10.0;
```

#### Пример с двумя тирами (hot/cold) {: #plugin_enable_hotcold }

Для настройки миграций задайте значения для 16 параметров (по числу баз данных в Radix):

```sql
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_0='hot';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_1='hot';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_2='hot';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_3='hot';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_4='cold';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_5='cold';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_6='cold';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_7='cold';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_8='cold';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_9='cold';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_10='cold';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_11='cold';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_12='cold';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_13='cold';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_14='cold';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_15='cold';

ALTER PLUGIN radix 0.10.0 ADD SERVICE radix TO TIER hot;
ALTER PLUGIN radix 0.10.0 ADD SERVICE radix TO TIER cold;
```

Для выполнения миграции:

```sql
ALTER PLUGIN radix MIGRATE TO 0.10.0 OPTION(TIMEOUT=300);
```

Для включения плагина в кластере:

```sql title="Убедитесь, что задан адрес, который будет слушать Radix"
ALTER PLUGIN radix 0.10.0 ENABLE OPTION(TIMEOUT=30);
```

Чтобы убедиться в том, что плагин успешно добавлен и запущен, выполните запрос:

```sql
SELECT * FROM _pico_plugin;
```

В строке, соответствующей плагину Radix, в колонке `enabled` должно быть значение `true`.

#### Пример с одним тиром {: #plugin_enable_single }

Если в кластере используется только один тир `default`, настройка миграций будет выглядеть так:

```sql
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_0='default';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_1='default';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_2='default';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_3='default';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_4='default';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_5='default';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_6='default';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_7='default';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_8='default';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_9='default';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_10='default';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_11='default';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_12='default';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_13='default';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_14='default';
ALTER PLUGIN radix 0.10.0 SET migration_context.tier_15='default';
ALTER PLUGIN radix 0.10.0 ADD SERVICE radix TO TIER default;
```

## Настройка {: #configuration }

Для настройки плагина используйте файл конфигурации, который можно
применить к плагину с помощью [Picodata Pike] или [инвентарного файла
Ansible].

Пример файла конфигурации:

```yaml
clients:                 # ограничения клиентских соединений
    max_clients: 10000
    max_input_buffer_size: 1073741824
    max_output_buffer_size: 1073741824
cluster_mode: true       # какой флаг отдавать в команде `info cluster`
sentinel_enabled: false  # режим совместимости с Redis Sentinel
redis_compatibility:
    enabled_deprecated_commands: []
    enforce_one_slot_transactions: false
authorization_mode:
    state: Off
```

[Picodata Pike]: ../tutorial/create_plugin.md#pike_plugin_config_apply
[инвентарного файла Ansible]: ../admin/deploy_ansible.md#plugin_management

### Сетевые настройки {: #network_settings }

- `RADIX_LISTEN_ADDR` — Radix откроет сокет по указанному адресу и будет его слушать.
- `RADIX_ADVERTISE_ADDR` — Radix будет использовать этот адрес в кластерных и sentinel-командах.

!!! note
    Если процессы не изолированы (например, развёртывание произведено без использования Docker или Kubernetes), то
    необходимо указывать разные `RADIX_LISTEN_ADDR` на каждой ноде, чтобы избежать конфликтов портов.

### clients

#### max_clients

Максимальное количество клиентов, которые могут подключиться к одному
узлу Radix. При достижении максимального числа `max_clients` новые
соединения будут отклоняться, пока количество клиентов не станет снова
меньше `max_clients`. Отклоненные соединения увеличивают счетчик метрики
`rejected_connections` (пока доступна только через `INFO STATS`).

#### max_input_buffer_size

Максимальный размер входящего буфера. Если данный параметр у соединения
будет превышен, то соединение будет закрыто. Закрытые по этой причине
соединения увеличивают счетчик метрики
`client_query_buffer_limit_disconnections` (пока доступна только через
`INFO STATS`).

#### max_output_buffer_size

Максимальный размер исходящего буфера. Если данный параметр у соединения
будет превышен, то соединение будет закрыто. Закрытые по этой причине
соединения увеличивают счетчик метрики
`client_output_buffer_limit_disconnections` (пока доступна только через
`INFO STATS`).

### redis_compatibility

#### enabled_deprecated_commands

Список устаревших команд Redis через запятую, которые будут доступны при
работе с Radix.

#### enforce_one_slot_transactions

Проводить все SQL-транзакции принудительно в рамках одного слота Redis.
Данное поведение включено по умолчанию.

### cluster_mode

Данная настройка влияет только на вывод команды `info cluster`.

### sentinel_enabled

Включает режим совместимости с [Redis Sentinel](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/).

#### Пример миграции приложения на Radix {: #sentinel_topology }

В настройках приложения следует указывать `service_name` (`master_name`) и адрес сервера Redis Sentinel из одного тира.
Например, `service_name` — `cold_4`, а адрес Sentinel — `cold_4_1`, если топология кластера похожа на топологию ниже.

```yaml
# вариант топологии Радикса
hot:
  hot_1:
  - hot_1_1
  - hot_1_2
  hot_2:
  - hot_2_1
  - hot_2_2
cold:
  cold_1:
  - cold_1_1
  - cold_1_2
  cold_2:
  - cold_2_1
  - cold_2_2
  cold_3:
  - cold_3_1
  - cold_3_2
  cold_4:
  - cold_4_1
  - cold_4_2
```

### authorization_mode

Управляет состоянием авторизации в Радиксе. Возможные значения:

- `{ "state": "on", "default_user_name": "<str>" }` — авторизация включена, пользователь по умолчанию задан. Используется для клиентов, которые подключаются с помощью `AUTH password` без имени пользователя.
- `{ "state": "on" }` — авторизация включена, пользователь по умолчанию не задан. В этом случае команды `AUTH password` без имени пользователя работать не будут.
- `{ "state": "off" }` — авторизация выключена (значение по умолчанию).

#### Предустановленные роли {: #auth_roles }

- Глобальные:
  - `radix_reader` — доступ на чтение ко всем данным,
  - `radix_writer` — доступ на запись ко всем данным.
- Локальные для каждой БД:
  - `radix_reader_0` … `radix_reader_15`
  - `radix_writer_0` … `radix_writer_15`

#### Примеры использования {: #auth_examples }

##### Миграция с кластера с директивой `requirepass` {: #requirepass }

```sql
ALTER PLUGIN radix 0.10.0 SET radix.authorization_mode = '{ "state": "On", "default_user_name": "default_radix_user" }';
CREATE USER default_radix_user WITH PASSWORD 'S0m1Str2ngP3ssword';
GRANT radix_reader TO default_radix_user;
GRANT radix_writer TO default_radix_user;
```

```bash
$ redis-cli -p 7301
127.0.0.1:7301> GET abc
(error) NOAUTH Authentication required
127.0.0.1:7301> AUTH S0m1Str2ngP3ssword
OK
127.0.0.1:7301> SET abc 123
OK
```

##### Использование LDAP {: #ldapuser }

```sql
ALTER PLUGIN radix 0.10.0 SET radix.authorization_mode = '{ "state": "On", "default_user_name": "default_radix_user" }';
CREATE USER default_radix_user USING ldap;
GRANT radix_reader TO default_radix_user;
GRANT radix_writer TO default_radix_user;
```

##### Использование Argus для синхронизации пользователей {: #argus }

```yaml
argus:
  searches:
    - role: "radix_reader"
      base: "dc=example,dc=org"
      filter: "<filter>"
      attr: "cn"
    - role: "radix_writer"
      base: "dc=example,dc=org"
      filter: "<filter>"
      attr: "cn"
```

##### Разделение доступов по БД {: #access_separation }

```sql
ALTER PLUGIN radix 0.10.0 SET radix.authorization_mode = '{ "state": "On" }';

CREATE USER app_1_user WITH PASSWORD 'pwd1';
GRANT radix_reader_0 TO app_1_user;
GRANT radix_writer_0 TO app_1_user;

CREATE USER app_2_user WITH PASSWORD 'pwd2';
GRANT radix_reader_0 TO app_2_user;
GRANT radix_writer_2 TO app_2_user;

CREATE USER app_3_user WITH PASSWORD 'pwd3';
GRANT radix_reader_0 TO app_3_user;
GRANT radix_writer_5 TO app_3_user;
```

## Использование {: #usage }

Для работы с Radix используйте клиентскую программу `redis-cli`. Подключение осуществляется по адресу, заданному в `RADIX_ADVERTISE_ADDR` (или `RADIX_LISTEN_ADDR`, если публичный адрес не указан).

```bash
redis-cli -p 7301
```

## Поддерживаемые команды {: #supported_commands }

### Управление кластером {: #cluster_management }

#### cluster getkeysinslot {: #cluster_getkeysinslot }

```sql
CLUSTER GETKEYSINSLOT slot count
```

Возвращает набор ключей, которые, в соответствии со своими хэш-суммами,
относятся к указанному слоту. Второй аргумент ограничивает максимальное
количество возвращаемых ключей.

#### cluster keyslot {: #cluster_keyslot }

```sql
CLUSTER KEYSLOT key
```

Позволяет узнать, к какому хэш-слоту относится указанный в команде ключ.

#### cluster myid {: #cluster_myid }

```sql
CLUSTER MYID
```

Возвращает идентификатор текущего узла кластера (INSTANCE UUID).

#### cluster myshardid {: #cluster_myshardid }

```sql
CLUSTER MYSHARDID
```

Возвращает идентификатор текущего репликасета, в который входит текущий
узел кластера (REPLICASET UUID).

#### cluster nodes {: #cluster_nodes }

```sql
CLUSTER NODES
```

Возвращает информацию о текущем составе и конфигурации узлов кластера,
включая номера бакетов, относящихся к узлам.

#### cluster replicas {: #cluster_replicas }

```sql
CLUSTER REPLICAS node-id
```

Возвращает состав реплицированных узлов (т.е. состав репликасета)

#### cluster shards {: #cluster_shards }

```sql
CLUSTER SHARDS
```

Возвращает подробную информацию о шардах кластера.

#### cluster slots {: #cluster_slots }

```sql
CLUSTER SLOTS
```

Возвращает информацию о соответствии слотов инстансам кластера.

#### ping

```sql
PING [message]
```

Возвращает `PONG`, если аргумент не указан, в противном случае
возвращает строкой аргумент, который пришел. Эта команда полезна для:

- проверки того, живо ли еще соединение
- проверки способности сервера обслуживать данные — ошибка возвращается,
  если это не так (например, при загрузке из постоянного хранилища или
  обращении к устаревшей реплике)
- измерения задержки

### Управление соединениями {: #connection_management }

#### auth

```sql
auth password
```

Производит аутентификацию пользователем по умолчанию. Имя пользователя должно быть задано
в конфигурации плагина в параметре `default_user_name`.

```sql
auth username password
```

Производит аутентификацию выбранным пользователем.

#### reset

```sql
reset
```

Сбрасывает соединение в состояние по умолчанию:

- откатывается текущая транзакция, если она была открыта,
- сбрасываются наблюдения за ключами, которые раньше были установлены командой WATCH,
- если были открыты курсоры командами SCAN/HSCAN, то они закрываются,
- сбрасывается авторизация, потребуется её пройти заново.

#### select

```sql
SELECT index
```

Получение логической базы данных Redis с указанным нулевым числовым
индексом. Новые соединения всегда используют базу данных 0.

### Общие команды {: #general }

#### dbsize

```sql
DBSIZE
```

Возвращает количество ключей в базе данных

#### del

```sql
DEL key [key ...]
```

Удаляет указанные ключи. Несуществующие ключи игнорируются.

#### exists

```sql
EXISTS key [key ...]
```

Проверяет, существует ли указанный ключ `key` и возвращает число совпадений.
Например, запрос `EXISTS somekey somekey` вернет `2`.

#### expire

```sql
EXPIRE key seconds [NX | XX | GT | LT]
```

Устанавливает срок жизни (таймаут) для ключа `key` в секундах (TTL, time
to live). По истечении таймаута ключ будет автоматически удален. В
терминологии Redis ключ с установленным тайм-аутом часто называют
_волатильным_.

Тайм-аут будет сброшен только командами, которые удаляют или
перезаписывают содержимое ключа, включая DEL, SET и GET/SET. Это
означает, что все операции, которые концептуально изменяют значение,
хранящееся в ключе, не заменяя его новым, оставляют таймаут нетронутым.

#### expireat

```sql
EXPIREAT key unix-time-seconds [NX | XX | GT | LT]
```

Устанавливает срок жизни (таймаут) для ключа `key` подобно
[EXPIRE](#expire), но вместо оставшегося числа секунд (TTL, time to
live) использует абсолютное время Unix timestamp — число секунд,
прошедших с 01.01.1970. Если максимальное число секунд превышено (т.е.
дата отсчета находится ранее 01.01.1970), то ключ будет автоматически
удален.

Дополнительные параметры `EXPIREAT`:

- `NX` — установить срок жизни только если он не был ранее установлен
- `XX` — установить срок жизни только если ключ уже имеет ранее установленный срок
- `GT` — установить срок жизни только если он превышает ранее установленный срок
- `LT` — установить срок жизни только если он меньше ранее установленного срока

#### expiretime

```sql
EXPIRETIME key
```

Возвращает срок жизни (таймаут) ключа `key` в секундах согласно формату
Unix timestamp.

#### keys

```sql
KEYS pattern
```

Возвращает все ключи, соответствующие шаблону.

Поддерживаются шаблоны в стиле _glob_:

- `h?llo` соответствует hello, hallo и hxllo
- `h*llo` соответствует hllo и heeeello
- `h[ae]llo` соответствует hello и hallo, но не hillo
- `h[^e]llo` соответствует hallo, hbllo, ... но не hello
- `h[a-b]llo` соответствует hallo и hbllo

#### persist

```sql
PERSIST key
```

Удаляет существующий таймаут для ключа `key`, превращая его из непостоянного
(ключ с установленным сроком действия) в постоянный (ключ, срок действия
которого никогда не истечет, поскольку таймаут для него не установлен).

#### pexpire

```sql
PEXPIRE key milliseconds [NX | XX | GT | LT]
```

Устанавливает срок жизни (таймаут) для ключа `key` подобно
[EXPIRE](#expire), но в миллисекундах.

#### pexpireat

```sql
PEXPIREAT key unix-time-milliseconds [NX | XX | GT | LT]
```

Устанавливает срок жизни (таймаут) для ключа `key` подобно
[EXPIREAT](#expireat), но в миллисекундах.

#### pexpiretime

```sql
PEXPIRETIME key
```

Возвращает срок жизни (таймаут) ключа `key` подобно
[EXPIRETIME](#expiretime), но в миллисекундах.

#### pttl

```sql
PTTL key
```

Возвращает оставшееся время жизни ключа `key` подобно [TTL](#ttl), но в
миллисекундах.

#### scan

```sql
SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
```

Команда `SCAN` используется для инкрементного итерационного просмотра
коллекции элементов в выбранной в данный момент базе данных Redis.

#### ttl

```sql
TTL key
```

Возвращает оставшееся время жизни ключа `key`, для которого установлен
таймаут. Эта возможность интроспекции позволяет клиенту Redis проверить,
сколько секунд данный ключ будет оставаться частью набора данных.

Команда возвращает `-2`, если ключ не существует.

Команда возвращает `-1`, если ключ существует, но не имеет связанного с
ним истечения срока действия.

#### type

```sql
TYPE key
```

Возвращает строковое представление типа значения, хранящегося по адресу
ключа `key`. Могут быть возвращены следующие типы:

- `string`
- `list`
- `set`
- `zset`
- `hash`
- `stream`

#### unlink

```sql
UNLINK key [key ...]
```

Выполняет асинхронное удаление ключей. Работает точно также, как и `DEL`,
за исключением того, что фактическое удаление данных происходит в фоне.

Можно использовать для повышения отзывчивости приложения.

### Хэш-команды {: #hash }

#### hdel

```sql
HDEL key field [field ...]
```

Удаляет указанные поля из хэша, хранящегося по адресу ключа `key`.
Указанные поля, которые не существуют в этом хэше, игнорируются. Удаляет
хэш, если в нем не осталось полей. Если `key` не существует, он
рассматривается как пустой хэш, и эта команда возвращает `0`.

#### hexists

```sql
HEXISTS key field
```

Возвращает, является ли поле `field` существующим полем в хэше, хранящемся по
адресу ключа `key`.


#### hget

```sql
HGET key field
```

Возвращает значение, связанное с полем `field` в хэше, хранящемся по
адресу ключа `key`.

#### hgetall

```sql
HGETALL key
```

Возвращает все поля и значения хэша, хранящегося по адресу ключа `key`. В
возвращаемом значении за именем каждого поля следует его значение,
поэтому длина ответа будет в два раза больше размера хэша.

#### hincrby

```sql
HINCRBY key field increment
```

Увеличивает число, хранящееся в поле `field`, в хэше, хранящемся в ключе
`key`, на инкремент. Если ключ не существует, создается новый ключ,
содержащий хэш. Если поле не существует, то перед выполнением операции
его значение устанавливается в `0`.

Диапазон значений, поддерживаемых `HINCRBY`, ограничен 64-битными
знаковыми целыми числами.

#### hkeys

```sql
HKEYS key
```

Возвращает все имена полей в хэше, хранящемся по адресу ключа `key`.

#### hlen

```sql
HLEN key
```

Возвращает количество полей, содержащихся в хэше, хранящемся по адресу
ключа `key`.

#### hmget

```sql
HMGET key field [field ...]
```

Возвращает значения указанных полей из хеша.

#### hmset

```sql
HMSET key field value [field value ...]
```

Выставляет значения указанным полям для заданного хэша.

??? warning "Примечание"
    Вместо этой команды необходимо использовать команду `HSET`
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN RADIX 0.10.0 SET radix.redis_compatibility = '{ "enabled_deprecated_commands": ["hmset" ] }';
    ```

#### hscan

```sql
HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]
```

Работает подобно [SCAN](#scan), но с некоторым отличием: `HSCAN`
выполняет итерацию полей типа Hash и связанных с ними значений.

#### hset

```sql
HSET key field value [field value ...]
```

Устанавливает указанные поля в соответствующие им значения в хэше,
хранящемся по адресу ключа `key`.

Эта команда перезаписывает значения указанных полей, которые существуют
в хэше. Если ключ не существует, создается новый ключ, содержащий хэш.

#### hvals

```sql
HVALS key
```

Возвращает значения всех полей в хэше, хранящиеся по адресу ключа `key`.

### Команды для сортированных списков {: #ordered_sets }

??? warning "Примечание"
    Данный раздел документации в работе

Поддерживаемые команды:

- bzmpop
- bzpopmax
- bzpopmin
- zadd
- zcard
- zcount
- zdiff
- zdiffstore
- zincrby
- zinter
- zintercard
- zinterstore
- zlexcount
- zmpop
- zmscore
- zpopmax
- zpopmin
- zrandmember
- zrange
- zrangestore
- zrank
- zrem
- zremrangebylex
- zremrangebyrank
- zremrangebyscore
- zrevrank
- zscan
- zscore
- zunion
- zunionstore

Устаревшие, но всё ещё поддерживаемые, команды:

- zrangebylex
- zrangebyscore
- zrevrange
- zrevrangebylex
- zrevrangebyscore


### Команды для списков {: #lists }

#### blmove

```sql
BLMOVE source destination <LEFT | RIGHT> <LEFT | RIGHT> timeout
```

Работает аналогично [LMPOP](#lmpop), но с использованием блокировки.
Если исходный список (`source`) пуст, то команда будет ждать наполнения
списка в течение указанного в `timeout` времени (в секундах), и в случае
неудачи вернет ошибку. Если таймаут установить в `0`, то блокировка
будет бесконечной.

#### blmpop

```sql
BLMPOP timeout numkeys key [key ...] <LEFT | RIGHT> [COUNT count]
```

Работает аналогично [LMOVE](#lmove), но с использованием блокировки.
Если все указанные списки ключей пусты, то команда будет ждать
наполнения любого из них в течение указанного в `timeout` времени (в
секундах), и в случае неудачи вернет ошибку. Если таймаут установить в
`0`, то блокировка будет бесконечной.

#### blpop

```sql
BLPOP key [key ...] timeout
```

Работает аналогично [LPOP](#lpop), но с использованием блокировки.
Если указанный список пуст, то команда будет ждать его наполнения
в течение указанного в `timeout` времени (в секундах), и в случае
неудачи вернет ошибку. Если таймаут установить в `0`, то блокировка
будет бесконечной.

#### brpop

```sql
BRPOP key [key ...] timeout
```

Работает аналогично [RPOP](#lpop), но с использованием блокировки.
Поведение механизма блокировки аналогично таковому для [BLPOP](#blpop).

#### lindex

```sql
LINDEX key index
```

Возвращает элемент с указанным индексом (`index`) из списка, хранящегося
по указанному ключу `key`. Индекс `0` означает первый элемент списка,
`-1` — последний и т.д.

Примеры:

```sql
redis> LPUSH mylist "World"
(integer) 1
redis> LPUSH mylist "Hello"
(integer) 2
redis> LINDEX mylist 0
"Hello"
redis> LINDEX mylist -1
"World"
redis> LINDEX mylist 3
(nil)
redis>
```

#### linsert

```sql
LINSERT key <BEFORE | AFTER> pivot element
```

Вставляет в список, хранящийся по ключу `key`, элемент (`element`) до
(`BEFORE`) или после (`AFTER`) указанного другого элемента (`pivot`).
Если указан несуществующий ключ, то команда ничего не сделает. Если по
указанному ключу нет списка, то команда вернет ошибку.

Примеры:

```sql
redis> RPUSH mylist "Hello"
(integer) 1
redis> RPUSH mylist "World"
(integer) 2
redis> LINSERT mylist BEFORE "World" "There"
(integer) 3
redis> LRANGE mylist 0 -1
1) "Hello"
2) "There"
3) "World"
redis>
```

#### llen

```sql
LLEN key
```

Возвращает длину (количество элементов) списка, хранящегося по ключу
`key`. Если указан несуществующий ключ, то команда вернет `0`. Если по
указанному ключу нет списка, то команда вернет ошибку.

Примеры:

```sql
redis> LPUSH mylist "World"
(integer) 1
redis> LPUSH mylist "Hello"
(integer) 2
redis> LLEN mylist
(integer) 2
```

#### lmove

```sql
LMOVE source destination <LEFT | RIGHT> <LEFT | RIGHT>
```

Перемещает первый/последний элемент первого списка (`source`) в
начало/конец второго списка (`destination`).

Примеры:

```sql
redis> RPUSH mylist "one"
(integer) 1
redis> RPUSH mylist "two"
(integer) 2
redis> RPUSH mylist "three"
(integer) 3
redis> LMOVE mylist myotherlist RIGHT LEFT
"three"
redis> LMOVE mylist myotherlist LEFT RIGHT
"one"
redis> LRANGE mylist 0 -1
1) "two"
redis> LRANGE myotherlist 0 -1
1) "three"
2) "one"
redis>
```

#### lmpop

```sql
LMPOP numkeys key [key ...] <LEFT | RIGHT> [COUNT count]
```

Извлекает (и удаляет) один или несколько (`count`) элементов в начале
(`LEFT`) или в конце (`REFT`) из первого непустого
списка ключей (`key`) в перечне списков ключей.

Примеры:

```sql
redis> LMPOP 2 non1 non2 LEFT COUNT 10
(nil)
redis> LPUSH mylist "one" "two" "three" "four" "five"
(integer) 5
redis> LMPOP 1 mylist LEFT
1) "mylist"
2) 1) "five"
redis> LRANGE mylist 0 -1
1) "four"
2) "three"
3) "two"
4) "one"
redis> LMPOP 1 mylist RIGHT COUNT 10
1) "mylist"
2) 1) "one"
   2) "two"
   3) "three"
   4) "four"
redis> LPUSH mylist "one" "two" "three" "four" "five"
(integer) 5
redis> LPUSH mylist2 "a" "b" "c" "d" "e"
(integer) 5
redis> LMPOP 2 mylist mylist2 right count 3
1) "mylist"
2) 1) "one"
   2) "two"
   3) "three"
redis> LRANGE mylist 0 -1
1) "five"
2) "four"
redis> LMPOP 2 mylist mylist2 right count 5
1) "mylist"
2) 1) "four"
   2) "five"
redis> LMPOP 2 mylist mylist2 right count 10
1) "mylist2"
2) 1) "a"
   2) "b"
   3) "c"
   4) "d"
   5) "e"
redis> EXISTS mylist mylist2
(integer) 0
redis>
```

#### lpop

```sql
LPOP key [count]
```

Извлекает (и удаляет) указанное число (`count`) первых элементов,
хранящихся в списке по адресу ключа `key`. Без аргумента `count` команда
извлекает один первый элемент в начале списка.

Примеры:

```sql
redis> RPUSH mylist "one" "two" "three" "four" "five"
(integer) 5
redis> LPOP mylist
"one"
redis> LPOP mylist 2
1) "two"
2) "three"
redis> LRANGE mylist 0 -1
1) "four"
2) "five"
```

#### lpos

```sql
LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]
```

Возвращает индекс найденного в списке, хранящегося по ключу `key`,
элемента (`element`). Без дополнительных аргументов эта команда
просканирует список слева направо и вернет индекс первого найденного
элемента. Нумерация элементов начинается с `0`.

Пример:

```sql
> RPUSH mylist a b c 1 2 3 c c
> LPOS mylist c
2
```

Параметр `RANK` позволяет вывести другой (по счету `rank`) найденный
элемент в случае, если их несколько. Отрицательной значение `rank`
означает, что нумерация результата будет вестись справа налево.

Примеры:

```sql
> LPOS mylist c RANK 2
6
> LPOS mylist c RANK -1
7
```

Параметр `COUNT` позволяет вывести позиции всех (по счету `num-matches`)
найденных элементов.

Пример:

```sql
> LPOS mylist c COUNT 2
[2,6]
```

При совместном использовании `COUNT` и `RANK` можно изменить точку
отсчета, с которой будет производиться поиск совпадений.

Пример:

```sql
> LPOS mylist c RANK -1 COUNT 2
[7,6]
```

#### lpush

```sql
LPUSH key element [element ...]
```

Вставляет указанные элементы в начала списка, хранящегося по ключу
`key`.

Примеры:

```sql
redis> LPUSH mylist "world"
(integer) 1
redis> LPUSH mylist "hello"
(integer) 2
redis> LRANGE mylist 0 -1
1) "hello"
2) "world"
```

#### lpushx

```sql
LPUSHX key element [element ...]
```

Работает аналогично [LPUSH](#lpush), но проверяет, что указанный ключ
`key` существует. В противном случае команда ничего не делает (в отличие
от `LPUSH`).

#### lrange

```sql
LRANGE key start stop
```

Возвращает диапазон элементов списка, хранящегося по ключу `key`.
Позиция `start` обозначает начало диапазона, `stop` — его конец. При
указании отрицательных значений можно использовать диапазон, отсчитанный
справа налево. Нумерация элементов списка начинается с нуля.
Некорректный диапазон будет воспринят либо как пустой список (если
`start` превышает максимальный номер элемента), либо как корректный с
отсечением пустой части (если `stop` превышает максимальный номер
элемента). Следует учитывать, что при прямом отсчете элементов слева
направо значение `stop` будет включено в состав элементов. То есть,
диапазон `LRANGE list 0 10` будет содержать 11 элементов.

Примеры:

```sql
redis> RPUSH mylist "one"
(integer) 1
redis> RPUSH mylist "two"
(integer) 2
redis> RPUSH mylist "three"
(integer) 3
redis> LRANGE mylist 0 0
1) "one"
redis> LRANGE mylist -3 2
1) "one"
2) "two"
3) "three"
redis> LRANGE mylist -100 100
1) "one"
2) "two"
3) "three"
redis> LRANGE mylist 5 10
(empty array)
```

#### lrem

```sql
LREM key count element
```

Удаляет из списка, хранящегося по ключу `key`, указанное количество
(`count`) найденных элементов (`element`). Положительное значение `count`
означает поиск слева направо, отрицательное — справа налево. При
значении `0` будут удалены все найденные элементы.

Примеры:

```sql
redis> RPUSH mylist "hello"
(integer) 1
redis> RPUSH mylist "hello"
(integer) 2
redis> RPUSH mylist "foo"
(integer) 3
redis> RPUSH mylist "hello"
(integer) 4
redis> LREM mylist -2 "hello"
(integer) 2
redis> LRANGE mylist 0 -1
1) "hello"
2) "foo"
redis>
```

#### lset

```sql
LSET key index element
```

Устанавливает индекс (`index`) для добавляемого элемента (`element`).
Таким образом можно затереть один элемент списка и заменить его новым
значением.

Примеры:

```sql
redis> RPUSH mylist "one"
(integer) 1
redis> RPUSH mylist "two"
(integer) 2
redis> RPUSH mylist "three"
(integer) 3
redis> LSET mylist 0 "four"
"OK"
redis> LSET mylist -2 "five"
"OK"
redis> LRANGE mylist 0 -1
1) "four"
2) "five"
3) "three"
redis>
```

#### ltrim

```sql
LTRIM key start stop
```

Обрезает список, хранящийся по ключу `key`, задавая его размер с помощью
диапазона. При указании отрицательных значений можно использовать
диапазон, отсчитанный справа налево. Нумерация элементов списка
начинается с нуля. Некорректный диапазон будет воспринят либо как пустой
список (если `start` превышает максимальный номер элемента) c удалением
ключей, либо как корректный с увеличением границ списка (если `stop`
превышает максимальный номер элемента).

Типичное применение `LTRIM`:

```sql
LPUSH mylist someelement
LTRIM mylist 0 99
```

Эти команды добавят в список значение `someelement` и при этом установят
емкость списка равной 100 элементам.

Дополнительные примеры:

```sql
redis> RPUSH mylist "one"
(integer) 1
redis> RPUSH mylist "two"
(integer) 2
redis> RPUSH mylist "three"
(integer) 3
redis> LTRIM mylist 1 -1
"OK"
redis> LRANGE mylist 0 -1
1) "two"
2) "three"
redis>
```

#### rpop

```sql
RPOP key [count]
```

Извлекает (и удаляет) указанное число (`count`) последних элементов,
хранящихся в списке по адресу ключа `key`. Без аргумента `count` команда
извлекает один первый элемент в начале списка.

Примеры:

```sql
redis> RPUSH mylist "one" "two" "three" "four" "five"
(integer) 5
redis> RPOP mylist
"five"
redis> RPOP mylist 2
1) "four"
2) "three"
redis> LRANGE mylist 0 -1
1) "one"
2) "two"
```

#### rpush

```sql
RPUSH key element [element ...]
```

Работает аналогично [LPUSH](#lpush), но добавляет элементы в конец
списка.

#### rpushx

```sql
RPUSHX key element [element ...]
```

Работает аналогично [RPUSH](#rpush), но проверяет существование ключа
`key` и то, что этот ключ содержит список. В противном случае команда
ничего не делает.

### Команды управления подпиской (Pub/Sub) {: #pubsub }

Pub/Sub — механизм для отправки сообщений между клиентами через каналы.

#### psubscribe

```sql
PSUBSCRIBE pattern [pattern ...]
```

Подписывает клиента на получение данных согласно указанному шаблону (`pattern`). Примеры шаблонов:

- `h?llo` подписывает на _hello_, _hallo_ и _hxllo_
- `h*llo` подписывает на _hllo_ и _heeeello_
- `h[ae]llo`подписывает на _hello_ и _hallo_, но не _hillo_

#### publish

```sql
PUBLISH channel message
```

Размещает сообщение (`message`) в указанном канале (`channel`).
Сообщение будет доступно клиентам вне зависимости от того, к какому узлу
кластера они подключены.

#### pubsub channels  {: #pubsub_channels }

```sql
PUBSUB CHANNELS [pattern]
```

Выводит список активных каналов. Канал считается активным, если на него
есть хотя бы один подписчик (подписка на шаблоны (`pattern`) не
считается). Если в команде не указан шаблон (`pattern`), то будут
выведены все активные каналы. В противном случае будут выведены только
те активные каналы, которые соответствуют шаблону.

#### pubsub numpat {: #pubsub_numpat }

```sql
PUBSUB NUMPAT
```

Выводит список уникальных шаблонов, на которые были произведены подписки
со стороны клиентов (с помощью команды [PSUBSCRIBE](#psubscribe)). Не
следует путать вывод этой команды с общим числом клиентов.

#### pubsub numsub {: #pubsub_numsub }

```sql
PUBSUB NUMSUB [channel [channel ...]]
```

Выводит список всех подписчиков указанных каналов. Подписчики на шаблоны
(`pattern`) не считаются.

#### punsubscribe

```sql
PUNSUBSCRIBE [pattern [pattern ...]]
```

Отписывает клиента от указанных шаблонов. Если ни один канал (`pattern`)
не указан, то клиент будет отписан от всех шаблонов.

#### subscribe

```sql
SUBSCRIBE channel [channel ...]
```

Подписывает клиента на получение данных из указанных каналов
(`channel`). Cм. также [PSUBSCRIBE](#psubscribe).

#### unsubscribe

```sql
UNSUBSCRIBE [channel [channel ...]]
```

Отписывает клиента от указанных каналов. Если ни один канал (`channel`)
не указан, то клиент будет отписан от всех каналов.

### Команды для строк {: #string }

#### get

```sql
GET key
```

Получает значение ключа `key`. Если ключ не существует, возвращается
специальное значение `nil`. Если значение, хранящееся в ключе, не
является строкой, возвращается ошибка, поскольку `GET` работает только
со строковыми значениями.

#### getrange

```sql
GETRANGE key start end
```

Возвращает подстроку из значения, хранящегося по указанному ключу.
Границы подстроки определяют аргументами `start` и `end`.

#### incr

```sql
INCR key
```

Увеличивает значение, хранящееся по указанному ключу, на `1.` Если
указанный ключ не существует, то его значение
принимается за `0`.

#### incrby

```sql
INCRBY key increment
```

Увеличивает значение, хранящееся по указанному ключу, на величину
`increment`. Если указанный ключ не существует, то его значение
принимается за `0`.

#### incrbyfloat

```sql
INCRBYFLOAT key increment
```

Увеличивает значение, хранящееся по указанному ключу, на величину
`increment`, но при этом поддерживает дробные и отрицательные значения.
Если указанный ключ не существует, то его значение принимается за `0`.

#### psetex

```sql
PSETEX key milliseconds value
```

Устанавливает значение и срок жизни (таймаут) для ключа `key` подобно
[SETEX](#setex), но в миллисекундах.

??? warning "Примечание"
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN RADIX 0.10.0 SET radix.redis_compatibility = '{ "enabled_deprecated_commands": ["psetex" ] }';
    ```

#### set

```sql
SET key value [NX | XX] [GET] [EX seconds | PX milliseconds |
  EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]
```

Сохраняет строковое значение в ключе. Если ключ уже содержит значение,
оно будет перезаписано, независимо от его типа. Любое предыдущее
ограничение таймаута, связанное с ключом, отменяется при успешном
выполнении операции `SET`.

Параметры:

- `EX` — установка указанного времени истечения срока действия в
  секундах (целое положительное число)
- `PX` — установка указанного времени истечения в миллисекундах (целое
  положительное число)
- `EXAT` — установка указанного времени Unix, в которое истекает срок
  действия ключа, в секундах (целое положительное число)
- `PXAT` — установка указанного времени Unix, по истечении которого срок
  действия ключа истечет, в миллисекундах (целое положительное число)
- `NX` — установка значение ключа только в том случае, если он еще
  не существует
- `XX` — установка значение ключа только в том случае, если он уже
  существует
- `KEEPTTL` — сохранить время жизни, связанное с ключом
- `GET` — возвращает старую строку, хранящуюся по адресу ключа, или
  `nil`, если ключ не существовал. Возвращается ошибка и `SET`
  прерывается, если значение, хранящееся по адресу ключа `key`, не
  является строкой.

#### setex

```sql
SETEX key seconds value
```

Устанавливает для ключа `key` значение `value` и срок жизни (таймаут) в секундах.
Аналогичный результат достигается так:

```sql
SET key value EX seconds
```

??? warning "Примечание"
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN RADIX 0.10.0 SET radix.redis_compatibility = '{ "enabled_deprecated_commands": ["setex" ] }';
    ```

Установка некорректного значения вернет ошибку.

#### setnx

```sql
SETNX key value
```

Устанавливает для ключа `key` значение `value` только если такого ключа
ранее не было.

??? warning "Примечание"
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN RADIX 0.10.0 SET radix.redis_compatibility = '{ "enabled_deprecated_commands": ["setnx" ] }';
    ```

#### strlen

```sql
STRLEN key
```

Возвращает длину текстового значения, хранящегося по
указанному ключу.

### Команды для получения информации о Sentinel {: #sentinel }

Радикс поддерживает необходимый минимум команд для того, чтобы приложения
могли получать адреса серверов Пикодаты, если эти приложения написаны
с поддержкой Sentinel.

По умолчанию перечисленные ниже команды отключены. Для того, чтобы
включить, используйте запрос:

```sql
ALTER PLUGIN radix 0.10.0 set radix.sentinel_enabled = 'true';
```

#### sentinel get-master-addr-by-name {: #sentinel-get-master-addr-by-name }

```sql
SENTINEL GET-MASTER-ADDR-BY-NAME <replicaset name>
```

Возвращает адрес Радикса для заданного репликасета.

#### sentinel master {: #sentinel-master }

```sql
SENTINEL MASTER <replicaset name>
```

Выводит мастера для заданного репликасета. Радикс возвращает мастера репликасета с соотвествующим именем.

#### sentinel masters {: #sentinel-masters }

```sql
SENTINEL MASTERS
```

Возвращает список репликасетов, которые есть в системе.

#### sentinel myid {: #sentinel-myid }

```sql
SENTINEL MYID
```

Возвращает id текущего инстанса

#### sentinel replicas {: #sentinel-replicas }

```sql
SENTINEL REPLICAS <replicaset name>
```

Показывает список реплик для заданного репликасета.

#### sentinel sentinels {: #sentinel-sentinels }

```sql
SENTINEL SENTINELS <replicaset name>
```

Показывает список сентинелей для заданного репликасета. Радикс возвращает мастера репликасета с соотвествующим именем.

### Команды для скриптов {: #scripting }

Radix поддерживает следующие команды для работы с Lua-скриптами:

#### eval

```sql
EVAL script numkeys [key [key ...]] [arg [arg ...]]
```

Вызывает Lua-скрипт. Первый аргумент — исходный код Lua-скрипта
(`script`). Следующий за ним аргумент — количество передаваемых ключей
(`numkeys`) и далее сами ключи и их аргументы.

Пример:

```sql
> EVAL "return ARGV[1]" 0 hello
"hello"
```

Поддерживаемые скриптовые функции для `EVAL`:

- `redis.call(command) `— вызов команды Redis и вывод ее результата (при его наличии)
- `redis.pcall(command)` — аналог `redis.call()`, но с гарантированным возвратом ответа, что удобно для анализа ошибок команд
- `redis.log(level, message)` — запись в журнал инстанса сообщения с указанием уровня важности. Например, `redis.log(redis.LOG_WARNING, 'Something is terribly wrong')`
- `redis.sha1hex(x)` — возврат шестнадцатеричного SHA1-хэша для указанного числа _x_
- `redis.status_reply(x)` — возврат статуса состояния _x_ в виде строки
- `redis.error_reply` — возврат состояния _x_ в виде строки
- `redis.REDIS_VERSION` — возврат текущей версии Redis в виде строки в формате Lua
- `redis.REDIS_VERSION_NUM `— возврат текущей версии Redis в виде номера

#### evalro

```sql
EVAL_RO script numkeys [key [key ...]] [arg [arg ...]]
```

Вызывает Lua-скрипт аналогично [eval](#eval), но в режиме "только
чтение", т.е. без модификации данных БД.

#### evalsha

```sql
EVALSHA sha1 numkeys [key [key ...]] [arg [arg ...]]
```

Вызывает Lua-скрипт аналогично [eval](#eval), но в качестве аргумента
принимает не сам скрипт, а его SHA1-хэш из кэша скриптов.

#### evalsharo

```sql
EVALSHA_RO sha1 numkeys [key [key ...]] [arg [arg ...]]
```

Вызывает Lua-скрипт аналогично [evalsha](#evalsha), но в режиме "только
чтение", т.е. без модификации данных БД.

#### script exists {: #script_exists }

```sql
SCRIPT EXISTS sha1 [sha1 ...]
```

Возвращает информацию о существовании скрипта с указанным хэшем SHA1 в
кэше скриптов.

#### script load {: #script_load }

```sql
SCRIPT LOAD script
```

Загружает скрипт в кэш скриптов. Работает идемпотентно (т.е.
подразумевая, что такой скрипт уже есть в хранилище).

### Команды для транзакций {: #transactions }

#### discard

```sql
DISCARD
```

Удаляет все команды из очереди исполнения

#### exec

```sql
EXEC
```

Исполняет все команды в очереди в рамках единой транзакции.

#### multi

```sql
MULTI
```

Обозначает момент блокировки транзакции. Последующие команды будут
исполняться одна за другой при помощи [exec](#exec).

#### unwatch

```sql
UNWATCH key [key ...]
```

Удаляет все ключи из списка наблюдения [watch](#watch).

#### watch

```sql
WATCH key [key ...]
```

Включает проверку значений указанных ключей для последующих транзакций.

### Команды управления и диагностики {: #server }

#### flushall

```sql
FLUSHALL [ASYNC | SYNC]
```

Очищает все базы данных.

- SYNC: синхронно, т.е. команда вернёт управление только после полной очистки БД.
- ASYNC: асинхронно, команда вернёт управление быстрее, данные очистятся в фоне.

Если ни одна из опций не указана, используется режим SYNC

#### flushdb

```sql
FLUSHDB [ASYNC | SYNC]
```

Очищает текущую базу данных.

- SYNC: синхронно, т.е. команда вернёт управление только после полной очистки БД.
- ASYNC: асинхронно, команда вернёт управление быстрее, данные очистятся в фоне.

Если ни одна из опций не указана, используется режим SYNC

#### info

```sql
INFO [section [section ...]]
```

Возвращает информацию о сервере, подключенных клиентах, нагрузке на
текущий узел и прочую статистику. Параметр `section` позволяет уточнить
запрос, ограничив его нужной секцией. Доступные секции:

- `server`
- `clients`
- `memory`
- `persistence`
- `stats`
- `replication`
- `cpu`
- `modules`
- `cluster`
- `keyspace`
- `errorstats`
- `commandstats`

??? example "Образец вывода полного набора сведений"
    ```
    127.0.0.1:7379> info
    # Server
    radix_version:0.10.0
    picodata_version:25.2.1-19-g283377900
    picodata_cluster_name:my_cluster
    picodata_cluster_uuid:928ab3b9-fe7f-4aff-a740-2e6700fc2960
    redis_version:7.4.0
    redis_git_sha1:a50a984f18925124a58f41616408b9d32ba73f79
    redis_git_dirty:0
    redis_build_id:
    redis_mode:standalone
    os:Fedora Linux 6.14.6-300.fc42.x86_64 x86_64
    arch_bits:64
    monotonic_clock:POSIX clock_gettime with CLOCK_MONOTONIC
    multiplexing_api:epoll
    atomicvar_api:c11-builtin
    gcc_version:rustc 1.86.0 (05f9846f8 2025-03-31)
    process_id:2844495
    process_supervised:no
    run_id:f83b08c241514fe7bd9ffa8906896734
    tcp_port:7379
    server_time_usec:1750776554886845
    uptime_in_seconds:30
    uptime_in_days:0
    hz:3500
    configured_hz:0
    lru_clock:0
    executable:/usr/local/bin/picodata
    config_file:
    io_threads_active:1

    # Clients
    connected_clients:1
    cluster_connections:0
    maxclients:0
    client_recent_max_input_buffer:0
    client_recent_max_output_buffer:8192
    blocked_clients:0
    tracking_clients:0
    pubsub_clients:0
    watching_clients:0
    clients_in_timeout_table:0
    total_watched_keys:0
    total_blocking_keys:0
    total_blocking_keys_on_nokey:0

    # Memory
    used_memory:117440512
    used_memory_human:112.00M
    used_memory_rss:137404416
    used_memory_rss_human:131.04M
    used_memory_peak:117440512
    used_memory_peak_human:112.00M
    used_memory_peak_perc:100.00
    used_memory_overhead:83886080
    used_memory_startup:117440512
    used_memory_dataset:33554432
    used_memory_dataset_perc:28.57
    allocator_allocated:117440512
    allocator_active:117440512
    allocator_resident:137404416
    total_system_memory:33285476352
    total_system_memory_human:31.00G
    used_memory_lua:13287519
    used_memory_vm_eval:13287519
    used_memory_lua_human:12.67M
    used_memory_scripts_eval:0
    number_of_cached_scripts:0
    number_of_functions:0
    number_of_libraries:0
    used_memory_vm_functions:0
    used_memory_vm_total:13287519
    used_memory_vm_total_human:12.67M
    used_memory_functions:0
    used_memory_scripts:0
    used_memory_scripts_human:0B
    maxmemory:0
    maxmemory_human:0B
    maxmemory_policy:allkeys-lru
    allocator_frag_ratio:50.00
    allocator_frag_bytes:33554432
    allocator_muzzy:0
    allocator_rss_ratio:NaN
    allocator_rss_bytes:0
    mem_not_counted_for_evict:0
    mem_replication_backlog:0
    mem_total_replication_buffers:0
    mem_fragmentation_ratio:NaN
    mem_fragmentation_bytes:0
    mem_clients_normal:16384
    mem_allocator:slab
    active_defrag_running:0
    lazyfree_pending_objects:0
    lazyfreed_objects:0
    slab_info_items_size:2402512
    slab_info_items_used:401520
    slab_info_items_used_ratio:16.71
    slab_info_quota_size:67108864
    slab_info_quota_used:33554432
    slab_info_quota_used_ratio:50
    slab_info_arena_size:33554432
    slab_info_arena_used:4137072
    slab_info_arena_used_ratio:12.3

    # Persistence
    loading:0
    async_loading:0

    # Stats
    total_connections_received:1
    total_commands_processed:3
    instantaneous_ops_per_sec:0
    total_net_input_bytes:84
    total_net_output_bytes:880
    total_net_repl_input_bytes:0
    total_net_repl_output_bytes:0
    instantaneous_input_kbps:0.00
    instantaneous_output_kbps:0.03
    instantaneous_input_repl_kbps:0.00
    instantaneous_output_repl_kbps:0.00
    rejected_connections:0
    sync_full:0
    sync_partial_ok:0
    sync_partial_err:0
    expired_keys:0
    evicted_keys:0
    keyspace_hits:0
    keyspace_misses:0
    pubsub_channels:0
    pubsub_patterns:0
    latest_fork_usec:0
    migrate_cached_sockets:0
    unexpected_error_replies:0
    total_error_replies:0
    total_reads_processed:4
    total_writes_processed:3
    client_query_buffer_limit_disconnections:0
    client_output_buffer_limit_disconnections:0
    reply_buffer_expands:0
    reply_buffer_shrinks:0

    # Replication
    role:master
    connected_slaves:0
    master_failover_state:no-failover
    master_replid:f02d94ba-47fa-40b7-a48f-c33fb208dd0d
    master_replid2:f02d94ba-47fa-40b7-a48f-c33fb208dd0d
    master_repl_offset:82972
    second_repl_offset:82972
    repl_backlog_active:0
    repl_backlog_size:0
    repl_backlog_first_byte_offset: 0
    repl_backlog_histlen:0
    master_host:
    master_port:0
    master_link_status:down
    master_last_io_seconds_ago: 0
    master_sync_in_progress: 0
    slave_read_repl_offset:82972
    slave_repl_offset:82972
    slave_priority:0
    slave_read_only:0
    replica_announced:0
    master_sync_total_bytes: 0
    master_sync_read_bytes:0
    master_sync_left_bytes:0
    master_sync_perc:0
    master_sync_last_io_seconds_ago:0
    master_link_down_since_seconds:0
    min_slaves_good_slaves:0
    slave0: i1, , running, 0, 0

    # CPU
    used_cpu_sys:2.078520
    used_cpu_user:9.099032
    used_cpu_sys_children:0.000000
    used_cpu_user_children:0.000000
    used_cpu_sys_main_thread:1.016605
    used_cpu_user_main_thread:8.037833

    # Modules

    # Errorstats

    # Cluster
    cluster_enabled:1

    # Keyspace

    # Commandstats
    cmdstat_info:calls=1,usec=154,usec_per_call=0,rejected_calls=0,failed_calls=0

    # Sentinel
    sentinel_masters:4
    sentinel_tilt:0
    sentinel_tilt_since_seconds:0
    sentinel_running_scripts:0
    sentinel_scripts_queue_length:0
    sentinel_simulate_failure_flags:0
    ```

#### memory usage {: #memory_usage }

```sql
MEMORY USAGE key [SAMPLES count]
```

Показывает объем ОЗУ, занимаемый указанным ключом `key`. Параметр
`SAMPLES` позволяет указать число дочерних элементов ключа (если такие
имеются), объем которых также будет учтен. По умолчанию, значение
`SAMPLES` равно 5. Для учета всех дочерних элементов следует указать
`SAMPLES 0`.

## Журнал изменений {: #changelog }

### 0.10.0 - 2025-09-02 {: #0.10.0 }

#### Новая функциональность {: #0.10.0-novaia-funktsional-nost }

- FLUSHDB должен очищать и ordered set спейсы
- By default auth should be disabled
- Remove chrono
- Implement auth method
- Implement mset
- Add sentinel section to info
- Introduce sentinel
- Introduce ordered sets
- Implement flush && flushall
- Split RADIX\_ADDR into listen/advertise
- Add hmget/hmset, readonly, reset, unlink cmds

#### Исправления {: #0.10.0-ispravleniia }

- Явно прописываем WAIT APPLIED LOCALLY для асинхронного FLUSHDB
- Pubsub should work with su
- Обрабатывать ошибку "доступа нет" от ядра
- Remove key type if the last list element was popped
- Do not block in scripts
- Rename timeout func
- Reschedule list lock remove action if there are pending actions
- Use defer
- Remove locks if command went into a timeout (only blpop rn)
- Do not reverse values and scores for zscan, add tests for new behaviour
- Zscan
- Fail on decode bucket in \`get\_buckets\`
- Final fixes in auth method
- Create type on zdiffstore
- Use instance name for replica
- Use the original redis error for evalsha
- Close conn on client handle return
- Use box.info.replication to check replication status in INFO REPLICATION
- Use cas for patsub dml ops

#### Производительность {: #0.10.0-proizvoditel-nost }

- :zap: move debug logs to debug and raise default level to info

#### Документация {: #0.10.0-dokumentatsiia }

- Авторизация, описание и примеры использования
- Исправим документацию по сентинелю
- Добавим документацию конфигурацию клиента sentinel
- Добавим скрипт для автоматического обновления списка поддерживаемых команд

#### Внутренние улучшения {: #0.10.0-vnutrennie-uluchsheniia }

- Переименуем миграции, чтобы была нумерация последовательная
- Stringify the field name of a stat for a subtraction warning

#### Тестирование {: #0.10.0-testirovanie }

- Перепишем тесты с шелла и пайпов на psql

#### Прочее {: #0.10.0-prochee }

- Remove docs from gamayun scan
- Обновим список поддерживаемых команд
- Review fix
- Добавим лицензию в файлы
- Удалим docker-compose.yml из анализа гамаюна
- License update
- Remove unneeded dirs for gamayun
- :page\_facing\_up: опечатку исправим
- Add license-check job
- :page\_facing\_up: добавил лицензию на плагин
- Do not build in ci pipeline, block tests, until linting is done

#### Build {: #0.10.0-build }

- Remove PIKE\_DATA\_DIR
- Remove TARGET\_ROOT, remove unused parameters in the cluster config
- Pack old migrations, use new migrations locally
- Update dependencies
- Update the makefile to use new pike features

#### Deps {: #0.10.0-deps }

- Обновим пикодату до 25.3.2

### 0.9.0 - 2025-06-25 {: #0.9.0 }

#### Новая функциональность {: #0.9.0-novaia-funktsional-nost }

- Watch empty keys too
- Add picodata's cluster\_name and cluster\_uuid to server info
- Add a config option to enforce same-slot transactions

#### Исправления {: #0.9.0-ispravleniia }

- Handle all commands in transactions, even if they have no bucket\_id
- TYPE должна возвращать "none" на ключах, которых нет
- Don't panic on empty del cmd call
- Conn dead lock while receive on drop

#### Производительность {: #0.9.0-proizvoditel-nost }

- Implement partial list deserialization

#### Внутренние улучшения {: #0.9.0-vnutrennie-uluchsheniia }

- :arrow\_up: запускаю cargo update для обновления токио
- :arrow\_up: обновляю picodata-plugin до 25.2.2
- :rotating\_light: будем в бенчмарке использовать crypto/rand вместо math/rand

#### Тестирование {: #0.9.0-testirovanie }

- :adhesive\_bandage: грязный трюк с прогоном теста на тире с 1 репликасетом
- Увеличиваем таймаут в тесте пабсаба
- :adhesive\_bandage: добавить небольшой таймаут после старта кластера, чтобы он закончил с ребалансом

#### Прочее {: #0.9.0-prochee }

- Fetch tags for gamayun
- Останавливаю кластер перед тем, как забрать артефакты
- Пробуем запустить тесты ещё и на альте
- Передадим прошлую версию в Гамаюн
- :heavy\_minus\_sign: удаляю неиспользуемые dev-dependencies
- Wait for quality gate
- Подставим версию 3 в Cargo.lock, ничего не сломается.
- Удалим пароль для админского юзера из топологии
- :coffin: удалим старые неиспользуемые луашки
- :construction\_worker: добавляю Гамаюна

### 0.8.0 - 2025-06-04 {: #0.8.0 }

#### Новая функциональность {: #0.8.0-novaia-funktsional-nost }

- Add more script telemetry

#### Исправления {: #0.8.0-ispravleniia }

- Don't panic when downstream is not in the follow state or upstream's fiber is invalid

#### Документация {: #0.8.0-dokumentatsiia }

- Add redis\_compatibility user documentation

### 0.7.0 - 2025-05-28 {: #0.7.0 }

#### Новая функциональность {: #0.7.0-novaia-funktsional-nost }

- Add deprecated set commands
- Use non-blocking variants of commands in transactions
- Implement more expire commands
- Add expiretime
- Make scripts transactional
- Implement hvals

#### Исправления {: #0.7.0-ispravleniia }

- Correctly close client connections on listener drop
- :bug: используем правильный запрос для получения информации о репликасете
- Rust 1.87.0
- Pop crash

#### Документация {: #0.7.0-dokumentatsiia }

- Создаем тикет на обновление доков радикса, а не аргуса
- Обновим пользовательскую документацию

#### Прочее {: #0.7.0-prochee }

- :arrow\_up: обновим пикодатный плагин до 25.2.1
- 🔨 обновим редис до 8.0 в кластере для тестов
- :construction\_worker: при релизе создаём тикет в пикодату на обновление документации
- :bug: исправим пути файлов в релизе и приложим файл от бендера всегда

#### Build {: #0.7.0-build }

- :arrow\_up: picodata 25.1.2

### 0.6.1 - 2025-04-28 {: #0.6.1 }

#### Документация {: #0.6.1-dokumentatsiia }

- :memo: обновим документацию

#### Прочее {: #0.6.1-prochee }

- :construction\_worker: вернём redos

### 0.6.0 - 2025-04-18 {: #0.6.0 }

#### Новая функциональность {: #0.6.0-novaia-funktsional-nost }

- ✨ add functions to the redis object in lua scripts, add SCRIPT LOAD, SCRIPT EXISTS commands
- Use custom radix string type
- Use negative indexing for lists
- Introduce transactions

#### Исправления {: #0.6.0-ispravleniia }

- :bug: удалим падающую миграцию
- Command processed metric
- Deadlocks for blocking commands on same bucket
- :bug: бакеты из другого тира всегда удалённые
- :technologist: исправим упаковку релиза после мёржа пики
- :technologist: исправим сообщение
- Mem stat
- 🚑 fix the plugin file layout for pike
- :ambulance: provide replication\_factor setting in picodata.yaml

#### Производительность {: #0.6.0-proizvoditel-nost }

- Try to optimize list op

#### Документация {: #0.6.0-dokumentatsiia }

- Опишем конфигурацию миграций в пользовательской документации

#### Внутренние улучшения {: #0.6.0-vnutrennie-uluchsheniia }

- :loud\_sound: поправим сообщение для лога, в случае ошибки

#### Прочее {: #0.6.0-prochee }

- :fire: удалить лишние файлы
- Fix path to cargo2junit
- Fix clippy format warnings
- :rotating\_light: rust 1.86.0
- :construction\_worker: используем новые образа для упаковки
- :construction\_worker: поправим бендера в мейне
- :construction\_worker: добавим новые ОС в процесс сборки
- :hammer: положим редис-кластер в репу с командой для запуска
- 🩹 match the topology with the main branch's, move env variables to topology.toml

#### Build {: #0.6.0-build }

- Используем пайк 2.1.0 для билда
- :adhesive\_bandage: сделал по два репликасета на каждый тир как и в оригинальном кластере
- :arrow\_up: introduce pike 2.0.0

### 0.5.2 - 2025-03-19 {: #0.5.2 }

#### Прочее {: #0.5.2-prochee }

- :technologist: добавим отлаженного Бендера

### 0.5.1 - 2025-03-13 {: #0.5.1 }

#### Исправления {: #0.5.1-ispravleniia }

- :adhesive\_bandage: проверяем на андерфлоу при вычитании на статистике

#### Производительность {: #0.5.1-proizvoditel-nost }

- :zap: если бакет локальный, то не ходить по рпц

#### Документация {: #0.5.1-dokumentatsiia }

- :memo: исправим разметку в readme

#### Тестирование {: #0.5.1-testirovanie }

- :construction\_worker: исправим \`make test\_ci\`, чтобы совпадало с реальностью
- :white\_check\_mark: переведём бенч на кластерный клиент

#### Прочее {: #0.5.1-prochee }

- :construction\_worker: отсылаем нотификацию о релизе в спецчат в телеге

### 0.5.0 - 2025-03-06 {: #0.5.0 }

#### Новая функциональность {: #0.5.0-novaia-funktsional-nost }

- :sparkles: реализуем новую команду \`dbsize\` для проверки состояния кластера
- :building\_construction: используем CRC16/XMODEM для сегментирования
- :construction\_worker: теперь паники будут в файловых логах
- Allow multitier mode
- Eval

#### Исправления {: #0.5.0-ispravleniia }

- Deadlock on single mode for blocking ops
- :bug: cluster getkeysinslot исправлена
- :bug: используем UUID ноды и репликасета в ответе на myid, myshardid
- :card\_file\_box: fix migrations
- Tests data cleanup
- Eval ptr propagation
- Eval ptr propagation
- Parse timeout arguments to f64 not i64

#### Документация {: #0.5.0-dokumentatsiia }

- :memo: обновим документацию для пользователя
- :memo: ADR для мультитирного (многорядного?) радикса

#### Внутренние улучшения {: #0.5.0-vnutrennie-uluchsheniia }

- :recycle: зафиксируем, что работаем только с 16384 бакетами
- :recycle: вынесем \`RedisBucketId\` и \`PicodataBucketId\` в отдельные файлы
- :recycle: режим выполнения команды не отделим от бакета выполнения команды
- :recycle: типизируем айдишники бакетов
- :recycle: переименовываем ID в Name, потому что мы использовали имена
- :recycle: адаптировал запуск к запуску в нескольких тирах

#### Структура кода {: #0.5.0-struktura-koda }

- :rotating\_light: отформатировал код
- :recycle: поправил комменты и ошибку к методу insert\_patsubscriber

#### Тестирование {: #0.5.0-testirovanie }

- :white\_check\_mark: исправил тест на cluster nodes

#### Прочее {: #0.5.0-prochee }

- :construction\_worker: переедем на образ с явно выставленным стабильным растом
- :green\_heart: укажем полный путь до cargo2junit, пока его нет в базовом образе
- Add warn log for attempting sub from 0 value to stat macro
- :construction\_worker: поправим пути к карго
- :construction\_worker: попробуем новый базовый образ для пикодаты
- Fix lints for rust 1.85
- Rename radix nodes migration in manifest
- Add deploy to EE repo (pdg)
- :construction\_worker: временно разрешим тестам падать
- :white\_check\_mark: запускаем тесты в ci теперь
- :technologist: делаем удобный запуск кластера пикодаты
- Rename replace\_patsubscriber to insert\_patsubscriber

#### Bench {: #0.5.0-bench }

- List

#### Build {: #0.5.0-build }

- \`make pico\_radix\_release\` для запуска релизного радикса
- :arrow\_up: обновимся до пикодаты 25.1
- На \`pico\_stop\` убиваем пикодату из \`PICODATA\_BINARY\_PATH\`, а не просто \`picodata\`
- :construction\_worker: можно запускать тесты как на CI, но локально

### 0.4.4 - 2025-01-13 {: #0.4.4 }

#### Исправления {: #0.4.4-ispravleniia }

- Lpop and rpop are used to panic

#### Прочее {: #0.4.4-prochee }

- Bump version
- Fix lints for rust 1.84
- Reduce unsafe usage
- More benches
- Stress test

#### Bench {: #0.4.4-bench }

- Add benches for hash commands

### 0.4.3 - 2024-12-24 {: #0.4.3 }

#### Новая функциональность {: #0.4.3-novaia-funktsional-nost }

- Implement incrs

### 0.4.1 - 2024-12-18 {: #0.4.1 }

#### Новая функциональность {: #0.4.1-novaia-funktsional-nost }

- Implement writeln\_crlf
- Support expire for hash and list

#### Исправления {: #0.4.1-ispravleniia }

- :bug: исправим всё-таки #62, надо возвращать в протоколе правильно ошибку
- Declare dummy RUSAGE\_THREAD for macos

#### Структура кода {: #0.4.1-struktura-koda }

- Melformed -> malformed

#### Build {: #0.4.1-build }

- Добавим возможность запустить вторую копию

### 0.4.0 - 2024-12-10 {: #0.4.0 }

#### Новая функциональность {: #0.4.0-novaia-funktsional-nost }

- :loud\_sound: фильтрация логов для бедных
- Добавил версию пикодаты в вывод \`info server\`
- Implements cluster ids commands
- Implement getkeysinslot
- Implement cluster keyslot
- Reduce size of persistence section
- Use migrations for redis db creation
- Implement keyspace info section
- Implement replication info section
- Change Astralinux from Orel to 1.7, 1.8 version added
- Collect memory stat
- Add cmdstat to info
- Add client configuration. support input/output buffer shrinks, preallocate and reuse client output buffer
- Add redis insight support
- Use subkey in bucket calculation
- Implement info stub
- Implement missing list commands
- Implement (b)lmpop
- Implement (b)lmove
- Implement rpush
- Implement lpop and rpop
- Implement basic list commands
- Use borrowed string in a stored type

#### Исправления {: #0.4.0-ispravleniia }

- RESP is using CRLF as line ending
- Delete type on del call

#### Производительность {: #0.4.0-proizvoditel-nost }

- Мелкая оптимизация при создании хешсета
- Fetch replicasets only for broadcasted commands
- Increase performance for hset command

#### Документация {: #0.4.0-dokumentatsiia }

- :memo: актуализируем документацию

#### Внутренние улучшения {: #0.4.0-vnutrennie-uluchsheniia }

- :recycle: сделали более явным клонирование
- :art: разбил либу инфо на более мелкие и локализованные файлы
- :art: перенёс отдельные части \`info\` на уровень модуля этой команды
- :rotating\_light: удовлетворил требования нового стабильного раста

#### Прочее {: #0.4.0-prochee }

- :bookmark: нарежем 0.3.0 релиз
- Change type error message
- Log improvements
- :construction\_worker: используем шаблонный CI
- Add perf results in commands docs

#### Build {: #0.4.0-build }

- :construction\_worker: поправил докерфайлы для установки всегда новой пикодаты
- :heavy\_plus\_sign: переводим плагин на picodata-plugin сдк

### 0.2.0 - 2024-10-04 {: #0.2.0 }

#### Новая функциональность {: #0.2.0-novaia-funktsional-nost }

- Nonblocking gather executor
- Write to pubsub locally if possible
- Implement pubsub commands
- Reuse user connection buffer in command decode

#### Исправления {: #0.2.0-ispravleniia }

- :adhesive\_bandage: добавил скрипт по умолчанию для пикодаты

#### Производительность {: #0.2.0-proizvoditel-nost }

- :alembic: add scripts for running performance tests

#### Документация {: #0.2.0-dokumentatsiia }

- :speech\_balloon: save supported commands into docs
- :hammer: good enough Readme

#### Структура кода {: #0.2.0-struktura-koda }

- :art: добавил символ конца строки в конец моих файлов
- :art: add checks module to make code readable

#### Прочее {: #0.2.0-prochee }

- :sparkles: Заливаем артефакты в нексус.
- Init python tests
- Rename redisproto to radix
- Change name of package
- :hammer: set up docker compose for every artifact

### 0.1.1 - 2024-09-13 {: #0.1.1 }

#### Новая функциональность {: #0.1.1-novaia-funktsional-nost }

- Support single node mode

#### Исправления {: #0.1.1-ispravleniia }

- Execute set on master
- Replicaset decode
- Connection fibers leak

#### Документация {: #0.1.1-dokumentatsiia }

- :hammer: good enough Readme

#### Прочее {: #0.1.1-prochee }

- Add pack for all supported by picodata OS

### 0.1.0 - 2024-09-09 {: #0.1.0 }

#### Новая функциональность {: #0.1.0-novaia-funktsional-nost }

- Support eval command without enabling it
- Use bytes crate to use views in original clients buffer
- Implement scan and hscan command
- Redisproto

#### Исправления {: #0.1.0-ispravleniia }

- Decode tests

#### Внутренние улучшения {: #0.1.0-vnutrennie-uluchsheniia }

- Fix rust toolchain to stable
- Clippy warnings

#### Прочее {: #0.1.0-prochee }

- Proper layout again, let's hope, it's final version.
- Fix folder layout of artifacts.
- Fix artifacts collection
- Skip everything on main branch, because we use fast-forward only.
- Main should build artifacts always
- Init
- Lints
- Remove useless clusters dir
- Basic Makefile
