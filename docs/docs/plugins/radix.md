# Radix

В данном разделе приведены сведения о Radix, плагине для СУБД Picodata.

!!! tip "Picodata Enterprise"
    Функциональность плагина доступна только в коммерческой версии Picodata.

## Общие сведения {: #intro }

Radix — реализация [Redis](https://ru.wikipedia.org/wiki/Redis) на базе
Picodata, предназначенная для замены существующих инсталляций Redis.

Плагин Radix состоит из одноименного сервиса (`radix`), реализующего
Redis на базе СУБД Picodata. Каждый экземпляр Radix открывает еще один
порт для подключения в дополнение к уже открытым.

При использовании Picodata c плагином Radix нет необходимости в
отдельной инфраструктуре Redis Sentinel, так как каждый узел Picodata
выполняет роль прокси ко всем данным Redis.

## Установка {: #install }

### Предварительные действия {: #prerequisites }

Установка плагина Radix, в целом, соответствует общей процедуре
установки плагинов в Picodata, но имеет ряд особенностей.
<!-- вставить ссылку на туториал по плагинам, когда он будет залит -->

Процедура установки включает:

- установку адреса, который будет слушать Radix (например, `export
  RADIX_ADDR=0.0.0.0:7379`). Эта настройка также доступна для
  инвентарного файла Ansible (см. [ниже](#addr)). Если в одном
  пространстве имен (например, на одном хосте) запущено несколько
  инстансов Picodata, то нужно задать для них отличающиеся значения
  `RADIX_ADDR`.
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

Для подключение плагина последовательно выполните следующие SQL-команды
в административной консоли Picodata:


Для создания плагина в кластере и регистрации его сервиса на доступных тирах:

```sql title="В примере использованы два тира: `default` и `extra`"
CREATE PLUGIN radix 0.7.0;
ALTER PLUGIN radix 0.7.0 ADD SERVICE radix TO TIER default;
ALTER PLUGIN radix 0.7.0 ADD SERVICE radix TO TIER extra;
```

Для настройки миграций задайте значения для 16 параметров (по числу баз данных в Radix):

```sql
ALTER PLUGIN radix 0.7.0 SET migration_context.tier_0='default';
ALTER PLUGIN radix 0.7.0 SET migration_context.tier_1='default';
ALTER PLUGIN radix 0.7.0 SET migration_context.tier_2='default';
ALTER PLUGIN radix 0.7.0 SET migration_context.tier_3='default';
ALTER PLUGIN radix 0.7.0 SET migration_context.tier_4='default';
ALTER PLUGIN radix 0.7.0 SET migration_context.tier_5='default';
ALTER PLUGIN radix 0.7.0 SET migration_context.tier_6='default';
ALTER PLUGIN radix 0.7.0 SET migration_context.tier_7='default';
ALTER PLUGIN radix 0.7.0 SET migration_context.tier_8='extra';
ALTER PLUGIN radix 0.7.0 SET migration_context.tier_9='extra';
ALTER PLUGIN radix 0.7.0 SET migration_context.tier_10='extra';
ALTER PLUGIN radix 0.7.0 SET migration_context.tier_11='extra';
ALTER PLUGIN radix 0.7.0 SET migration_context.tier_12='extra';
ALTER PLUGIN radix 0.7.0 SET migration_context.tier_13='extra';
ALTER PLUGIN radix 0.7.0 SET migration_context.tier_14='extra';
ALTER PLUGIN radix 0.7.0 SET migration_context.tier_15='extra';
```

Для выполнения миграции:

```sql
ALTER PLUGIN radix MIGRATE TO 0.7.0 OPTION(TIMEOUT=300);
```

Для включения плагина в кластере:

```sql title="Убедитесь, что задан адрес, который будет слушать Radix"
ALTER PLUGIN radix 0.7.0 ENABLE OPTION(TIMEOUT=30);
```

Чтобы убедиться в том, что плагин успешно добавлен и запущен, выполните запрос:

```sql
SELECT * FROM _pico_plugin;
```

В строке, соответствующей плагину Radix, в колонке `enabled` должно быть значение `true`.

## Настройка {: #configuration }

Для настройки плагина используйте файл конфигурации, который можно
применить к плагину с помощью [Picodata Pike] или [инвентарного файла
Ansible].

Пример файла конфигурации:

```yaml
addr: 0.0.0.0:7301 # адрес, который будет слушать Radix
clients:           # ограничения клиентских соединений
    max_clients: 10000
    max_input_buffer_size: 1073741824
    max_output_buffer_size: 1073741824
cluster_mode: true # какой флаг отдавать в команде `info cluster`
```

[Picodata Pike]: ../tutorial/create_plugin.md#pike_plugin_config_apply
[инвентарного файла Ansible]: ../tutorial/deploy_ansible.md#plugin_management

Для изменения доступны следующие параметры:

### addr

Адрес, по которому Radix откроет сокет и будет его слушать.

Имейте ввиду, что данные параметры одинаковы для всех узлов Picodata, на
которых развернут Radix. Следовательно, если у вас процессы не
изолированы каждый в своем сетевом пространстве имен (т.е. не
используется Docker или K8S), то вам надо отдельно для каждого узла
прописать переменную окружения `RADIX_ADDR` для того, чтобы не было
конфликтов по портам.

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

## Использование {: #usage }

В консоли для общения с Redis используется клиентская программа
`redis-cli.` Для подключения к инстансу Picodata по протоколу Redis
используйте адрес, заданный ранее в переменной `RADIX_ADDR`:

```shell
redis-cli -p 7379
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
элемента. Нумерация элментов начинается с `0`.

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
`key` и то, что этот ключ содержит список. В противном случае команжа
ничего не делает.

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
    ALTER PLUGIN RADIX 0.7.0 SET radix.redis_compatibility = '{ "enabled_deprecated_commands": ["psetex" ] }';
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
    ALTER PLUGIN RADIX 0.7.0 SET radix.redis_compatibility = '{ "enabled_deprecated_commands": ["setex" ] }';
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
    ALTER PLUGIN RADIX 0.7.0 SET radix.redis_compatibility = '{ "enabled_deprecated_commands": ["setnx" ] }';
    ```

#### strlen

```sql
STRLEN key
```

Возвращает длину текстового значения, хранящегося по
указанному ключу.

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
чтение", т.е. беез модификации данных БД.

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
чтение", т.е. беез модификации данных БД.

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

#### exec

```sql
EXEC
```

Исполняет все команды в очереди в виде единой транзакции.

#### discard

```sql
DISCARD
```

Удаляет все команды из очереди исполнения

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



