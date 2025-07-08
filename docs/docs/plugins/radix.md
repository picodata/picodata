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
CREATE PLUGIN radix 0.9.0;
ALTER PLUGIN radix 0.9.0 ADD SERVICE radix TO TIER default;
ALTER PLUGIN radix 0.9.0 ADD SERVICE radix TO TIER extra;
```

Для настройки миграций задайте значения для 16 параметров (по числу баз данных в Radix):

```sql
ALTER PLUGIN radix 0.9.0 SET migration_context.tier_0='default';
ALTER PLUGIN radix 0.9.0 SET migration_context.tier_1='default';
ALTER PLUGIN radix 0.9.0 SET migration_context.tier_2='default';
ALTER PLUGIN radix 0.9.0 SET migration_context.tier_3='default';
ALTER PLUGIN radix 0.9.0 SET migration_context.tier_4='default';
ALTER PLUGIN radix 0.9.0 SET migration_context.tier_5='default';
ALTER PLUGIN radix 0.9.0 SET migration_context.tier_6='default';
ALTER PLUGIN radix 0.9.0 SET migration_context.tier_7='default';
ALTER PLUGIN radix 0.9.0 SET migration_context.tier_8='extra';
ALTER PLUGIN radix 0.9.0 SET migration_context.tier_9='extra';
ALTER PLUGIN radix 0.9.0 SET migration_context.tier_10='extra';
ALTER PLUGIN radix 0.9.0 SET migration_context.tier_11='extra';
ALTER PLUGIN radix 0.9.0 SET migration_context.tier_12='extra';
ALTER PLUGIN radix 0.9.0 SET migration_context.tier_13='extra';
ALTER PLUGIN radix 0.9.0 SET migration_context.tier_14='extra';
ALTER PLUGIN radix 0.9.0 SET migration_context.tier_15='extra';
```

Для выполнения миграции:

```sql
ALTER PLUGIN radix MIGRATE TO 0.9.0 OPTION(TIMEOUT=300);
```

Для включения плагина в кластере:

```sql title="Убедитесь, что задан адрес, который будет слушать Radix"
ALTER PLUGIN radix 0.9.0 ENABLE OPTION(TIMEOUT=30);
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
[инвентарного файла Ansible]: ../admin/deploy_ansible.md#plugin_management

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
    ALTER PLUGIN RADIX 0.9.0 SET radix.redis_compatibility = '{ "enabled_deprecated_commands": ["psetex" ] }';
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
    ALTER PLUGIN RADIX 0.9.0 SET radix.redis_compatibility = '{ "enabled_deprecated_commands": ["setex" ] }';
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
    ALTER PLUGIN RADIX 0.9.0 SET radix.redis_compatibility = '{ "enabled_deprecated_commands": ["setnx" ] }';
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

### Команды управления и диагностики {: #server }

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
    radix_version:0.9.0
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


