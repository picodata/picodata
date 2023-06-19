# Описание публичного API Picodata

Публичный интерфейс Picodata состоит из нескольких разделов:

- [Lua API](#lua-api)
- [Proc API](#proc-api) — интерфейс хранимых процедур

По функциональности они во многом повторяют друг друга. Выбор
конкретного интерфейса зависит от протокола, по которому происходит
подключение.

<!-- ############################################################ -->
## Lua API

Данный раздел интерфейса больше подходит для использования в
интерактивной консоли (`picodata run` или `picodata connect`).

Пример:

```
picodata> pico.help()
-- Печатает встроенную справку
```

Вызов функций `pico.*` возможен и через `iproto`, но влечет за собой
накладные расходы связанные с конфертацией из MessagePack формата в Lua
и обратно.

<!-- TODO: Error handling guideline -->

| Функция                       | Описание                          |
|-------------------------------|-----------------------------------|
| [pico.VERSION](#picoversion) | Версия Picodata.                  |
| [pico.args](#picoargs)       | Вывод аргументов запуска `picodata run`. |
| [pico.raft_get_index()](#picoraft_get_index) | Чтение индекса raft-журнала.
| [pico.raft_propose_nop()](#picoraft_propose_nop) | Добавление в raft-журнал запись `Nop` (no operation).
| [pico.cas()](#picocas) | Запрос на изменение параметров методом [Compare and Swap](glossary.md#as-compare-and-swap).
| [pico.raft_status()](#picoraft_status) | Получение данных о текущем состоянии raft-журнала ([терм](glossary.md#term), [лидер](glossary.md#raft-_1) и т.д.)
| [pico.exit()](#picoexit) | Корректное завершение работы указанного инстанса Picodata.
| [pico.expel()](#picoexpel) | [Контролируемый вывод](cli.md#expel) инстанса Picodata из кластера.
| [pico.raft_timeout_now()](#picoraft_timeout_now) |  Вызов таймаута raft-узла прямо сейчас, объявление новых выборов в raft-группе.
| [pico.instance_info()](#picoinstance_info) | Получение информации об инстансе Picodata (идентификаторы, уровни ([grade](glossary.md#grade)) и прочее).
| [pico.whoami()](#picowhoami) | Отображение данных о текущем инстансе.
| [pico.raft_compact_log()](#picoraft_compact_log) | [Компактизация](glossary.md#raft-raft-log-compaction) raft-журнала c удалением указанного числа наиболее старых записей.
| [pico.help()](#picohelp) | Доступ к встроенной справочной системе Picodata.
| [pico.create_space()](#picocreate_space) | Создание спейса в Picodata.

### pico.PICODATA_VERSION

Строковая переменная (не функция), которая содержит версию Picodata.
Семантика соответствует календарному версионированию ([Calendar
Versioning][calver]) с форматом `YY.0M.MICRO`.

[calver]: https://calver.org/#scheme

Пример:

```console
picodata> pico.VERSION
---
- 23.06.0
...
```

### pico.args

Lua-таблица (не функция) с [параметрами запуска](cli.md#run) инстанса,
которые были переданы в виде переменных окружения или аргументов
командной строки.

<!-- RTFS: https://git.picodata.io/picodata/picodata/picodata/-/blob/master/src/args.rs -->
- `cluster_id`: (_string_)
- `data_dir`: (_string_)
- `instance_id`: (optional _string_)
- `advertise_address`: (optional _string_)
- `listen`: (_string_)
- `peers`: ({_string_})
- `failure_domain`: ({[_string_] = _string_})
- `replicaset_id`: (optional _string_)
- `init_replication_factor`: (_number_)
- `script`: (optional _string_)
- `log_level`: (_string_)
- `http_listen`: (optional _string_)

Возвращаемое значение:

(_table_)

Пример:

```console
picodata> pico.args
---
- cluster_id: demo
  failure_domain: {}
  log_level: info
  init_replication_factor: 2
  listen: localhost:3302
  peers:
  - localhost:3301
  data_dir: ./data/i2
...
```
### pico.raft_get_index
Кворумное чтение текущего индекса raft-журнала

```lua
function raft_get_index(timeout)
```
Параметры:

- `timeout`: (_number_)

Функция принимает в качестве параметра число секунд (>0), в течение которых
будет длиться таймаут операции чтения.

Пример:

```console
picodata> pico.raft_get_index(1)
---
- 42
...
```
### pico.raft_propose_nop
Добавляет в raft-журнал запись `Nop` (no operation). Используется для обновления raft-журнала путем добавления в него свежей записи.
Функция не имеет передаваемых параметров.

### pico.cas
Отправляет запрос на вызов/изменение параметра с учетом его текущего
значения (проверяется на лидере). Если проверка не выявляет конфликтов,
то запрос успешно выполняется и в raft-журнале фиксируется новый индекс
схемы данных. Проверка значения параметра называется _проверкой
предиката_. Предикат состоит из трёх частей:

- номера индекса схемы данных;
- номера терма;
- диапазона значений самого проверяемого параметра (`ranges`).

Предоставление предиката при cas-запросе опционально. Если его не
указывать, то по умолчанию cas-запрос будет использовать текущий индекс,
текущий терм и пустой диапазон (разрешающий любые значения).

Функция применется только для
глобальных спейсов и работает на всем кластере.

```lua
function cas(dml[, predicate])
```

Параметры:

 - `dml`: (_table_)

    Таблица с полями:

    - `kind` (_string_), варианты: 'insert' | 'replace' | 'update' | 'delete'
    - `space` (_stringLua_)
    - `tuple` (optional _table_), обязательно для `insert` и `replace`
    - `key` (optional _table_), обязательно для `update` и `delete`
    - `ops` (optional _table_), обязательно для `update`

 - `predicate`: (optional _table_)

  Таблица с полями:

    - `index` (optional _number_), default: current applied index
    - `term` (optional _number_), default: current term
    - `ranges` (optional _table_ {table CasRange,...})
        default: {} (empty table)



Пример без указания предиката:
```lua
pico.cas({
    kind = 'insert',
    space = 'friends_of_peppa',
    tuple = {1, 'Suzy'},
})
```

Пример с указанием предиката (проверка, что никакие другие записи не были добавлены ранее):
```lua
pico.cas({
    kind = 'replace',
    space = 'friends_of_peppa',
    tuple = {2, 'Rebecca'},
}, {
    ranges = {{
        space = 'friends_of_peppa',
        key_min = { kind = 'excluded', key = {1,} },
        key_max = { kind = 'unbounded' },
    }},
})
```

Возвращаемое значение:

(_number_)

Функция возвращает индекс сделанной записи в raft-журнале.

### pico.raft_status
Получение данных о текущем состоянии raft-журнала ([терм](glossary.md#терм-term), лидер и т.д.). Функция не имеет передаваемых параметров.

Возвращаемые поля:

- `id` (_number_)
- `term` (_number_)
- `leader_id` (_number_ | _nil_),
    поле может быть пустым если в текущем терме нет лидера или он еще неизвестен
- `raft_state` (_string_),
    варианты: 'Follower' | 'Candidate' | 'Leader' | 'PreCandidate'


Пример:

```console
picodata> pico.raft_status()
---
- term: 2
  leader_id: 1
  raft_state: Leader
  id: 1
...
```

Возвращаемое значение:

(_table_)


### pico.exit
Корректное завершение работы указанного инстанса Picodata.

```lua
function exit([code])
```

Параметры:

- `[code]`: (_table_)

В качестве параметров функция может принимать [код
выхода](https://linuxconfig.org/list-of-exit-codes-on-linux),
обозначающий состояние завершения процесса.

Результат работы: 

Завершение текущего инстанса, завершение системных
процессов, связанных инстансом. В выводе `stdout` будет присутствовать
строка `graceful shutdown succeeded`, после чего будет возвращено
управление командному интерпретатору.

Перезапуск инстанса позволит ему снова войти в состав кластера в статусе `follower`.

### pico.expel

```lua
function expel("instance_id")
```
Выводит инстанс из кластера, но не завершает его работу. Может быть запущена только один раз для определенного инстанса.

Параметры:

- `instance_id`: (_string_)

Пример:

```lua
pico.expel("i2")
```
Возвращаемые значения:

- (_true_) — при успешном выполнении;
- (_nil_, _string_) — при ошибке;

Результат работы:

На инстансе, с которого была вызвана функция, в консоли `stdout` появятся сообщения:
```
downgrading instance i2
reconfiguring sharding
```
И далее для оставшихся в raft-группе инстансов сообщения вида:
```
calling rpc::sharding
```
Инстанс в состоянии `expelled` останется запущенным. Если его остановить и запустить снова, то он не присоединится к raft-группе.


### pico.instance_info

Функция показывает информацию о текущем инстансе Picodata

```lua
function instance_info(instance)
```
Параметры:

- `instance`: (_string_)

Возвращаемое значение: 

(_table_)

Таблица с полями:

- `raft_id `(_number_)
- `advertise_address` (_string_)
- `instance_id` (_string_)
- `instance_uuid` (_string_)
- `replicaset_id `(_string_)
- `replicaset_uuid` (_string_)
- `current_grade` (_table_).
   `variant` (_string_), варианты: 'Offline' | 'Online' | 'Expelled';
   `incarnation` (_number_) 
- `target_grade` (_table_).
   `variant` (_string_),  варианты: 'Offline' | 'Replicated' | 'ShardingInitialized' | 'Online' | 'Expelled';
   `incarnation`(_number_),

   
Пример:

```lua

 picodata> pico.instance_info()
 ---
 - raft_id: 1
advertise_address: localhost:3301
instance_id: i1
instance_uuid: 68d4a766-4144-3248-aeb4-e212356716e4
replicaset_id: r1
replicaset_uuid: e0df68c5-e7f9-395f-86b3-30ad9e1b7b07
current_grade:
  variant: Online
  incarnation: 26
target_grade:
  variant: Online
  incarnation: 26
...
```
### pico.raft_timeout_now
Вызов таймаута raft-узла прямо сейчас, объявление новых выборов в raft-группе.

Функция используется для явного и сознательного завершения текущего терма и объявления новых выборов в raft-группе. Функция не имеет передаваемых параметров.

После вызова функции в выводе `stdout` будут отражены этапы новых выборов (пример для инстанса в `raft_id`=`3`):

Объявление новых выборов:

  ```lua
  received MsgTimeoutNow from 3 and starts an election to get leadership., from: 3, term: 4, raft_id: 3
  ```

Начало выборов:

  ```lua
  starting a new election, term: 4, raft_id: 3
  ```

Превращение текущего инстанса в кандидаты в лидеры:

  ```lua
  became candidate at term 5, term: 5, raft_id: 3
  ```

Объявление голосования:

  ```lua
  broadcasting vote request, to: [4, 1], log_index: 54, log_term: 4, term: 5, type: MsgRequestVote, raft_id: 3
  ```

Получение голосов:

  ```lua
  received votes response, term: 5, type: MsgRequestVoteResponse, approvals: 2, rejections: 0, from: 4, vote: true, raft_id: 3
  ```

Объявление результата выборов:

  ```lua
  became leader at term 5, term: 5, raft_id: 3
  ```

В отсутствие других кандидатов, инстанс, инициировавший
`raft_timeout_now`, с большой вероятностью (при наличии кворума) сам
станет лидером по результатам выборов.


### pico.whoami

```lua
function whoami()
```

Возвращает идентификаторы инстанса.

Возвращаемое значение:

(_table_)

Таблица с полями:

- `raft_id`: (_number_)
- `cluster_id`: (_string_)
- `instance_id`: (_string_)

Пример:

```console
picodata> pico.whoami()
- raft_id: 1
  cluster_id: demo
  instance_id: i1
```


### pico.raft_compact_log

Компактизирует raft-журнал до указанного индекса (не включая сам индекс).

Функция имеет эффект только на текущем инстансе.

```lua
function raft_compact_log(up_to)
```

Параметры:

- `up_to`: (_number_) (_optional_, default: `inf`)

Возвращаемое значение:

(_number_)

Функция возвращает значение `first_index` — индекс первой записи в raft-журнале.

### pico.help
Функция предоставляет доступ к встроенной справочной системе в Picodata.

```lua
function help(topic)
```
Параметры:

- `topic`: (_string_) (_optional_, default: ``)

Пример:
```
picodata> pico.help("help")
  -- Shows this message
```
Далее будет показан список разделов (`topics`).

### pico.create_space
Создание спейса (пространства для хранения данных) в Picodata

```lua
function create_space(opts)
```

Параметры:

- `opts`: (_table_)

Состав таблицы:

- `name` (_string_)
- `format` (_table_ {_table_ SpaceField,...}), see pico.help('table SpaceField')
- `primary_key `(_table_ {_string_,...}), with field names
- `id` (optional _number_), default: implicitly generated
- `distribution` (_string_), one of 'global' | 'sharded'
    in case it's sharded, either `by_field` (for explicit sharding)
    or `sharding_key`+`sharding_fn` (for implicit sharding) options
    must be supplied.
- `by_field` (optional _string_), usually 'bucket_id'
- `sharding_key `(optional _table_ {string,...}) with field names
- `sharding_fn` (optional _string_), only default 'murmur3' is supported for now
- `timeout` (_number_), in seconds

Возвращаемое значение:

(_number_)

Примеры:

Создание глобального спейса с двумя полями:

```lua
pico.create_space({
    name = 'friends_of_peppa',
    format = {
        {name = 'id', type = 'unsigned', is_nullable = false},
        {name = 'name', type = 'string', is_nullable = false},
    },
    primary_key = {'id'},
    distribution = 'global',
    timeout = 3,
})
```
Добавление данных происходит через функцию `pico.cas`:

```lua
pico.cas({
    kind = 'insert',
    space = 'friends_of_peppa',
    key = {1, 'Suzy'},
})
```

Создание шардированного спейса с двумя полями:

```lua
pico.create_space({
    name = 'wonderland',
    format = {
        {name = 'property', type = 'string', is_nullable = false},
        {name = 'value', type = 'any', is_nullable = true}
    },
    primary_key = {'property'},
    distribution = 'sharded',
    sharding_key = {'property'},
    timeout = 3,
})
```
Добавление данных в шардированный спейс происходит с помощью [VShard API](https://www.tarantool.io/en/doc/latest/reference/reference_rock/vshard/vshard_router/):

```lua
local bucket_id = vshard.router.bucket_id_mpcrc32('unicorns')
vshard.router.callrw(bucket_id, 'box.space.wonderland:insert', {{'unicorns', 12}})
```
---
[Исходный код страницы](https://git.picodata.io/picodata/picodata/docs/-/blob/main/docs/api.md)
