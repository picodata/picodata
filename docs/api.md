# Публичный API Picodata

Описание публичного интерфейса Picodata состоит из нескольких разделов:

- [Lua API](#lua-api) — интерфейс Lua
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
picodata> pico.help("help")
-- Печатает данную справку
-- и список доступных разделов (topics)
```

Вызов функций `pico.*` возможен и через `iproto`, но влечет за собой
накладные расходы, связанные с конвертацией из формата MessagePack в Lua
и обратно.

<!-- TODO: Error handling guideline -->

| Функция                       | Описание                          |
|-------------------------------|-----------------------------------|
| [pico.LUA_API_VERSION](#picolua_api_version) | Вывод версии Lua API.                  |
| [pico.PICODATA_VERSION](#picopicodata_version) | Вывод версии Picodata.                  |
| [pico.abort_ddl](#picoabort_ddl)       | Отмена ожидающей операции по изменению схемы данных. |
| [pico.args](#picoargs)       | Вывод аргументов запуска `picodata run`. |
| [pico.cas()](#picocas) | Запрос на изменение параметров методом [Compare and Swap](glossary.md#as-compare-and-swap).
| [pico.create_role()](#picocreate_role) | Создание роли.
| [pico.create_space()](#picocreate_space) | Создание спейса.
| [pico.create_user()](#picocreate_user) | Создание пользователя.
| [pico.drop_role()](#picodrop_role) | Удаление роли.
| [pico.drop_space()](#picodrop_space) | Удаление спейса.
| [pico.drop_user()](#picodrop_user) | Удаление пользователя.
| [pico.exit()](#picoexit) | Корректное завершение работы указанного инстанса Picodata.
| [pico.expel()](#picoexpel) | [Контролируемый вывод](cli.md#expel) инстанса Picodata из кластера.
| [pico.grant_privilege()](#picogrant_privilege) | Назначение права пользователю или роли.
| [pico.help()](#picohelp) | Доступ к встроенной справочной системе Picodata.
| [pico.instance_info()](#picoinstance_info) | Получение информации об инстансе Picodata (идентификаторы, уровни ([grade](glossary.md#grade)) и прочее).
| [pico.raft_compact_log()](#picoraft_compact_log) | [Компактизация](glossary.md#raft-raft-log-compaction) raft-журнала c удалением указанного числа наиболее старых записей.
| [pico.raft_get_index()](#picoraft_get_index) | Получение текущего примененного индекса raft-журнала.
| [pico.raft_propose_nop()](#picoraft_propose_nop) | Добавление в raft-журнал запись `Nop` (no operation).
| [pico.raft_read_index()](#picoraft_read_index) | Кворумное чтение индекса raft-журнала.
| [pico.raft_status()](#picoraft_status) | Получение данных о текущем состоянии raft ([терм](glossary.md#term), [лидер](glossary.md#leader) и т.д.)
| [pico.raft_timeout_now()](#picoraft_timeout_now) | Немедленное объявление новых выборов в raft-группе.
| [pico.raft_wait_index()](#picoraft_wait_index) |  Ожидание локального применения указанного raft-индекса.
| [pico.revoke_privilege()](#picorevoke_privilege) |  Удаление права у пользователя или роли.
| [pico.wait_vclock()](#picowait_vclock) | Ожидание момента, когда значение [Vclock](glossary.md#vclock-vector-clock) достигнет целевого.
| [pico.whoami()](#picowhoami) | Отображение данных о текущем инстансе.




<!-- pico.create_user
pico.change_password
pico.drop_user
pico.grant_privilege
pico.revoke_privilege -->

### pico.LUA_API_VERSION

Строковая переменная (не функция), которая содержит версию Lua API Picodata.
Формат соответствует семантическому версионированию ([Semantic
Versioning][semver]).

[semver]: https://semver.org/

Пример:

```console
picodata> pico.LUA_API_VERSION
---
- 1.0.0
...

```

### pico.PICODATA_VERSION

Строковая переменная (не функция), которая содержит версию Picodata.
Формат соответствует календарному версионированию ([Calendar
Versioning][calver]) с форматом `YY.0M.MICRO`.

[calver]: https://calver.org/#scheme

Пример:

```console
picodata> pico.PICODATA_VERSION
---
- 23.06.0
...
```
### pico.abort_ddl

Отменяет изменение схемы данных.

```lua
function abort_ddl(timeout)
```
Параметры:

- `timeout`: (_number_)

Функция принимает в качестве параметра число секунд, в течение которых
она будет ожидать подтверждения отмены операции. Возвращает индекс
соответствующей операции `DdlAbort` в raft-журнале, либо ошибку в случае
отсутствия ожидающих операций.

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

Функция применяется только для
глобальных спейсов и работает на всем кластере.

```lua
function cas(dml[, predicate])
```

Параметры:

- `dml`: (_table_):
    - `kind` (_string_), варианты: `'insert'` | `'replace'` | `'update'` | `'delete'`
    - `space` (_stringLua_)
    - `tuple` (optional _table_), обязательно для `'insert'` и `'replace'`
    - `key` (optional _table_), обязательно для `'update'` и `'delete'`
    - `ops` (optional _table_), обязательно для `'update'`

- `predicate`: (optional _table_):
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

### pico.create_role
Создает новую роль на каждом инстансе кластера. Функция генерирует для
raft-журнала запись, которая при применении создает роль. Для ожидания
локального создания используется таймаут. Результатом успешного
выполнения функции является индекс соответствующей записи в
raft-журнале.

```lua
function create_role(name, [opts])
```
Параметры:
- `name` (_string_), имя роли
- `opts`: (_table_), необязательная таблица:
    - `if_not_exists` (_boolean_), при значении `true` роль не будет создана если другая роль с таким же именем уже существует
    - `timeout` (_number_), в секундах

Возвращаемое значение:

(_number_) с номером raft-индекса или <br>(_nil_, _error_ (ошибка в виде Lua-объекта)) в случае ошибки


### pico.create_space
Создает спейс (пространство для хранения данных). Результатом работы
функции является созданный глобально и доступный с текущего инстанса
спейс. Функция возвращает индекс соответствующей записи `Op::DdlCommit`
в raft-журнале, которая требуется для синхронизации с остальными
инстансами.

```lua
function create_space(opts)
```

Параметры:

- `opts`: (_table_):
    - `name` (_string_)
    - `format` (_table_ {_table_ SpaceField,...}), см. [table SpaceField](#table-spacefield)
    - `primary_key `(_table_ {_string_,...}) с именами полей
    - `id` (optional _number_), по умолчанию генерируется автоматически
    - `distribution` (_string_), варианты: `'global'` | `'sharded'`
        при использовании `sharded` также нужно использовать параметр `by_field`
        или связку параметров `sharding_key`+`sharding_fn`.
    - `by_field` (optional _string_), обычно используется `bucket_id`
    - `sharding_key `(optional _table_ {string,...}) с именами полей
    - `sharding_fn` (optional _string_), поддерживается пока только функция `murmur3`
    - `timeout` (_number_), в секундах

Возвращаемое значение:

(_number_) или <br>(_nil_, _string_) в случае ошибки

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
Запись в глобальный спейс происходит посредством функции [`pico.cas`](#picocas):

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
### pico.create_user
Создает нового пользователя на каждом инстансе кластера. Функция
генерирует для raft-журнала запись, которая при применении создает
пользователя. Для ожидания локального создания используется
таймаут. Результатом успешного выполнения функции является индекс записи
в raft-журнале на момент, когда пользователь уже гарантированно
существует.

```lua
function create_user(user, password, [opts])
```
Параметры:

- `user` (_string_), имя пользователя
- `password` (_string_), пароль пользователя
- `opts`: (_table_), необязательная таблица:
    - `if_not_exists` (_boolean_), при значении `true` пользователь не будет создан если другой пользователь с таким же именем уже существует
    - `timeout` (_number_), в секундах

Возвращаемое значение:

(_number_) с номером raft-индекса или <br>(_nil_, _error_ (ошибка в виде Lua-объекта)) в случае ошибки

  _Примечание_: Возможна ситуация, когда функция возвращает ошибку
  таймаута, но успевает создать пользователя локально. Данное действие в
  дальнейшем может быть как отменено, так и применено на уровне
  кластера, несмотря на то, что при повторном вызове функция будет
  возвращать сообщение "пользователь уже существует".

### pico.drop_role
Удаляет роль и связанные с ней сущности с каждого инстанса кластера.
Например, будут удалены спейсы, владельцем которых была эта роль.
Функция генерирует для raft-журнала запись, которая при применении
удаляет роль. Для ожидания локального удаления используется
таймаут. Результатом успешного выполнения функции является индекс записи
в raft-журнале на момент, когда роль уже гарантированно не
существует.

```lua
function drop_role (role, [opts])
```

Параметры:

- `role` (_string_), имя роли
- `opts`: (_table_), необязательная таблица:
    - `if_exists` (_boolean_), при значении `true` никакое действие не будет совершено если роли с таким именем не существует
    - `timeout` (_number_), в секундах

Возвращаемое значение:

(_number_) с номером raft-индекса или <br>(_nil_, _error_ (ошибка в виде Lua-объекта)) в случае ошибки

### pico.drop_space
Удаляет спейс (пространство для хранения данных) на всех инстансах
кластера. Функция ожидает глобального удаления спейса. Если ожидание
превышает таймаут, функция возвращает ошибку.

```lua
function drop_space(space, [opts])
```

Параметры:

- `space` (_number_ | _string_), id или имя спейса 
- `opts`: (_table_), необязательная таблица:
    - `timeout` (_number_), в секундах

Возвращаемое значение:

(_number_) с номером raft-индекса или <br>(_nil_, _error_ (ошибка в виде Lua-объекта)) в случае ошибки

### pico.drop_user
Удаляет пользователя и связанные с ним сущности с каждого инстанса
кластера. Например, будут удалены спейсы, владельцем которых был этот
пользователь. Функция генерирует для raft-журнала запись, которая при
применении удаляет пользователя. Для ожидания локального удаления
используется таймаут. Результатом успешного выполнения функции является
индекс записи в raft-журнале на момент, когда пользователя уже
гарантированно не существует.

```lua
function drop_user (user, [opts])
```
Параметры:

- `user` (_string_), имя пользователя 
- `opts`: (_table_), необязательная таблица:
    - `if_exists` (_boolean_), при значении `true` никакое действие не будет совершено если пользователя с таким именем не существует
    - `timeout` (_number_), в секундах

Возвращаемое значение:

(_number_) с номером raft-индекса или <br>(_nil_, _error_ (ошибка в виде Lua-объекта)) в случае ошибки

  _Примечание_: Возможна ситуация, когда функция возвращает ошибку
  таймаута, но успевает удалить пользователя локально. Данное действие в
  дальнейшем может быть как отменено, так и применено на уровне
  кластера, несмотря на то, что при повторном вызове функция будет
  возвращать сообщение "пользователь не найден".



### pico.exit
Корректно завершает работу указанного инстанса Picodata.

```lua
function exit([code])
```

Параметры:

- `code`: (_table_)

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
Выводит инстанс из кластера, но не завершает его работу. Может быть
запущена только один раз для инстанса с определенным `instance_id`.

```lua
function expel("instance_id")
```

Параметры:

- `instance_id`: (_string_)

Пример:

```lua
pico.expel("i2")
```
Возвращаемые значения:

- (_true_) — при успешном выполнении;
- <br>(_nil_, _string_) — при ошибке;

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

### pico.grant_privilege
Назначает право пользователю или роли на всех инстансах кластера.

```lua
function grant_privilege(grantee, privilege, object_type, [object_name], [opts])
```
Параметры:

- `grantee` (_string_), имя пользователя или роли 
- `privilege` (_string_), название права, варианты: `'read'` | '`write'
  `| `'execute'` | `'session' `| `'usage'` | `'create'` | `'drop'` |
          `'alter'` | `'reference'` | `'trigger'` | `'insert'` | `'update'` | `'delete'`
- `object_type` (_string_), тип целевого объекта, варианты: `'universe'`
  | `'space'` | `'sequence'` | `'function'` | `'role'` | `'user'`
- `object_name` (_string_), имя целевого объекта (необязательный
  параметр). Можно не указывать при адресации совокупностей целевых
  объектов (см. примеры [ниже](#grant_pr))
- `opts`: (_table_), необязательная таблица:
    - `timeout` (_number_), в секундах

Возвращаемое значение:

(_number_) с номером raft-индекса или <br>(_nil_, _error_ (ошибка в виде Lua-объекта)) в случае ошибки

  _Примечание_: Возможна ситуация, когда функция возвращает ошибку
  таймаута, но успевает назначить право. Данное действие в
  дальнейшем может быть как отменено, так и применено на уровне
  кластера, несмотря на то, что при повторном вызове функция будет
  возвращать сообщение "право уже назначено".

Примеры:<a name="grant_pr"></a>

Выдать право на чтение спейса 'Fruit' пользователю 'Dave':
```lua
pico.grant_privilege('Dave', 'read', 'space', 'Fruit')
```

Выдать пользователю 'Dave' право исполнять произвольный код Lua:
```lua
pico.grant_privilege('Dave', 'execute', 'universe')
```

Выдать пользователю 'Dave' право создавать новых пользователей:
```lua
pico.grant_privilege('Dave', 'create', 'user')
```

Выдать право на запись в спейс 'Junk' для роли 'Maintainer':
```lua
pico.grant_privilege('Maintainer', 'write', 'space', 'Junk')
```

Назначить роль 'Maintainer' пользователю 'Dave':
```lua
pico.grant_privilege('Dave', 'execute', 'role', 'Maintainer')
```

### pico.help
Предоставляет доступ к встроенной справочной системе Picodata.

```lua
function help(topic)
```
Параметры:

- `topic`: (_string_) (_optional_)

Пример:
```
picodata> pico.help("help")
-- Печатает данную справку
-- и список доступных разделов (topics)

```

### pico.instance_info
Показывает информацию о текущем инстансе Picodata

```lua
function instance_info(instance)
```
Параметры:

- `instance`: (_string_)

Возвращаемое значение: 

- (_table_):
    - `raft_id `(_number_)
    - `advertise_address` (_string_)
    - `instance_id` (_string_)
    - `instance_uuid` (_string_)
    - `replicaset_id `(_string_)
    - `replicaset_uuid` (_string_)
    - `current_grade` (_table_).
      `variant` (_string_), варианты: `'Offline'` | `'Online'` | `'Expelled'`;
      `incarnation` (_number_) 
    - `target_grade` (_table_).
      `variant` (_string_),  варианты: `'Offline'` | `'Replicated'` | `'ShardingInitialized'` | `'Online'` | `'Expelled'`;
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

Функция возвращает значение `first_index` — индекс первой записи в
raft-журнале.

### pico.raft_get_index
Получает текущий примененный индекс raft-журнала.

```lua
function raft_get_index()
```

Возвращаемое значение:

(_number_)

### pico.raft_propose_nop
Добавляет в raft-журнал запись `Nop` (no operation). Используется для
обновления raft-журнала путем добавления в него свежей записи. Функция
не имеет передаваемых параметров.

### pico.raft_read_index
Выполняет кворумное чтение по следующему принципу:

  1. Инстанс направляет запрос (`MsgReadIndex`) лидеру raft-группы. В
     случае, если лидера в данный момент нет, функция возвращает ошибку
     'raft: proposal dropped'.
  2. Raft-лидер запоминает текущий `commit_index` и отправляет всем узлам
     в статусе `follower` сообщение (heartbeat) с тем, чтобы убедиться,
     что он все еще является лидером.
  3. Как только получение этого сообщения подтверждается
     большинством `follower`-узлов, лидер возвращает этот индекс
     инстансу.
  4. Инстанс дожидается применения (apply) указанного raft-индекса. Если
     таймаут истекает раньше, функция возвращает ошибку 'timeout'.

```lua
function raft_read_index(timeout)
```
Параметры:

- `timeout`: (_number_)

Функция принимает в качестве параметра число секунд, в течение которых
она ожидает ответа от инстанса.

Пример:

```console
picodata> pico.raft_read_index(1)
---
- 42
...
```

Возвращаемое значение:

(_number_) или <br>(_nil_, _string_) в случае ошибки

### pico.raft_status
Получает данные о текущем состоянии raft-узла
([терм](glossary.md#term), [лидер](glossary.md#leader) и т.д.). Функция
не имеет передаваемых параметров.

Возвращаемые поля:

- `id` (_number_)
- `term` (_number_)
- `leader_id` (_number_ | _nil_),
    поле может быть пустым если в текущем терме нет лидера или он еще неизвестен
- `raft_state` (_string_),
    варианты: `'Follower'` | `'Candidate'` | `'Leader'` | `'PreCandidate'`


Возвращаемое значение:

(_table_) или <br>(_nil_, _string_) в случае ошибки (если raft-узел еще не
инициализирован).

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


### pico.raft_timeout_now
Вызывает таймаут raft-узла и немедленно объявляет новые выборы в
raft-группе.

Функция используется для явного завершения текущего
терма и объявления новых выборов в raft-группе. Функция не имеет
передаваемых параметров.

После вызова функции в выводе `stdout` будут отражены этапы новых
выборов (пример для инстанса в `raft_id`=`3`):

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

### pico.raft_wait_index
Ожидает применения (apply) на инстансе указанного raft-индекса. Функция
возвращает текущий примененный индекс raft-журнала, который может быть
равен или превышать указанный.

```lua
function raft_wait_index(target, timeout)
```
Параметры:

- `target`: (_number_)
- `timeout`: (_number_)

Возвращаемое значение:

(_number_) или <br>(_nil_, _string_) в случае ошибки

Если за указанное время (`timeout`) функция не успеет получить индекс, она
вернет сообщение об ошибке.

### pico.revoke_privilege
Удаляет право пользователя или роли на всех инстансах кластера.

```lua
function revoke_privilege(grantee, privilege, object_type, [object_name], [opts])
```
Параметры:

- `grantee` (_string_), имя пользователя или роли 
- `privilege` (_string_), название права, варианты: `'read'` | '`write'
  `| `'execute'` | `'session' `| `'usage'` | `'create'` | `'drop'` |
          `'alter'` | `'reference'` | `'trigger'` | `'insert'` | `'update'` | `'delete'`
- `object_type` (_string_), тип целевого объекта, варианты: `'universe'`
  | `'space'` | `'sequence'` | `'function'` | `'role'` | `'user'`
- `object_name` (_string_), имя целевого объекта (необязательный
  параметр). Можно не указывать при адресации совокупностей целевых
  объектов (аналогично действию `grant_privilege`, см. примеры [выше](#grant_pr))
- `opts`: (_table_), необязательная таблица:
    - `timeout` (_number_), в секундах

Возвращаемое значение:

(_number_) с номером raft-индекса или <br>(_nil_, _error_ (ошибка в виде Lua-объекта)) в случае ошибки

  _Примечание_: Возможна ситуация, когда функция возвращает ошибку
  таймаута, но успевает удалить право. Данное действие в
  дальнейшем может быть как отменено, так и применено на уровне
  кластера, несмотря на то, что при повторном вызове функция будет
  возвращать сообщение "право не назначено".

### pico.wait_vclock
Ожидает момента, когда значение
[Vclock](glossary.md#vclock-vector-clock) в Tarantool достигнет
целевого.

```lua
function wait_vclock(target, timeout)
```
Параметры:

- `target`: (_table Vclock_)
- `timeout`: (_number_)

### pico.whoami

```lua
function whoami()
```

Возвращает идентификаторы инстанса.

Возвращаемое значение:

- (_table_):
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

### table CasBound
Lua-таблица, используемая для обозначения минимального (`key_min`) или
максимального (`key_max`) значения в таблице `table CasRange`.

Поля:

- `kind` (_string_), варианты: `'included'` | `'excluded'` | `'unbounded'`
- `key` (optional _table_), обязательно для вариантов `included` и `excluded`

### table CasRange
Lua-таблица, задающая диапазон значений. Используется для обозначения
предиката (проверяемых данных) в функции [pico.cas()](#picocas).

Поля:

- `space` (_string_)
- `key_min` (table CasBound), см. [выше](#table-casbound)
- `key_max` (table CasBound)

Пример:

```lua
local unbounded = { kind = 'unbounded' }
local including_1 = { kind = 'included', key = {1,} }
local excluding_3 = { kind = 'excluded', key = {3,} }

local range_a = {
    space = 'friends_of_peppa',
    key_min = unbounded,
    key_max = unbounded,
}

-- [1, 3)
local range_a = {
    space = 'friends_of_peppa',
    key_min = including_1,
    key_max = excluding_3,
}

```

### table SpaceField
Lua-таблица, описывающее поле в составе спейса (см. [pico.create_space](#picocreate_space)).

Поля:

- `name` (_string_)
- `type` (_string_)
- `is_nullable` (_boolean_)

Пример:
```lua
{name = 'id', type = 'unsigned', is_nullable = false}
{name = 'value', type = 'unsigned', is_nullable = false}
```

См. также:

- Описание [space_object:format()](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/format/)
- Описание [типов полей в спейсах Tarantool](https://docs.rs/tarantool/latest/tarantool/space/enum.FieldType.html)


### table Vclock
Lua-таблица, отражающая соответствие `id` инстанса его
[LSN-номеру](glossary.md#lsn-log-sequence-number).

Пример:
```lua
{[0] = 2, [1] = 101}
{[0] = 148, [1] = 9086, [3] = 2}
```
См. подробнее описание [Vclock](glossary.md#vclock-vector-clock). Нулевое значение Vclock зарезервировано
для отслеживания локальных изменений, которые не реплицируются.

---
[Исходный код страницы](https://git.picodata.io/picodata/picodata/docs/-/blob/main/docs/api.md)
