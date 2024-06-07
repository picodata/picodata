# Публичный API Picodata

Описание публичного интерфейса Picodata состоит из нескольких разделов:

- [Lua API](#lua_api) — интерфейс Lua
- Proc API — интерфейс хранимых процедур

По функциональности они во многом повторяют друг друга. Выбор
конкретного интерфейса зависит от протокола, по которому происходит
подключение.

## Lua API {: #lua_api }

Данный раздел интерфейса относится к работе в [консоли
Picodata](../tutorial/connecting.md) при использовании ввода в режиме Lua.

Пример:

```
picodata> pico.help("help")
-- Печатает данную справку
-- и список доступных разделов (topics)
```

Вызов функций `pico.*` возможен и через `iproto`, но влечет за собой
накладные расходы, связанные с конвертацией из формата MessagePack в Lua
и обратно.

| Функция                       | Описание                          |
|-------------------------------|-----------------------------------|
| [pico.LUA_API_VERSION](#pico_lua_api_version) | Версия Lua API.
| [pico.PICODATA_VERSION](#pico_picodata_version) | Версия Picodata.
| [pico.abort_ddl](#pico_abort_ddl) | Отмена ожидающей операции по изменению схемы данных.
| [pico.args](#pico_args) | Вывод аргументов запуска `picodata run`.
| [pico.cas()](#pico_cas) | Запрос на изменение параметров методом [Compare and Swap](../overview/glossary.md#cas).
| [pico.change_password()](#pico_change_password) | Изменение пароля пользователя.
| [pico.create_role()](#pico_create_role) | Создание роли.
| [pico.create_table()](#pico_create_table) | Создание таблицы.
| [pico.create_user()](#pico_create_user) | Создание пользователя.
| [pico.drop_role()](#pico_drop_role) | Удаление роли.
| [pico.drop_table()](#pico_drop_table) | Удаление таблицы.
| [pico.drop_user()](#pico_drop_user) | Удаление пользователя.
| [pico.exit()](#pico_exit) | Корректное завершение работы указанного инстанса.
| [pico.expel()](#pico_expel) | [Контролируемый вывод](cli.md#expel) инстанса из кластера.
| [pico.grant_privilege()](#pico_grant_privilege) | Назначение привилегии пользователю или роли.
| [pico.help()](#pico_help) | Доступ к встроенной справочной системе.
| [pico.instance_info()](#pico_instance_info) | Получение информации об инстансе (идентификаторы, уровни ([grade](../overview/glossary.md#grade)) и прочее).
| [pico.raft_compact_log()](#pico_raft_compact_log) | [Компактизация](../overview/glossary.md#raft_log_compaction) raft-журнала c удалением указанного числа наиболее старых записей.
| [pico.raft_get_index()](#pico_raft_get_index) | Получение текущего примененного индекса raft-журнала.
| [pico.raft_log()](#pico_raft_log) | Чтение содержимого raft-журнала.
| [pico.raft_propose_nop()](#pico_raft_propose_nop) | Добавление в raft-журнал запись `Nop` (no operation).
| [pico.raft_read_index()](#pico_raft_read_index) | Кворумное чтение индекса raft-журнала.
| [pico.raft_status()](#pico_raft_status) | Получение данных о текущем состоянии raft ([терм](../overview/glossary.md#term), [лидер](../overview/glossary.md#leader) и т.д.).
| [pico.raft_term()](#pico_raft_term) | Получение номера терма (текущего или для указанной записи).
| [pico.raft_wait_index()](#pico_raft_wait_index) | Ожидание локального применения указанного raft-индекса.
| [pico.revoke_privilege()](#pico_revoke_privilege) | Удаление привилегии у пользователя или роли.
| [pico.sql()](#pico_sql) | Выполнение кластерных SQL-запросов.
| [pico.wait_ddl_finalize()](#pico_wait_ddl_finalize) | Ожидание применения (финализации) DDL-операции.
| [pico.wait_vclock()](#pico_wait_vclock) | Ожидание момента, когда значение [Vclock](../overview/glossary.md#vclock) достигнет целевого.
| [pico.whoami()](#pico_whoami) | Отображение данных о текущем инстансе.

### pico.LUA_API_VERSION {: #pico_lua_api_version }

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

### pico.PICODATA_VERSION {: #pico_picodata_version }

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

### pico.abort_ddl {: #pico_abort_ddl }

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

### pico.args {: #pico_args }

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

### pico.cas {: #pico_cas }

Функция проверяет предикат на лидере. Если проверка не выявляет
конфликтов, то функция добавляет новую запись в raft-журнал и возвращает
ее индекс (пока еще не зафиксированный в кластере). Функция применяется
только для глобальных таблиц и работает на всем кластере.

Предикат состоит из трёх частей:

- номера индекса схемы данных (`index`);
- номера терма (`term`);
- диапазона значений самого проверяемого параметра (`ranges`).

Если предикат не указан, он будет автоматически сгенерирован на
основе текущих `index` и `term` и пустого диапазона `ranges`. Запрос без
диапазонов `ranges` означает "запись вслепую" и поэтому не
рекомендуется к использованию.

Функция возвращает индекс сделанной записи в raft-журнале.

```lua
function cas(dml[, predicate])
```

Параметры:

- `dml`: (_table_):
    - `kind` (_string_), варианты: `'insert'` | `'replace'` | `'update'` | `'delete'`
    - `table` (_string_)
    - `tuple` (optional _table_), обязательно для `'insert'` и `'replace'`
    - `key` (optional _table_), обязательно для `'update'` и `'delete'`
    - `ops` (optional _table_), обязательно для `'update'`

- `predicate`: (optional _table_):
    - `index` (optional _number_), default: current applied index
    - `term` (optional _number_), default: current term
    - `ranges` (optional _table_ {table CasRange,...})
        default: {} (empty table)

Возвращаемое значение:

(_number_) или <br>(_nil_, _string_) в случае ошибки

Пример без указания предиката:

```lua
pico.cas({
    kind = 'insert',
    table = 'WAREHOUSE',
    tuple = {11, 99, 'chunks', 'light'},
})
```

Здесь, `11` — значение первой колонки (`id`), `99` — значение `bucket_id`.

Пример с указанием предиката (проверка, что никакие другие записи не
были добавлены ранее):

```lua
pico.cas({
    kind = 'replace',
    table = 'WAREHOUSE',
    tuple = {11, 99, 'chunks', 'light'},
}, {
    ranges = {{
        table = 'WAREHOUSE',
        key_min = { kind = 'excluded', key = {1,} },
        key_max = { kind = 'unbounded' },
    }},
})
```

### pico.change_password {: #pico_change_password }

Изменяет пароль пользователя на всех инстансах кластера. Функция
генерирует для raft-журнала запись, которая при применении изменяет
пароль пользователя. Для ожидания локального создания используется
таймаут. Результатом успешного выполнения функции является индекс
соответствующей записи в raft-журнале. Новый пароль должен отличаться от
старого (иначе запрос игнорируется), а также удовлетворять
[требованиям](../tutorial/access_control.md#allowed_passwords) по длине
и сложности пароля.

Данная функция учитывает ограничения по длине пароля, определяемые
параметром `password_min_length` в системной таблице `_pico_property` (по
умолчанию требуется пароль не короче 8 символов).

```lua
function change_password(user, password, [opts])
```

Параметры:

- `user` (_string_), имя пользователя
- `password` (_string_), пароль пользователя
- `opts`: (optional _table_), таблица:
    - `auth_type` (optional _string_), тип аутентификации,
      варианты: `'chap-sha1'` | `'md5'` | `'ldap'`. По умолчанию
      используется значение из `box.cfg.auth_type`.
    - `timeout` (optional _number_), число в секундах. По
      умолчанию используется бесконечное значение.

Возвращаемое значение:

(_number_) с номером raft-индекса или <br>(_nil_, _error_ (ошибка в виде Lua-объекта)) в случае ошибки.

!!! note "Примечание"
    Если эта функция возвращает ошибку таймаута, то запрос
    НЕ отменяется и изменение может быть применено некоторое время спустя.
    По этой причине повторные вызовы функции с теми же аргументами всегда
    безопасны.

### pico.create_role {: #pico_create_role }

Создает новую роль на каждом инстансе кластера. Функция генерирует для
raft-журнала запись, которая при применении создает роль. Для ожидания
локального создания используется таймаут. Результатом успешного
выполнения функции является индекс соответствующей записи в
raft-журнале. Если такая роль уже существует, то запрос игнорируется.

```lua
function create_role(name, [opts])
```

Параметры:

- `name` (_string_), имя роли
- `opts`: (optional_table_), таблица:
    - `timeout` (optional _number_), число в секундах. По умолчанию
      используется бесконечное значение.

Возвращаемое значение:

(_number_) с номером raft-индекса или <br>(_nil_, _error_ (ошибка в виде Lua-объекта)) в случае ошибки.

!!! note "Примечание"
    Если эта функция возвращает ошибку таймаута, то запрос
    НЕ отменяется и изменение может быть применено некоторое время спустя.
    По этой причине повторные вызовы функции с теми же аргументами всегда
    безопасны.

### pico.create_table {: #pico_create_table }

Создает таблицу. Функция завершается после того, как таблица создана
глобально и доступна с текущего инстанса. Функция возвращает индекс
соответствующей записи `Op::DdlCommit` в raft-журнале, который
необходим для синхронизации с остальными инстансами. Если такая
таблица уже существует, то запрос игнорируется.

```lua
function create_table(opts)
```

Параметры:

- `opts`: (_table_):
    - `name` (_string_)
    - `format` (_table_ {_table_ TableField,...}), см. [table TableField](#tablefield_table)
    - `primary_key `(_table_ {_string_,...}) с именами полей
    - `id` (optional _number_), по умолчанию генерируется автоматически
    - `distribution` (_string_), варианты: `'global'` | `'sharded'`
        при использовании `sharded` также нужно использовать параметр `by_field`
        или связку параметров `sharding_key`+`sharding_fn`.
    - `by_field` (optional _string_), обычно используется `bucket_id`
    - `sharding_key `(optional _table_ {string,...}) с именами полей
    - `sharding_fn` (optional _string_), поддерживается пока только функция `murmur3`
    - `engine` (optional _string_), движок хранения данных в БД;
      варианты: `'memtx'` | `'vinyl'`. По умолчанию используется
      `'memtx'`. См [подробнее](../overview/glossary.md#db_engine).
    - `timeout` (optional _number_), число в секундах. По умолчанию
      используется бесконечное значение.

Возвращаемое значение:

(_number_) или <br>(_nil_, _string_) в случае ошибки

Примеры:

Создание глобальной таблицы с двумя полями:

```lua
pico.create_table({
    name = 'WAREHOUSE',
    format = {
        {name = 'ID', type = 'unsigned', is_nullable = false},
        {name = 'ITEM', type = 'string', is_nullable = false},
        {name = 'TYPE', type = 'string', is_nullable = false}
    },
    primary_key = {'ID'},
    distribution = 'global',
    timeout = 3,
})
```

Запись в глобальную таблицу происходит посредством функции [`pico.cas`](#pico_cas):

```lua
pico.cas({
    kind = 'insert',
    table = 'WAREHOUSE',
    tuple = {1, 'bricks', 'heavy'},
})
```

Для чтения из глобальной таблицы используется box-API Tarantool:

```
box.space.WAREHOUSE:fselect()
```

Создание шардированной таблицы с двумя полями:

```lua
pico.create_table({
    name = 'DELIVERIES',
    format = {
        {name = 'nmbr', type = 'unsigned', is_nullable = false},
        {name = 'product', type = 'string', is_nullable = true},
        {name = 'quantity', type = 'unsigned', is_nullable = true},
    },
    primary_key = {'product'},
    distribution = 'sharded',
    sharding_key = {'product'},
    timeout = 3,
})
```

Вычисление совместимого с SQL хеша для параметра `bucket_id`:

```lua
local key = require('key_def').new({{fieldno = 2, type = 'string'}})
local tuple = box.tuple.new({'metalware'})
local bucket_id = key:hash(tuple) % vshard.router.bucket_count()
```

Добавление данных в шардированную таблицу происходит с помощью [VShard API](https://www.tarantool.io/en/doc/latest/reference/reference_rock/vshard/vshard_router/):

```lua
local bucket_id = vshard.router.bucket_id_mpcrc32('metalware')
vshard.router.callrw(bucket_id, 'box.space.DELIVERIES:insert', {{1, 'metalware', 2000}})
```

!!! note "Примечание"
    Данная функция требует обновления состояния на всех
    инстансах кластера и ожидает от них ответа. Возможна ситуация, когда
    запрос возвратит ошибку таймаута, так как за отведенное время успеют
    ответить не все инстансы. При этом запрос НЕ отменится и изменение
    может быть применено некоторое время спустя. По этой причине повторные
    вызовы функции с теми же аргументами всегда безопасны.

### pico.create_user {: #pico_create_user }

Создает нового пользователя на каждом инстансе кластера. Функция
генерирует для raft-журнала запись, которая при применении создает
пользователя. Для ожидания локального создания используется
таймаут. Результатом успешного выполнения функции является индекс записи
в raft-журнале на момент, когда пользователь уже гарантированно
существует. Если такой пользователь уже существует, то запрос игнорируется.

Данная функция учитывает ограничения по длине пароля для создаваемого
пользователя, определяемые параметром `password_min_length` в системной
таблице `_pico_property` (по умолчанию требуется пароль не короче 8
символов).

```lua
function create_user(user, password, [opts])
```

Параметры:

- `user` (_string_), имя пользователя
- `password` (_string_), пароль пользователя
- `opts`: (optional _table_), таблица:
    - `auth_type` (optional _string_), тип аутентификации, варианты:
      `'chap-sha1'` | `'md5'` | `'ldap'`
    - `timeout` (optional _number_), число в секундах. По умолчанию
      используется бесконечное значение.

Возвращаемое значение:

(_number_) с номером raft-индекса или <br>(_nil_, _error_ (ошибка в виде Lua-объекта)) в случае ошибки.

!!! note "Примечание"
    Если эта функция возвращает ошибку таймаута, то запрос
    НЕ отменяется и изменение может быть применено некоторое время спустя.
    По этой причине повторные вызовы функции с теми же аргументами всегда
    безопасны.

### pico.drop_role {: #pico_drop_role }

Удаляет роль и связанные с ней сущности с каждого инстанса кластера.
Например, будут удалены таблицы, владельцем которых была эта роль.
Функция генерирует для raft-журнала запись, которая при применении
удаляет роль. Для ожидания локального удаления используется
таймаут. Результатом успешного выполнения функции является индекс записи
в raft-журнале на момент, когда роль уже гарантированно не
существует. Если роли и так нет, то запрос игнорируется.

```lua
function drop_role (role, [opts])
```

Параметры:

- `role` (_string_), имя роли
- `opts`: (optional _table_), таблица:
    - `timeout` (optional _number_), число в секундах. По умолчанию
      используется бесконечное значение.

Возвращаемое значение:

(_number_) с номером raft-индекса или <br>(_nil_, _error_ (ошибка в виде Lua-объекта)) в случае ошибки.

!!! note "Примечание"
    Если эта функция возвращает ошибку таймаута, то запрос
    НЕ отменяется и изменение может быть применено некоторое время спустя.
    По этой причине повторные вызовы функции с теми же аргументами всегда
    безопасны.

### pico.drop_table {: #pico_drop_table }

Удаляет таблицу (пространство для хранения данных) на всех инстансах
кластера. Функция ожидает глобального удаления таблицы. Если ожидание
превышает таймаут, функция возвращает ошибку. Если таблицы не
существует, то запрос игнорируется.

```lua
function drop_table(table, [opts])
```

Параметры:

- `table` (_number_ | _string_), id или имя таблицы
- `opts`: (optional _table_), таблица:
    - `timeout` (optional _number_), число в секундах. По умолчанию
      используется бесконечное значение.

Возвращаемое значение:

(_number_) с номером raft-индекса или <br>(_nil_, _error_ (ошибка в виде Lua-объекта)) в случае ошибки.

!!! note "Примечание"
    Данная функция требует обновления состояния на всех
    инстансах кластера и ожидает от них ответа. Возможна ситуация, когда
    запрос возвратит ошибку таймаута, так как за отведенное время успеют
    ответить не все инстансы. При этом запрос НЕ отменится и изменение
    может быть применено некоторое время спустя. По этой причине повторные
    вызовы функции с теми же аргументами всегда безопасны.

### pico.drop_user {: #pico_drop_user }

Удаляет пользователя и связанные с ним сущности с каждого инстанса
кластера. Например, будут удалены таблицы, владельцем которых был этот
пользователь. Функция генерирует для raft-журнала запись, которая при
применении удаляет пользователя. Для ожидания локального удаления
используется таймаут. Результатом успешного выполнения функции является
индекс записи в raft-журнале на момент, когда пользователя уже
гарантированно не существует. Если его и так нет, то запрос
игнорируется.

```lua
function drop_user (user, [opts])
```

Параметры:

- `user` (_string_), имя пользователя
- `opts`: (optional _table_), таблица:
    - `timeout` (optional _number_), число в секундах. По умолчанию
      используется бесконечное значение.

Возвращаемое значение:

(_number_) с номером raft-индекса или <br>(_nil_, _error_ (ошибка в виде Lua-объекта)) в случае ошибки.

!!! note "Примечание"
    Если эта функция возвращает ошибку таймаута, то запрос
    НЕ отменяется и изменение может быть применено некоторое время спустя.
    По этой причине повторные вызовы функции с теми же аргументами всегда
    безопасны.

### pico.exit {: #pico_exit }

Корректно завершает работу указанного инстанса.

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

### pico.expel {: #pico_expel }

Выводит инстанс из кластера, но не завершает его работу. Может быть
запущена только один раз для инстанса с определенным `instance_id`.
Инстанс в состоянии `expelled` останется запущенным. Если его остановить
и запустить снова, то он не присоединится к raft-группе.

```lua
function expel("instance_id")
```

Параметры:

- `instance_id`: (_string_)

Возвращаемые значения:

- (_true_) — при успешном выполнении;
- <br>(_nil_, _string_) — при ошибке;

Пример:

```lua
pico.expel("i2")
```

### pico.grant_privilege {: #pico_grant_privilege }

Назначает привилегию пользователю или роли на всех инстансах кластера.
Функция генерирует для raft-журнала запись, которая при применении
назначает указанную привилегию. Для ожидания локального создания используется
таймаут. Результатом успешного выполнения функции является индекс
соответствующей записи в raft-журнале. Если указанная привилегия уже была
ранее назначена, то запрос игнорируется.

```lua
function grant_privilege(grantee, privilege, object_type, [object_name], [opts])
```

Параметры:

- `grantee` (_string_), имя пользователя или роли
- `privilege` (_string_), название привилегии, варианты: `'read'` | '`write'`
  | `'execute'` | `'session' `| `'usage'` | `'create'` | `'drop'` |
          `'alter'` | `'insert'` | `'update'` | `'delete'`
- `object_type` (_string_), тип целевого объекта, варианты: `'universe'`
  | `'table'` | `'sequence'` | `'function'` | `'role'` | `'user'`
- `object_name` (optional _string_), имя целевого объекта. Можно не
  указывать при адресации совокупностей целевых объектов (см. примеры
  [ниже](#pico_grant_privilege))
- `opts`: (optional _table_), таблица:
    - `timeout` (optional _number_), число в секундах. По умолчанию
      используется бесконечное значение.

Возвращаемое значение:

(_number_) с номером raft-индекса или <br>(_nil_, _error_ (ошибка в виде Lua-объекта)) в случае ошибки.

!!! note "Примечание"
    Если эта функция возвращает ошибку таймаута, то запрос
    НЕ отменяется и изменение может быть применено некоторое время спустя.
    По этой причине повторные вызовы функции с теми же аргументами всегда
    безопасны.

Примеры:<a name="grant_pr"></a>

Выдать только что созданному пользователю `Dave` привилегию `create
table`, чтобы он мог создавать таблицы, записывать в них данные и читать
их:

```lua
pico.grant_privilege('Dave', 'create', 'table')
```

Выдать привилегию на чтение таблицы 'Fruit' пользователю 'Dave':

```lua
pico.grant_privilege('Dave', 'read', 'table', 'Fruit')
```

Выдать пользователю 'Dave' привилегию исполнять произвольный код Lua:

```lua
pico.grant_privilege('Dave', 'execute', 'universe')
```

Выдать пользователю 'Dave' привилегию создавать новых пользователей:

```lua
pico.grant_privilege('Dave', 'create', 'user')
```

Выдать привилегию на запись в таблицу 'Junk' для роли 'Maintainer':

```lua
pico.grant_privilege('Maintainer', 'write', 'table', 'Junk')
```

Назначить роль 'Maintainer' пользователю 'Dave':

```lua
pico.grant_privilege('Dave', 'execute', 'role', 'Maintainer')
```

!!! note "Примечание"
    Глобальные привилегии (на весь класс объектов) может
    выдавать только Администратор СУБД (`admin`)

### pico.help {: #pico_help }

Предоставляет доступ к встроенной справочной системе.

```lua
function help(topic)
```

Параметры:

- `topic`: (optional _string_)

Пример:

```
picodata> pico.help("help")
-- Печатает данную справку
-- и список доступных разделов (topics)
```

### pico.instance_info {: #pico_instance_info }

Показывает информацию о текущем инстансе.

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
      `variant` (_string_), варианты: `'Offline'` | `'Replicated'` | `'ShardingInitialized'` | `'Online'` | `'Expelled'`;
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

### pico.raft_compact_log {: #pico_raft_compact_log }

Компактизирует raft-журнал до указанного индекса (не включая сам индекс).

Функция имеет эффект только на текущем инстансе.

```lua
function raft_compact_log(up_to)
```

Параметры:

- `up_to`: (optional _number_), значение по умолчанию: `inf`

Возвращаемое значение:

(_number_)

Функция возвращает значение `first_index` — индекс первой записи в
raft-журнале.

### pico.raft_get_index {: #pico_raft_get_index }

Получает текущий примененный индекс raft-журнала.

```lua
function raft_get_index()
```

Возвращаемое значение:

(_number_)

### pico.raft_log {: #pico_raft_log }

Позволяет ознакомиться с содержимым raft-журнала в человекочитаемом
формате. Содержимое журнала предоставляется в виде таблицы со строками,
подобно тому как `fselect` выводит содержимое таблиц.

Параметр `opt.justify_contents` можно использовать для изменения
выравнивания столбцов таблицы.

Параметр `opts.max_width` позволяет задать максимальную ширину таблицы в
знаках. Если фактически таблица шире, то часть данных будет обрезана.
Если этот параметр не указывать, то по умолчанию в локальной сессии
будет использоваться максимальная ширина терминала (для удаленной сессии
используется другое значение, см. ниже).

```lua
function raft_log([opts])
```

Параметры:

- `opts`: (_table_), таблица:
    - `justify_contents` (_string_), варианты: `'center'` | `'left'` |
      `'right'`, вариант по умолчанию: `'center'`
    - `max_width` (_number_), значение по умолчанию для удаленной
      сессии: `80` знаков.

Возвращаемое значение:

(_table_)

Пример:

```console
pico.raft_log({justify_contents = 'center', max_width = 100})
```

### pico.raft_propose_nop {: #pico_raft_propose_nop }

Добавляет в raft-журнал запись `Nop` (no operation). Используется для
обновления raft-журнала путем добавления в него свежей записи. Функция
не имеет передаваемых параметров.

### pico.raft_read_index {: #pico_raft_read_index }

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

Возвращаемое значение:

(_number_) или <br>(_nil_, _string_) в случае ошибки.

Пример:

```console
picodata> pico.raft_read_index(1)
---
- 42
...
```

### pico.raft_status {: #pico_raft_status }

Получает данные о текущем состоянии raft-узла
([терм](../overview/glossary.md#term), [лидер](../overview/glossary.md#leader) и т.д.). Функция
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

### pico.raft_term {: #pico_raft_term }

Возвращает номера терма для указанной записи, либо текущий номер терма если запись не указана.

```lua
function raft_term([index])
```

Параметры:

- `index`: (optional _number_), номер raft-записи

Возвращаемое значение:

(_number_) или <br>(_nil_, _string_) в случае ошибки.


### pico.raft_wait_index {: #pico_raft_wait_index }

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

(_number_) или <br>(_nil_, _string_) в случае ошибки.

Если за указанное время (`timeout`) функция не успеет получить индекс, она
вернет сообщение об ошибке.

### pico.revoke_privilege {: #pico_revoke_privilege }

Удаляет привилегию пользователя или роли на всех инстансах кластера. Функция
генерирует для raft-журнала запись, которая при применении удаляет
указанную привилегию. Для ожидания локального создания используется таймаут.
Результатом успешного выполнения функции является индекс соответствующей
записи в raft-журнале. Если указанной привилегии у пользователя нет, то
запрос игнорируется.

```lua
function revoke_privilege(grantee, privilege, object_type, [object_name], [opts])
```

Параметры:

- `grantee` (_string_), имя пользователя или роли
- `privilege` (_string_), название привилегии, варианты: `'read'` | '`write'`
  `| `'execute'` | `'session' `| `'usage'` | `'create'` | `'drop'` |
          `'alter'` | `'insert'` | `'update'` | `'delete'`
- `object_type` (_string_), тип целевого объекта, варианты: `'universe'`
  | `'table'` | `'sequence'` | `'function'` | `'role'` | `'user'`
- `object_name` (optional _string_), имя целевого объекта. Можно не
  указывать при адресации совокупностей целевых объектов (аналогично
  действию `grant_privilege`, см. примеры [выше](#pico_grant_privilege))
- `opts`: (optional _table_), таблица:
    - `timeout` (optional _number_), число в секундах. По умолчанию
      используется бесконечное значение.

Возвращаемое значение:

(_number_) с номером raft-индекса или <br>(_nil_, _error_ (ошибка в виде
Lua-объекта)) в случае ошибки.

!!! note "Примечание"
    Если эта функция возвращает ошибку таймаута, то запрос
    НЕ отменяется и изменение может быть применено некоторое время спустя.
    По этой причине повторные вызовы функции с теми же аргументами всегда
    безопасны.

### pico.sql {: #pico_sql }

Выполняет кластерные (распределенные) SQL-запросы по следующей схеме:

- каждый запрос обрабатывается и проверяется на корректность, после чего
  составляется распределенный план запроса для выполнения на текущем
  узле маршрутизации;
- план запроса посылается на все узлы хранения в виде последовательных
  фрагментов по принципу "снизу вверх". Промежуточные результаты
  выполнения локальных запросов сохраняются в памяти
  узла-маршрутизатора.

```lua
function sql(query[, params], [traceable])
```

Параметры:

- `query` (_string_)
- `params`: (optional _table_), список параметров
- `traceable` (optional _boolean_), признак явной трассировки запроса

Возвращаемое значение:

- [`table DqlResult`](#dql_table), при чтении данных;
- [`table DmlResult`](#dml_table), при модификации данных;
- (_nil_, _string_) в случае ошибки.

Пример создания шардированной таблицы:

```sql
pico.sql([[
    create table "wonderland" (
        "property" text not null,
        "value" integer,
        primary key ("property")
    ) using memtx distributed by ("property")
    option (timeout = 3.0)
]])
---
- row_count: 1
...
```

Пример удаления шардированной таблицы:

```sql
pico.sql([[
    drop table "wonderland"
    option (timeout = 3.0)
]])
---
- row_count: 1
...
```

Пример параметризированной вставки в шардированную таблицу:

```sql
pico.sql([[
  insert into "wonderland" ("property", "value") values (?, ?)]],
  {"dragon", 13}
)
---
- row_count: 1
...
```

Пример чтения из шардированной таблицы:

```sql
pico.sql([[
  select * from "wonderland" where "property" = 'dragon'
  ]])
---
- metadata:
    - {'name': 'property', 'type': 'string'}
    - {'name': 'value', 'type': 'integer'}
  rows:
    - ['dragon', 13]
...
```

Пример создания пользователя:

```lua
pico.sql([[
  create user "alice"
  with password 't0tallysecret'
  using chap-sha1
  ]])
```

См. также:

- [Работа с данным SQL](../tutorial/sql_examples.md)

### pico.wait_ddl_finalize {: #pico_wait_ddl_finalize }

Ожидает применения (финализации) DDL-операции для указанного
raft-индекса. Возвращает raft-индекс финализированной записи.

```lua
function wait_ddl_finalize(index, opts)
```

Параметры:

- `index` (_number_), raft-index
- `opts`: (_table_), таблица:
    - `timeout` (_number_), в секундах (значение по умолчанию: 3 с.)

Возвращаемое значение:

(_number_) с номером raft-индекса или <br>(_nil_, _string_) в случае ошибки.

### pico.wait_vclock {: #pico_wait_vclock }

Ожидает момента, когда значение
[Vclock](../overview/glossary.md#vclock) в Tarantool достигнет
целевого.

```lua
function wait_vclock(target, timeout)
```

Параметры:

- `target`: (_table Vclock_)
- `timeout`: (_number_)

### pico.whoami {: #pico_whoami }

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

### table CasBound {: #casbound }

Lua-таблица, используемая для обозначения минимального (`key_min`) или
максимального (`key_max`) значения в таблице `table CasRange`.

Поля:

- `kind` (_string_), варианты: `'included'` | `'excluded'` | `'unbounded'`
- `key` (optional _table_), обязательно для вариантов `included` и `excluded`

### table CasRange {: #casrange }

Lua-таблица, задающая диапазон значений. Используется для обозначения
предиката (проверяемых данных) в функции [pico.cas()](#pico_cas).

Поля:

- `table` (_string_)
- `key_min` (table CasBound), см. [выше](#casbound)
- `key_max` (table CasBound)

Пример:

```lua
local unbounded = { kind = 'unbounded' }
local including_1 = { kind = 'included', key = {1,} }
local excluding_3 = { kind = 'excluded', key = {3,} }

local range_a = {
    table = 'WAREHOUSE',
    key_min = unbounded,
    key_max = unbounded,
}

-- [1, 3)
local range_a = {
    table = 'WAREHOUSE',
    key_min = including_1,
    key_max = excluding_3,
}
```

### table DqlResult {: #dql_table }

Lua-таблица, содержащая данные чтения из шардированной таблицы.

Поля:

- `metadata` (_table_), массив столбцов таблицы (таблицы) в формате `{{name = string, type = string}, ...}`.
- `rows` (_table_), результат выполнения читающего запроса в формате `{row, ...}`.

### table DmlResult {: #dml_table }

Lua-таблица, содержащая количество измененных строк при модификации шардированной таблицы.

Поля:

- `row_count` (_number_), количество измененных строк.

### table TableField {: #tablefield_table }

Lua-таблица, описывающая поле в составе таблицы (см. [pico.create_table](#pico_create_table)).

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
- Описание [типов полей Tarantool](https://docs.rs/tarantool/latest/tarantool/space/enum.FieldType.html)

### table Vclock {: #vclock_table }

Lua-таблица, отражающая соответствие `id` инстанса его
[LSN-номеру](../overview/glossary.md#lsn).

Пример:

```lua
{[0] = 2, [1] = 101}
{[0] = 148, [1] = 9086, [3] = 2}
```

См. подробнее описание [Vclock](../overview/glossary.md#vclock). Нулевое значение Vclock зарезервировано
для отслеживания локальных изменений, которые не реплицируются.
