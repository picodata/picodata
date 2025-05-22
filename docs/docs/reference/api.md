---
search:
  exclude: true
---

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
(admin) lua> pico.help("help");
-- Печатает данную справку
-- и список доступных разделов (topics)
```

Вызов функций `pico.*` возможен и через `iproto`, но влечет за собой
накладные расходы, связанные с конвертацией из формата MessagePack в Lua
и обратно.

| Функция                       | Описание                          |
|-------------------------------|-----------------------------------|
| [pico.LUA_API_VERSION](#pico_lua_api_version) | Версия Lua API.
| [pico.PICODATA_VERSION](#pico_picodata_version) | Версия инстанса.
| [pico.abort_ddl](#pico_abort_ddl) | Отмена ожидающей операции по изменению схемы данных.
| [pico.cas()](#pico_cas) | Запрос на изменение параметров методом [Compare and Swap](../overview/glossary.md#cas).
| [pico.exit()](#pico_exit) | Корректное завершение работы указанного инстанса.
| [pico.expel()](#pico_expel) | [Контролируемый вывод](cli.md#expel) инстанса из кластера.
| [pico.help()](#pico_help) | Доступ к встроенной справочной системе.
| [pico.instance_info()](#pico_instance_info) | Получение информации об инстансе (идентификаторы, уровни ([state](../overview/glossary.md#state)) и прочее).
| [pico.raft_compact_log()](#pico_raft_compact_log) | [Компактизация](../overview/glossary.md#raft_log_compaction) raft-журнала c удалением указанного числа наиболее старых записей.
| [pico.raft_log()](#pico_raft_log) | Чтение содержимого raft-журнала.
| [pico.raft_propose_nop()](#pico_raft_propose_nop) | Добавление в raft-журнал запись `Nop` (no operation).
| [pico.raft_read_index()](#pico_raft_read_index) | Кворумное чтение индекса raft-журнала.
| [pico.raft_status()](#pico_raft_status) | Получение данных о текущем состоянии raft ([терм](../overview/glossary.md#term), [лидер](../overview/glossary.md#leader) и т.д.).
| [pico.raft_term()](#pico_raft_term) | Получение номера терма (текущего или для указанной записи).
| [pico.raft_wait_index()](#pico_raft_wait_index) | Ожидание локального применения указанного raft-индекса.
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
(admin) lua> pico.LUA_API_VERSION;
---
- 1.0.0
...
```

### pico.PICODATA_VERSION {: #pico_picodata_version }

Строковая переменная (не функция), которая содержит версию инстанса.
Формат соответствует календарному версионированию ([Calendar
Versioning][calver]) с форматом `YY.0M.MICRO`.

[calver]: https://calver.org/#scheme

Пример:

```console
(admin) lua> pico.PICODATA_VERSION;
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

### pico.cas {: #pico_cas }

Функция проверяет предикат на лидере. Если проверка не выявляет
конфликтов, то функция добавляет новую запись в raft-журнал и возвращает
ее индекс (пока еще не зафиксированный в кластере). Функция применяется
только для глобальных таблиц и работает на всем кластере.

Предикат состоит из трех частей:

- номера индекса схемы данных (`index`);
- номера терма (`term`);
- диапазона значений самого проверяемого параметра (`ranges`).

Если предикат не указан, он будет автоматически сгенерирован на основе
текущих `index` и `term`, а также диапазона `ranges`, применимого к
текущей операции.

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
    table = 'warehouse',
    tuple = {6, 99, 'chunks', 'light'},
});
```

Здесь, `6` — значение первой колонки (`id`), `99` — значение `bucket_id`.

Пример с указанием предиката (проверка, что никакие другие записи не
были добавлены ранее):

```lua
pico.cas({
    kind = 'replace',
    table = 'warehouse',
    tuple = {6, 99, 'chunks', 'light'},
}, {
    ranges = {{
        table = 'warehouse',
        key_min = { kind = 'excluded', key = {1,} },
        key_max = { kind = 'unbounded' },
    }},
});
```

Если пользователь явно указывает `ranges`, они добавляются к тем,
которые неявно проверяются в любом случае (в примере выше — это диапазон
по таблице 'warehouse' и первичному ключа добавляемого кортежа)

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
запущена только один раз для инстанса с определенным `instance_name`.
Инстанс в состоянии `expelled` останется запущенным. Если его остановить
и запустить снова, то он не присоединится к raft-группе.

```lua
function expel("instance_name")
```

Параметры:

- `instance_name`: (_string_)

Возвращаемые значения:

- (_true_) — при успешном выполнении;
- <br>(_nil_, _string_) — при ошибке;

Пример:

```lua
pico.expel("i2");
```

### pico.help {: #pico_help }

Предоставляет доступ к встроенной справочной системе.

```lua
function help(topic)
```

Параметры:

- `topic`: (optional _string_)

Пример:

```
(admin) lua> pico.help("help");
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
    - `iproto_advertise` (_string_)
    - `name` (_string_)
    - `uuid` (_string_)
    - `replicaset_name` (_string_)
    - `replicaset_uuid` (_string_)
    - `current_state` (_table_).
      `variant` (_string_), варианты: `'Offline'` | `'Online'` | `'Expelled'`;
      `incarnation` (_number_)
    - `target_state` (_table_).
      `variant` (_string_), варианты: `'Offline'` | `'Online'` | `'Expelled'`;
      `incarnation`(_number_),

Пример:

```lua
(admin) lua> pico.instance_info();
 ---
- target_state:
    variant: Online
    incarnation: 1
  uuid: 97048f02-e1d5-4062-b079-a2e2c674cb01
  iproto_advertise: 127.0.0.1:3301
  tier: default
  raft_id: 1
  replicaset_uuid: 29d45b4e-8e1a-4973-b8cb-062881bacb11
  current_state:
    variant: Online
    incarnation: 1
  name: i1
  replicaset_name: r1

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
(admin) lua> pico.raft_read_index(1);
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
(admin) lua> pico.raft_status();
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
function sql(query[, params])
```

Параметры:

- `query` (_string_)
- `params`: (optional _table_), список параметров

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
]]);
---
- row_count: 1
...
```

Пример удаления шардированной таблицы:

```sql
pico.sql([[
    drop table "wonderland"
    option (timeout = 3.0)
]]);
---
- row_count: 1
...
```

Пример параметризированной вставки в шардированную таблицу:

```sql
pico.sql([[
  insert into "wonderland" ("property", "value") values (?, ?)]],
  {"dragon", 13}
);
---
- row_count: 1
...
```

Пример чтения из шардированной таблицы:

```sql
pico.sql([[
  select * from "wonderland" where "property" = 'dragon'
  ]]);
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
  using md5
  ]]);
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
    - `cluster_name`: (_string_)
    - `instance_name`: (_string_)

Пример:

```console
(admin) lua> pico.whoami();
- raft_id: 1
  cluster_name: demo
  instance_name: i1
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
local unbounded = { kind = 'unbounded' };
local including_1 = { kind = 'included', key = {1,} };
local excluding_3 = { kind = 'excluded', key = {3,} };

local range_a = {
    table = 'warehouse',
    key_min = unbounded,
    key_max = unbounded,
};

-- [1, 3)
local range_a = {
    table = 'warehouse',
    key_min = including_1,
    key_max = excluding_3,
};
```

### table DqlResult {: #dql_table }

Lua-таблица, содержащая данные чтения из шардированной таблицы.

Поля:

- `metadata` (_table_), массив столбцов таблицы (таблицы) в формате `{{name = string, type = string}, ...}`
- `rows` (_table_), результат выполнения читающего запроса в формате `{row, ...}`

### table DmlResult {: #dml_table }

Lua-таблица, содержащая количество измененных строк при модификации шардированной таблицы.

Поля:

- `row_count` (_number_), количество измененных строк.

### table TableField {: #tablefield_table }

Lua-таблица, описывающая поле в составе таблицы.

Поля:

- `name` (_string_)
- `type` (_string_)
- `is_nullable` (_boolean_)

Пример:

```lua
{name = 'id', type = 'unsigned', is_nullable = false};
{name = 'value', type = 'unsigned', is_nullable = false};
```

См. также:

- Описание [space_object:format()](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/format/)
- Описание [типов полей Tarantool](https://docs.rs/tarantool/latest/tarantool/space/enum.FieldType.html)

### table Vclock {: #vclock_table }

Lua-таблица, отражающая соответствие `id` инстанса его
[LSN-номеру](../overview/glossary.md#lsn).

Пример:

```lua
{[0] = 2, [1] = 101};
{[0] = 148, [1] = 9086, [3] = 2};
```

См. подробнее описание [Vclock](../overview/glossary.md#vclock). Нулевое значение Vclock зарезервировано
для отслеживания локальных изменений, которые не реплицируются.
