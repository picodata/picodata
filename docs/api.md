# Описание публичного API Picodata
## Общие сведения

Picodata позволяет использовать RPC (remote procedure call), т.е.
вызывать публично доступные удаленные хранимые процедуры в консоли
подключения к инстансу Picodata через Lua-интерфейс. Подключение может
осуществляться как из Bash-консоли (`tarantoolctl connect`), так и с
помощью внешних коннекторов (например из Python).

## Поддерживаемые функции

Поддерживается вызов следующих функций и хранимых процедур:
```
tarantool> _G.pico.
_G.pico.VERSION
_G.pico.args
_G.pico.raft_read_index(
_G.pico.raft_propose_nop(
_G.pico.cas(
_G.pico.raft_status(
_G.pico.exit(
_G.pico.expel(
_G.pico.raft_timeout_now(
_G.pico.instance_info(
_G.pico.whoami(
_G.pico.raft_compact_log(
```
## Описание функций
Описание функций и хранимых процедур приведено ниже:

- `_G.pico.VERSION`. Вывод версии Picodata.
- `_G.pico.args`. Вывод данных о запущенном инстансе.
- `_G.pico.raft_read_index()`. Чтение индекса Raft-журнала.
- `_G.pico.raft_propose_nop()`. Добавляет в Raft-журнал запись `Nop` (no operation).
- `_G.pico.cas()`. Запрос на изменение параметров методом [Compare and Swap](glossary.md#сas-compare-and-swap).
- `_G.pico.raft_status()`. Получение данных о текущем состоянии Raft-журнала ([терм](glossary.md#терм-term), лидер и т.д.)
- `_G.pico.exit()`. Корректное завершение работы указанного инстанса Picodata.
- `_G.pico.expel()`. [Контролируемый вывод](cli.md#описание-команды-expel) инстанса Picodata из кластера.
- `_G.pico.raft_timeout_now()`.  Вызывает таймаут Raft-узла прямо сейчас, инициируеют новые выборы в Raft-группе.
- `_G.pico.instance_info()`. Получение информации об инстансе Picodata (идентификаторы, уровни ([grade](glossary.md#грейд-grade)) и прочее).
- `_G.pico.whoami()`. Отображение данных о текущем пользователе (судя по всему, ещё не реализовано).
- `_G.pico.raft_compact_log()`. [Обрезание](glossary.md#компактизация-raft-журнала-raft-log-compaction) Raft-журнала c удалением указанного числа наиболее старых записей.

## Описание функций
### `VERSION` (string)

Содержит версию Picodata. Семантика соответствует календарному
версионированию ([Calendar Versioning][calver]) с форматом
`YY.0M.MICRO`.

[calver]: https://calver.org/#scheme

**Пример**

```console
tarantool> _G.pico.VERSION
---
- 22.11.0
...
```
### args (string)
Выводит набор параметров текущего инстанса, в частности:
- ID кластера;
- ключи домена отказа (см. [подробнее](glossary.md#домен-отказа-failure_domain));
- уровень журналирования;
- указанный исходный фактор репликации (см. [подробнее](glossary.md#фактор-репликации-replication_factor));
- адрес (хост:порт), по которому инстанс принимает соединения;
- адреса соседних инстансов (peers);
- рабочую директорию, в которой инстанс хранит снимки состояния и рабочие файлы.

**Возвращаемое значение** (_table_)

Таблица с полями:
- `cluster_id`: (_string_)
- `failure_domain`: (_table_ {[_string_] = _string_})
- `log_level`: (_string_)
- `init_replication_factor`: (_number_)
- `listen`: (_string_)
- `peers`: (_table_ {_string_})
- `data_dir`: (_string_)


**Пример**
```console
tarantool> _G.pico.args
---
- cluster_id: demo
  failure_domain: []
  log_level: info
  init_replication_factor: 2
  listen: localhost:3302
  peers:
  - localhost:3301
  data_dir: inst2
...
```
### instance_info()
Функция показывает информацию о текущем инстансе Picodata

```lua
function instance_info(instance)
```
**Параметры**
- `instance`: (_string_)

**Возвращаемое значение** (_table_)

Таблица с полями:

- `target_grade`: (_table_)
  - `variant`: (_string_)
  - `incarnation` (_number_)
- `instance_id`: (_string_)
- `instance_uuid`: (_number_)
- `raft_id`: (_string_)
- `current_grade`: (_table_)
  - `variant`: (_string_)
  - `incarnation`: (_number_)
- `replicaset_uuid`: (_number_)
- `replicaset_id`: (_string_)
- `advertise_address`: (_string_)

**Пример**

```console
tarantool> _G.pico.instance_info(i2)
---
- target_grade:
    variant: Online
    incarnation: 1
  instance_id: i4
  instance_uuid: 826cbe5e-6979-3191-9e22-e39deef142f0
  raft_id: 4
  current_grade:
    variant: Online
    incarnation: 1
  replicaset_uuid: eff4449e-feb2-3d73-87bc-75807cb23191
  replicaset_id: r2
  advertise_address: localhost:3304
...
```

### `expel` (function)

```lua
function expel("instance_id")
```
Выводит инстанс из кластера, но не завершает его работу. Может быть запущена только один раз для определенного инстанса.

**Параметры**
- `instance_id`: (_string_)

**Пример**
```
pico.expel("i2") 
```
У функции нет непосредственно возвращаемых значений

**Результат работы**

На инстансе, с которого была вызвана функция, в консоли `stdout` появятся сообщения:
```
downgrading instance i2
reconfiguring sharding
```
И далее для оставшихся в raft-группе инстансов сообщения вида:
```
calling rpc::sharding
```
Инстанс в состоянии `expelled` останется запущенным. При его перезапуске он не присоединится снова к raft-группе.


### `whoami` (function)

```lua
function whoami()
```

Возвращает идентификаторы инстанса.

**Возвращаемое значение** (_table_)

Таблица с полями:

- `raft_id`: (_number_)
- `cluster_id`: (_string_)
- `instance_id`: (_string_)

**Пример**

```console
tarantool> _G.pico.whoami()
- raft_id: 1
  cluster_id: demo
  instance_id: i1
```
### cas (function)
Отправляет запрос на вызов/изменение параметра с учетом его текущего
значения (проверяется на лидере). Функция работает на всем кластере.

```lua
function cas({args},...)
```

**Параметры**

 - `args`: (_string_ = '_string_' | {_table_} )

**Пример**
```
pico.cas({space = 'test', kind = 'insert', tuple = {13, 37} }, { timeout = 1 }) 
```

**Возвращаемое значение** (_number_)

Функция возвращает индекс сделанной записи в raft-журнале.

### `raft_compact_log` (function)

Компактизирует raft-журнал до указанного индекса (не включая сам индекс).

Функция имеет эффект только на текущем инстансе.

```lua
function raft_compact_log(up_to)
```

**Параметры**

- `up_to`: (_number_) (_optional_, default: `inf`)

**Возвращаемое значение** (_number_)

Функция возвращает значение `first_index` — индекс первой записи в raft-журнале.

