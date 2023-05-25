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
- `_G.pico.args`. Вывод аргументов командной строки, с которыми был запущен инстанс.
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

## Примеры использования функций
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

