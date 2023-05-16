# Описание публичного API Picodata
## Lua-интерфейс и хранимые процедуры

Picodata позволяет использовать RPC (remote procedure call), т.е. вызывать публично доступные удаленные хранимые процедуры в консоли подключения к инстансу Picodata через Lua-интерфейс.

Поддерживается вызов следующих функций и хранимых процедур:
```
tarantool> _G.pico.
_G.pico.VERSION               _G.pico.raft_log(
_G.pico.add_migration(        _G.pico.raft_propose_nop(
_G.pico.args                  _G.pico.raft_read_index(
_G.pico.cas(                  _G.pico.raft_status(
_G.pico.exit(                 _G.pico.raft_tick(
_G.pico.expel(                _G.pico.raft_timeout_now(
_G.pico.instance_info(        _G.pico.space
_G.pico.log                   _G.pico.sql(
_G.pico.migrate(              _G.pico.trace(
_G.pico.push_schema_version(  _G.pico.whoami(
_G.pico.raft_compact_log(   
```
Описание функций и хранимых процедур приведено ниже:

- `_G.pico.VERSION`. Вывод версии Picodata.
- `_G.pico.add_migration`(). Создание новой миграции (перехода к новой схеме данных).
- `_G.pico.args`. Передача собственных аргументов (?).
- `_G.pico.cas()`. Запрос на изменение параметров методом [Compare and Swap](glossary.md#сas-compare-and-swap)
- `_G.pico.exit()`. Завершение работы указанного инстанса Picodata.
- `_G.pico.expel()`. [Контролируемый вывод](cli.md#описание-команды-expel) инстанса Picodata из кластера.
- `_G.pico.instance_info()`. Получение информации об инстансе Picodata (идентификаторы, уровни ([grade](glossary.md#грейд-grade)) и прочее).
- `_G.pico.log`. Просмотр журнала инстанса.
- `_G.pico.migrate()`. Применение миграции.
- `_G.pico.push_schema_version()`. Увеличение номера версии схемы данных.
- `_G.pico.raft_compact_log()`. [Обрезание](glossary.md#компактизация-raft-журнала-raft-log-compaction) Raft-журнала до указанного числа последних записей.
- `_G.pico.raft_log()`. Вывод содержимого Raft-журнала.
- `_G.pico.raft_propose_nop()`. Что-то делает, но непонятно что.
- `_G.pico.raft_read_index()`. Чтение индекса Raft-журнала.
- `_G.pico.raft_status()`. Получение данных о текущем состоянии Raft-журнала ([терм](glossary.md#терм-term), лидер и т.д.)
- `_G.pico.raft_tick()`. Можно передавать число неких "тактов" (?). 
- `_G.pico.raft_timeout_now()`. Установка времени таймаута Raft-журнала (?).
- `_G.pico.space`. Выводит данные о спейсах (таблицах), хранящихся на данном инстансе.
- `_G.pico.sql()`. Предположу, что в скобки можно поместить SQL-запрос.
- `_G.pico.trace()`. Тут тоже в кавычках ожидается строка с SQL-запросом.
- `_G.pico.whoami()`. Отображение данных о текущем пользователе (судя по всему, ещё не реализовано).

Подробнее об устройстве кластера и репликасетов Picodata см. разделе [Инструкция по развертыванию](../deploy)
