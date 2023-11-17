# Описание системных таблиц

Данный раздел описывает [таблицы](../overview/glossary.md#table)
Picodata, используемые для служебных нужд. Перечисленные системные
таблицы являются глобальными.

## Описание схемы данных {: #schema }

### _pico_space {: #_pico_space }

Содержит информацию о пользовательских
[таблицах](../overview/glossary.md#table) Picodata.

Поля:

* `id`: (_unsigned_)
* `name`: (_string_)
* `distribution`: (_array_)
* `format`: (_array_)
* `schema_version`: (_unsigned_) версия схемы, в которой таблица была
  создана. Используется при восстановлении из снапшота для корректной
  обработки шардированных таблиц. Неявно инкрементируется при вызове
  функции `space_object:truncate()`
* `operable`: (_boolean_) признак доступности таблицы на запись.
  Используется в процессе создания и удаления таблиц

Индексы:

* `id` (unique), parts: `id`
* `name` (unique), parts: `name`

### _pico_index {: #_pico_index }

Содержит информацию об [индексах](../overview/glossary.md#index) БД.

Поля:

* `space_id`: (_unsigned_)
* `id`: (_unsigned_)
* `name`: (_string_)
* `local`: (_boolean_)
* `parts`: (_array_)
* `schema_version`: (_unsigned_)
* `operable`: (_boolean_)
* `unique`: (_boolean_)

Индексы:

* `id` (unique), parts: `[space_id, id]`
* `name` (unique), parts: `[space_id, name]`
