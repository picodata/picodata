# Хранение системных таблиц в памяти

<span class="supported">поддерживается с версии 25.4.1</span>

Для системных таблиц [memtx](../overview/glossary.md#db_engine) использует
**отдельный slab-аллокатор**, изолированный от аллокатора пользовательских
таблиц. Каждый из двух аллокаторов работает с собственной квотой
(`memtx_memory` и `memtx_system_memory`), собственным `slab_arena` и
собственными пулами для индексов и итераторов. Тем самым исчерпание
памяти на пользовательской нагрузке не блокирует работу с системным каталогом
кластера, изменения схемы и операции [governor](../overview/glossary.md#governor),
а DDL-операции получают более предсказуемый, не зависящий от пользовательской
нагрузки бюджет памяти.

## Распределение таблиц по аллокаторам {: #memtx_system_allocator_routing }

В системный аллокатор попадают все таблицы с идентификатором в диапазоне
`[0, 1024]`. Сюда входят:

* служебные таблицы Tarantool (`_space`, `_index`, `_user`, `_priv`, `_func`,
  `_schema` и т. п., идентификаторы `[0, 511]`);
* системные таблицы Picodata `_pico_*` и таблицы `_bucket`/`_pico_bucket`
  (идентификаторы `[512, 1024]`) — см. [список системных таблиц](system_tables.md#schema).

Все пользовательские таблицы (с идентификатором ≥ 1025), создаваемые через
[CREATE TABLE](../reference/sql/create_table.md), используют пользовательский
аллокатор.
При низкоуровневом создании таблицы через C/Lua space API для нее необходимо
явно указать свободный идентификатор из пользовательского диапазона (≥ 1025).

Например, так:
```lua
space_id = 1025

if box.space._space:get{space_id} ~= nil then
    error(("space id %d is already occupied"):format(space_id))
end

s = box.schema.space.create('my_user_space', {
    id = space_id,
    engine = 'memtx',
})

s:format({
    { name = 'id', type = 'unsigned' },
    { name = 'name', type = 'string' },
})

s:create_index('primary', {
    parts = {
        { field = 'id', type = 'unsigned' },
    },
})
```

При исчерпании квоты **пользовательского** аллокатора запросы
[INSERT](../reference/sql/insert.md) и [UPDATE](../reference/sql/update.md)
к пользовательским таблицам отклоняются с ошибкой *ER_MEMORY_ISSUE*.
Системный каталог и системные операции при этом продолжают работать.

!!! warning "Исчерпание системного аллокатора приводит к остановке инстанса"
    Системный аллокатор обслуживает в том числе таблицы
    [Raft](../overview/glossary.md#raft) `_raft_log` и `_raft_state`. Запись
    в эти таблицы выполняется на каждом такте работы Raft (новые записи
    журнала, обновление `term`/`vote`/`commit`, фиксация конфигурации
    кластера) и обязательна для корректной работы алгоритма консенсуса.

    Если квота системного аллокатора исчерпана, очередная попытка записи
    raft-состояния завершится ошибкой *ER_MEMORY_ISSUE*. Инстанс не способен
    продолжать работу в этом состоянии: транзакция будет откачена, а сам
    инстанс — **аварийно остановлен**. После остановки штатный супервизор
    (systemd, container runtime и т. п.) запускает инстанс повторно, и тот
    воссоединяется с кластером.

    На уровне кластера это проявляется как отказ инстанса: пока работает
    кворум здоровых инстансов в репликасете и raft-группе, кластер сохраняет
    доступность; при одновременном исчерпании квоты на нескольких инстансах
    возможна потеря кворума.

    Чтобы избежать этой ситуации:

    * выделяйте системному аллокатору запас памяти относительно фактического
      использования — следите за `box.slab.system_info().quota_used_ratio`
      (см. [мониторинг](#memtx_system_allocator_monitoring)) и настраивайте
      оповещения при приближении к 100%;
    * ограничивайте рост `_raft_log` параметрами
      [`raft_wal_size_max`](../reference/db_config.md#raft_wal_size_max) и
      [`raft_wal_count_max`](../reference/db_config.md#raft_wal_count_max)
      команды [ALTER SYSTEM](../reference/sql/alter_system.md);
    * учитывайте, что raft-журнал не может быть сжат дальше индекса самой
      отстающей реплики, и устраняйте долго отстающих фолловеров.

## Конфигурирование {: #memtx_system_allocator_config }

Размеры пулов задаются независимо двумя параметрами:

* [`instance.memtx.memory`](../reference/config.md#instance_memtx_memory)
  (CLI: [`--memtx-memory`](../reference/cli.md#run_memtx_memory)) — квота
  пользовательского аллокатора. По умолчанию — 64 МБ.
* [`instance.memtx.system_memory`](../reference/config.md#instance_memtx_system_memory)
  (CLI: [`--memtx-system-memory`](../reference/cli.md#run_memtx_system_memory)) —
  квота системного аллокатора. По умолчанию — 256 МБ, минимальное значение —
  32 МБ.

Оба параметра можно увеличивать в рантайме (через `box.cfg{...}`), но
уменьшение квоты ниже текущего объема выделенной памяти не поддерживается.

При расчете требуемой оперативной памяти на инстанс следует учитывать
**сумму** обоих бюджетов (см. [Рекомендации по сайзингу](../admin/sizing.md#sizing_memtx)).

## Мониторинг {: #memtx_system_allocator_monitoring }

Для каждого из аллокаторов доступен независимый набор Lua-функций с
одинаковой структурой возвращаемых данных:

| Пользовательский аллокатор | Системный аллокатор   | Назначение                                    |
|----------------------------|-----------------------|-----------------------------------------------|
| `box.slab.info()`          | `box.slab.system_info()`  | Сводная статистика по квоте и арене        |
| `box.slab.stats()`         | `box.slab.system_stats()` | Детализация по слабам разных размеров      |
| `box.slab.check()`         | `box.slab.system_check()` | Проверка целостности slab-аллокатора        |

Пример:

```lua
-- Объем системной квоты и текущее использование
box.slab.system_info().quota_size
box.slab.system_info().quota_used
box.slab.system_info().quota_used_ratio
```

### Метрики Prometheus {: #memtx_system_allocator_prometheus_metrics }

Те же показатели по обоим аллокаторам экспортируются на HTTP-эндпоинте
`/metrics` (см. [конфигурацию HTTP](../reference/config.md#instance_http_listen)).
Имена метрик системного аллокатора зеркальны метрикам пользовательского:

| Пользовательский аллокатор    | Системный аллокатор                  |
|-------------------------------|--------------------------------------|
| `tnt_slab_quota_size`         | `tnt_slab_system_quota_size`         |
| `tnt_slab_quota_used`         | `tnt_slab_system_quota_used`         |
| `tnt_slab_quota_used_ratio`   | `tnt_slab_system_quota_used_ratio`   |
| `tnt_slab_arena_size`         | `tnt_slab_system_arena_size`         |
| `tnt_slab_arena_used`         | `tnt_slab_system_arena_used`         |
| `tnt_slab_arena_used_ratio`   | `tnt_slab_system_arena_used_ratio`   |
| `tnt_slab_items_size`         | `tnt_slab_system_items_size`         |
| `tnt_slab_items_used`         | `tnt_slab_system_items_used`         |
| `tnt_slab_items_used_ratio`   | `tnt_slab_system_items_used_ratio`   |

Учитывая, что исчерпание квоты системного аллокатора приводит к
аварийной остановке инстанса (см. предупреждение выше), рекомендуется
настраивать оповещение по `tnt_slab_system_quota_used_ratio` с порогом, например, 75%.
