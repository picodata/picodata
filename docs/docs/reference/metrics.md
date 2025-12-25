---
search:
  exclude: true
---

# Справочник метрик

Метрики инстанса Picodata поступают в формате, совместимом с Prometheus.

Ниже приведен пример метрик в алфавитном порядке с описанием полей.

## Метрики Picodata {: #picodata_metrics }

### pico_cas_errors_total

Общее количество CAS-операций над глобальными таблицами, которые завершились с ошибкой

Тип: table

### pico_cas_ops_duration

Гистограмма длительности выполнения CAS-операций над глобальными
таблицами (в миллисекундах)

Тип: histogram

### pico_cas_records_total

Общее количество записей, записанных с помощью CAS-операций в глобальные таблицы

Тип: table

### pico_governor_changes_total

Число изменений статуса говернора (полезно при отладке)

Тип: counter

### pico_instance_state

Текущее состояние инстанса (Online/Offline/Expelled)

Тип: gauge

### pico_raft_applied_index

Текущий applied index в Raft

Тип: gauge

### pico_raft_commit_index

Текущий commit index в Raft

Тип: gauge

### pico_raft_leader_id

Текущий id лидера в Raft (0 если не было лидера на момент вызова)

Тип: gauge

### pico_raft_state

Текущая роль Raft (Follower/Candidate/Leader/PreCandidate)

Тип: gauge

### pico_raft_term

Текущий терм в Raft

Тип: gauge

### pico_rpc_request_duration

Гистограмма длительности выполнения запросов RPC (в миллисекундах)

Тип: histogram

### pico_rpc_request_errors_total

Общее количество запросов RPC, которые завершились ошибкой

Тип: counter

### pico_rpc_request_total

Общее количество выполненных запросов RPC

Тип: counter

### pico_sql_global_dml_query_total

Общее количество DML-запросов, выполненных над глобальными таблицами

Тип: counter

### pico_sql_global_dml_query_retries_total

Общее количество DML-запросов над глобальными таблицами, которые
завершились с ошибками и поэтому были запущены повторно

Тип: counter

### pico_sql_query_duration

Гистограмма длительности выполнения SQL-запросов (в миллисекундах)

Тип: histogram

### pico_sql_query_errors_total

Общее количество SQL-запросов, которые завершились ошибкой

Тип: counter

### pico_sql_query_total

Общее количество выполненных SQL-запросов

Тип: counter

### pico_sql_replicas_read_total

Общее количество исполнений DQL-запросов на репликах (см. [read_preference](./db_config.md#read_preference))

Тип: counter

### pico_sql_tx_splits_total

Общее количество разбиений запросов на части, произошедших при фиксации
изменений при выполнении DML-запросов

Тип: counter

### pico_sql_yield_sleep_duration

Гистограмма времени ожидания SQL-запроса во время переключения выполнения
в режиме неблокирующего SQL

Тип: histogram

### pico_sql_yields_total

Общее количество переключений выполнения в режиме неблокирующего SQL

Тип: counter

## Метрики Tarantool {: #tarantool_metrics }

### lj_gc_allocated_total

Общий объем выделенной памяти / Total amount of allocated memory

Тип: counter

`lj_gc_allocated_total 17531951`

### lj_gc_cdatanum

Количество выделенных объектов cdata / Amount of allocated cdata objects

Тип: gauge

`lj_gc_cdatanum 3591`

### lj_gc_freed_total

Общий объем освобожденной памяти / Total amount of freed memory

Тип: counter

`lj_gc_freed_total 14048950`

### lj_gc_memory

Размер выделенной в данный момент памяти / Memory currently allocated

Тип: gauge

`lj_gc_memory 3483001`

### lj_gc_steps_atomic_total

Общее количество инкрементных шагов GC (atomic state) / Total count of incremental GC steps (atomic state)

Тип: counter

`lj_gc_steps_atomic_total 18`

### lj_gc_steps_finalize_total

Общее количество инкрементных шагов GC (finalize state) / Total count of incremental GC steps (finalize state)

Тип: counter

`lj_gc_steps_finalize_total 220`

### lj_gc_steps_pause_total

Общее количество инкрементных GC-шагов (состояние паузы) / Total count of incremental GC steps (pause state)

Тип: counter

`lj_gc_steps_pause_total 16`

### lj_gc_steps_propagate_total

Общее количество инкрементных шагов GC (propagate state) / Total count of incremental GC steps (propagate state)

Тип: counter

`lj_gc_steps_propagate_total 65979`

### lj_gc_steps_sweep_total

Общее количество инкрементных шагов GC (sweep state) / Total count of incremental GC steps (sweep state)

Тип: counter

`lj_gc_steps_sweep_total 3529`

### lj_gc_steps_sweepstring_total

Общее количество инкрементных шагов GC / Total count of incremental GC steps (sweepstring state)

Тип: counter

`lj_gc_steps_sweepstring_total 153088`

### lj_gc_strnum

Количество выделенных строковых объектов / Amount of allocated string objects

Тип: gauge

`lj_gc_strnum 13589`

### lj_gc_tabnum

Количество выделенных табличных объектов / Amount of allocated table objects

Тип: gauge

`lj_gc_tabnum 6717`

### lj_gc_udatanum

Количество выделенных объектов udata / Amount of allocated udata objects

Тип: gauge

`lj_gc_udatanum 84`

### lj_jit_mcode_size

Общий размер всех выделенных областей для машинного кода / Total size of all allocated machine code areas

Тип: gauge

`lj_jit_mcode_size 65536`

### lj_jit_snap_restore_total

Общее количество быстрых восстановлений / Overall number of snap restores

Тип: counter

`lj_jit_snap_restore_total 459`

### lj_jit_trace_abort_total

Общее число событий отмены / Overall number of abort traces

Тип: counter

`lj_jit_trace_abort_total 69`

### lj_jit_trace_num

Количество трассировок JIT / Amount of JIT traces

Тип: gauge

`lj_jit_trace_num 94`

### lj_strhash_hit_total

Общее число придержанных строк / Total number of strings being interned

Тип: counter

`lj_strhash_hit_total 215655`

### lj_strhash_miss_total

Общее число событий выделения памяти для строк во время жизни платформы
 / Total number of strings allocations during the platform lifetime

Тип: counter

`lj_strhash_miss_total 29396`

### tnt_cfg_current_time

Системное время инстанса формате Unix timestamp

Тип: gauge

`tnt_cfg_current_time 1717160267`

### tnt_cpu_system_time

Использование ЦП, системное / CPU system time usage

Тип: gauge

`tnt_cpu_system_time 0.092386`

### tnt_cpu_user_time

Использование ЦП, пользовательское / CPU user time usage

Тип: gauge

`tnt_cpu_user_time 0.077745`

### tnt_election_leader

ID узла-лидера в текущем терме / Leader node ID in the current term

Тип: gauge

`tnt_election_leader 0`

### tnt_election_state

Состояние выборов на узле / Election state of the node

Тип: gauge

`tnt_election_state 0`

### tnt_election_term

Терм текущих выборов / Current election term

Тип: gauge

`tnt_election_term 1`

### tnt_election_vote

ID узла, за которого голосует текущий узел / ID of a node the current node votes for

Тип: gauge

`tnt_election_vote 0`

### tnt_ev_loop_epilog_time

Время завершения для цикла событий, мс / Event loop epilog time (ms)

Тип: gauge

`tnt_ev_loop_epilog_time 0.001`

### tnt_ev_loop_prolog_time

Время предварительной работы цикла событий, мс / Event loop prolog time (ms)

Тип: gauge

`tnt_ev_loop_prolog_time 0`

### tnt_ev_loop_time

Время цикла событий, мс / Event loop time (ms)

Тип: gauge

`tnt_ev_loop_time 0.008`

### tnt_fiber_amount

Количество файберов / Amount of fibers

Тип: gauge

`tnt_fiber_amount 29`

### tnt_fiber_csw

Общее количество переключений контекста для файбера / Overall number of fiber context switches

Тип: gauge

`tnt_fiber_csw 384`

### tnt_fiber_memalloc

Объем ОЗУ, зарезервированной для файберов / Amount of memory reserved for fibers

Тип: gauge

`tnt_fiber_memalloc 15102656`

### tnt_fiber_memused

Объем ОЗУ, использованной файберами / Amount of memory used by fibers

Тип: gauge

`tnt_fiber_memused 57`

### tnt_info_lsn

Счетчик LSN инстанса / LSN of the instance

Тип: gauge

`tnt_info_lsn 3235`

### tnt_info_memory_cache

Кэш памяти / Memory cache

Тип: gauge

`tnt_info_memory_cache 0`

### tnt_info_memory_data

Число байтов в кэше для хранения кортежей при использовании движка Vinyl

Тип: gauge

`tnt_info_memory_data 206784`

### tnt_info_memory_index

Число байтов, использованных для индексирования пользовательских данных.
Включает в себя объемы памяти memtx и vinyl, индекс страниц vinyl и
фильтры bloom vinyl.

Тип: gauge

`tnt_info_memory_index 2523136`

### tnt_info_memory_lua

Количество байтов, используемых для среды выполнения Lua. Память Lua
ограничена 2 ГБ на каждый инстанс. Мониторинг этого показателя может
предотвратить переполнение памяти.

Тип: gauge

`tnt_info_memory_lua 3808516`

### tnt_info_memory_net

Количество байтов, используемых для буферов ввода/вывода сети.

Тип: gauge

`tnt_info_memory_net 1654784`

### tnt_info_memory_tx

Количество байтов, используемых активными транзакциями.
Для движка vinyl это общий размер всех выделенных объектов
(struct ``txv``, struct ``vy_tx``, struct ``vy_read_interval``)
и кортежей, закрепленных для этих объектов.

Тип: gauge

`tnt_info_memory_tx 0`

### tnt_info_uptime

Время в секундах с момента запуска экземпляра

Тип: gauge

`tnt_info_uptime 8`

### tnt_info_vclock

Номер LSN в vclock.
Эта метрика всегда имеет метку «{id=«id»}»,
где «id» — номер инстанса в репликасете.

Тип: gauge

`tnt_info_vclock{id="1"} 3235`<br>
`tnt_info_vclock{id="0"} 432`

### tnt_memtx_mvcc_conflicts

Память, выделяемая в случае конфликтов транзакций.
Этот показатель всегда имеет метку «{kind=«...»}»,
которая может принимать следующие значения:

* «total» — общий объем байтов, выделенных для конфликтов.
* «average» — среднее значение для всех текущих транзакций (общий объем памяти в байтах / количество транзакций).
* «max» — максимальное количество байтов, выделенных для конфликтов на одну транзакцию.

Тип: gauge

`tnt_memtx_mvcc_conflicts{kind="average"} 0`<br>
`tnt_memtx_mvcc_conflicts{kind="total"} 0`<br>
`tnt_memtx_mvcc_conflicts{kind="max"} 0`

### tnt_memtx_mvcc_trackers

Трекеры, которые отслеживают чтение транзакций.
Эта метрика всегда имеет метку ``{kind=«...»}``,
которая может иметь следующие значения:

* «total» — общий объем выделенной памяти (в байтах) для трекеров всех текущих транзакций.
* «average» — среднее значение для всех текущих транзакций (общий объем памяти в байтах / количество транзакций).
* «max» — максимальное количество трекеров, выделенных на одну транзакцию (в байтах).

Тип: gauge

`tnt_memtx_mvcc_trackers{kind="average"} 0`<br>
`tnt_memtx_mvcc_trackers{kind="total"} 0`<br>
`tnt_memtx_mvcc_trackers{kind="max"} 0`

### tnt_memtx_mvcc_tuples_read_view_retained

Кортежи, которые не используются активными транзакциями чтения-записи,
но используются транзакциями только для чтения (т.е. в режиме чтения).
Эти кортежи больше не находятся в индексе, но MVCC не позволяет их удалять.
Эта метрика всегда имеет метку «{kind=«...»}»,
которая может иметь следующие значения:

* «count» — количество сохраненных кортежей «read_view» / количество историй.
* «total» — объем байтов, используемых сохраненными кортежами «read_view».

Тип: gauge

`tnt_memtx_mvcc_tuples_read_view_retained{kind="total"} 0`<br>
`tnt_memtx_mvcc_tuples_read_view_retained{kind="count"} 0`

### tnt_memtx_mvcc_tuples_read_view_stories

Кортежи, которые не используются активными транзакциями чтения-записи,
но используются транзакциями только для чтения (т. е. в режиме чтения).
Эта метрика всегда имеет метку «{kind=«...»}»,
которая может иметь следующие значения:

* «count» — количество кортежей «read_view» / количество историй.
* «total» — количество байтов, используемых кортежами «read_view» историй.

Тип: gauge

`tnt_memtx_mvcc_tuples_read_view_stories{kind="total"} 0`
`tnt_memtx_mvcc_tuples_read_view_stories{kind="count"} 0`

### tnt_memtx_mvcc_tuples_tracking_retained

Кортежи, которые не используются напрямую в транзакциях, но используются MVCC для отслеживания чтений.
Эти кортежи больше не находятся в индексе, но MVCC не позволяет их удалять.
Эта метрика всегда имеет метку ``{kind=«...»}``,
которая может иметь следующие значения:

* «count» — количество сохраненных кортежей «tracking» / количество историй.
* «total» — объем байтов, используемых сохраненными кортежами «tracking».

Тип: gauge

`tnt_memtx_mvcc_tuples_tracking_retained{kind="total"} 0`<br>
`tnt_memtx_mvcc_tuples_tracking_retained{kind="count"} 0`

### tnt_memtx_mvcc_tuples_tracking_stories

Кортежи, которые не используются напрямую в транзакциях, но используются MVCC для отслеживания чтений.
Эта метрика всегда имеет метку «{kind=«...»}»,
которая может иметь следующие значения:

* «count» — количество «отслеживаемых» кортежей / количество отслеживаемых историй.
* «total» — количество байтов, используемых историями «отслеживаемых» кортежей.

Тип: gauge

`tnt_memtx_mvcc_tuples_tracking_stories{kind="total"} 0`
`tnt_memtx_mvcc_tuples_tracking_stories{kind="count"} 0`

### tnt_memtx_mvcc_tuples_used_retained

Кортежи, которые используются активными транзакциями чтения-записи.
Но они больше не находятся в индексе, однако MVCC не позволяет их удалить.
Эта метрика всегда имеет метку «{kind=«...»}»,
которая может принимать следующие значения:

* «count» — количество сохраненных «использованных» кортежей / количество историй.
* «total» — количество байтов, используемых сохраненными «used» кортежами.

Тип: gauge

`tnt_memtx_mvcc_tuples_used_retained{kind="total"} 0`<br>
`tnt_memtx_mvcc_tuples_used_retained{kind="count"} 0`

### tnt_memtx_mvcc_tuples_used_stories

Кортежи, которые используются активными транзакциями чтения-записи.
Эта метрика всегда имеет метку «{kind=«...»}»,
которая может иметь следующие значения:

* «count» — количество «использованных» кортежей / количество историй.
* «total» — количество байтов, используемых «использованными» кортежами историй.

Тип: gauge

`tnt_memtx_mvcc_tuples_used_stories{kind="total"} 0`<br>
`tnt_memtx_mvcc_tuples_used_stories{kind="count"} 0`

### tnt_memtx_tnx_statements

Число операциий транзакции. Например, пользователь начал транзакцию и выполнил в ней действие `space:replace{0, 1}`.
Внутри эта операция превратится в ``statement`` для текущей транзакции.
Эта метрика всегда имеет метку ``{kind=«...»}``,
которая может принимать следующие значения:

* «total» — количество байтов, выделенных для операторов всех текущих транзакций.
* «average» — среднее количество байтов, используемых транзакциями для операторов (`txn.statements.total` байтов / количество открытых транзакций).
* «max» — максимальное количество байтов, используемых одной из текущих транзакций для операторов.

Тип: gauge

`tnt_memtx_tnx_statements{kind="average"} 0`<br>
`tnt_memtx_tnx_statements{kind="total"} 0`<br>
`tnt_memtx_tnx_statements{kind="max"} 0`

### tnt_memtx_tnx_system

Данные внутренних компонентов: журналов, точек сохранения.
Эта метрика всегда имеет метку «{kind=«...»}»,
которая может принимать следующие значения:

* «total» — память, выделенная внутренними компонентами для всех текущих транзакций.
* «average» — средний объем памяти, выделенный внутренними компонентами (общий объем памяти / количество всех текущих транзакций).
* «max» — максимальное количество байтов, выделенных внутренними компонентами на одну транзакцию.

Тип: gauge

`tnt_memtx_tnx_system{kind="average"} 0`<br>
`tnt_memtx_tnx_system{kind="total"} 0`<br>
`tnt_memtx_tnx_system{kind="max"} 0`

### tnt_memtx_tnx_user

Данные пользовательского уровня. В Tarantool C API есть функция
`box_txn_alloc()`, с помощью которой пользователь может выделить память
для текущей транзакции. Эта метрика всегда имеет метку ``{kind=«...»}``,
которая может принимать следующие значения:

* «total» — память, выделенная функцией `box_txn_alloc()` для всех текущих транзакций.
* «average» — среднее значение по транзакциям (общее количество выделенных байтов / количество всех текущих транзакций).
* «max» — максимальное количество байтов, выделенных функцией `box_txn_alloc()` для одной транзакции.

Тип: gauge

`tnt_memtx_tnx_user{kind="average"} 0`<br>
`tnt_memtx_tnx_user{kind="total"} 0`<br>
`tnt_memtx_tnx_user{kind="max"} 0`

### tnt_net_connections_current

Количество текущих активных соединений / Current connections amount

Тип: gauge

`tnt_net_connections_current 3`

### tnt_net_connections_total

Общее количество соединений / Connections total amount

Тип: counter

`tnt_net_connections_total 7`

### tnt_net_per_thread_connections_current

Текущее количество соединений / Current connections amount

Тип: gauge

`tnt_net_per_thread_connections_current{thread="1"} 3`

### tnt_net_per_thread_connections_total

Общее число соединений / Connections total amount

Тип: counter

`tnt_net_per_thread_connections_total{thread="1"} 7`

### tnt_net_per_thread_received_total

Всего получено в байтах / Totally received in bytes

Тип: counter

`tnt_net_per_thread_received_total{thread="1"} 1129`

### tnt_net_per_thread_requests_current

Ожидающие запросы / Pending requests

Тип: gauge

`tnt_net_per_thread_requests_current{thread="1"} 0`

### tnt_net_per_thread_requests_in_progress_current

Количество запросов на один поток, обрабатываемых в данный момент в потоке tx / Count of requests currently being processed in the tx thread

Тип: gauge

`tnt_net_per_thread_requests_in_progress_current{thread="1"} 0`

### tnt_net_per_thread_requests_in_progress_total

Общее число выполняющихся в данный момент запросов / Requests in progress total amount

Тип: counter

`tnt_net_per_thread_requests_in_progress_total{thread="1"} 37`

### tnt_net_per_thread_requests_in_stream_queue_current

Число запросов, ждущих в данный момент в очередях потоков / Count of requests currently waiting in queues of streams

Тип: gauge

`tnt_net_per_thread_requests_in_stream_queue_current{thread="1"} 0`

### tnt_net_per_thread_requests_in_stream_queue_total

Общее число запросов, размещенных в очередях потоков / Total count of requests, which was placed in queues of streams

Тип: counter

`tnt_net_per_thread_requests_in_stream_queue_total{thread="1"} 0`

### tnt_net_per_thread_requests_total

Общее число запросов на поток с момента запуска инстанса / Requests total amount

Тип: counter

`tnt_net_per_thread_requests_total{thread="1"} 44`

### tnt_net_per_thread_sent_total

Общее число отправленных байтов на поток с момента запуска инстанса / Totally sent in bytes

Тип: counter

`tnt_net_per_thread_sent_total{thread="1"} 64500`

### tnt_net_received_total

Всего получено в байтах с момента запуска инстанса

Тип: counter

`tnt_net_received_total 1129`

### tnt_net_requests_current

Количество ожидающих запросов / Pending requests

Тип: gauge

`tnt_net_requests_current 0`

### tnt_net_requests_in_progress_current

Количество запросов, обрабатываемых в данный момент в потоке tx / Count of requests currently being processed in the tx thread

Тип: gauge

`tnt_net_requests_in_progress_current 0`

### tnt_net_requests_in_progress_total

Общее число запросов на стадии выполнения / Requests in progress total amount

Тип: counter

`tnt_net_requests_in_progress_total 37`

### tnt_net_requests_in_stream_queue_current

Количество запросов, ожидающих в данный момент в очередях потоков / Count of requests currently waiting in queues of streams

Тип: gauge

`tnt_net_requests_in_stream_queue_current 0`

### tnt_net_requests_in_stream_queue_total

Общее количество запросов, размещенных в очередях потоков / Total count of requests, which was placed in queues of streams

Тип: counter

`tnt_net_requests_in_stream_queue_total 0`

### tnt_net_requests_total

Общее количество обработанных запросов с момента запуска инстанса
Тип: counter

`tnt_net_requests_total 44`

### tnt_net_sent_total

Общее число отправленных байтов с момента запуска инстанса

Тип: counter

`tnt_net_sent_total 64500`

### tnt_read_only

Признак "только для чтения" для инстанса / Is instance read only

Тип: gauge

`tnt_read_only 0`

### tnt_runtime_lua

Размер сборщика мусора Lua в байтах

Тип: gauge

`tnt_runtime_lua 3511325`

### tnt_runtime_tuple

Количество байтов, используемых для кортежей (кроме кортежей, принадлежащих memtx и vinyl)

Тип: gauge

`tnt_runtime_tuple 0`

### tnt_runtime_used

Количество байтов, используемых для среды выполнения Lua

Тип: gauge

`tnt_runtime_used 75497472`

### tnt_slab_arena_size

Общий объем памяти, доступный для хранения кортежей и индексов.
Включает выделенные, но в настоящее время свободные блоки.

Тип: gauge

`tnt_slab_arena_size 33554432`

### tnt_slab_arena_used

Эффективная память, используемая для хранения как кортежей, так и индексов.
Игнорирует выделенные, но в настоящее время свободные слабы.

Тип: gauge

`tnt_slab_arena_used 2729920`

### tnt_slab_arena_used_ratio

Показатель использования арены элементами slab / Slab arena_used_ratio info

Тип: gauge

`tnt_slab_arena_used_ratio 8.1`

### tnt_slab_items_size

Размер элементов slab / Slab items_size info

Тип: gauge

`tnt_slab_items_size 1505312`

### tnt_slab_items_used

Число используемых элементов slab / Slab items_used info

Тип: gauge

`tnt_slab_items_used 206784`

### tnt_slab_items_used_ratio

Показатель использования элементов slab / Slab items_used_ratio info

Тип: gauge

`tnt_slab_items_used_ratio 13.74`

### tnt_slab_quota_size

Объем памяти, доступный для хранения кортежей и индексов.
Равен `memtx_memory`.

Тип: gauge

`tnt_slab_quota_size 67108864`

### tnt_slab_quota_used

Объем памяти, который уже зарезервирован аллокатором для slab.

Тип: gauge

`tnt_slab_quota_used 33554432`

### tnt_slab_quota_used_ratio

Квота использования выделенной памяти / Slab quota_used_ratio info

Тип: gauge

`tnt_slab_quota_used_ratio 50`

### tnt_space_bsize

Общее количество байтов во всех кортежах.
Эта метрика всегда имеет 2 метки: ``{name=«test», engine="memtx"}``,
где:

* `name` — имя таблицы
* `engine` — движок таблицы

Тип: gauge

`tnt_space_bsize{name="ORDERS",engine="memtx"} 79`<br>
`tnt_space_bsize{name="ITEMS",engine="memtx"} 70`<br>
`tnt_space_bsize{name="WAREHOUSE",engine="memtx"} 86`

### tnt_space_index_bsize

Общее количество байтов, занимаемых индексом.
Эта метрика всегда имеет 2 метки: ``{name=«test», index_name="pk"}``,

Тип: gauge

`tnt_space_index_bsize{name="ORDERS",index_name="ORDERS_pkey"} 49152`<br>
`tnt_space_index_bsize{name="ITEMS",index_name="ITEMS_pkey"} 49152`<br>
`tnt_space_index_bsize{name="WAREHOUSE",index_name="WAREHOUSE_pkey"} 49152`<br>
`tnt_space_index_bsize{name="WAREHOUSE",index_name="WAREHOUSE_bucket_id"} 49152`<br>
`tnt_space_index_bsize{name="ITEMS",index_name="ITEMS_bucket_id"} 49152`<br>
`tnt_space_index_bsize{name="ORDERS",index_name="ORDERS_bucket_id"} 49152`

### tnt_space_len

Количество записей в таблице.
Эта метрика всегда имеет 2 метки: ``{name=«test», engine="memtx"}``,
где:

* `name` — имя таблицы
* `engine` — движок таблицы

Тип: gauge

`tnt_space_len{name="ORDERS",engine="memtx"} 5`<br>
`tnt_space_len{name="ITEMS",engine="memtx"} 5`<br>
`tnt_space_len{name="WAREHOUSE",engine="memtx"} 5`

### tnt_space_total_bsize

Общий размер кортежей и всех индексов в пространстве.
Эта метрика всегда имеет 2 метки: ``{name=«test», engine="memtx"}``,
где:

* `name` — имя таблицы
* `engine` — движок таблицы

Тип: gauge

`tnt_space_total_bsize{name="ORDERS",engine="memtx"} 98383`<br>
`tnt_space_total_bsize{name="ITEMS",engine="memtx"} 98374`<br>
`tnt_space_total_bsize{name="WAREHOUSE",engine="memtx"} 98390`

### tnt_stats_op_total

Общее количество вызовов с момента запуска сервера

Тип: counter

`tnt_stats_op_total{operation="update"} 0`<br>
`tnt_stats_op_total{operation="delete"} 0`<br>
`tnt_stats_op_total{operation="execute"} 0`<br>
`tnt_stats_op_total{operation="auth"} 4`<br>
`tnt_stats_op_total{operation="error"} 1`<br>
`tnt_stats_op_total{operation="upsert"} 0`<br>
`tnt_stats_op_total{operation="call"} 9`<br>
`tnt_stats_op_total{operation="commit"} 23`<br>
`tnt_stats_op_total{operation="eval"} 0`<br>
`tnt_stats_op_total{operation="replace"} 30`<br>
`tnt_stats_op_total{operation="rollback"} 3`<br>
`tnt_stats_op_total{operation="select"} 612`<br>
`tnt_stats_op_total{operation="begin"} 26`<br>
`tnt_stats_op_total{operation="prepare"} 0`<br>
`tnt_stats_op_total{operation="insert"} 4`

### tnt_synchro_queue_busy

Признак занятости очереди синхронизации / Is synchro queue busy

Тип: gauge

`tnt_synchro_queue_busy 0`

### tnt_synchro_queue_len

Число транзакций, которые собирают в данный момент подтверждения / Amount of transactions are collecting confirmations now

Тип: gauge

`tnt_synchro_queue_len 0`

### tnt_synchro_queue_owner

Идентификатор инстанса текущего мастера синхронной репликации.

Тип: gauge

`tnt_synchro_queue_owner 0`

### tnt_synchro_queue_term

Терм очереди синхронизации / Synchro queue term

Тип: gauge

`tnt_synchro_queue_term 0`

### tnt_vinyl_disk_data_size

Объем данных, сохраняемых в файлах / Amount of data stored in files

Тип: gauge

`tnt_vinyl_disk_data_size 0`

### tnt_vinyl_disk_index_size

Размер индекса, сохраняемого в файлах / Amount of index stored in files

Тип: gauge

`tnt_vinyl_disk_index_size 0`

### tnt_vinyl_memory_bloom_filter

Размер фильтра bloom / Size of bloom filter

Тип: gauge

`tnt_vinyl_memory_bloom_filter 0`

### tnt_vinyl_memory_level0 {: #tnt_vinyl_memory_level0 }

Терм очереди синхронизации / Size of in-memory storage of an LSM tree

Тип: gauge

`tnt_vinyl_memory_level0 0`

### tnt_vinyl_memory_page_index

Размер индексов страниц / Size of page indexes

Тип: gauge

`tnt_vinyl_memory_page_index 0`

### tnt_vinyl_memory_tuple_cache

Количество байт, выделяемое для кортежа / Number of bytes that are being used for tuple

Тип: gauge

`tnt_vinyl_memory_tuple_cache 0`

### tnt_vinyl_regulator_blocked_writers

Количество файберов, заблокированных из-за ожидания выделения квоты движком Vinyl / The number of fibers that are blocked waiting for Vinyl level0 memory quota

Тип: gauge

`tnt_vinyl_regulator_blocked_writers 0`

### tnt_vinyl_regulator_dump_bandwidth

Среднее расчетное время снятия дампа / Estimated average rate at which dumps are done

Тип: gauge

`tnt_vinyl_regulator_dump_bandwidth 10485760`

### tnt_vinyl_regulator_dump_watermark

Точка начала снятия дампа / Point when dumping must occur

Тип: gauge

`tnt_vinyl_regulator_dump_watermark 134217715`

### tnt_vinyl_regulator_rate_limit

Предел скорости записи / Write rate limit

Тип: gauge

`tnt_vinyl_regulator_rate_limit 10485760`

### tnt_vinyl_regulator_write_rate

Средний показатель скорости недавних записей на диск / Average rate at which recent writes to disk are done

Тип: gauge

`tnt_vinyl_regulator_write_rate 0`

### tnt_vinyl_scheduler_dump_time

Общее время, потраченное воркерами на сохранение дампа / Total time spent by all worker threads performing dump

Тип: gauge

`tnt_vinyl_scheduler_dump_time 0`

### tnt_vinyl_scheduler_dump_total

Число завершенных дампов / The count of completed dumps

Тип: counter

`tnt_vinyl_scheduler_dump_total 0`

### tnt_vinyl_scheduler_tasks

Количество задач движка Vinyl / Vinyl tasks count

Тип: gauge

`tnt_vinyl_scheduler_tasks{status="failed"} 0`<br>
`tnt_vinyl_scheduler_tasks{status="inprogress"} 0`<br>
`tnt_vinyl_scheduler_tasks{status="completed"} 0`

### tnt_vinyl_tx_commit

Число коммитов / Count of commits

Тип: gauge

`tnt_vinyl_tx_commit 0`

### tnt_vinyl_tx_conflict

Количество конфликтов транзакций / Count of transaction conflicts

Тип: gauge

`tnt_vinyl_tx_conflict 0`

### tnt_vinyl_tx_read_views

Число открытых заданий чтения / Count of open read views

Тип: gauge

`tnt_vinyl_tx_read_views 0`

### tnt_vinyl_tx_rollback

Количество откатов / Count of rollbacks

Тип: gauge

`tnt_vinyl_tx_rollback 0`
