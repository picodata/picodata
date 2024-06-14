# Справочник метрик

Метрики инстанса Picodata поступают в формате, совместимом с Prometheus.

Ниже приведен пример метрик с описанием полей.

```lua
-- Размер индекса, сохраняемого в файлах
# HELP tnt_vinyl_disk_index_size Amount of index stored in files
# TYPE tnt_vinyl_disk_index_size gauge
tnt_vinyl_disk_index_size 0

-- Признак "только для чтения" для инстанса
# HELP tnt_read_only Is instance read only
# TYPE tnt_read_only gauge
tnt_read_only 0

-- Объем данных, сохраняемых в файлах
# HELP tnt_vinyl_disk_data_size Amount of data stored in files
# TYPE tnt_vinyl_disk_data_size gauge
tnt_vinyl_disk_data_size 0

-- Общий объем освобожденной памяти
# HELP lj_gc_freed_total Total amount of freed memory
# TYPE lj_gc_freed_total counter
lj_gc_freed_total 17618094

-- Тапл среды исполнения
# HELP tnt_runtime_tuple Runtime tuple
# TYPE tnt_runtime_tuple gauge
tnt_runtime_tuple 0

-- Использование ЦП, пользовательское
# HELP tnt_cpu_user_time CPU user time usage
# TYPE tnt_cpu_user_time gauge
tnt_cpu_user_time 1.44747

-- Размер индексов страниц
# HELP tnt_vinyl_memory_page_index Size of page indexes
# TYPE tnt_vinyl_memory_page_index gauge
tnt_vinyl_memory_page_index 0

-- Общее количество инкрементных GC-шагов (состояние паузы)
# HELP lj_gc_steps_pause_total Total count of incremental GC steps (pause state)
# TYPE lj_gc_steps_pause_total counter
lj_gc_steps_pause_total 17

-- Квота использования выделенной памяти
# HELP tnt_slab_quota_used_ratio Slab quota_used_ratio info
# TYPE tnt_slab_quota_used_ratio gauge
tnt_slab_quota_used_ratio 50

-- Количество сохраненных следящих строк
# HELP tnt_memtx_mvcc_tuples_tracking_retained Number of retained `tracking` tuples / number of stories
# TYPE tnt_memtx_mvcc_tuples_tracking_retained gauge
tnt_memtx_mvcc_tuples_tracking_retained{kind="total"} 0
tnt_memtx_mvcc_tuples_tracking_retained{kind="count"} 0

-- Размер выделенной памяти
# HELP tnt_slab_arena_size Slab arena_size info
# TYPE tnt_slab_arena_size gauge
tnt_slab_arena_size 33554432

-- Использование ЦП, системное
# HELP tnt_cpu_system_time CPU system time usage
# TYPE tnt_cpu_system_time gauge
tnt_cpu_system_time 2.029658

-- Количество байт, выделяемое для тапла
# HELP tnt_vinyl_memory_tuple_cache Number of bytes that are being used for tuple
# TYPE tnt_vinyl_memory_tuple_cache gauge
tnt_vinyl_memory_tuple_cache 0

-- Общее время, потраченное воркерами на сохранение дампа
# HELP tnt_vinyl_scheduler_dump_time Total time spent by all worker threads performing dump
# TYPE tnt_vinyl_scheduler_dump_time gauge
tnt_vinyl_scheduler_dump_time 0

-- Общее количество запросов
# HELP tnt_net_requests_total Requests total amount
# TYPE tnt_net_requests_total counter
tnt_net_requests_total 655

-- Общее количество соединений
# HELP tnt_net_connections_total Connections total amount
# TYPE tnt_net_connections_total counter
tnt_net_connections_total 7

-- Количество задач движка Vinyl
# HELP tnt_vinyl_scheduler_tasks Vinyl tasks count
# TYPE tnt_vinyl_scheduler_tasks gauge
tnt_vinyl_scheduler_tasks{status="failed"} 0
tnt_vinyl_scheduler_tasks{status="inprogress"} 0
tnt_vinyl_scheduler_tasks{status="completed"} 0

-- Общее количество запросов
# HELP tnt_info_memory_tx Memory tx
# TYPE tnt_info_memory_tx gauge
tnt_info_memory_tx 0

-- Количество задач движка Memtx
# HELP tnt_runtime_used Runtime used
# TYPE tnt_runtime_used gauge
tnt_runtime_used 75497472

-- Максимальное число байтов, выделенное внутренним механизмом на транзакцию
# HELP tnt_memtx_tnx_system The maximum number of bytes allocated by internals per transaction
# TYPE tnt_memtx_tnx_system gauge
tnt_memtx_tnx_system{kind="average"} 0
tnt_memtx_tnx_system{kind="total"} 0
tnt_memtx_tnx_system{kind="max"} 0

-- Количество "занятых" таплов / количество историй
# HELP tnt_memtx_mvcc_tuples_used_stories Number of `used` tuples / number of stories
# TYPE tnt_memtx_mvcc_tuples_used_stories gauge
tnt_memtx_mvcc_tuples_used_stories{kind="total"} 0
tnt_memtx_mvcc_tuples_used_stories{kind="count"} 0

-- Общее число отправленных байтов
# HELP tnt_net_sent_total Totally sent in bytes
# TYPE tnt_net_sent_total counter
tnt_net_sent_total 206841

-- Количество файберов
# HELP tnt_fiber_amount Amount of fibers
# TYPE tnt_fiber_amount gauge
tnt_fiber_amount 29

-- Количество ожидающих запросов
# HELP tnt_net_requests_current Pending requests
# TYPE tnt_net_requests_current gauge
tnt_net_requests_current 0

-- Состояние выборов на узле
# HELP tnt_election_state Election state of the node
# TYPE tnt_election_state gauge
tnt_election_state 0

-- ID узла, за которого проголосовал текущий узел
# HELP tnt_election_vote ID of a node the current node votes for
# TYPE tnt_election_vote gauge
tnt_election_vote 0

-- Общее число запросов, размещенных в очередях потоков
# HELP tnt_net_per_thread_requests_in_stream_queue_total Total count of requests, which was placed in queues of streams
# TYPE tnt_net_per_thread_requests_in_stream_queue_total counter
tnt_net_per_thread_requests_in_stream_queue_total{thread="1"} 0

-- Терм очереди синхронизации
# HELP tnt_synchro_queue_term Synchro queue term
# TYPE tnt_synchro_queue_term gauge
tnt_synchro_queue_term 0

-- Размер, который занимает LSM-дерево в ОЗУ
# HELP tnt_vinyl_memory_level0 Size of in-memory storage of an LSM tree
# TYPE tnt_vinyl_memory_level0 gauge
tnt_vinyl_memory_level0 0

-- Общее число отправленных байтов на поток
# HELP tnt_net_per_thread_sent_total Totally sent in bytes
# TYPE tnt_net_per_thread_sent_total counter
tnt_net_per_thread_sent_total{thread="1"} 206841

-- Количество откатов
# HELP tnt_vinyl_tx_rollback Count of rollbacks
# TYPE tnt_vinyl_tx_rollback gauge
tnt_vinyl_tx_rollback 0

-- Количество файберов, уже используемых в ОЗУ
# HELP tnt_fiber_memused Fibers memused
# TYPE tnt_fiber_memused gauge
tnt_fiber_memused 57

-- Число транзакций, которые собирают в данный момент подтверждения
# HELP tnt_synchro_queue_len Amount of transactions are collecting confirmations now
# TYPE tnt_synchro_queue_len gauge
tnt_synchro_queue_len 0

-- Количество файберов, для которых выделено место в ОЗУ
# HELP tnt_fiber_memalloc Fibers memalloc
# TYPE tnt_fiber_memalloc gauge
tnt_fiber_memalloc 15102656

-- Количество выделенных строковых объектов
# HELP lj_gc_strnum Amount of allocated string objects
# TYPE lj_gc_strnum gauge
lj_gc_strnum 14449

-- Общий объём спейсов, в байтах
# HELP tnt_space_total_bsize Space total bsize
# TYPE tnt_space_total_bsize gauge
tnt_space_total_bsize{name="ORDERS",engine="memtx"} 98383
tnt_space_total_bsize{name="ITEMS",engine="memtx"} 98374
tnt_space_total_bsize{name="WAREHOUSE",engine="memtx"} 98390

-- Размер элементов slab
# HELP tnt_slab_items_size Slab items_size info
# TYPE tnt_slab_items_size gauge
tnt_slab_items_size 1505312

-- Количество файберов, заблокированных из-за ожидания выделения квоты движком Vinyl
# HELP tnt_vinyl_regulator_blocked_writers The number of fibers that are blocked waiting for Vinyl level0 memory quota
# TYPE tnt_vinyl_regulator_blocked_writers gauge
tnt_vinyl_regulator_blocked_writers 0

-- Размер индекса в байтах
# HELP tnt_space_index_bsize Index bsize
# TYPE tnt_space_index_bsize gauge
tnt_space_index_bsize{name="ORDERS",index_name="ORDERS_pkey"} 49152
tnt_space_index_bsize{name="ITEMS",index_name="ITEMS_pkey"} 49152
tnt_space_index_bsize{name="WAREHOUSE",index_name="WAREHOUSE_pkey"} 49152
tnt_space_index_bsize{name="WAREHOUSE",index_name="WAREHOUSE_bucket_id"} 49152
tnt_space_index_bsize{name="ITEMS",index_name="ITEMS_bucket_id"} 49152
tnt_space_index_bsize{name="ORDERS",index_name="ORDERS_bucket_id"} 49152

-- Количество запросов, обрабатываемых в данный момент в потоке tx
# HELP tnt_net_requests_in_progress_current Count of requests currently being processed in the tx thread
# TYPE tnt_net_requests_in_progress_current gauge
tnt_net_requests_in_progress_current 0

-- Размер занятой памяти
# HELP tnt_slab_arena_used Slab arena_used info
# TYPE tnt_slab_arena_used gauge
tnt_slab_arena_used 2731688

-- Среднее расчетное время снятия дампа
# HELP tnt_vinyl_regulator_dump_bandwidth Estimated average rate at which dumps are done
# TYPE tnt_vinyl_regulator_dump_bandwidth gauge
tnt_vinyl_regulator_dump_bandwidth 10485760

-- Размер занятой квоты памяти
# HELP tnt_slab_quota_used Slab quota_used info
# TYPE tnt_slab_quota_used gauge
tnt_slab_quota_used 33554432

-- Количество запросов на один поток, обрабатываемых в данный момент в потоке tx
# HELP tnt_net_per_thread_requests_in_progress_current Count of requests currently being processed in the tx thread
# TYPE tnt_net_per_thread_requests_in_progress_current gauge
tnt_net_per_thread_requests_in_progress_current{thread="1"} 0

-- Кэш памяти
# HELP tnt_info_memory_cache Memory cache
# TYPE tnt_info_memory_cache gauge
tnt_info_memory_cache 0

-- Общее количество быстрых восстановлений
# HELP lj_jit_snap_restore_total Overall number of snap restores
# TYPE lj_jit_snap_restore_total counter
lj_jit_snap_restore_total 791

-- Время предварительной работы цикла событий, мс
# HELP tnt_ev_loop_prolog_time Event loop prolog time (ms)
# TYPE tnt_ev_loop_prolog_time gauge
tnt_ev_loop_prolog_time 0

-- Показатель рантайма Lua
# HELP tnt_runtime_lua Runtime lua
# TYPE tnt_runtime_lua gauge
tnt_runtime_lua 6591028

-- Число используемых элементов slab
# HELP tnt_slab_items_used Slab items_used info
# TYPE tnt_slab_items_used gauge
tnt_slab_items_used 208552

-- Общее количество инкрементных шагов GC
# HELP lj_gc_steps_sweepstring_total Total count of incremental GC steps (sweepstring state)
# TYPE lj_gc_steps_sweepstring_total counter
lj_gc_steps_sweepstring_total 169472

-- Показатель использования арены элементами slab
# HELP tnt_slab_arena_used_ratio Slab arena_used_ratio info
# TYPE tnt_slab_arena_used_ratio gauge
tnt_slab_arena_used_ratio 8.1

-- Количество запросов, ожидающих в данный момент в очередях потоков
# HELP tnt_net_requests_in_stream_queue_current count of requests currently waiting in queues of streams
# TYPE tnt_net_requests_in_stream_queue_current gauge
tnt_net_requests_in_stream_queue_current 0

-- Размер квоты памяти
# HELP tnt_slab_quota_size Slab quota_size info
# TYPE tnt_slab_quota_size gauge
tnt_slab_quota_size 67108864

-- Текущее время конфигурации Tarantool
# HELP tnt_cfg_current_time Tarantool cfg time
# TYPE tnt_cfg_current_time gauge
tnt_cfg_current_time 1717163122

-- Количество конфликтов транзакций
# HELP tnt_vinyl_tx_conflict Count of transaction conflicts
# TYPE tnt_vinyl_tx_conflict gauge
tnt_vinyl_tx_conflict 0

-- Количество текущих соединений
# HELP tnt_net_connections_current Current connections amount
# TYPE tnt_net_connections_current gauge
tnt_net_connections_current 3

-- Максимальное число трекеров, выделенных на транзакцию
# HELP tnt_memtx_mvcc_trackers Maximum trackers allocated per transaction
# TYPE tnt_memtx_mvcc_trackers gauge
tnt_memtx_mvcc_trackers{kind="average"} 0
tnt_memtx_mvcc_trackers{kind="total"} 0
tnt_memtx_mvcc_trackers{kind="max"} 0

-- Число завершенных дампов
# HELP tnt_vinyl_scheduler_dump_total The count of completed dumps
# TYPE tnt_vinyl_scheduler_dump_total counter
tnt_vinyl_scheduler_dump_total 0

-- Общее количество инкрементных шагов GC (sweep state)
# HELP lj_gc_steps_sweep_total Total count of incremental GC steps (sweep state)
# TYPE lj_gc_steps_sweep_total counter
lj_gc_steps_sweep_total 5109

-- Показатель использования элементов slab
# HELP tnt_slab_items_used_ratio Slab items_used_ratio info
# TYPE tnt_slab_items_used_ratio gauge
tnt_slab_items_used_ratio 13.85

-- Общее число выполняющихся в данный момент запросов
# HELP tnt_net_per_thread_requests_in_progress_total Requests in progress total amount
# TYPE tnt_net_per_thread_requests_in_progress_total counter
tnt_net_per_thread_requests_in_progress_total{thread="1"} 648

-- Владелец очереди синхронизации
# HELP tnt_synchro_queue_owner Synchro queue owner
# TYPE tnt_synchro_queue_owner gauge
tnt_synchro_queue_owner 0

-- Размер спейса в байтах
# HELP tnt_space_bsize Space bsize
# TYPE tnt_space_bsize gauge
tnt_space_bsize{name="ORDERS",engine="memtx"} 79
tnt_space_bsize{name="ITEMS",engine="memtx"} 70
tnt_space_bsize{name="WAREHOUSE",engine="memtx"} 86

-- Размер "чистой" памяти
# HELP tnt_info_memory_net Memory net
# TYPE tnt_info_memory_net gauge
tnt_info_memory_net 1654784

-- Количество "следящих" таплов / историй
# HELP tnt_memtx_mvcc_tuples_tracking_stories Number of `tracking` tuples / number of tracking stories.
# TYPE tnt_memtx_mvcc_tuples_tracking_stories gauge
tnt_memtx_mvcc_tuples_tracking_stories{kind="total"} 0
tnt_memtx_mvcc_tuples_tracking_stories{kind="count"} 0

-- Размер памяти Lua
# HELP tnt_info_memory_lua Memory lua
# TYPE tnt_info_memory_lua gauge
tnt_info_memory_lua 6683540

-- Общее число событий выделения памяти для строк во время жизни платформы
# HELP lj_strhash_miss_total Total number of strings allocations during the platform lifetime
# TYPE lj_strhash_miss_total counter
lj_strhash_miss_total 31588

-- Индекс памяти
# HELP tnt_info_memory_index Memory index
# TYPE tnt_info_memory_index gauge
tnt_info_memory_index 2523136

-- Число срабатываний VClock
# HELP tnt_info_vclock VClock
# TYPE tnt_info_vclock gauge
tnt_info_vclock{id="1"} 3235
tnt_info_vclock{id="0"} 475

-- Средний показатель скорости недавних записей на диск
# HELP tnt_vinyl_regulator_write_rate Average rate at which recent writes to disk are done
# TYPE tnt_vinyl_regulator_write_rate gauge
tnt_vinyl_regulator_write_rate 0

-- Общее количество инкрементных шагов GC (propagate state)
# HELP lj_gc_steps_propagate_total Total count of incremental GC steps (propagate state)
# TYPE lj_gc_steps_propagate_total counter
lj_gc_steps_propagate_total 79601

-- Предел скорости записи
# HELP tnt_vinyl_regulator_rate_limit Write rate limit
# TYPE tnt_vinyl_regulator_rate_limit gauge
tnt_vinyl_regulator_rate_limit 10485760

-- Общее число соединений
# HELP tnt_net_per_thread_connections_total Connections total amount
# TYPE tnt_net_per_thread_connections_total counter
tnt_net_per_thread_connections_total{thread="1"} 7

-- Максимальное число байтов, использованное одной транзакцией на выполнение выражения
# HELP tnt_memtx_tnx_statements Maximum number of bytes used by one transaction for statements
# TYPE tnt_memtx_tnx_statements gauge
tnt_memtx_tnx_statements{kind="average"} 0
tnt_memtx_tnx_statements{kind="total"} 0

-- Общее число событий отмены
tnt_memtx_tnx_statements{kind="max"} 0
# HELP lj_jit_trace_abort_total Overall number of abort traces
# TYPE lj_jit_trace_abort_total counter
lj_jit_trace_abort_total 133

-- Общий размер всех выделенных областей для машинного кода
# HELP lj_jit_mcode_size Total size of all allocated machine code areas
# TYPE lj_jit_mcode_size gauge
lj_jit_mcode_size 131072

-- Текущее количество соединений
# HELP tnt_net_per_thread_connections_current Current connections amount
# TYPE tnt_net_per_thread_connections_current gauge
tnt_net_per_thread_connections_current{thread="1"} 3

-- Максимальное число байтов, выделенное функцией `box_txn_alloc()` на одну транзакцию
# HELP tnt_memtx_tnx_user The maximum number of bytes allocated by `box_txn_alloc()` function per transaction
# TYPE tnt_memtx_tnx_user gauge
tnt_memtx_tnx_user{kind="average"} 0
tnt_memtx_tnx_user{kind="total"} 0
tnt_memtx_tnx_user{kind="max"} 0

-- Общее количество инкрементных шагов GC (finalize state)
# HELP lj_gc_steps_finalize_total Total count of incremental GC steps (finalize state)
# TYPE lj_gc_steps_finalize_total counter
lj_gc_steps_finalize_total 13995

-- Размер фильтра bloom
# HELP tnt_vinyl_memory_bloom_filter Size of bloom filter
# TYPE tnt_vinyl_memory_bloom_filter gauge
tnt_vinyl_memory_bloom_filter 0

-- Количество выделенных объектов udata
# HELP lj_gc_udatanum Amount of allocated udata objects
# TYPE lj_gc_udatanum gauge
lj_gc_udatanum 167

-- Размер выделенной в данный момент памяти
# HELP lj_gc_memory Memory currently allocated
# TYPE lj_gc_memory gauge
lj_gc_memory 6679524

-- Количество выделенных объектов cdata
# HELP lj_gc_cdatanum Amount of allocated cdata objects
# TYPE lj_gc_cdatanum gauge
lj_gc_cdatanum 46187

-- Количество выделенных табличных объектов
# HELP lj_gc_tabnum Amount of allocated table objects
# TYPE lj_gc_tabnum gauge
lj_gc_tabnum 8572

-- Общее количество запросов, размещенных в очередях потоков
# HELP tnt_net_requests_in_stream_queue_total Total count of requests, which was placed in queues of streams
# TYPE tnt_net_requests_in_stream_queue_total counter
tnt_net_requests_in_stream_queue_total 0

-- Данные csw файберов
# HELP tnt_fiber_csw Fibers csw
# TYPE tnt_fiber_csw gauge
tnt_fiber_csw 10155

-- Общее число запросов на поток
# HELP tnt_net_per_thread_requests_total Requests total amount
# TYPE tnt_net_per_thread_requests_total counter
tnt_net_per_thread_requests_total{thread="1"} 655

-- Общее количество инкрементных шагов GC (atomic state)
# HELP lj_gc_steps_atomic_total Total count of incremental GC steps (atomic state)
# TYPE lj_gc_steps_atomic_total counter
lj_gc_steps_atomic_total 20

-- Число коммитов
# HELP tnt_vinyl_tx_commit Count of commits
# TYPE tnt_vinyl_tx_commit gauge
tnt_vinyl_tx_commit 0

-- Время жизни Tarantool
# HELP tnt_info_uptime Tarantool uptime
# TYPE tnt_info_uptime gauge
tnt_info_uptime 564

-- Общее число придержанных строк
# HELP lj_strhash_hit_total Total number of strings being interned
# TYPE lj_strhash_hit_total counter
lj_strhash_hit_total 289615

-- Терм текущих выборов
# HELP tnt_election_term Current election term
# TYPE tnt_election_term gauge
tnt_election_term 1

-- ID узла-лидера в текущем терме
# HELP tnt_election_leader Leader node ID in the current term
# TYPE tnt_election_leader gauge
tnt_election_leader 0

-- Количество оставленных таплов `read_view` / количество историй
# HELP tnt_memtx_mvcc_tuples_read_view_retained Number of retained `read_view` tuples / number of stories
# TYPE tnt_memtx_mvcc_tuples_read_view_retained gauge
tnt_memtx_mvcc_tuples_read_view_retained{kind="total"} 0
tnt_memtx_mvcc_tuples_read_view_retained{kind="count"} 0

-- Признак занятости очереди синхронизации
# HELP tnt_synchro_queue_busy Is synchro queue busy
# TYPE tnt_synchro_queue_busy gauge
tnt_synchro_queue_busy 0

-- Общее число операций
# HELP tnt_stats_op_total Total amount of operations
# TYPE tnt_stats_op_total counter
tnt_stats_op_total{operation="update"} 0
tnt_stats_op_total{operation="delete"} 0
tnt_stats_op_total{operation="execute"} 0
tnt_stats_op_total{operation="auth"} 4
tnt_stats_op_total{operation="error"} 1
tnt_stats_op_total{operation="upsert"} 0
tnt_stats_op_total{operation="call"} 65
tnt_stats_op_total{operation="commit"} 23
tnt_stats_op_total{operation="eval"} 0
tnt_stats_op_total{operation="replace"} 30
tnt_stats_op_total{operation="rollback"} 3
tnt_stats_op_total{operation="select"} 7295
tnt_stats_op_total{operation="begin"} 26
tnt_stats_op_total{operation="prepare"} 0
tnt_stats_op_total{operation="insert"} 4

-- Точка начала снятия дампа
# HELP tnt_vinyl_regulator_dump_watermark Point when dumping must occur
# TYPE tnt_vinyl_regulator_dump_watermark gauge
tnt_vinyl_regulator_dump_watermark 134217715

-- Число запросов, ждущих в данный момент в очередях потоков
# HELP tnt_net_per_thread_requests_in_stream_queue_current Count of requests currently waiting in queues of streams
# TYPE tnt_net_per_thread_requests_in_stream_queue_current gauge
tnt_net_per_thread_requests_in_stream_queue_current{thread="1"} 0

-- Максимальное число байтов, выделенное для конфликтов в расчете на одну транзакцию
# HELP tnt_memtx_mvcc_conflicts Maximum bytes allocated for conflicts per transaction
# TYPE tnt_memtx_mvcc_conflicts gauge
tnt_memtx_mvcc_conflicts{kind="average"} 0
tnt_memtx_mvcc_conflicts{kind="total"} 0
tnt_memtx_mvcc_conflicts{kind="max"} 0

-- Время завершения для цикла событий, мс
# HELP tnt_ev_loop_epilog_time Event loop epilog time (ms)
# TYPE tnt_ev_loop_epilog_time gauge
tnt_ev_loop_epilog_time 0.001

-- Время цикла событий, мс
# HELP tnt_ev_loop_time Event loop time (ms)
# TYPE tnt_ev_loop_time gauge
tnt_ev_loop_time 0.005

-- Длина спейса
# HELP tnt_space_len Space length
# TYPE tnt_space_len gauge
tnt_space_len{name="ORDERS",engine="memtx"} 5
tnt_space_len{name="ITEMS",engine="memtx"} 5
tnt_space_len{name="WAREHOUSE",engine="memtx"} 5

-- Количество трассировок JIT
# HELP lj_jit_trace_num Amount of JIT traces
# TYPE lj_jit_trace_num gauge
lj_jit_trace_num 161

-- Ожидающие запросы
# HELP tnt_net_per_thread_requests_current Pending requests
# TYPE tnt_net_per_thread_requests_current gauge
tnt_net_per_thread_requests_current{thread="1"} 0

-- Всего получено в байтах
# HELP tnt_net_per_thread_received_total Totally received in bytes
# TYPE tnt_net_per_thread_received_total counter
tnt_net_per_thread_received_total{thread="1"} 10715

-- Общий объем выделенной памяти
# HELP lj_gc_allocated_total Total amount of allocated memory
# TYPE lj_gc_allocated_total counter
lj_gc_allocated_total 24297618

-- Число открытых заданий чтения
# HELP tnt_vinyl_tx_read_views Count of open read views
# TYPE tnt_vinyl_tx_read_views gauge
tnt_vinyl_tx_read_views 0

-- Количество придержанных таплов `used` / количество историй
# HELP tnt_memtx_mvcc_tuples_used_retained Number of retained `used` tuples / number of stories
# TYPE tnt_memtx_mvcc_tuples_used_retained gauge
tnt_memtx_mvcc_tuples_used_retained{kind="total"} 0
tnt_memtx_mvcc_tuples_used_retained{kind="count"} 0

-- Количество таплов `read_view` / количество историй
# HELP tnt_memtx_mvcc_tuples_read_view_stories Number of `read_view` tuples / number of stories
# TYPE tnt_memtx_mvcc_tuples_read_view_stories gauge
tnt_memtx_mvcc_tuples_read_view_stories{kind="total"} 0
tnt_memtx_mvcc_tuples_read_view_stories{kind="count"} 0

-- Общее число запросов на стадии выполнения
# HELP tnt_net_requests_in_progress_total Requests in progress total amount
# TYPE tnt_net_requests_in_progress_total counter
tnt_net_requests_in_progress_total 648

-- Всего получено в байтах
# HELP tnt_net_received_total Totally received in bytes
# TYPE tnt_net_received_total counter
tnt_net_received_total 10715

-- Данные памяти
# HELP tnt_info_memory_data Memory data
# TYPE tnt_info_memory_data gauge
tnt_info_memory_data 208552

-- Счетчик LSN
# HELP tnt_info_lsn Tarantool lsn
# TYPE tnt_info_lsn gauge
tnt_info_lsn 3235
```
