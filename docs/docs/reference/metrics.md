---
search:
  exclude: true
---

# Справочник метрик

Метрики инстанса Picodata поступают в формате, совместимом с Prometheus.

Ниже приведен пример метрик с описанием полей.

## Метрики Picodata {: #picodata_metrics }

### pico_governor_changes_total

Текущий говернор. При отладке позволяет понять, сколько раз происходила смена говернора

### pico_sql_query_total

Общее количество выполненных SQL-запросов

### pico_sql_query_errors_total

Общее количество SQL-запросов, которые привели к ошибкам

### pico_sql_query_duration

Длительность выполнения SQL-запросов (в миллисекундах)

### pico_rpc_request_total

Общее количество выполненных запросов RPC

### pico_rpc_request_errors_total

Общее количество запросов RPC, которые привели к ошибкам

### pico_rpc_request_duration

Длительность выполнения запросов RPC (в миллисекундах)

### pico_cas_records_total

Общее количество записей, записанных с помощью CAS-операций в глобальные таблицы

### pico_cas_errors_total

Общее количество CAS операций над глобальными таблицами, которые привели к ошибке

### pico_cas_ops_duration

Длительность выполнения CAS-операций над глобальными таблицами (в миллисекундах)

### pico_instance_state

Текущее состояние инстанса (Online/Offline/Expelled)

### pico_raft_applied_index

Текущий applied index в Raft

### pico_raft_commit_index

Текущий commit index в Raft

### pico_raft_state

Текущее состояние инстанса в raft-кворуме (Follower/Candidate/Leader/PreCandidate)

### pico_raft_term

Текущий терм в Raft

### pico_raft_leader_id

Текущий id лидера в Raft (0 если не было лидера на момент вызова)


## Метрики Tarantool {: #tarantool_metrics }


### tnt_vinyl_disk_index_size

Размер индекса, сохраняемого в файлах / Amount of index stored in files

Тип: gauge

`tnt_vinyl_disk_index_size 0`

### tnt_read_only

Признак "только для чтения" для инстанса / Is instance read only

Тип: gauge

`tnt_read_only 0`

### tnt_memtx_tnx_user

Максимальное число байтов, выделенное функцией `box_txn_alloc()` на одну транзакцию / The maximum number of bytes allocated by `box_txn_alloc()` function per transaction

Тип: gauge

`tnt_memtx_tnx_user{kind="average"} 0`<br>
`tnt_memtx_tnx_user{kind="total"} 0`<br>
`tnt_memtx_tnx_user{kind="max"} 0`

### lj_gc_freed_total

Общий объем освобожденной памяти / Total amount of freed memory

Тип: counter

`lj_gc_freed_total 14048950`

### tnt_runtime_tuple

Тапл среды исполнения / Runtime tuple

Тип: gauge

`tnt_runtime_tuple 0`

### tnt_vinyl_regulator_rate_limit

Предел скорости записи / Write rate limit

Тип: gauge

`tnt_vinyl_regulator_rate_limit 10485760`

### tnt_vinyl_memory_page_index

Размер индексов страниц / Size of page indexes

Тип: gauge

`tnt_vinyl_memory_page_index 0`

### lj_gc_steps_pause_total

Общее количество инкрементных GC-шагов (состояние паузы) / Total count of incremental GC steps (pause state)

Тип: counter

`lj_gc_steps_pause_total 16`

### tnt_slab_arena_used_ratio

Показатель использования арены элементами slab / Slab arena_used_ratio info

Тип: gauge

`tnt_slab_arena_used_ratio 8.1`

### tnt_memtx_mvcc_tuples_tracking_retained

Количество сохраненных следящих строк или число историй / Number of retained `tracking` tuples / number of stories

Тип: gauge

`tnt_memtx_mvcc_tuples_tracking_retained{kind="total"} 0`<br>
`tnt_memtx_mvcc_tuples_tracking_retained{kind="count"} 0`

### tnt_slab_arena_used

Размер занятой памяти / Slab arena_used info

Тип: gauge

`tnt_slab_arena_used 2729920`

### tnt_cpu_system_time

Использование ЦП, системное / CPU system time usage

Тип: gauge

`tnt_cpu_system_time 0.092386`

### tnt_vinyl_memory_tuple_cache

Количество байт, выделяемое для тапла / Number of bytes that are being used for tuple

Тип: gauge

`tnt_vinyl_memory_tuple_cache 0`

### tnt_memtx_mvcc_tuples_read_view_stories

Количество таплов `read_view` / количество историй / Number of `read_view` tuples / number of stories

Тип: gauge

`tnt_memtx_mvcc_tuples_read_view_stories{kind="total"} 0`
`tnt_memtx_mvcc_tuples_read_view_stories{kind="count"} 0`

### tnt_net_requests_total

Общее количество запросов / Requests total amount

Тип: counter

`tnt_net_requests_total 44`

### lj_strhash_hit_total

Общее число придержанных строк / Total number of strings being interned

Тип: counter

`lj_strhash_hit_total 215655`

### tnt_vinyl_scheduler_tasks

Количество задач движка Vinyl / Vinyl tasks count

Тип: gauge

`tnt_vinyl_scheduler_tasks{status="failed"} 0`<br>
`tnt_vinyl_scheduler_tasks{status="inprogress"} 0`<br>
`tnt_vinyl_scheduler_tasks{status="completed"} 0`

### tnt_info_memory_tx

Память tx / Memory tx

Тип: gauge

`tnt_info_memory_tx 0`

### tnt_runtime_used

Использование памяти среды исполнения / Runtime used

Тип: gauge

`tnt_runtime_used 75497472`

### tnt_memtx_tnx_system

Максимальное число байтов, выделенное внутренним механизмом на транзакцию / The maximum number of bytes allocated by internals per transaction

Тип: gauge

`tnt_memtx_tnx_system{kind="average"} 0`<br>
`tnt_memtx_tnx_system{kind="total"} 0`<br>
`tnt_memtx_tnx_system{kind="max"} 0`

### lj_gc_steps_propagate_total

Общее количество инкрементных шагов GC (propagate state) / Total count of incremental GC steps (propagate state)

Тип: counter

`lj_gc_steps_propagate_total 65979`

### tnt_net_sent_total

Общее число отправленных байтов / Totally sent in bytes

Тип: counter

`tnt_net_sent_total 64500`

### tnt_fiber_amount

Количество файберова/ Amount of fibers

Тип: gauge

`tnt_fiber_amount 29`

### tnt_memtx_mvcc_tuples_used_retained

Количество придержанных таплов `used` / количество историй / Number of retained `used` tuples / number of stories

Тип: gauge

`tnt_memtx_mvcc_tuples_used_retained{kind="total"} 0`<br>
`tnt_memtx_mvcc_tuples_used_retained{kind="count"} 0`

### tnt_election_state

Состояние выборов на узле / Election state of the node

Тип: gauge

`tnt_election_state 0`

### tnt_election_vote

ID узла, за которого проголосовал текущий узел / ID of a node the current node votes for

Тип: gauge

`tnt_election_vote 0`

### tnt_net_per_thread_requests_in_stream_queue_total

Общее число запросов, размещенных в очередях потоков / Total count of requests, which was placed in queues of streams

Тип: counter

`tnt_net_per_thread_requests_in_stream_queue_total{thread="1"} 0`

### tnt_synchro_queue_term

Synchro queue term

Тип: gauge

`tnt_synchro_queue_term 0`

### tnt_vinyl_memory_level0 {: #tnt_vinyl_memory_level0 }

Терм очереди синхронизации / Size of in-memory storage of an LSM tree

Тип: gauge

`tnt_vinyl_memory_level0 0`

### tnt_net_per_thread_sent_total

Общее число отправленных байтов на поток / Totally sent in bytes

Тип: counter

`tnt_net_per_thread_sent_total{thread="1"} 64500`

### tnt_vinyl_tx_rollback

Количество откатов / Count of rollbacks

Тип: gauge

`tnt_vinyl_tx_rollback 0`

### tnt_cfg_current_time

Текущее время конфигурации Tarantool / Tarantool cfg time

Тип: gauge

`tnt_cfg_current_time 1717160267`

### tnt_synchro_queue_len

Число транзакций, которые собирают в данный момент подтверждения / Amount of transactions are collecting confirmations now

Тип: gauge

`tnt_synchro_queue_len 0`

### tnt_net_per_thread_requests_in_stream_queue_current

Число запросов, ждущих в данный момент в очередях потоков / Count of requests currently waiting in queues of streams

Тип: gauge

`tnt_net_per_thread_requests_in_stream_queue_current{thread="1"} 0`

### tnt_net_per_thread_requests_in_progress_current

Количество запросов на один поток, обрабатываемых в данный момент в потоке tx / Count of requests currently being processed in the tx thread

Тип: gauge

`tnt_net_per_thread_requests_in_progress_current{thread="1"} 0`

### tnt_net_connections_current

Количество текущих соединений / Current connections amount

Тип: gauge

`tnt_net_connections_current 3`

### tnt_net_per_thread_requests_current

Ожидающие запросы / Pending requests

Тип: gauge

`tnt_net_per_thread_requests_current{thread="1"} 0`

### tnt_net_per_thread_requests_total

Общее число запросов на поток / Requests total amount

Тип: counter

`tnt_net_per_thread_requests_total{thread="1"} 44`

### tnt_info_memory_net

Размер "чистой" памяти / Memory net

Тип: gauge

`tnt_info_memory_net 1654784`

### lj_gc_cdatanum

Количество выделенных объектов cdata / Amount of allocated cdata objects

Тип: gauge

`lj_gc_cdatanum 3591`

### tnt_net_per_thread_connections_total

Общее число соединений / Connections total amount

Тип: counter

`tnt_net_per_thread_connections_total{thread="1"} 7`

### tnt_vinyl_regulator_dump_watermark

Точка начала снятия дампа / Point when dumping must occur

Тип: gauge

`tnt_vinyl_regulator_dump_watermark 134217715`

### tnt_memtx_mvcc_tuples_used_stories

Количество "занятых" таплов / количество историй / Number of `used` tuples / number of stories

Тип: gauge

`tnt_memtx_mvcc_tuples_used_stories{kind="total"} 0`<br>
`tnt_memtx_mvcc_tuples_used_stories{kind="count"} 0`

### tnt_fiber_memused

Количество файберов, уже используемых в ОЗУ / Fibers memused

Тип: gauge

`tnt_fiber_memused 57`

### tnt_info_memory_cache

Кэш памяти / Memory cache

Тип: gauge

`tnt_info_memory_cache 0`

### lj_jit_snap_restore_total

Общее количество быстрых восстановлений / Overall number of snap restores

Тип: counter

`lj_jit_snap_restore_total 459`

### tnt_ev_loop_prolog_time

Время предварительной работы цикла событий, мс / Event loop prolog time (ms)

Тип: gauge

`tnt_ev_loop_prolog_time 0`

### tnt_runtime_lua

Показатель рантайма Lua / Runtime lua

Тип: gauge

`tnt_runtime_lua 3511325`

### tnt_net_requests_in_progress_current

Количество запросов, обрабатываемых в данный момент в потоке tx / Count of requests currently being processed in the tx thread

Тип: gauge

`tnt_net_requests_in_progress_current 0`

### lj_gc_steps_sweepstring_total

Общее количество инкрементных шагов GC / Total count of incremental GC steps (sweepstring state)

Тип: counter

`lj_gc_steps_sweepstring_total 153088`

### tnt_net_requests_in_stream_queue_total

Общее количество запросов, размещенных в очередях потоков / Total count of requests, which was placed in queues of streams

Тип: counter

`tnt_net_requests_in_stream_queue_total 0`

### tnt_net_requests_in_stream_queue_current

Количество запросов, ожидающих в данный момент в очередях потоков / Count of requests currently waiting in queues of streams

Тип: gauge

`tnt_net_requests_in_stream_queue_current 0`

### tnt_net_requests_in_progress_total

Общее число запросов на стадии выполнения / Requests in progress total amount

Тип: counter

`tnt_net_requests_in_progress_total 37`

### tnt_fiber_memalloc

Количество файберов, для которых выделено место в ОЗУ / Fibers memalloc

Тип: gauge

`tnt_fiber_memalloc 15102656`

### tnt_vinyl_tx_conflict

Количество конфликтов транзакций / Count of transaction conflicts

Тип: gauge

`tnt_vinyl_tx_conflict 0`

### tnt_slab_items_used_ratio

Показатель использования элементов slab / Slab items_used_ratio info

Тип: gauge

`tnt_slab_items_used_ratio 13.74`

### tnt_memtx_mvcc_trackers

Максимальное число трекеров, выделенных на транзакцию / Maximum trackers allocated per transaction

Тип: gauge

`tnt_memtx_mvcc_trackers{kind="average"} 0`<br>
`tnt_memtx_mvcc_trackers{kind="total"} 0`<br>
`tnt_memtx_mvcc_trackers{kind="max"} 0`

### tnt_vinyl_scheduler_dump_total

Число завершенных дампов / The count of completed dumps

Тип: counter

`tnt_vinyl_scheduler_dump_total 0`

### lj_gc_steps_sweep_total

Общее количество инкрементных шагов GC (sweep state) / Total count of incremental GC steps (sweep state)

Тип: counter

`lj_gc_steps_sweep_total 3529`

### tnt_net_requests_current

Количество ожидающих запросов / Pending requests

Тип: gauge

`tnt_net_requests_current 0`

### tnt_net_per_thread_requests_in_progress_total

Общее число выполняющихся в данный момент запросов / Requests in progress total amount

Тип: counter

`tnt_net_per_thread_requests_in_progress_total{thread="1"} 37`

### tnt_synchro_queue_owner

Владелец очереди синхронизации / Synchro queue owner

Тип: gauge

`tnt_synchro_queue_owner 0`

### tnt_space_bsize

Размер таблицы в байтах / Space bsize

Тип: gauge

`tnt_space_bsize{name="ORDERS",engine="memtx"} 79`<br>
`tnt_space_bsize{name="ITEMS",engine="memtx"} 70`<br>
`tnt_space_bsize{name="WAREHOUSE",engine="memtx"} 86`

### tnt_net_connections_total

Общее количество соединений / Connections total amount

Тип: counter

`tnt_net_connections_total 7`

### tnt_memtx_mvcc_tuples_tracking_stories

Количество "следящих" таплов / историй / Number of `tracking` tuples / number of tracking stories.

Тип: gauge

`tnt_memtx_mvcc_tuples_tracking_stories{kind="total"} 0`
`tnt_memtx_mvcc_tuples_tracking_stories{kind="count"} 0`

### tnt_net_received_total

Всего получено в байтах / Totally received in bytes

Тип: counter

`tnt_net_received_total 1129`

### lj_strhash_miss_total

Общее число событий выделения памяти для строк во время жизни платформы
 / Total number of strings allocations during the platform lifetime

Тип: counter

`lj_strhash_miss_total 29396`

### tnt_election_term

Терм текущих выборов / Current election term

Тип: gauge

`tnt_election_term 1`

### tnt_info_vclock

Число срабатываний VClock / VClock

Тип: gauge

`tnt_info_vclock{id="1"} 3235`<br>
`tnt_info_vclock{id="0"} 432`

### tnt_slab_quota_used

Размер занятой квоты памяти / Slab quota_used info

Тип: gauge

`tnt_slab_quota_used 33554432`

### tnt_synchro_queue_busy

Признак занятости очереди синхронизации / Is synchro queue busy

Тип: gauge

`tnt_synchro_queue_busy 0`

### lj_jit_trace_abort_total

Общее число событий отмены / Overall number of abort traces

Тип: counter

`lj_jit_trace_abort_total 69`

### tnt_space_index_bsize

Index bsize

Тип: gauge

`tnt_space_index_bsize{name="ORDERS",index_name="ORDERS_pkey"} 49152`<br>
`tnt_space_index_bsize{name="ITEMS",index_name="ITEMS_pkey"} 49152`<br>
`tnt_space_index_bsize{name="WAREHOUSE",index_name="WAREHOUSE_pkey"} 49152`<br>
`tnt_space_index_bsize{name="WAREHOUSE",index_name="WAREHOUSE_bucket_id"} 49152`<br>
`tnt_space_index_bsize{name="ITEMS",index_name="ITEMS_bucket_id"} 49152`<br>
`tnt_space_index_bsize{name="ORDERS",index_name="ORDERS_bucket_id"} 49152`

### tnt_memtx_tnx_statements

Максимальное число байтов, использованное одной транзакцией на выполнение выражения / Maximum number of bytes used by one transaction for statements

Тип: gauge

`tnt_memtx_tnx_statements{kind="average"} 0`<br>
`tnt_memtx_tnx_statements{kind="total"} 0`<br>
`tnt_memtx_tnx_statements{kind="max"} 0`

### tnt_info_lsn

Счетчик LSN / Tarantool lsn

Тип: gauge

`tnt_info_lsn 3235`

### lj_jit_mcode_size

Общий размер всех выделенных областей для машинного кода / Total size of all allocated machine code areas

Тип: gauge

`lj_jit_mcode_size 65536`

### tnt_net_per_thread_connections_current

Текущее количество соединений / Current connections amount

Тип: gauge

`tnt_net_per_thread_connections_current{thread="1"} 3`

### tnt_info_memory_lua

Память Lua / Memory lua

Тип: gauge

`tnt_info_memory_lua 3808516`

### tnt_info_memory_index

Память индекса / Memory index

Тип: gauge

`tnt_info_memory_index 2523136`

### lj_gc_strnum

Количество выделенных строковых объектов / Amount of allocated string objects

Тип: gauge

`lj_gc_strnum 13589`

### tnt_vinyl_regulator_dump_bandwidth

Среднее расчетное время снятия дампа / Estimated average rate at which dumps are done

Тип: gauge

`tnt_vinyl_regulator_dump_bandwidth 10485760`

### tnt_stats_op_total

Общее число операций / Total amount of operations

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

### tnt_vinyl_scheduler_dump_time

Общее время, потраченное воркерами на сохранение дампа / Total time spent by all worker threads performing dump

Тип: gauge

`tnt_vinyl_scheduler_dump_time 0`

### tnt_vinyl_memory_bloom_filter

Размер фильтра bloom / Size of bloom filter

Тип: gauge

`tnt_vinyl_memory_bloom_filter 0`

### tnt_election_leader

ID узла-лидера в текущем терме / Leader node ID in the current term

Тип: gauge

`tnt_election_leader 0`

### tnt_fiber_csw

Данные csw для файберов / Fibers csw

Тип: gauge

`tnt_fiber_csw 384`

### tnt_slab_quota_size

Размер квоты памяти / Slab quota_size info

Тип: gauge

`tnt_slab_quota_size 67108864`

### tnt_slab_arena_size

Размер выделенной памяти / Slab arena_size info

Тип: gauge

`tnt_slab_arena_size 33554432`

### tnt_vinyl_tx_commit

Число коммитов / Count of commits

Тип: gauge

`tnt_vinyl_tx_commit 0`

### tnt_info_uptime

Время работы Tarantool / Tarantool uptime

Тип: gauge

`tnt_info_uptime 8`

### tnt_slab_items_size

Размер элементов slab / Slab items_size info

Тип: gauge

`tnt_slab_items_size 1505312`

### tnt_vinyl_regulator_blocked_writers

Количество файберов, заблокированных из-за ожидания выделения квоты движком Vinyl / The number of fibers that are blocked waiting for Vinyl level0 memory quota

Тип: gauge

`tnt_vinyl_regulator_blocked_writers 0`

### lj_gc_udatanum

Количество выделенных объектов udata / Amount of allocated udata objects

Тип: gauge

`lj_gc_udatanum 84`

### tnt_memtx_mvcc_tuples_read_view_retained

Количество оставленных таплов `read_view` / количество историй / Number of retained `read_view` tuples / number of stories

Тип: gauge

`tnt_memtx_mvcc_tuples_read_view_retained{kind="total"} 0`<br>
`tnt_memtx_mvcc_tuples_read_view_retained{kind="count"} 0`

### lj_gc_tabnum

Количество выделенных табличных объектов / Amount of allocated table objects

Тип: gauge

`lj_gc_tabnum 6717`

### lj_gc_steps_atomic_total

Общее количество инкрементных шагов GC (atomic state) / Total count of incremental GC steps (atomic state)

Тип: counter

`lj_gc_steps_atomic_total 18`

### tnt_space_total_bsize

Общий объем таблиц, в байтах / Space total bsize

Тип: gauge

`tnt_space_total_bsize{name="ORDERS",engine="memtx"} 98383`<br>
`tnt_space_total_bsize{name="ITEMS",engine="memtx"} 98374`<br>
`tnt_space_total_bsize{name="WAREHOUSE",engine="memtx"} 98390`

### lj_gc_memory

Размер выделенной в данный момент памяти / Memory currently allocated

Тип: gauge

`lj_gc_memory 3483001`

### tnt_memtx_mvcc_conflicts

Максимальное число байтов, выделенное для конфликтов в расчете на одну транзакцию / Maximum bytes allocated for conflicts per transaction

Тип: gauge

`tnt_memtx_mvcc_conflicts{kind="average"} 0`<br>
`tnt_memtx_mvcc_conflicts{kind="total"} 0`<br>
`tnt_memtx_mvcc_conflicts{kind="max"} 0`

### tnt_ev_loop_epilog_time

Время завершения для цикла событий, мс / Event loop epilog time (ms)

Тип: gauge

`tnt_ev_loop_epilog_time 0.001`

### tnt_ev_loop_time

Время цикла событий, мс / Event loop time (ms)

Тип: gauge

`tnt_ev_loop_time 0.008`

### tnt_space_len

Длина таблицы / Space length

Тип: gauge

`tnt_space_len{name="ORDERS",engine="memtx"} 5`<br>
`tnt_space_len{name="ITEMS",engine="memtx"} 5`<br>
`tnt_space_len{name="WAREHOUSE",engine="memtx"} 5`

### lj_jit_trace_num

Количество трассировок JIT / Amount of JIT traces

Тип: gauge

`lj_jit_trace_num 94`

### lj_gc_steps_finalize_total

Общее количество инкрементных шагов GC (finalize state) / Total count of incremental GC steps (finalize state)

Тип: counter

`lj_gc_steps_finalize_total 220`

### tnt_net_per_thread_received_total

Всего получено в байтах / Totally received in bytes

Тип: counter

`tnt_net_per_thread_received_total{thread="1"} 1129`

### lj_gc_allocated_total

Общий объем выделенной памяти / Total amount of allocated memory

Тип: counter

`lj_gc_allocated_total 17531951`

### tnt_vinyl_tx_read_views

Число открытых заданий чтения / Count of open read views

Тип: gauge

`tnt_vinyl_tx_read_views 0`

### tnt_slab_quota_used_ratio

Квота использования выделенной памяти / Slab quota_used_ratio info

Тип: gauge

`tnt_slab_quota_used_ratio 50`

### tnt_slab_items_used

Число используемых элементов slab / Slab items_used info

Тип: gauge

`tnt_slab_items_used 206784`

### tnt_cpu_user_time

Использование ЦП, пользовательское / CPU user time usage

Тип: gauge

`tnt_cpu_user_time 0.077745`

### tnt_vinyl_disk_data_size

Объем данных, сохраняемых в файлах / Amount of data stored in files

Тип: gauge

`tnt_vinyl_disk_data_size 0`

### tnt_info_memory_data

Память данных / Memory data

Тип: gauge

`tnt_info_memory_data 206784`

### tnt_vinyl_regulator_write_rate

Средний показатель скорости недавних записей на диск / Average rate at which recent writes to disk are done

Тип: gauge

`tnt_vinyl_regulator_write_rate 0`
