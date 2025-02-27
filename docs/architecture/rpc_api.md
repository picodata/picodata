# Интерфейс RPC API

В данном документе описан Интерфейс RPC API — основной интерфейс
взаимодействия с инстансом Picodata. Вызов RPC-функций происходит по
протоколу [iproto] через [коннектор][connectors] `net.box` или любой
другой.

[iproto]: https://www.tarantool.io/en/doc/2.11/dev_guide/internals/box_protocol/
[connectors]: https://www.tarantool.io/en/doc/2.11/book/connectors/

RPC API используется в следующих сценариях:

- Внешние системы могут вызывать функции через коннектор
- Инстансы взаимодействуют друг с другом под служебной учетной записью
  `pico_service`
- Тестирование pytest использует для подключения клиент tarantool-python
- Подключение `picodata connect` использует вызов [`.proc_sql_dispatch`](#proc_sql_dispatch)
<!-- - Синтаксис вызова из Lua: `box.func[".proc_version_info"]:call()` -->
<!-- - Команда `picodata expel` использует вызов [.proc_expel_instance](#proc_expel_instance) -->

<!-- TODO Описать для всех функций
- В какой версии RPC API добавлена
- Йилдит ли
-->

## Детали реализации {: #implementation_details }

Функции RPC API представляют собой хранимые процедуры Tarantool
`box.func`. Аргументы и возвращаемые значения функций описаны в системе типов
[msgpack](https://msgpack.org/).

### Особенности энкодинга {: #proc_encoding }

 - `optional`-поле (например `optional MP_INT`) отличается от обычного тем,
   что на его месте может присутствовать как ожидаемый msgpack-тип (`MP_INT`),
   так и `MP_NIL`. Тем не менее, при формировании запроса, нельзя пропускать
   это поле. При отсутствии значения всегда должен быть указан `MP_NIL`.

 - в случае успешного выполнения RPC-функции (`IPROTO_OK`) возвращаемое
   msgpack-значение всегда дополнительно обернуто в `MP_ARRAY`.

 - в случае ошибки выполнения RPC-функции (`Result::Err` в терминах Rust) в ответе
   будет iproto-сообщение с типом `IPROTO_TYPE_ERROR`, содержащее в себе
   [описание](https://www.tarantool.io/en/doc/latest/dev_guide/internals/iproto/format/#error-responses)
   ошибки.

### Привилегии {: #proc_privileges }

Хранимые процедуры Tarantool не входят в модель управления доступом
Picodata. На них невозможно выдать или отозвать привилегии. Авторизация
запросов происходит по следующему принципу:

- функции public доступны для роли `public`, которая автоматически выдается
  всем новым пользователям. В этом случае авторизуется не вызов функции,
  а сам запрос

- привилегиями на вызов остальных функций обладают Администратор
  СУБД (`admin`) и служебная учетная запись `pico_service`

## Public API {: #public_api }

### .proc_version_info {: #proc_version_info }

```rust
fn proc_version_info() -> VersionInfo
```

Возвращает информацию о версиях Picodata и отдельных ее компонентах.

Возвращаемое значение:

- (MP_MAP `VersionInfo`):
    - `picodata_version`: (MP_STR) версия инстанса
      <!-- TODO ссылка на политику версионирования -->
    - `rpc_api_version`: (MP_STR) версия RPC API согласно семантическому
      версионированию [Semantic Versioning][semver]

[semver]: https://semver.org/

### .proc_sql_dispatch {: #proc_sql_dispatch }

```rust
fn proc_sql_dispatch(pattern, params) -> Result
```

Выполняет распределенный SQL-запрос.

Аргументы:

- `pattern`: (MP_STR) запрос SQL
- `params`: (MP_ARRAY) параметры для использования в `pattern` в случае
  [параметризованного запроса][parametrization]

Возвращаемое значение:

- (MP_MAP `DqlResult`) при чтении данных
  <br>Поля:
    - `metadata` (MP_ARRAY), массив описаний столбцов таблицы в формате
      `MP_ARRAY [ MP_MAP { name = MP_STR, type = MP_STR }, ...]`
    - `rows` (MP_ARRAY), результат выполнения читающего запроса в формате
      `MP_ARRAY [ MP_ARRAY row, ...]`
- (MP_MAP `DmlResult`) при модификации данных
  <br>Поля:
    - `row_count` (MP_INT), количество измененных строк

См. также:

- [Инструкции и руководства — Работа с данными SQL](../tutorial/sql_examples.md)

[parametrization]: ../reference/sql/parametrization.md

## Service API {: #service_api }

### .proc_apply_schema_change {: #proc_apply_schema_change }

```rust
fn proc_apply_schema_change(term, applied, timeout) -> Result
```

Дожидается момента, когда raft применит запись с заданным индексом и
термом перед тем как делать что-то еще, чтобы синхронизировать состояние
[глобальных системных таблиц](./system_tables.md). Возвращает ошибку,
если времени не хватило.

Применяет текущие изменения глобальной схемы к локальному состоянию инстанса.

Эту хранимую процедуру вызывает только
[governor](../overview/glossary.md#governor) в рамках алгоритма
отказоустойчивой смены кластерной схемы данных.
<!-- ./clusterwide_schema.md#two_phase_algorithm -->

Текущие изменения схемы инстанс читает из системной таблицы
[`_pico_property`](./system_tables.md#_pico_property). В ней по ключам
 `pending_schema_change` и `pending_schema_version` соответственно
находятся описание текущей
[DDL](https://ru.wikipedia.org/wiki/Data_Definition_Language)-операции и
версия глобальной схемы после применения этой операции.

В случае если изменение не применимо на текущем инстансе из-за конфликта
хранимых данных, в ответ посылается сообщение с причиной конфликта.

Параметры:

- `term`: (MP_INT `RaftTerm`)
- `applied`: (MP_INT `RaftIndex`)
- `timeout`: (MP_INT | MP_FLOAT) в секундах

Возвращаемое значение:

- *Ok*: отсутствует
- *Abort*: `cause` (MP_MAP `ErrorInfo`):
    - `error_code`: (MP_INT)
    - `message`: (MP_STR)
    - `instance_name`: (MP_STR)

### .proc_before_online {: #proc_before_online }

```rust
fn proc_before_online(term, applied, timeout)
```

На текущем инстансе включает все плагины со статусом `enable = true`
и инициализирует протокол PostgreSQL. Вызывает коллбэки [`on_start`] для
всех сервисов всех плагинов.

[Губернатор][g] вызывает данную хранимую процедуру при повторной
инициализации инстансов после их временного выключения или выхода из строя.

[`on_start`]: https://docs.rs/picodata-plugin/latest/picodata_plugin/plugin/interface/trait.Service.html#method.on_start

См. также:

* [Инструкции и руководства — Добавление узлов](../tutorial/node_add.md)

Параметры:

- `term`: (MP_INT `RaftTerm`)
- `applied`: (MP_INT `RaftIndex`)
- `timeout`: (MP_INT | MP_FLOAT) в секундах

### .proc_cas {: #proc_cas }

```rust
fn proc_cas(cluster_name, predicate, op, as_user) -> (RaftIndex, RaftTerm)
```

Выполняется только на [raft-лидере](../overview/glossary.md#raft_leader), в
противном случае немедленно возвращает ошибку.

Исполняет кластерную операцию [compare-and-swap](../overview/glossary.md#cas).

Проверяет предикат. Если проверка не выявляет конфликтов, то добавляет
новую запись в raft-журнал и возвращает ее индекс (пока еще не зафиксированный в
кластере). Операция применяется только для [глобальных таблиц](../overview/glossary.md#table)
и работает на всем кластере.

Предикат состоит из трех частей:

- индекс записи в [raft-журнале](../overview/glossary.md#log_replication);
- номера [терма](../overview/glossary.md#term);
- диапазона значений самого проверяемого параметра (`ranges`).

Диапазон `ranges` опционален, так как Picodata автоматически проверяет
диапазон значений на основе запрашиваемой операции. Возможность указать
`ranges` явно позволяет добавить проверку дополнительных значений
(например, если они не изменяются операцией, но неявно могут влиять на
нее).

Параметры:

- `cluster_name`: (MP_STR)
- `predicate`: (MP_MAP `Predicate`)
- `op`: (MP_MAP `Op`)
- `as_user`: (MP_INT)

Возвращаемое значение:

- `index`: (MP_INT) raft-индекс
- `term`: (MP_INT) raft-терм

### .proc_disable_service {: #proc_disable_service }

```rust
fn proc_disable_service(term, applied, timeout)
```

Отключает сервис плагина на текущем инстансе, вызывая коллбэк [`on_stop`].

[Губернатор][g] вызывает данную хранимую процедуру при изменении
[конфигурации плагина][p_c].

[g]: ../overview/glossary.md#governor
[p_c]: plugins.md#plugin_config
[`on_stop`]: https://docs.rs/picodata-plugin/latest/picodata_plugin/plugin/interface/trait.Service.html#method.on_stop

Параметры:

- `term`: (MP_INT `RaftTerm`)
- `applied`: (MP_INT `RaftIndex`)
- `timeout`: (MP_INT | MP_FLOAT) в секундах

### .proc_discover {: #proc_discover }

```rust
fn proc_discover(request, request_to) -> Result
```

Эту хранимую процедуру вызывают инстансы Picodata во время запуска с пустым
состоянием ([bootstrap](../overview/glossary.md#bootstrap)) в рамках реализации
алгоритма [discovery](./discovery.md).

Параметры:

- `request`: (MP_MAP):
    - `tmp_id`: (MP_STR)
    - `peers`: (MP_ARRAY of MP_STR)
- `request_to`: (MP_STR) адрес, по которому доступен текущий инстанс

Возвращаемое значение:

- *LeaderElection*: (MP_MAP `LeaderElection`) — если алгоритм продолжается:
    - `tmp_id`: (MP_STR)
    - `peers`: (MP_ARRAY of MP_STR)
- *Done*: (MP_MAP `Role`) — если результат уже известен:
    - `leader_address`: (MP_STR) адрес лидера

### .proc_enable_plugin {: #proc_enable_plugin }

```rust
fn proc_enable_plugin(term, applied, timeout) -> Result
```

Включает плагин на текущем инстансе. Вызывает коллбэки [`on_start`]
для всех сервисов плагина.

[Губернатор][g] вызывает данную хранимую процедуру при включении плагина
на выбранных инстансах.

В случае возвращения ошибки включение плагина будет прервано на каждом
из выбранных инстансов.

См. также:

* [Механизм плагинов ~ Включение](plugins.md#plugin_enable)

Параметры:

- `term`: (MP_INT `RaftTerm`)
- `applied`: (MP_INT `RaftIndex`)
- `timeout`: (MP_INT | MP_FLOAT) в секундах

Возвращаемое значение:

- *Ok*: отсутствует
- *Abort*: `cause` (MP_MAP `ErrorInfo`):
    - `error_code`: (MP_INT)
    - `message`: (MP_STR)
    - `instance_name`: (MP_STR)

### .proc_enable_service {: #proc_enable_service }

```rust
fn proc_enable_service(term, applied, timeout)
```

Включает сервис плагина на текущем инстансе, вызывая коллбэк [`on_start`].

[Губернатор][g] вызывает данную хранимую процедуру при изменении
[конфигурации плагина][p_c].

Параметры:

- `term`: (MP_INT `RaftTerm`)
- `applied`: (MP_INT `RaftIndex`)
- `timeout`: (MP_INT | MP_FLOAT) в секундах

### .proc_expel {: #proc_expel }

```rust
fn proc_expel(cluster_name, instance_uuid)
```

Выполняется только на [raft-лидере](../overview/glossary.md#raft_leader), в
противном случае немедленно возвращает ошибку.

Удаляет указанный инстанс из кластера. На его место в будущем может прийти
инстанс с тем же `instance_name`, однако `raft_id` уходящего инстанса больше
никогда не будет использоваться.

Параметры:

- `cluster_name`: (MP_STR)
- `instance_uuid`: (MP_STR)

### .proc_expel_redirect {: #proc_expel_redirect }

```rust
fn proc_expel_redirect(cluster_name, instance_uuid)
```

Вызывает [`.proc_expel`](#proc_expel) на текущем [raft-лидере](../overview/glossary.md#raft_leader).

<!-- С большой вероятностью скоро будет удалена. -->

Параметры:

- `cluster_name`: (MP_STR)
- `instance_uuid`: (MP_STR)

### .proc_get_config {: #.proc_get_config }

```rust
fn proc_get_config() -> Result
```

Возвращает совокупную конфигурацию инстанса, в которой для каждого параметра
указано значение `value` и его источник `source`.

Возможные типы данных:

* `value`: (MP_MAP | MP_INT | MP_STR | MP_BOOL | MP_ARRAY | MP_FLOAT)
* `source`: (MP_STR)

Возможные источники значений параметров — поле `source`:

* `"default"` — по умолчанию
* `"config_file"` — [файл конфигурации]
* `"commandline_or_environment"` — параметры команды [`picodata
  run`](../reference/cli.md#run) или переменные окружения

Возвращаемое значение:

* `config` (MP_MAP) — словарь, повторяющий формат [файла конфигурации]

[файл конфигурации]: ../reference/config.md#config_file_description
[файла конфигурации]: ../reference/config.md#config_file_description

### .proc_get_index {: #proc_get_index }

```rust
fn proc_get_index() -> RaftIndex
```

Возвращает текущий примененный (applied) индекс raft-журнала.

Возвращаемое значение:

* (MP_INT `RaftIndex`)

### .proc_get_vclock {: #proc_get_vclock }

```rust
fn proc_get_vclock() -> Vclock
```

Возвращает текущее значение [Vclock].

Возвращаемое значение:

* (MP_MAP `Vclock`)

### .proc_get_vshard_config {: #proc_get_vshard_config }

```rust
fn proc_get_vshard_config(tier_name) -> Result
```

Возвращает Vshard-конфигурацию [тира] `tier_name`. Если тир не указан,
возвращается Vshard-конфигурация тира текущего инстанса.

Возвращаемое значение:

* (MP_MAP `VshardConfig`)

<!--
- `VshardConfig`: (MP_MAP)
    - `replicaset_uuid`: (MP_MAP)
        - `replicas`: (MP_MAP)
            - `instance_uuid`: (MP_MAP)
                - `master`: (MP_BOOL)
                - `uri`: (MP_STR)
                - `name`: (MP_STR)
        - `weight`: (MP_FLOAT)
-->

[тира]: ../overview/glossary.md#tier

### .proc_instance_info {: #proc_instance_info }

```rust
fn proc_instance_info(instance_name) -> InstanceInfo
```

Возвращает информацию о запрашиваемом инстансе из кластера.

При вызове без параметров возвращает информацию о текущем инстансе.

Аргументы:

- `instance_name`: (optional MP_STR)

Возвращаемое значение:

- (MP_MAP `InstanceInfo`):
    - `raft_id`: (MP_UINT)
    - `iproto_advertise`: (MP_STR)
    - `name`: (MP_STR)
    - `uuid`: (MP_STR)
    - `replicaset_name`: (MP_STR)
    - `replicaset_uuid`: (MP_STR)
    - `cluster_name`: (MP_STR)
    - `current_state`: (MP_MAP [`State`](../overview/glossary.md#state)) — текущее состояние инстанса
      <br>формат: `MP_MAP { variant = MP_STR, incarnation = MP_UINT}`
      <br>возможные значения `variant`: `Offline`, `Online`, `Expelled`
    - `target_state`: (MP_MAP [`State`](../overview/glossary.md#state)) — целевое состояние инстанса
    - `tier`: (MP_STR)
    - `picodata_version`: (MP_STR) версия инстанса

### .proc_load_plugin_dry_run {: #proc_load_plugin_dry_run }

```rust
fn proc_load_plugin_dry_run(term, applied, timeout) -> Result
```

Проверяет возможность включения плагина на текущем инстансе.

[Губернатор][g] вызывает данную хранимую процедуру при [установке
плагина][p_i].

[p_i]: plugins.md#plugin_install

Параметры:

- `term`: (MP_INT `RaftTerm`)
- `applied`: (MP_INT `RaftIndex`)
- `timeout`: (MP_INT | MP_FLOAT) в секундах

Возвращаемое значение:
- *Ok*: отсутствует

### .proc_raft_info {: #proc_raft_info }

```rust
fn proc_raft_info() -> RaftInfo
```

Возвращает информацию о состоянии raft-узла на текущем инстансе.

Возвращаемое значение:

- (MP_MAP `RaftInfo`):
    - `id`: (MP_INT) `raft_id` текущего узла
    - `term`: (MP_INT) текущий [терм](../overview/glossary.md#term)
    - `applied`: (MP_INT) текущий примененный индекс raft-журнала
    - `leader_id`: (MP_INT) `raft_id` лидера или `0` если в текущем
      терме его нет
    - `state` (MP_STR)
      <br>возможные значения: `Follower`, `Candidate`, `Leader`, `PreCandidate`

### .proc_raft_interact {: #proc_raft_interact }

```rust
fn proc_raft_interact(raft_messages)
```

Эту хранимую процедуру вызывают все инстансы Picodata для передачи внутренних
сообщений друг другу в рамках реализации алгоритма [raft](../overview/glossary.md#raft).

Параметры:

- `raft_messages`: (MP_ARRAY of MP_ARRAY)

### .proc_raft_join {: #proc_raft_join }

```rust
fn proc_raft_join(cluster_name, instance_name, replicaset_name, advertise_address, failure_domain, tier, picodata_version) -> Result
```

Выполняется только на [raft-лидере](../overview/glossary.md#raft_leader),
в противном случае немедленно возвращает ошибку.

Эту хранимую процедуру вызывают инстансы Picodata, присоединяющиеся к кластеру,
то есть еще не состоящие в [raft-группе](../overview/glossary.md#raft).

См. также:

- [Жизненный цикл инстанса](./instance_lifecycle.md#fn_start_join)

Параметры:

- `cluster_name`: (MP_STR)
- `instance_name`: (MP_STR | MP_NIL)
- `replicaset_name`: (MP_STR | MP_NIL) идентификатор [репликасета](../overview/glossary.md#replicaset)
- `advertise_address`: (MP_STR)
- `failure_domain`: (MP_MAP) [домен отказа](../overview/glossary.md#failure_domain)
- `tier`: (MP_STR) идентификатор [тира]
- `picodata_version`: (MP_STR) версия инстанса

Возвращаемое значение:

- (MP_MAP `Response`):
    - `instance`: (MP_MAP): кортеж из системной таблицы [`_pico_instance`](./system_tables.md#_pico_instance),
                            соответствующий присоединяющемуся инстансу
    - `peer_addresses`: (MP_ARRAY): набор адресов некоторых инстансов кластера
        - (MP_MAP `PeerAddress`):
            - `raft_id`: (MP_INT)
            - `address`: (MP_STR)
    - `box_replication`: (MP_ARRAY of MP_STR): адреса всех реплик в репликасете
                                              присоединяющегося инстанса

### .proc_raft_promote {: #proc_raft_promote }

```rust
fn proc_raft_promote()
```

Завершает текущий [raft-терм](../overview/glossary.md#term) и объявляет выборы
нового [лидера](../overview/glossary.md#raft_leader). Предлагает себя
как кандидата в лидеры raft-группы. Если других кандидатов не обнаружится,
текущий инстанс с большой вероятностью станет новым лидером.

### .proc_raft_snapshot_next_chunk {: #proc_raft_snapshot_next_chunk }

```rust
fn proc_raft_snapshot_next_chunk(entry_id, position) -> Result
```

Возвращает следующий отрезок данных [raft-снапшота](../overview/glossary.md#snapshot).

Эту хранимую процедуру вызывают только инстансы с ролью [raft follower](../overview/glossary.md#node_states)
в рамках процесса [актуализации raft-журнала](../overview/glossary.md#actualization).

Текущий инстанс проверяет наличие снапшота соответствующего состоянию
raft-журнала на момент, когда актуальными были указанные
[терм](../overview/glossary.md#term) и
[индекс](../overview/glossary.md#log_replication). При этом, снапшот
должен существовать на момент вызова этой процедуры, так как он
создается автоматически когда raft-лидер получает запрос на актуализацию
raft-журнала и обнаруживает, что его журнал был
[компактизирован](../overview/glossary.md#raft_log_compaction).

Снапшот представляет из себя последовательность кортежей [глобальных
таблиц](../overview/glossary.md#table), как системных так и
пользовательских. В этой последовательности кортежи сгруппированы по
таблицам и упорядочены в соответствие с первичными ключами таблиц.
Последовательность таблиц в снапшоте детерминирована и совпадает между
разными версиями снапшотов.

Следующий отрезок снапшота определяется параметрами: идентификатором
последней таблицы, кортежи которой были получены в предыдущем отрезке, и
количеством кортежей этой таблицы, полученных за все предыдущие отрезки
снапшота.

Параметры:

- `entry_id` (MP_MAP `RaftEntryId`):
    - `index`: (MP_INT)
    - `term`: (MP_INT)
- `position`: (MP_MAP `SnapshotPosition`)
    - `space_id`: (MP_INT)
    - `tuple_offset`: (MP_INT)

Возвращаемое значение:

- `snapshot_data` (MP_MAP `SnapshotData`):
    - `schema_version`: (MP_INT)
    - `space_dumps`: (MP_ARRAY):
        - (MP_MAP `SpaceDump`):
            - `space_id`: (MP_INT)
            - `tuples`: (MP_ARRAY of MP_ARRAY) «сырые» кортежи таблицы
    - `next_chunk_position`: (MP_MAP `SnapshotPosition` | MP_NIL)

### .proc_read_index {: #proc_read_index }

```rust
fn proc_read_index(timeout) -> RaftIndex
```

Выполняет кворумное чтение по следующему принципу:

  1. Инстанс направляет запрос (`MsgReadIndex`) лидеру raft-группы. В
     случае, если лидера в данный момент нет, функция возвращает ошибку
     'raft: proposal dropped'
  2. Raft-лидер запоминает текущий `commit_index` и отправляет всем узлам
     в статусе `follower` сообщение (heartbeat) с тем, чтобы убедиться,
     что он все еще является лидером
  3. Как только получение этого сообщения подтверждается
     большинством `follower`-узлов, лидер возвращает этот индекс
     инстансу
  4. Инстанс дожидается применения (apply) указанного raft-индекса. Если
     таймаут истекает раньше, функция возвращает ошибку 'timeout'

Параметры:

- `timeout`: (MP_INT | MP_FLOAT) в секундах

Возвращаемое значение:

* (MP_INT `RaftIndex`)

### .proc_replication {: #proc_replication }

```rust
fn proc_replication(is_master, replicaset_peers)
```

Обновляет конфигурацию топологии репликации текущего инстанса с остальными
инстансами из одного репликасета.

Данную хранимую процедуру вызывает только [governor](../overview/glossary.md#governor)
в рамках алгоритма автоматической смены топологии кластера.

Первоисточником принадлежности инстанса к тому или иному репликасету является
информация в системной таблице [`_pico_instance`](./system_tables.md#_pico_instance),
однако при вызове этой хранимой процедуры список адресов инстансов указывается
явно из-за ограничений на репликацию системных таблиц: так как данные системных
таблиц распространяются через механизм репликации, его нужно настроить перед
тем, как данные смогут реплицироваться.

Для репликации Picodata использует топологию full-mesh, при которой каждая реплика
поддерживает соединение с каждой другой репликой. При этом,
[мастер-реплика](../overview/glossary.md#replicaset) в каждом репликасете только
одна. Признак того, что текущая реплика должна стать мастером, передается
соответствующим параметром.

См. также:

- [Governor — централизованное управление кластером](./topology_management.md#governor)
- [Репликация в Tarantool](https://www.tarantool.io/ru/doc/latest/concepts/replication/)

Параметры:

- `is_master`: (MP_BOOL)
- `replicaset_peers`: (MP_ARRAY of MP_STR)

### .proc_replication_demote {: #proc_replication_demote }

```rust
fn proc_replication_demote() -> Vclock
```

Переводит текущий инстанс в состояние [резервной реплики](../overview/glossary.md#replicaset).

Данную хранимую процедуру вызывает только [governor](../overview/glossary.md#governor)
в рамках алгоритма автоматической смены текущего мастера репликасета.

См. также:

 - [Governor — централизованное управление кластером](./topology_management.md#governor)

Получив такой запрос, инстанс сразу переходит в режим read-only и отправляет в
ответ текущее значение своего [Vclock], которое
дальше используется для синхронизации новой мастер-реплики.

См. [`.proc_replication`](#proc_replication) о том, как в Picodata настраивается репликация.

Возвращаемое значение:

- `vclock`: (MP_MAP `Vclock`)

### .proc_replication_sync {: #proc_replication_sync }

```rust
fn proc_replication_sync(vclock, timeout)
```

Обеспечивает репликацию Tarantool для текущего инстанса, пока значение его
[Vclock] не станет равным `vclock`.

Параметры:

- `vclock`: (MP_MAP `Vclock`)
- `timeout`: (MP_INT | MP_FLOAT) в секундах

[Vclock]: ../overview/glossary.md#vclock

### .proc_rpc_dispatch {: #proc_rpc_dispatch }

```rust
fn proc_rpc_dispatch(..) -> Result
```

Точка входа для [RPC-системы] плагинов.

Эту хранимую процедуру вызывают плагины, использующие RPC-систему
для взаимодействия между инстансами кластера.

Аргументы:

- `path`: (MP_STR) имя RPC-эндпоинта
- `input`: (MP_BIN) аргументы запроса
- `context`: (MP_MAP [`Context`]) — метаинформация о запросе:
  <br>Обязательные поля:
    - `request_id`: (MP_EXT MP_UUID)
    - `plugin_name`: (MP_STR)
    - `service_name`: (MP_STR)
    - `plugin_version`: (MP_STR)

Возвращаемое значение:

- `output`: (MP_BIN) определяется автором плагина

[`Context`]: https://docs.rs/picodata-plugin/latest/picodata_plugin/transport/context/struct.Context.html
[RPC-системы]: https://docs.rs/picodata-plugin/latest/picodata_plugin/transport/rpc/index.html

### .proc_runtime_info {: #proc_runtime_info }

```rust
fn proc_runtime_info() -> RuntimeInfo
```

Возвращает служебную информацию.

Возвращаемое значение:

- (MP_MAP `RuntimeInfo`):
    - `raft`: (MP_MAP [`RaftInfo`](#proc_raft_info))
    - `internal`: (MP_MAP `InternalInfo`)
      <br>формат: `MP_MAP { main_loop_status = MP_STR,
      governor_loop_status = MP_STR, governor_step_counter = MP_INT }`
    - `http`: (optional MP_MAP `HttpServerInfo`)
      <br>формат: `MP_MAP { host = MP_STR, port = MP_UINT }`
      <br>поле отсутствует в ответе, если инстанс запущен без параметра
      [`picodata run --http-listen`](../reference/cli.md#run_http_listen)
    - `version_info`: (MP_MAP [`VersionInfo`](#proc_version_info))
    - `slab_info`: (MP_MAP `SlabInfo`)

### .proc_sharding {: #proc_sharding }

```rust
fn proc_sharding(term, applied, timeout)
```

Дожидается применения raft-записи с заданным индексом и термом перед тем как
делать что-то еще, чтобы синхронизовать состояние [глобальных системных
таблиц](./system_tables.md). Возвращает ошибку, если времени не хватило.

Обновляет конфигурацию [шардирования данных](../overview/glossary.md#vshard)
между [репликасетами](../overview/glossary.md#replicaset).

Эту хранимую процедуру вызывает только [governor](../overview/glossary.md#governor)
в рамках алгоритма автоматической смены топологии кластера.

См. также:

 - [Governor — централизованное управление кластером](./topology_management.md#governor)

В системной таблице [`_pico_tier`](system_tables.md#_pico_tier) хранятся
две версии конфигурации распределения
[сегментов](../overview/glossary.md#segment): текущая и целевая
(target). Этим версиям соответствуют колонки
`current_vshard_config_version` и `target_vshard_config_version`.

Параметры:

- `term`: (MP_INT `RaftTerm`)
- `applied`: (MP_INT `RaftIndex`)
- `timeout`: (MP_INT | MP_FLOAT) в секундах

### .proc_sharding_bootstrap {: #proc_sharding_bootstrap }

```rust
fn proc_sharding_bootstrap(term, applied, timeout, tier)
```

Дожидается применения raft-записи с заданным индексом и термом перед тем как
делать что-то еще, чтобы синхронизовать состояние [глобальных системных
таблиц](./system_tables.md). Возвращает ошибку, если времени не хватило.

Инициирует распределение [сегментов](../overview/glossary.md#segment)
между [репликасетами](../overview/glossary.md#replicaset).

Эту хранимую процедуру вызывает только [governor](../overview/glossary.md#governor)
в рамках алгоритма автоматической смены топологии кластера.

См. также:

 - [Governor — централизованное управление кластером](./topology_management.md#governor)

За жизненный цикл кластера эта процедура вызывается ровно один раз, как
только в кластере появляется первый репликасет, удовлетворяющий соответствующему
[фактору репликации](../overview/glossary.md#replication_factor).

Параметры:

- `term`: (MP_INT `RaftTerm`)
- `applied`: (MP_INT `RaftIndex`)
- `timeout`: (MP_INT | MP_FLOAT) в секундах
- `tier`: (MP_STR)

### .proc_sql_execute {: #proc_sql_execute }

```rust
fn proc_sql_execute(..) -> Result
```

Выполняет часть плана SQL-запроса на локальном инстансе.

Аргументы:

* *Сериализованный план запроса*

Возвращаемое значение:

- (MP_MAP `DqlResult`) при чтении данных
  <br>Поля:
    - `metadata` (MP_ARRAY), массив описаний столбцов таблицы в формате
      `MP_ARRAY [ MP_MAP { name = MP_STR, type = MP_STR }, ...]`
    - `rows` (MP_ARRAY), результат выполнения читающего запроса в формате
      `MP_ARRAY [ MP_ARRAY row, ...]`
- (MP_MAP `DmlResult`) при модификации данных
  <br>Поля:
    - `row_count` (MP_INT), количество измененных строк

Для более высокоуровневого RPC смотрите [`.proc_sql_dispatch`](#proc_sql_dispatch)

### .proc_update_instance {: #proc_update_instance }

```rust
fn proc_update_instance(instance_name, cluster_name, current_state, target_state, failure_domain, dont_retry, picodata_version)
```

Выполняется только на [raft-лидере](../overview/glossary.md#raft_leader), в
противном случае немедленно возвращает ошибку.

Обновляет информацию об указанном инстансе.

Эту хранимую процедуру вызывают инстансы Picodata, уже состоящие в кластере,
чтобы обновить [свое целевое состояние](../overview/glossary.md#state) при
перезапуске или в рамках [штатного выключения](../architecture/topology_management.md/#graceful_shutdown).

Лидер, получив такой запрос, проверяет консистентность параметров (например, что
новый домен отказа не противоречит конфигурации репликасета), и выполняет
[CaS](../overview/glossary.md#cas)-операцию по применению изменений к глобальной
системной таблице [`_pico_instance`](./system_tables.md#_pico_instance).

По умолчанию, если в [raft-журнал](../overview/glossary.md#raft) попала
конфликтующая CaS-операция, запрос повторяется.

Параметры:

- `instance_name`: (MP_STR)
- `cluster_name`: (MP_STR)
- `current_state`: (MP_MAP `State` | MP_NIL), текущее состояние инстанса
- `target_state`: (MP_STR `StateVariant` | MP_NIL), целевое состояние инстанса
- `failure_domain`: (MP_MAP | MP_NIL) [домен отказа](../overview/glossary.md#failure_domain)
- `dont_retry`: (MP_BOOL), не повторять CaS запрос в случае конфликта
- `picodata_version`: (MP_STR), версия инстанса

### .proc_wait_bucket_count {: #proc_wait_bucket_count }

```rust
fn proc_wait_bucket_count(term, applied, timeout, expected_bucket_count)
```

Обеспечивает перенос сегментов из исключаемого репликасета, пока их
число в репликасете не станет равным нулю.

Параметры:

- `term`: (MP_INT)
- `applied`: (MP_INT)
- `timeout`: (MP_INT | MP_FLOAT) в секундах
- `expected_bucket_count`: (MP_INT)

### .proc_wait_index {: #proc_wait_index }

```rust
fn proc_wait_index(target, timeout) -> RaftIndex
```

Ожидает применения (apply) указанного raft-индекса на текущем инстансе.
Функция возвращает текущий примененный индекс raft-журнала, который
может быть равен или превышать указанный.

Параметры:

- `target`: (MP_INT)
- `timeout`: (MP_INT | MP_FLOAT) в секундах

Возвращаемое значение:

* (MP_INT `RaftIndex`)

### .proc_wait_vclock {: #proc_wait_vclock }

```rust
fn proc_wait_vclock(target, timeout) -> Vclock
```

Ожидает момента, когда текущее значение [Vclock] достигнет заданного.
Возвращает текущее значение Vclock, которое может быть равно или превышать
указанное.

Параметры:

- `target`: (MP_MAP `Vclock`)
- `timeout`: (MP_FLOAT) в секундах

Возвращаемое значение:

* (MP_MAP `Vclock`)
