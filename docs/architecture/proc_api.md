# Интерфейс Proc API

В данном документе описан интерфейс Proc API — основной интерфейс
взаимодействия с инстансом Picodata. Вызов функций происходит по
протоколу [iproto] через [коннектор][connectors] `net.box` или
любой другой.

[iproto]: https://www.tarantool.io/en/doc/2.11/dev_guide/internals/box_protocol/
[connectors]: https://www.tarantool.io/en/doc/2.11/book/connectors/

Proc API используется в следующих сценариях:

- Внешние системы могут вызывать функции через коннектор
- Инстансы взаимодействуют друг с другом под сервисной учетной записью
  `pico_service`
- Тестирование pytest использует для подключения клиент tarantool-python
- Поключение `picodata connect` использует вызов [.proc_sql](#proc_sql)
<!-- - Синтаксис вызова из Lua: `box.func[".proc_version_info"]:call()` -->
<!-- - Команда `picodata expel` использует вызов [.proc_expel_instance](#proc_expel_instance) -->

<!-- TODO Описать для всех функций
- В какой версии Proc API добавлена
- Йилдит ли
-->

## Детали реализации {: #implementation_details }

Функции Proc API представляют собой хранимые процедуры Tarantool
`box.func`. Аргументы функций описаны в системе типов
[msgpack](https://msgpack.org/).

Хранимые процедуры Tarantool не входят в модель управления доступом
Picodata. На них невозможно выдать или отозвать привилегии. Авторизация
запросов происходит по следующему принципу:

- Public функции доступны роли `public`, которая автоматически выдается
  всем новым пользователям. В этом случае авторизуется не вызов функции,
  а сам запрос.

- Привилегиями на вызов остальных функций обладают Администратор
  СУБД `admin` и сервисная учетная запись `pico_service`.

## Public API {: #public_api }

--------------------------------------------------------------------------------
### .proc_version_info {: #proc_version_info }

```rust
fn proc_version_info() -> VersionInfo
```

Возвращает информацию о версиях Picodata и отдельных ее компонентах.

Возвращаемое значение:

- (MP_MAP `VersionInfo`)

    - `picodata_version`: (MP_STR) версия Picodata
      <!-- TODO ссылка на политику версионирования -->
    - `proc_api_version`: (MP_STR) версия Proc API согласно семантическому
      версионированию ([Semantic Versioning][semver])

[semver]: https://semver.org/

--------------------------------------------------------------------------------
### .proc_sql {: #proc_sql }

```rust
fn proc_sql(query, options) -> Result
```

Выполняет распределенный SQL запрос.

Аргументы:

- `query`: (MP_STR) запрос SQL
- `options`: (optional MP_MAP) TODO

Возвращаемое значение:

- (MP_MAP `DqlResult`) при чтении данных
  <br>Поля:

    - `metadata` (MP_ARRAY), массив описаний столбцов таблицы в формате
      `MP_ARRAY [ MP_MAP { name = MP_STR, type = MP_STR }, ...]`
    - `rows` (MP_ARRAY), результат выполнения читающего запроса в формате
      `MP_ARRAY [ MP_ARRAY row, ...]`.

- (MP_MAP `DmlResult`) при модификации данных
  <br>Поля:

    - `row_count` (MP_INT), количество измененных строк

- (MP_NIL, MP_STR) в случае ошибки

См. также:

- [Инструкции и руководства — Работа с данными SQL](../tutorial/sql_examples.md)
- [Справочные материалы — Команды SQL](../reference/sql_queries.md)

## Service API {: #service_api }

--------------------------------------------------------------------------------
### .proc_raft_info {: #proc_raft_info }

```rust
fn proc_raft_info() -> RaftInfo
```

Возвращает информацию о состоянии raft-узла на текущем инстансе

Возвращаемое значение:

- (MP_MAP `RaftInfo`)

    - `id`: (MP_INT) `raft_id` текущего узла
    - `term`: (MP_INT) текущий [терм](../overview/glossary.md#term)
    - `applied`: (MP_INT) текущий примененный индекс raft-журнала
    - `leader_id`: (MP_INT) `raft_id` лидера или `0` если в текущем
      терме его нет
    - `state` (MP_STR)
      <br>возможные значения: `Follower`, `Candidate`, `Leader`, `PreCandidate`

--------------------------------------------------------------------------------
### .proc_instance_info {: #proc_instance_info }

```rust
fn proc_instance_info(instance_id) -> InstanceInfo
```

Возвращает информацию о запрашиваемом инстансе из кластера.

При вызове без параметров возвращает информацию о текущем инстансе.

Аргументы:

- `instance_id`: (optional MP_STR)

Возвращаемое значение:

- (MP_MAP `InstanceInfo`)
    - `raft_id`: (MP_UINT)
    - `advertise_address`: (MP_STR)
    - `instance_id`: (MP_STR)
    - `instance_uuid`: (MP_STR)
    - `replicaset_id`: (MP_STR)
    - `replicaset_uuid`: (MP_STR)
    - `cluster_id`: (MP_STR)
    - `current_grade`: (MP_MAP [`Grade`](../overview/glossary.md#grade)), текущее состояние инстанса
      <br>формат: `MP_MAP { variant = MP_STR, incarnation = MP_UINT}`
      <br>возможные значения `variant`: `Offline`, `Replicated`, `Online`, `Expelled`
    - `target_grade`: (MP_MAP [`Grade`](../overview/glossary.md#grade)), целевое состояние инстанса
    - `tier`: (MP_STR)

--------------------------------------------------------------------------------
### .proc_runtime_info {: #proc_runtime_info }

```rust
fn proc_runtime_info() -> RuntimeInfo
```

Возвращает служебную информацию.

Возвращаемое значение:

- (MP_MAP `RuntimeInfo`)
    - `raft`: (MP_MAP [RaftInfo](#proc_raft_info))
    - `version_info`: (MP_MAP [VersionInfo](#proc_version_info))
    - `internal`: (MP_MAP)
      <br>формат: `MP_MAP { main_loop_status = MP_STR,
      governor_loop_status = MP_STR}`
    - `http`: (optional MP_MAP `HttpInfo`)
      <br>формат: `MP_MAP { host = MP_STR, port = MP_UINT}`
      <br>поле отсутствует в ответе если инстанс запущен без параметра
      [picodata run --http-listen](../reference/cli.md#run_http_listen)

--------------------------------------------------------------------------------
### .proc_raft_promote {: #proc_raft_promote }

```rust
fn proc_raft_promote()
```

Завершает текущий [raft-терм](../overview/glossary.md#term) и объявляет выборы
нового [лидера](../overview/glossary.md#raft_leader). Предлагает себя
как кандидата в лидеры raft-группы. Если других кандидатов не обнаружится,
текущий инстанс с большой вероятностью станет новым лидером.

--------------------------------------------------------------------------------
### .proc_get_index {: #proc_get_index }

```rust
fn proc_get_index() -> RaftIndex
```

Возвращает текущий примененный (applied) индекс raft-журнала

Возвращаемое значение:

- (MP_INT)

--------------------------------------------------------------------------------
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

- (MP_INT)
- (MP_NIL, MP_STR) в случае ошибки

--------------------------------------------------------------------------------
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

- (MP_INT)
- (MP_NIL, MP_STR) в случае ошибки

--------------------------------------------------------------------------------
### .proc_get_vclock {: #proc_get_vclock }

```rust
fn proc_get_vclock() -> Vclock
```

Возвращает текущее значение [Vclock](../overview/glossary.md#vclock)

Возвращаемое значение:

- (MP_MAP `Vclock`)

------------------------------------------------------------------------------—-
### .proc_wait_vclock {: #proc_wait_vclock }

```
fn proc_wait_vclock(target, timeout) -> Vclock
```

Ожидает момента, когда текущее значение
[Vclock](../overview/glossary.md#vclock) достигнет заданного. Возвращает
текущее значение Vclock, которое может быть равно или превышать
указанное.

Параметры:

- `target`: (MP_MAP `Vclock`)
- `timeout`: (MP_FLOAT) в секундах

Возвращаемое значение:

- (MP_MAP `Vclock`)
- (MP_NIL, MP_STR) в случае ошибки


------------------------------------------------------------------------------—-
### .proc_apply_schema_change {: #proc_apply_schema_change }

```rust
fn proc_apply_schema_change(raft_term, raft_index, timeout) -> Result
```

Дожидается применения raft записи с заданным индексом и термом перед тем как
делать что-то ещё, чтобы синхронизовать состояние [глобальных системных
таблиц](./system_tables.md). Возвращает ошибку, если время не хватило.

Применяет текущие изменения глобальной схемы к локальному состоянию инстанса.

Этy хранимую процедуру вызывает только [governor](../overview/glossary.md#governor)
в рамках алгоритма отказоустойчивой смены кластерной схемы (подробнее он описан
[здесь](пока-нигде-не-описан)).

Текущие изменения схемы инстанс читает из системной таблицы
[_pico_property](./system_tables.md#_pico_property). В ней по ключам
соответственно `pending_schema_change` и `pending_schema_version` находятся
описание текущей [DDL](https://ru.wikipedia.org/wiki/Data_Definition_Language)
операции и версия глобальной схемы после применения этой операции.

В случае если изменение не применимо на текущем инстансе из-за конфликта
хранимых данных, в ответ посылается сообщение с причиной конфликта.

Параметры:

- `raft_term`: (MP_INT)
- `raft_index`: (MP_INT)
- `timeout`: (MP_INT | MP_FLOAT) в секундах

Возвращаемое значение:

- (MP_STR `Ok`)
- (MP_MAP, `{ "Abort": { "reason": MP_STR } }`) в случае ошибки


------------------------------------------------------------------------------—-
### .proc_replication {: #proc_replication }

```rust
fn proc_replication(is_master, replicaset_peers) -> Lsn
```

Обновляет конфигурацию топологии репликации текущего инстанса с остальными
инстансами из одного репликасета.

Этy хранимую процедуру вызывает только [governor](../overview/glossary.md#governor)
в рамках алгоритма автоматической смены топологии кластера (подробнее он описан
[здесь](пока-нигде-не-описан)).

Первоисточником принадлежности инстанса к тому или иному репликасету является
информация в системной таблице [_pico_instance](./system_tables.md#_pico_instance),
однако при вызове этой хранимой процедуры список адресов инстансев указывается
явно из-за ограничений на репликацию системных таблиц: так как данные системных
таблиц распространяются через механизм репликации, его нужно настроить перед
тем, как данные смогут реплицироваться.

Пикодата использует full-mesh тополгию репликации, при которой каждая реплика
поддерживает соединение с каждой другой репликой. При этом
[мастер-реплика](../overview/glossary.md#replicaset) в каждом репликасете только
одна. Признак того, что текущая реплика должна стать мастером передаётся
соответствующим параметром.

Подробнее про механизм репликации можно прочитать
[здесь](https://www.tarantool.io/ru/doc/latest/concepts/replication/).

В случае успешного завершения в ответ посылается [LSN](../overview/glossary.md#lsn)
текущего инстанса. Эта информация на данный момент используется только для
отладки.

Параметры:

- `is_master`: (MP_BOOL)
- `replicaset_peers`: (MP_ARRAY of MP_STR)

Возвращаемое значение:

- (MP_INT)


------------------------------------------------------------------------------—-
### .proc_replication_promote {: #proc_replication_promote }

```rust
fn proc_replication_promote(vclock, timeout)
```

Переводит текущий инстанс в состояние [мастер-реплики](../overview/glossary.md#replicaset)
предварительно синхронизуя его с остальными репликами.

Этy хранимую процедуру вызывает только [governor](../overview/glossary.md#governor)
в рамках алгоритма автоматической смены текущего мастера репликасета
(подробнее он описан [здесь](пока-нигде-не-описан)).

Получив такой запрос инстанс дожидается пока его локальный
[vclock](../overview/glossary.md#vclock) не догонит vclock предыдущей
мастер-реплики указанный в параметрах, или не закончится отведённое на это время.
Vclock предыдущего мастера governor получает после вызова на ней [#proc_replication_demote].

Синхронизация происходит в фоновом режиме засчёт механизма репликации.
Смотри [#proc_replication] о том, как в picodata настраивается репликация.

Параметры:

- `vclock`: (MP_MAP `Vclock`)
- `timeout`: (MP_INT | MP_FLOAT) в секундах


------------------------------------------------------------------------------—-
### .proc_replication_demote {: #proc_replication_demote }

```rust
fn proc_replication_demote() -> Vclock
```

Переводит текущий инстанс в состояние [резервной реплики](../overview/glossary.md#replicaset).

Этy хранимую процедуру вызывает только [governor](../overview/glossary.md#governor)
в рамках алгоритма автоматической смены текущего мастера репликасета
(подробнее он описан [здесь](пока-нигде-не-описан)).

Получив такой запрос инстанс сразу переходит в read-only режим и отправляет в
ответ текущее значение своего [vclock](../overview/glossary.md#vclock), которое
дальше используется для синхронизации новой мастер-реплики (см. [#proc_replication_promote]).

Смотри [#proc_replication] о том, как в picodata настраивается репликация.

Параметры:

- `vclock`: (MP_MAP ключ: MP_INT, значение: MP_INT)
- `timeout`: (MP_INT | MP_FLOAT) в секундах

Возвращаемое значение:

- (MP_MAP `Vclock`)


------------------------------------------------------------------------------—-
### .proc_sharding {: #proc_sharding }

```rust
fn proc_sharding(raft_term, raft_index, timeout, do_reconfigure)
```

Дожидается применения raft записи с заданным индексом и термом перед тем как
делать что-то ещё, чтобы синхронизовать состояние [глобальных системных
таблиц](./system_tables.md). Возвращает ошибку, если время не хватило.

Обновляет конфигурацию [шардирования данных](../overview/glossary.md#vshard)
между [репликасетами](../overview/glossary.md#replicaset).

Этy хранимую процедуру вызывает только [governor](../overview/glossary.md#governor)
в рамках алгоритма автоматической смены топологии кластера (подробнее он описан
[здесь](пока-нигде-не-описан)).

**ОСТОРОЖНО:** детали реализации и параметры запроса с большой вероятностью
скоро устареют.

Конфигурация распределения [бакетов](../overview/glossary.md#bucket) на данный
момент хранится в системной таблице [_pico_property](./system_tables.md#_pico_property)
по ключу `target_vshard_config`.

Параметр `do_reconfigure` указывает, нужно ли обновлять конфигурацию, в случае
если она уже применена на текущем инстансе, о чём говорит наличие луа переменной
`pico._vshard_is_configured`.

Параметры:

- `raft_term`: (MP_INT)
- `raft_index`: (MP_INT)
- `timeout`: (MP_INT | MP_FLOAT) в секундах
- `do_reconfigure` (MP_BOOL)


------------------------------------------------------------------------------—-
### .proc_sharding_bootstrap {: #proc_sharding_bootstrap }

```rust
fn proc_sharding_bootstrap(raft_term, raft_index, timeout)
```

Дожидается применения raft записи с заданным индексом и термом перед тем как
делать что-то ещё, чтобы синхронизовать состояние [глобальных системных
таблиц](./system_tables.md). Возвращает ошибку, если время не хватило.

Инициирует распределение [бакетов](../overview/glossary.md#bucket)
между [репликасетами](../overview/glossary.md#replicaset).

Этy хранимую процедуру вызывает только [governor](../overview/glossary.md#governor)
в рамках алгоритма автоматической смены топологии кластера (подробнее он описан
[здесь](пока-нигде-не-описан)).

За жизненный цикл кластера эта процедуры вызывается ровно один раз как
только в кластере появляется первый репликасет удовлетворяющий соответствующему
[фактору репликации](../overview/glossary.md#replication_factor).

Параметры:

- `raft_term`: (MP_INT)
- `raft_index`: (MP_INT)
- `timeout`: (MP_INT | MP_FLOAT) в секундах


------------------------------------------------------------------------------—-
### .proc_raft_snapshot_next_chunk {: #proc_raft_snapshot_next_chunk }

```rust
fn proc_raft_snapshot_next_chunk(raft_term, raft_index, snapshot_position)
```

Возвращает следующий отрезок данных [raft снапшота](../overview/glossary.md#snapshot).

Этy хранимую процедуру вызывают только инстансы с ролью [raft follower](../overview/glossary.md#node_states)
в рамках процесса [актуализации raft журнала](../overview/glossary.md#actualization).

Текущий инстанс проверяет наличие снапшота соответствующего состоянию raft
журнала на момент, когда актуальными были указанные [терм](../overview/glossary.md#term)
и [индекс](../overview/glossary.md#log_replication). При этом снапшот должен
существовать на момент вызова этой процедуры, так как он создаётся автоматически
когда raft лидер получает запрос на актуализацию raft журнала и обнаруживает,
что его журнал был [компактизирован](../overview/glossary.md#raft_log_compaction).

Снапшот (на момент написания этого текста) представляет из себя
последовательность кортежей [глобальных таблиц](../overview/glossary.md#table),
как системных так и пользовательских. В этой последовательности кортежи
сгруппированны по таблицам и упорядочены в соответствие с первичными ключами таблиц.
Последовательность таблиц в снапшоте детерминирована и совпадает между разными
версиями снапшотов.

Следующий отрезок снапшота определяется параметрами: идентификатор последней таблицы
кортежи которой были получены в предыдущем отрезке, и количество кортежей этой
таблицы полученных за все предыдущие отрезки снапшота.

Параметры:

- `raft_term`: (MP_INT)
- `raft_index`: (MP_INT)
- `snapshot_position`: (MP_MAP `SnapshotPosition`)
    - `table_id`: MP_INT
    - `tuple_offset`: MP_INT

Возвращаемое значение:

- (MP_MAP `SnapshotData`)
    - `schema_version`: MP_INT

    - `space_dumps`: MP_ARRAY:
        - MP_MAP:
            - `table_id`: MP_INT
            - `tuples`: MP_ARRAY of MP_ARRAY "сырые" кортежы таблицы

    - `next_position`: MP_NIL | MP_MAP `SnapshotPosition` (см. выше)


------------------------------------------------------------------------------—-
### .proc_cas {: #proc_cas }

```rust
fn proc_cas(cluster_id, predicate, op, as_user) -> (RaftIndex, RaftTerm)
```

Выполняется только на [raft-лидере](../overview/glossary.md#raft_leader), в
противном случае немедленно возвращает ошибку.

Исполняет [кластерную compare-and-swap](../overview/glossary.md#cas) операцию.

Проверяет предикат. Если проверка не выявляет конфликтов, то добавляет
новую запись в raft-журнал и возвращает её индекс (пока ещё не зафиксированный в
кластере). Операции применяется только для [глобальных таблиц](../overview/glossary.md#table)
и работает на всем кластере.

Предикат состоит из трёх частей:

- индекс записи в [raft журнале](../overview/glossary.md#log_replication);
- номера [терма](../overview/glossary.md#term);
- диапазона значений самого проверяемого параметра (ranges).

Параметры:

- `cluster_id`: (MP_STR)
- `predicate`: (MP_MAP `CasPredicate`)
- `op`: (MP_MAP `Op`)
- `as_user`: (MP_INT)

Возвращаемое значение:

- (MP_INT raft индекс)
- (MP_INT raft терм)


------------------------------------------------------------------------------—-
### .proc_raft_interact {: #proc_raft_interact }

```rust
fn proc_raft_interact(raft_messages)
```

Этy хранимую процедуру вызывают все инстансы Picodata для передачи внутренних
сообщений друг другу в рамках реализации алгоритма [raft](../overview/glossary.md#raft).

Параметры:

- `raft_messages`: (MP_ARRAY of MP_ARRAY)


------------------------------------------------------------------------------—-
### .proc_discover {: #proc_discover }

```rust
fn proc_discover(request, receiver)
```

Этy хранимую процедуру вызывают инстансы Picodata во время старта с пустым
состоянием ([bootstrap](../overview/glossary.md#bootstrap)) в рамках реализации
алгоритма [discovery](./discovery.md).

Параметры:

- `request`: (MP_MAP):
    - `tmp_id`: (MP_STR)
    - `peers`: (MP_ARRAY of MP_STR)
- `receiver`: (MP_STR) адрес по которому достигнут текущий инстанс

Возвращаемое значение:

- (MP_MAP)
    - `LeaderElection`: (MP_MAP) в случае если алгоритм продолжается
        - `tmp_id`: (MP_STR)
        - `peers`: (MP_ARRAY of MP_STR)

    - `Done`: (MP_MAP): в случае если результат уже известен
        - `leader_address`: (MP_STR): адрес лидера


------------------------------------------------------------------------------—-
### .proc_raft_join {: #proc_raft_join }

```rust
fn proc_raft_join(cluster_id, instance_id, replicaset_id, advertise_address, failure_domain, tier)
```

Выполняется только на [raft-лидере](../overview/glossary.md#raft_leader),
в противном случае немедленно возвращает ошибку.

Этy хранимую процедуру вызывают инстансы Picodata присоединяющиеся к кластеру,
то есть ещё не состоящие в [raft-группе](../overview/glossary.md#raft).

Смотри также [жизненный цикл инстанса](../architecture/instance_lifecycle.md#fn_start_join).

Параметры:

- `cluster_id`: (MP_STR),
- `instance_id`: (MP_STR | MP_NIL),
- `replicaset_id`: (MP_STR | MP_NIL) идентификатор [репликасета](../overview/glossary.md#replicaset),
- `advertise_address`: (MP_STR),
- `failure_domain`: (MP_MAP) [домен отказа](../overview/glossary.md#failure_domain),
- `tier`: (MP_STR) идентификатор [тира](../overview/glossary.md#tier),

Возвращаемое значение:

- `instance`: (MP_MAP): кортеж из системной таблицы [_pico_instance](./system_tables.md#_pico_instance),
                        соответствующий присоединяющемуся инстансу,

- `peer_addresses`: (MP_ARRAY): набор адресов некоторых инстансев кластера
    - (MP_MAP):
        - `raft_id`: (MP_INT),
        - `address`: (MP_STR),

- `box_replication`: (MP_ARRAY of MP_STR): адреса всех реплик в репликасете
                                           присоединяющегося инстанса


------------------------------------------------------------------------------—-
### .proc_update_instance {: #proc_update_instance }

```rust
fn proc_update_instance(instance_id, cluster_id, current_grade, target_grade, failure_domain, dont_retry)
```

Выполняется только на [raft-лидере](../overview/glossary.md#raft_leader), в
противном случае немедленно возвращает ошибку.

Обновляет информацию об указанном инстансе.

Этy хранимую процедуру вызывают инстансы Picodata уже состоящие в кластере
чтобы обновить [своё целевое состояние](../overview/glossary.md#grade) при
перезапуске или в рамках [штатного выключения](../architecture/topology_management.md/#graceful_shutdown).

Лидер получив такой запрос проверяет консистентность параметров (например, что
новый домен отказа не противоречит конфигурации репликасета), и выполняет
[CaS](../overview/glossary.md#cas) операцию по применению изменений к глобальной
системной таблице [_pico_instance](./system_tables.md#_pico_instance).

По умолчанию, если в [raft журнал](../overview/glossary.md#raft) попала
конфликтующая CaS операция, запрос повторяется.

Параметры:

- `instance_id`: (MP_STR),
- `cluster_id`: (MP_STR),
- `current_grade`: (MP_MAP `Grade`), текущее состояние инстанса
- `target_grade`: (MP_STR `GradeVariant`), целевое состояние инстанса
- `failure_domain`: (MP_MAP) [домен отказа](../overview/glossary.md#failure_domain),
- `dont_retry`: (MP_BOOL), не повторять CaS запрос в случае конфликта


------------------------------------------------------------------------------—-
### .proc_expel {: #proc_expel }

```rust
fn proc_expel(cluster_id, instance_id)
```

Выполняется только на [raft-лидере](../overview/glossary.md#raft_leader), в
противном случае немедленно возвращает ошибку.

Удаляет указанный инстанс из кластера. На его место в будущем может придти
инстанс с тем же `instance_id`, однако `raft_id` уходящего инстанса больше
никогда не будет использоваться.

Параметры:

- `cluster_id`: (MP_STR),
- `instance_id`: (MP_STR),


------------------------------------------------------------------------------—-
### .proc_expel_redirect {: #proc_expel_redirect }

```rust
fn proc_expel_redirect(cluster_id, instance_id)
```

Вызывает [proc_expel](#proc_expel) на текущем [raft лидере](../overview/glossary.md#raft_leader).

С большой вероятностью скоро будет удалена.

Параметры:

- `cluster_id`: (MP_STR),
- `instance_id`: (MP_STR),


------------------------------------------------------------------------------—-
### to be continued {: #TBC }
