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

---
### .proc_version_info {: #proc_version_info }

```rust
fn proc_version_info()
```

Возвращает информацию о версиях Picodata и отдельных ее компонентах.

Возвращаемое значение:

- (MP_MAP `VersionInfo`)

    - `picodata_version`: (MP_STR) версия Picodata
      <!-- TODO ссылка на политику версионирования -->
    - `proc_api_version`: (MP_STR) версия Proc API согласно семантическому
      версионированию ([Semantic Versioning][semver]{:target="_blank"})

[semver]: https://semver.org/

---
### .proc_sql {: #proc_sql }

```rust
fn proc_sql(query, options)
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

---
### .proc_raft_info {: #proc_raft_info }

```rust
fn proc_raft_info()
```

Возвращает информацию о состоянии raft-узла на текущем инстансе

Возвращаемое значение:

- (MP_MAP `RaftInfo`)

    - `id`: (MP_INT) `raft_id` текущего узла
    - `term`: (MP_INT) текущий [терм](../overview/glossary.md#term)
    - `applied`: (MP_INT) текущий примененный индекс raft-журнала
    - `leader_id`: (MP_INT) `raft_id` лидера или `0` если в текущем
      терме его нет
    - `raft_state` (MP_STR)
      <br>возможные значения: `Follower`, `Candidate`, `Leader`, `PreCandidate`

---
### .proc_get_index {: #proc_get_index }

```rust
fn proc_get_index()
```

Возвращает текущий примененный (applied) индекс raft-журнала

Возвращаемое значение:

- (MP_INT)

---
### .proc_read_index {: #proc_read_index }

```rust
fn proc_read_index(timeout)
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

---
### .proc_wait_index {: #proc_wait_index }

```rust
fn proc_wait_index(target, timeout)
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

---
### .proc_get_vclock {: #proc_get_vclock }

```rust
fn proc_get_vclock()
```

Возвращает текущее значение [Vclock](../overview/glossary.md#vclock)

Возвращаемое значение:

- (MP_MAP `Vclock`)

---
### .proc_wait_vclock {: #proc_wait_vclock }

```
fn proc_wait_vclock(target, timeout)
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

### to be continued {: #TBC }
