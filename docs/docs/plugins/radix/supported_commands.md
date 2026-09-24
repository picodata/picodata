# Поддерживаемые команды {: #supported_commands }

<style>

.tag {
    line-height: 1em;
    margin-left: 0.5em;
    width: 87em;
    padding: 0.3em 0.7em;
    border-radius: 1em;
    font-family: monospace;
    font-size: 10pt;
    background-color: #d9ead3;
}

.admin {
    background-color: #000000;
    color: white;
}

.blocking {
    background-color: #a8cb48;
}

.dangerous {
    background-color: #d56000;
    color: white;
}

.connection {
    background-color: #b959f5;
    color: white;
}

.fast {
    background-color: #153cff;
    color: white;
}

.hash {
    background-color: #fff04a;
}

.keyspace {
    background-color: #84ffb3;
}

.list {
    background-color: #28904e;
    color: white;
}

.pico {
    background-color: #f13737;
    color: white;
}

.pubsub {
    background-color: #814444;
    color: white;
}

.read {
    background-color: #ffb5e2;
}

.set {
    background-color: #3af5ff;
}

.scripting {
    background-color: #ffe7bd;
}

.slow {
    background-color: #acacac;
    color: white;
}

.sortedset {
    background-color: #95aac7;
}

.string {
    background-color: #b0deff;
}

.transaction {
    background-color: #ffbf87;
}

.write {
    background-color: #6d106f;
    color: white;
}
</style>


## Управление доступом {: #cluster_acl }

Управление доступом в Radix реализовано с помощью списков контроля
доступа (access control lists, ACL). С их помощью администратор может
ограничить доступ пользователей к определённым ключам и их значениям.
Команды сгруппированы в категории, что позволяет назначать пользователям
сразу нужные наборы прав доступа.

Реализованные в Radix ACL-команды и их категории описаны ниже.

### Категории ACL {: #acl_categories }

В Radix используются следующие ACL-категории для команд:

- <span class="tag admin">admin</span> — административные команды. Пользователям, работающим с данными БД, они обычно не нужны
- <span class="tag blocking">blocking</span> — команды, блокирующие выполнение других команд
- <span class="tag dangerous">dangerous</span></span> — потенциально опасные команды (с точки зрения сохранности данных)
- <span class="tag connection">connection</span> — команды, имеющие отношение к управлению соединениями
- <span class="tag fast">fast</span> — команды быстрого выполнения, на скорость которых не влияет количество элементов, хранящихся в целевом ключе
- <span class="tag hash">hash</span> — все команды, имеющие отношение к работе с хэшем
- <span class="tag keyspace">keyspace</span> — команды, работающие со значениями ключей
- <span class="tag list">list</span> — команды для работы со списками
- <span class="tag pico">pico</span> — команды, специфичные для Picodata
- <span class="tag pubsub">pubsub</span> — все команды, имеющие отношение к pubsub
- <span class="tag read">read</span> — команды, читающие данные из ключей
- <span class="tag set">set</span> — команды, устанавливающие значения
- <span class="tag scripting">scripting</span> — команды для работы со скриптами
- <span class="tag slow">slow</span> — команды медленного выполнения
- <span class="tag sortedset">sortedset</span> — команды, имеющие отношение к сортированным множествам
- <span class="tag string">string</span> — команды, работающие со строковыми данными
- <span class="tag transaction">transaction</span> — команды для работы с транзакциями
- <span class="tag write">write</span> — команды, записывающие данные в ключи

### acl cat {: #acl_cat }

```sql
ACL CAT [category]
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag slow">slow</span>

При использовании без дополнительных аргументов, данная команда выводит
список доступных категорий. При указании в качестве аргумента конкретной
категории, команда выведет список команд, входящих в неё.

### acl dryrun {: #acl_dryrun }

```sql
ACL DRYRUN username command [arg [arg ...]]
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Симулирует выполнение команды пользователем. `DRYRUN` удобен для
проверки того, что у пользователя достаточно прав на выполнение
указанной команды.

Пример:

```sql
> ACL SETUSER VIRGINIA +SET ~*
"OK"
> ACL DRYRUN VIRGINIA SET foo bar
"OK"
> ACL DRYRUN VIRGINIA GET foo
"User VIRGINIA has no permissions to run the 'get' command
```

### acl getuser {: #acl_getuser }

```sql
ACL GETUSER username
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Возвращает все доступные ACL-данные для указанного пользователя (флаги,
хэши паролей, разрешённые команды и т.д.).

### acl list {: #acl_list }

```sql
ACL LIST
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Возвращает список всех пользователей (кроме служебных
`guest`/`admin`/`pico-service`) с сопоставленными им правилами. Если для
пользователя ACL не заданы, то используются правила по умолчанию
(запрещены все команды/ключи/каналы).

### acl load {: #acl_load }


```sql
ACL LOAD
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Загружает на сервер Radix набор ACL-правил, определённых в файле
`aclfile` (задаётся в разделе [authorization_mode](radix_settings.md#auth_mode)
конфигурации Radix). Существующие правила ACL переписываются (сначала
удаляются все заданные правила ACL, а потом загружаются из файла).

!!! warning "Внимание!"
    ACL-файлы из Redis необходимо предварительно отредактировать. См. [подробнее](configuration.md#migrate_acl).

### acl log {: #acl_log }

```sql
ACL LOG [count | RESET]
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Выводит журнал последних событий, связанных с доступом к данным, включая:

- ошибки авторизации (например, при [AUTH](#auth))
- ошибки выполнения команд из-за недостатка прав

Недавние записи находятся в начале списка. Параметр `count` позволяет
указать количество выводимых записей (по умолчанию `10`). Параметр
`RESET` позволяет очистить журнал.

### acl save {: #acl_save }

```sql
ACL SAVE
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Записывает текущие ACL-правила на сервере Radix в `aclfile`. Для работы
этой команды необходимо настроить расположение файла `aclfile` задаётся
в разделе [authorization_mode](radix_settings.md#auth_mode) конфигурации Radix).

!!! warning "Внимание!"
    Формат ACL-файла в Radix соответствует формату
    Redis. Перед загрузкой этого файла командой `ACL LOAD` его необходимо
    предварительно отредактировать. См. [подробнее](configuration.md#migrate_acl).

### acl setuser {: #acl_setuser }

```sql
ACL SETUSER username [rule [rule ...]]
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Задаёт набор правил ACL для существующего пользователя Picodata.
Если пользователю ранее были заданы правила ACL, то данная
команда добавит новые правила к существующему списку, не затирая его.

Пример:

```sql
ACL SETUSER virginia on allkeys +set
ACL SETUSER virginia +get
> ACL LIST
1) "user virginia on -@allkeys +set +get"
```

Список доступных правил для работы с данными:

- `~<pattern>` — добавляет указанный шаблон ключа в список шаблонов
  ключей, доступных пользователю. Это предоставляет права как на чтение,
  так и на запись для ключей, соответствующих данному шаблону. Можно
  добавить несколько шаблонов ключей для одного и того же пользователя.
  Пример: `~objects:*`
- `%R~<pattern>` — добавляет указанный шаблон ключа для чтения. Он
  работает аналогично обычному шаблону ключа, но предоставляет права
  только на чтение из ключей, соответствующих данному шаблону
- `%W~<pattern>` — добавляет указанный шаблон записи ключей. Он работает
  аналогично обычному шаблону ключей, но предоставляет разрешение на
  запись только в те ключи, которые соответствуют данному шаблону
- `%RW~<pattern>` — аналогичен `~<pattern>`
- `allkeys` — предоставляет доступ ко всем ключам, аналогичен `~*`
- `resetkeys` — очищает все шаблоны ключей, ранее назначенные пользователю
- `&<pattern>` — добавляет указанный шаблон в стиле _glob_ в список
  шаблонов каналов Pub/Sub, доступных пользователю ([подробнее](#keys))
- `allchannels` — предоставляет доступ ко всем каналам Pub/Sub, аналогичен `&*`
- `resetchannels` — очищает все шаблоны каналов, ранее назначенные пользователю
- `+<command>` — добавляет команду в список команд, которые
  пользователь может выполнять. Может использоваться с символом `|` для
  разрешения подкоманд (например, `+config|get`)
- `+@<category>` — добавляет категорию команд в список команд, которые
  пользователь может выполнять (например, `+@string`). Список категорий доступен
  по команде [`ACL CAT`](#acl_cat)
- `allcommands` — добавляет все команды, имеющиеся на сервере, включая
  будущие команды, загружаемые через модули, для выполнения этим
  пользователем. Аналогичен `+@all`
- `-<command>` — удаляет команду из списка команд, которые
  пользователь может выполнять. Может использоваться с символом `|` для
  блокирования подкоманд (например, `-config|set`)
- `-@<category>` — действует противоположно `+<category>`, то есть,
  удаляет все команды категории из списка команд, которые
  пользователь может выполнять.
- `nocommands` — удаляет все права пользователя, лишая его возможности
  что-либо выполнять. Аналогичен `-@all`
- `(<rule list>)` — создаёт новый селектор для сопоставления правил.
  Селекторы применяются после прав пользователя и в том порядке, в
  котором они перечислены. Если команда соответствует либо правам
  пользователя, либо любому селектору, она разрешена
- `clearselectors` — удаляет все селекторы, привязанные к пользователю
- `reset` — удаляет все  правила работы с данными у пользователя. Они
  устанавливаются в состояние «выключено»: без паролей, без возможности
  выполнять какие-либо команды и без доступа к каким-либо ключам

### acl users {: #acl_users }

```sql
ACL USERS
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Выводит список пользователей и их ACL.

### acl whoami {: #acl_whoami }

```sql
ACL WHOAMI
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag slow">slow</span>

Выводит список текущего (прошедшего авторизацию) пользователя.

## Управление кластером {: #cluster_management }

### cluster getkeysinslot {: #cluster_getkeysinslot }

```sql
CLUSTER GETKEYSINSLOT slot count
```
<span class="tag">поддерживается с версии 0.4.0</span>
<span class="tag slow">slow</span>

Возвращает набор ключей, которые, в соответствии со своими хэш-суммами,
относятся к указанному слоту. Второй аргумент ограничивает максимальное
количество возвращаемых ключей.

### cluster info {: #cluster_info }

```sql
CLUSTER INFO
```
<span class="tag">поддерживается с версии 0.12.0</span>
<span class="tag slow">slow</span>

Возвращает основной набор параметров кластера. Пример:

```sql
127.0.0.1:7301> cluster info
cluster_state:ok
cluster_slots_assigned:16384
cluster_slots_ok:16384
cluster_slots_pfail:0
cluster_slots_fail:0
cluster_known_nodes:8
cluster_size:4
cluster_current_epoch:2
cluster_my_epoch:2
cluster_stats_messages_ping_sent:0
cluster_stats_messages_pong_sent:0
cluster_stats_messages_sent:0
cluster_stats_messages_ping_received:0
cluster_stats_messages_pong_received:0
cluster_stats_messages_meet_received:0
cluster_stats_messages_fail_received:0
cluster_stats_messages_received:0
```

### cluster keyslot {: #cluster_keyslot }

```sql
CLUSTER KEYSLOT key
```
<span class="tag">поддерживается с версии 0.4.0</span>
<span class="tag slow">slow</span>

Позволяет узнать, к какому хэш-слоту относится указанный в команде ключ.

### cluster myid {: #cluster_myid }

```sql
CLUSTER MYID
```
<span class="tag">поддерживается с версии 0.4.0</span>
<span class="tag slow">slow</span>

Возвращает идентификатор текущего узла кластера (INSTANCE UUID).

### cluster myshardid {: #cluster_myshardid }

```sql
CLUSTER MYSHARDID
```
<span class="tag">поддерживается с версии 0.4.0</span>
<span class="tag slow">slow</span>

Возвращает идентификатор текущего репликасета, в который входит текущий
узел кластера (REPLICASET UUID).

### cluster nodes {: #cluster_nodes }

```sql
CLUSTER NODES
```
<span class="tag">поддерживается с версии 0.5.0</span>
<span class="tag slow">slow</span>

Возвращает информацию о текущем составе и конфигурации узлов кластера,
включая номера бакетов, относящихся к узлам.

### cluster replicas {: #cluster_replicas }

```sql
CLUSTER REPLICAS node-id
```
<span class="tag">поддерживается с версии 0.5.0</span>
<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Возвращает состав реплицированных узлов (т.е. состав репликасета)

### cluster shards {: #cluster_shards }

```sql
CLUSTER SHARDS
```
<span class="tag">поддерживается с версии 0.5.0</span>
<span class="tag slow">slow</span>

Возвращает подробную информацию о шардах кластера.

### cluster slots {: #cluster_slots }

```sql
CLUSTER SLOTS
```
<span class="tag">поддерживается с версии 0.5.0</span>
<span class="tag slow">slow</span>

Возвращает информацию о соответствии слотов инстансам кластера.

### echo

```sql
ECHO message
```
<span class="tag">поддерживается с версии 0.11.0</span>
<span class="tag connection">connection</span>
<span class="tag fast">fast</span>

Возвращает сообщение (`message`).

### ping

```sql
PING [message]
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag connection">connection</span>
<span class="tag fast">fast</span>

Возвращает `PONG`, если аргумент не указан, в противном случае
возвращает строкой аргумент, который пришёл. Эта команда полезна для:

- проверки того, живо ли ещё соединение
- проверки способности сервера обслуживать данные — ошибка возвращается,
  если это не так (например, при загрузке из постоянного хранилища или
  обращении к устаревшей реплике)
- измерения задержки

### quit

```sql
QUIT
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag connection">connection</span>
<span class="tag fast">fast</span>

Отправляет серверу сигнал на закрытие соединения. Сервер исполнит запрос
после того как будут отправлены все ответы на уже обработанные запросы.
Данная команда относится к числу устаревших и не рекомендуется к
использованию — более правильно разрывать соединение на стороне клиента
когда оно больше не требуется.

??? warning "Примечание"
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN radix 1.1.1 SET radix.redis_compatibility = '{ "enforce_one_slot_transactions": true, "push_result_includes_popped_items": true, "include_radix_section_in_info_by_default": true, "disable_scatter_gather": true, "enabled_deprecated_commands": ["quit" ] }';
    ```

### readonly {: #cluster_readonly }

```sql
READONLY
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag connection">connection</span>
<span class="tag fast">fast</span>

Переводит сессию в режим,  в котором получение данных производится не с
лидеров репликасетов, а с резервных реплик (при факторе репликации ≥ 2).

## Управление соединениями {: #connection_management }

### auth  {: #auth }

```sql
AUTH password
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag connection">connection</span>
<span class="tag fast">fast</span>

Выполняет аутентификацию пользователя по умолчанию. Имя пользователя по
умолчанию должно быть задано в конфигурации плагина в параметре
`default_user_name`.

```sql
auth username password
```

Выполняет аутентификацию выбранного пользователя.

### client getname  {: #client_getname }

```sql
CLIENT GETNAME
```
<span class="tag">поддерживается с версии 1.0.5</span>
<span class="tag connection">connection</span>
<span class="tag slow">slow</span>

Возвращает имя соединения, заданное командой [CLIENT
SETNAME](#client_setname). Если имя не было задано, будет возвращено
значение `nil`.

### client help  {: #client_help }

```sql
CLIENT HELP
```
<span class="tag">поддерживается с версии 1.0.5</span>
<span class="tag connection">connection</span>
<span class="tag slow">slow</span>

Возвращает список поддерживаемых команд группы `CLIENT ...` и их краткое
описание.

### client id  {: #client_id }

```sql
CLIENT ID
```
<span class="tag">поддерживается с версии 1.0.5</span>
<span class="tag connection">connection</span>
<span class="tag slow">slow</span>

Возвращает идентификатор текущего соединения. Это полезно в следующих случаях:

- получение одного и того же идентификатора в разных запросах
  гарантирует, что соединение не обрывалось между ними
- большее значение идентификатора гарантирует более позднее время
  создания соединения

### client info  {: #client_info }

```sql
CLIENT INFO
```
<span class="tag">поддерживается с версии 1.0.5</span>
<span class="tag connection">connection</span>
<span class="tag slow">slow</span>

Возвращает подробное описание текущего соединения.

### client kill  {: #client_kill }

```sql
CLIENT KILL <ip:port | <[ID client-id] | [TYPE <NORMAL | MASTER |
  SLAVE | REPLICA | PUBSUB>] | [USER username] | [ADDR ip:port] |
  [LADDR ip:port] | [SKIPME <YES | NO>] | [MAXAGE maxage]
  [[ID client-id] | [TYPE <NORMAL | MASTER | SLAVE | REPLICA |
  PUBSUB>] | [USER username] | [ADDR ip:port] | [LADDR ip:port] |
  [SKIPME <YES | NO>] | [MAXAGE maxage] ...]>>
```
<span class="tag">поддерживается с версии 1.0.5</span>
<span class="tag admin">admin</span>
<span class="tag connection">connection</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Закрывает указанное клиентское соединение. Команда поддерживает два
формата. Старый формат:

```sql
CLIENT KILL ip:port
```

Значение `ip:port` должно совпадать с одной из строк, которые возвращает
команда [CLIENT LIST](#client_list) в поле `addr`.

Новый формат:

```sql
CLIENT KILL <filter> <value> ... ... <filter> <value>
```

В новом формате можно закрывать клиентские соединения по разным атрибутам, а
не только по адресу. Можно указать несколько фильтров одновременно. В этом
случае они объединяются логическим `AND`.

```sql
CLIENT KILL ADDR 127.0.0.1:12345 TYPE PUBSUB
```

Этот запрос закроет только клиентское соединение типа `pubsub` с указанным
адресом. При использовании нового формата команда возвращает не `OK` или
ошибку, а количество закрытых клиентских соединений. Это число может быть
равно нулю.

Параметры и варианты использования:

- `ip:port` — закрывает клиентское соединение по указанному адресу. Это устаревший формат с одним аргументом.
- `ID client-id` — закрывает только клиентское соединение с указанным уникальным идентификатором.
- `TYPE NORMAL | MASTER | SLAVE | REPLICA | PUBSUB` — закрывает только клиентские соединения указанного типа.
- `USER username` — закрывает только клиентские соединения, аутентифицированные как указанный пользователь ACL.
- `ADDR ip:port` — закрывает только клиентские соединения с указанного адреса.
- `LADDR ip:port` — закрывает только клиентские соединения, подключенные к указанному локальному адресу сервера.
- `SKIPME YES | NO` — определяет, нужно ли пропускать клиента, который вызвал команду. Значение `YES` используется по умолчанию и пропускает вызывающего клиента. Значение `NO` разрешает закрыть и его соединение.
- `MAXAGE maxage` — закрывает только клиентские соединения, возраст которых превышает указанное значение в секундах.

### client list  {: #client_list }

```sql
CLIENT LIST [TYPE <NORMAL | MASTER | REPLICA | PUBSUB>]
  [ID client-id [client-id ...]]
```
<span class="tag">поддерживается с версии 1.0.5</span>
<span class="tag admin">admin</span>
<span class="tag connection">connection</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Выводит список клиентских соединений.

Параметры и варианты использования:

- `TYPE NORMAL | MASTER | REPLICA | PUBSUB` — выводит только клиентские соединения указанного типа.
- `ID client-id [client-id ...]` — выводит только клиентские соединения с указанными идентификаторами.

Подробности:

Результат содержит сведения и статистику по клиентским соединениям. В выводе
могут быть следующие поля:

- `id` — уникальный 64-битный идентификатор клиента
- `addr` — адрес и порт клиента
- `name` — имя, заданное клиентом командой [CLIENT SETNAME](#client_setname)
- `age` — общее время существования соединения в секундах
- `idle` — время простоя соединения в секундах
- `flags` — флаги клиента (см. ниже)
- `db` — идентификатор текущей базы данных
- `sub` — количество подписок на каналы
- `psub` — количество подписок по шаблону
- `ssub` — количество подписок на шардированные каналы
- `multi` — количество команд в контексте `MULTI`/`EXEC`
- `user` — имя пользователя, под которым клиент аутентифицирован
- `lib-name` — имя используемой клиентской библиотеки
- `lib-ver` — версия клиентской библиотеки

Флаги клиента могут быть скомбинированы из следующих значений:

- `b` — клиент ожидает в блокирующей операции
- `N` — специальные флаги не установлены
- `P` — клиент является подписчиком Pub/Sub
- `r` — клиент работает в режиме `readonly` при обращении к узлу кластера
- `S` — клиент является соединением реплики с этим экземпляром
- `t` — у клиента включено отслеживание ключей для клиентского кеширования

### client no-evict  {: #client_noevict }

```sql
CLIENT NO-EVICT <ON | OFF>
```
<span class="tag">поддерживается с версии 1.0.5</span>
<span class="tag admin">admin</span>
<span class="tag connection">connection</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Управляет режимом вытеснения для текущего соединения. Если режим включён и
вытеснение клиентов настроено, текущее соединение исключается из процесса
вытеснения, даже когда превышен заданный порог. Если режим выключен, клиент
снова попадает в набор соединений, которые могут быть вытеснены.

Параметры и варианты использования:

- `ON` — включает защиту текущего соединения от вытеснения.
- `OFF` — выключает защиту текущего соединения от вытеснения.

### client no-touch  {: #client_notouch }

```sql
CLIENT NO-TOUCH <ON | OFF>
```
<span class="tag">поддерживается с версии 1.0.5</span>
<span class="tag connection">connection</span>
<span class="tag slow">slow</span>

Управляет тем, будут ли команды текущего клиента изменять статистику LRU/LFU
для ключей, к которым обращаются.

Параметры и варианты использования:

- `ON` — отключает обновление времени последнего обращения к ключам и счетчика LFU для команд этого соединения. Исключение составляет команда `TOUCH`.
- `OFF` — включает обычное обновление статистики LRU/LFU для команд этого соединения.

### client pause  {: #client_pause }

```sql
CLIENT PAUSE timeout [WRITE | ALL]
```
<span class="tag">поддерживается с версии 1.0.5</span>
<span class="tag admin">admin</span>
<span class="tag connection">connection</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Приостанавливает обработку команд клиентов на указанное время в миллисекундах.

Параметры и варианты использования:

- `timeout` — время приостановки клиентов в миллисекундах.
- `ALL` — режим по умолчанию, при котором блокируются все команды клиентов.
- `WRITE` — режим, при котором клиенты блокируются только при попытке выполнить команду записи.

Подробности:

- Команда останавливает обработку ожидающих команд обычных клиентов и
  клиентов Pub/Sub для указанного режима. Взаимодействие с репликами
  продолжается в обычном режиме. Клиент формально считается
  приостановленным, когда пытается выполнить команду, поэтому для
  неактивных клиентов сервер не выполняет дополнительную работу.
- Команда как можно быстрее возвращает `OK` вызывающему клиенту, поэтому
  само выполнение `CLIENT PAUSE` не приостанавливается.
- Когда указанное время истекает, все клиенты разблокируются, и сервер
  начинает обрабатывать команды, накопленные в буферах запросов во время
  паузы.
- В режиме `WRITE` команды `EVAL` и `EVALSHA` блокируют клиента для всех
  скриптов. Команды `PUBLISH` и `PFCOUNT` также блокируют клиента. Для
  команды `WAIT` подтверждения задерживаются, поэтому она выглядит
  заблокированной.
- Команда полезна для управляемого переключения клиентов с одного
  экземпляра Redis на другой. Например, при обновлении экземпляра
  администратор может приостановить клиентов с помощью `CLIENT PAUSE`,
  подождать, пока реплики обработают последний поток репликации от
  мастера, повысить одну из реплик до мастера и перенастроить клиентов
  на новый мастер.
- Режим `WRITE` останавливает трафик репликации, может быть отменен
  командой [CLIENT UNPAUSE](#client_unpause) и позволяет перенастроить
  старый мастер без риска принять записи после failover.

### client reply  {: #client_reply }

```sql
CLIENT REPLY <ON | OFF | SKIP>
```
<span class="tag">поддерживается с версии 1.0.5</span>
<span class="tag connection">connection</span>
<span class="tag slow">slow</span>

Управляет тем, будет ли сервер отправлять ответы на команды клиента. Это
полезно, когда клиент отправляет команды в режиме fire-and-forget, выполняет
массовую загрузку данных или получает постоянный поток новых данных в сценариях
кеширования.

Параметры и варианты использования:

- `ON` — режим по умолчанию: сервер возвращает ответ на каждую команду.
- `OFF` — сервер не отправляет ответы на команды клиента.
- `SKIP` — сервер пропускает ответ только для команды, которая идет сразу после `CLIENT REPLY SKIP`.

### client setinfo  {: #client_setinfo }

```sql
CLIENT SETINFO <LIB-NAME libname | LIB-VER libver>
```
<span class="tag">поддерживается с версии 1.0.5</span>
<span class="tag connection">connection</span>
<span class="tag slow">slow</span>

Задает информационные атрибуты текущего соединения. Эти атрибуты отображаются
в выводе команд [CLIENT LIST](#client_list) и [CLIENT INFO](#client_info).
Клиентские библиотеки обычно отправляют эту команду в pipeline после
аутентификации на каждом соединении и игнорируют ошибки, потому что могут быть
подключены к серверу, который не поддерживает такие атрибуты.

Параметры и варианты использования:

- `LIB-NAME libname` — задает имя клиентской библиотеки для текущего соединения.
- `LIB-VER libver` — задает версию клиентской библиотеки для текущего соединения.

Подробности:

- Длина этих атрибутов не ограничена, но в них нельзя использовать
  пробелы, переводы строк и другие непечатаемые символы, которые
  нарушили бы формат ответа [CLIENT LIST](#client_list).
- Официальные клиентские библиотеки могут расширять `lib-name`
  пользовательским суффиксом, чтобы передавать дополнительную информацию
  о клиенте. Например, высокоуровневые библиотеки могут сообщать свою
  версию, а итоговое значение `lib-name` может выглядеть как
  `jedis(redis-om-spring_v1.0.0)`. Фигурные скобки используются как
  разделители пользовательского суффикса, поэтому их не стоит
  использовать внутри самого суффикса.
- Для пользовательских суффиксов сторонних библиотек рекомендуется
  формат `(?<custom-name>[ -~]+)[ -~]v(?<custom-version>[\d\.]+)`.
  Несколько суффиксов можно разделять символом `;`.
- Команда `RESET` не очищает эти атрибуты.

### client setname  {: #client_setname }

```sql
CLIENT SETNAME connection-name
```
<span class="tag">поддерживается с версии 1.0.5</span>
<span class="tag connection">connection</span>
<span class="tag slow">slow</span>

Задает имя для текущего соединения.

Параметры и варианты использования:

- `connection-name` — имя, которое нужно назначить текущему соединению.

Подробности:

- Назначенное имя отображается в выводе [CLIENT LIST](#client_list), чтобы можно было определить клиента, открывшего конкретное соединение.
- Например, если Redis используется для реализации очереди, производители и потребители сообщений могут задавать имя соединения в соответствии со своей ролью.
- Длина имени ограничена только обычным пределом строк Redis, но в имени соединения нельзя использовать пробелы, потому что это нарушит формат ответа [CLIENT LIST](#client_list).
- Чтобы полностью удалить имя соединения, задайте пустую строку. Пустая строка не является допустимым именем соединения и используется специально для удаления имени.
- Имя соединения можно проверить командой [CLIENT GETNAME](#client_getname). Новые соединения создаются без имени.
- Имена соединений помогают отлаживать утечки соединений, вызванные ошибками в приложении.

### client unblock  {: #client_unblock }

```sql
CLIENT UNBLOCK client-id [TIMEOUT | ERROR]
```
<span class="tag">поддерживается с версии 1.0.5</span>
<span class="tag admin">admin</span>
<span class="tag connection">connection</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Разблокирует клиент, который заблокирован блокирующей операцией, например
`BRPOP` или `WAIT`.

Параметры и варианты использования:

- `client-id` — идентификатор клиента, которого нужно разблокировать.
- `TIMEOUT` — поведение по умолчанию: разблокировать клиента так, как если бы
  истек таймаут заблокированной команды.
- `ERROR` — разблокировать клиента с ошибкой `-UNBLOCKED`.

Подробности:

- Используйте эту команду, когда нужно отслеживать много ключей ограниченным
  числом соединений. Если процессу-потребителю нужно начать отслеживать еще один
  поток (stream), можно не открывать новое соединение: разблокируйте одно из
  соединений в пуле, добавьте новый ключ и снова выполните блокирующую команду.
- Для такого сценария создайте дополнительное управляющее соединение, которое
  будет отправлять `CLIENT UNBLOCK` при необходимости. Перед запуском
  блокирующей операции на каждом отслеживаемом соединении выполните `CLIENT ID`,
  чтобы получить идентификатор этого соединения. Когда нужно добавить или
  удалить ключ, используйте управляющее соединение, чтобы отправить `CLIENT
  UNBLOCK` для соединения с блокирующей командой. Блокирующая команда вернется,
  после чего ее можно выполнить снова с обновленным набором ключей.

Пример:

```text
-- Соединение A (блокирующее соединение):
CLIENT ID
2934
BRPOP key1 key2 key3 0
-- клиент заблокирован

-- Нужно добавить новый ключ.

-- Соединение B (управляющее соединение):
CLIENT UNBLOCK 2934
1

-- Соединение A (блокирующее соединение):
-- BRPOP возвращает таймаут.
NULL
BRPOP key1 key2 key3 key4 0
-- клиент снова заблокирован
```

### client unpause  {: #client_unpause }

```sql
CLIENT UNPAUSE
```
<span class="tag">поддерживается с версии 1.0.5</span>
<span class="tag admin">admin</span>
<span class="tag connection">connection</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Возобновляет обработку команд для всех клиентов, которые были приостановлены
командой [CLIENT PAUSE](#client_pause).

### hello

```sql
HELLO [protover [AUTH username password] [SETNAME clientname]]
```
<span class="tag">поддерживается с версии 1.0.6</span>
<span class="tag connection">connection</span>
<span class="tag fast">fast</span>

Возвращает подробности о сервере и подключении. Параметр `protover`
позволяет задать версию протокола RESP (2 или 3). На данный момент Radix
поддерживает только версию 2. Параметр `AUTH` позволяет явно указать имя
и пароль пользователя, под которым производится подключение. Параметр
SETNAME позволяет задать имя клиента (аналогично команде [CLIENT
SETNAME](#client_setname)).

### reset

```sql
RESET
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag connection">connection</span>
<span class="tag fast">fast</span>

Сбрасывает соединение в состояние по умолчанию:

- откатывается текущая транзакция, если она была открыта,
- сбрасываются наблюдения за ключами, которые раньше были установлены командой WATCH,
- если были открыты курсоры командами SCAN/HSCAN, то они закрываются,
- сбрасывается авторизация, потребуется её пройти заново.

### select

```sql
SELECT index
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag connection">connection</span>
<span class="tag fast">fast</span>

Получение логической базы данных Redis с указанным нулевым числовым
индексом. Новые соединения всегда используют базу данных 0.

## Общие команды {: #general }

### dbsize

```sql
DBSIZE
```
<span class="tag">поддерживается с версии 0.5.0</span>
<span class="tag fast">fast</span>
<span class="tag keyspace">keyspace</span>
<span class="tag read">read</span>

Возвращает количество ключей в базе данных

### del

```sql
DEL key [key ...]
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag keyspace">keyspace</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Удаляет указанные ключи. Несуществующие ключи игнорируются.

### exists

```sql
EXISTS key [key ...]
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag fast">fast</span>
<span class="tag keyspace">keyspace</span>
<span class="tag read">read</span>

Проверяет, существует ли указанный ключ `key` и возвращает число совпадений.
Например, запрос `EXISTS somekey somekey` вернёт `2`.

### expire

```sql
EXPIRE key seconds [NX | XX | GT | LT]
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag fast">fast</span>
<span class="tag keyspace">keyspace</span>
<span class="tag write">write</span>

Устанавливает срок жизни (таймаут) для ключа `key` в секундах (TTL, time
to live). По истечении таймаута ключ будет автоматически удален. В
терминологии Redis ключ с установленным тайм-аутом часто называют
_волатильным_.

Тайм-аут будет сброшен только командами, которые удаляют или
перезаписывают содержимое ключа, включая DEL, SET и GET/SET. Это
означает, что все операции, которые концептуально изменяют значение,
хранящееся в ключе, не заменяя его новым, оставляют таймаут нетронутым.

### expireat

```sql
EXPIREAT key unix-time-seconds [NX | XX | GT | LT]
```
<span class="tag">поддерживается с версии 0.7.0</span>
<span class="tag fast">fast</span>
<span class="tag keyspace">keyspace</span>
<span class="tag write">write</span>

Устанавливает срок жизни (таймаут) для ключа `key` подобно
[EXPIRE](#expire), но вместо оставшегося числа секунд (TTL, time to
live) использует абсолютное время Unix timestamp — число секунд,
прошедших с 01.01.1970. Если максимальное число секунд превышено (т.е.
дата отсчёта находится ранее 01.01.1970), то ключ будет автоматически
удален.

Дополнительные параметры `EXPIREAT`:

- `NX` — установить срок жизни только если он не был ранее установлен
- `XX` — установить срок жизни только если ключ уже имеет ранее установленный срок
- `GT` — установить срок жизни только если он превышает ранее установленный срок
- `LT` — установить срок жизни только если он меньше ранее установленного срока

### expiretime

```sql
EXPIRETIME key
```
<span class="tag">поддерживается с версии 0.7.0</span>
<span class="tag fast">fast</span>
<span class="tag keyspace">keyspace</span>
<span class="tag read">read</span>

Возвращает срок жизни (таймаут) ключа `key` в секундах согласно формату
Unix timestamp.

### keys

```sql
KEYS pattern
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag dangerous">dangerous</span>
<span class="tag keyspace">keyspace</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>

Возвращает все ключи, соответствующие шаблону.

Поддерживаются шаблоны в стиле _glob_:

- `h?llo` соответствует hello, hallo и hxllo
- `h*llo` соответствует hllo и heeeello
- `h[ae]llo` соответствует hello и hallo, но не hillo
- `h[^e]llo` соответствует hallo, hbllo, ... но не hello
- `h[a-b]llo` соответствует hallo и hbllo

### persist

```sql
PERSIST key
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag fast">fast</span>
<span class="tag keyspace">keyspace</span>
<span class="tag write">write</span>

Удаляет существующий таймаут для ключа `key`, превращая его из непостоянного
(ключ с установленным сроком действия) в постоянный (ключ, срок действия
которого никогда не истечёт, поскольку таймаут для него не установлен).

### pexpire

```sql
PEXPIRE key milliseconds [NX | XX | GT | LT]
```
<span class="tag">поддерживается с версии 0.7.0</span>
<span class="tag fast">fast</span>
<span class="tag keyspace">keyspace</span>
<span class="tag write">write</span>

Устанавливает срок жизни (таймаут) для ключа `key` подобно
[EXPIRE](#expire), но в миллисекундах.

### pexpireat

```sql
PEXPIREAT key unix-time-milliseconds [NX | XX | GT | LT]
```
<span class="tag">поддерживается с версии 0.7.0</span>
<span class="tag fast">fast</span>
<span class="tag keyspace">keyspace</span>
<span class="tag write">write</span>

Устанавливает срок жизни (таймаут) для ключа `key` подобно
[EXPIREAT](#expireat), но в миллисекундах.

### pexpiretime

```sql
PEXPIRETIME key
```
<span class="tag">поддерживается с версии 0.7.0</span>
<span class="tag fast">fast</span>
<span class="tag keyspace">keyspace</span>
<span class="tag read">read</span>

Возвращает срок жизни (таймаут) ключа `key` подобно
[EXPIRETIME](#expiretime), но в миллисекундах.

### pttl

```sql
PTTL key
```
<span class="tag">поддерживается с версии 0.7.0</span>
<span class="tag fast">fast</span>
<span class="tag keyspace">keyspace</span>
<span class="tag read">read</span>

Возвращает оставшееся время жизни ключа `key` подобно [TTL](#ttl), но в
миллисекундах.

### scan

```sql
SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag keyspace">keyspace</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>

Команда `SCAN` используется для инкрементного итерационного просмотра
коллекции элементов в выбранной в данный момент базе данных Redis.

### ttl

```sql
TTL key
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag fast">fast</span>
<span class="tag keyspace">keyspace</span>
<span class="tag read">read</span>

Возвращает оставшееся время жизни ключа `key`, для которого установлен
таймаут. Эта возможность интроспекции позволяет клиенту Redis проверить,
сколько секунд данный ключ будет оставаться частью набора данных.

Команда возвращает `-2`, если ключ не существует.

Команда возвращает `-1`, если ключ существует, но не имеет связанного с
ним истечения срока действия.

### type

```sql
TYPE key
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag fast">fast</span>
<span class="tag keyspace">keyspace</span>
<span class="tag read">read</span>

Возвращает строковое представление типа значения, хранящегося по адресу
ключа `key`. Могут быть возвращены следующие типы:

- `string`
- `list`
- `set`
- `zset`
- `hash`
- `stream`

### unlink

```sql
UNLINK key [key ...]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag keyspace">keyspace</span>
<span class="tag write">write</span>

Выполняет асинхронное удаление ключей. Работает точно также, как и `DEL`,
за исключением того, что фактическое удаление данных происходит в фоне.

Можно использовать для повышения отзывчивости приложения.

## Хэш-команды {: #hash }

### hdel

```sql
HDEL key field [field ...]
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag fast">fast</span>
<span class="tag hash">hash</span>
<span class="tag write">write</span>

Удаляет указанные поля из хэша, хранящегося по адресу ключа `key`.
Указанные поля, которые не существуют в этом хэше, игнорируются. Удаляет
хэш, если в нем не осталось полей. Если `key` не существует, он
рассматривается как пустой хэш, и эта команда возвращает `0`.

### hexists

```sql
HEXISTS key field
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag fast">fast</span>
<span class="tag hash">hash</span>
<span class="tag read">read</span>

Возвращает, является ли поле `field` существующим полем в хэше, хранящемся по
адресу ключа `key`.

### hget

```sql
HGET key field
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag fast">fast</span>
<span class="tag hash">hash</span>
<span class="tag read">read</span>

Возвращает значение, связанное с полем `field` в хэше, хранящемся по
адресу ключа `key`.

### hgetall

```sql
HGETALL key
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag hash">hash</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>

Возвращает все поля и значения хэша, хранящегося по адресу ключа `key`. В
возвращаемом значении за именем каждого поля следует его значение,
поэтому длина ответа будет в два раза больше размера хэша.

### hincrby

```sql
HINCRBY key field increment
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag fast">fast</span>
<span class="tag hash">hash</span>
<span class="tag write">write</span>

Увеличивает число, хранящееся в поле `field`, в хэше, хранящемся в ключе
`key`, на инкремент. Если ключ не существует, создаётся новый ключ,
содержащий хэш. Если поле не существует, то перед выполнением операции
его значение устанавливается в `0`.

Диапазон значений, поддерживаемых `HINCRBY`, ограничен 64-битными
знаковыми целыми числами.

### hkeys

```sql
HKEYS key
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag hash">hash</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>

Возвращает все имена полей в хэше, хранящемся по адресу ключа `key`.

### hlen

```sql
HLEN key
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag fast">fast</span>
<span class="tag hash">hash</span>
<span class="tag read">read</span>

Возвращает количество полей, содержащихся в хэше, хранящемся по адресу
ключа `key`.

### hmget

```sql
HMGET key field [field ...]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag hash">hash</span>
<span class="tag read">read</span>

Возвращает значения указанных полей из хэша.

### hmset

```sql
HMSET key field value [field value ...]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag hash">hash</span>
<span class="tag write">write</span>

Выставляет значения указанным полям для заданного хэша.

??? warning "Примечание"
    Вместо этой команды необходимо использовать команду `HSET`
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN radix 1.1.1 SET radix.redis_compatibility = '{ "enforce_one_slot_transactions": true, "push_result_includes_popped_items": true, "include_radix_section_in_info_by_default": true, "disable_scatter_gather": true, "enabled_deprecated_commands": ["hmset" ] }';
    ```

### hscan

```sql
HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag hash">hash</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>

Работает подобно [SCAN](#scan), но с некоторым отличием: `HSCAN`
выполняет итерацию полей типа Hash и связанных с ними значений.

### hset

```sql
HSET key field value [field value ...]
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag fast">fast</span>
<span class="tag hash">hash</span>
<span class="tag write">write</span>

Устанавливает указанные поля в соответствующие им значения в хэше,
хранящемся по адресу ключа `key`.

Эта команда перезаписывает значения указанных полей, которые существуют
в хэше. Если ключ не существует, создаётся новый ключ, содержащий хэш.

### hvals

```sql
HVALS key
```
<span class="tag">поддерживается с версии 0.7.0</span>
<span class="tag hash">hash</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>

Возвращает значения всех полей в хэше, хранящиеся по адресу ключа `key`.

## Команды для множеств {: #set_commands }

### sadd

```sql
SADD key member [member ...]
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag fast">fast</span>
<span class="tag set">set</span>
<span class="tag write">write</span>

Добавляет указанные элементы (`member`) к множеству, хранящемуся по
ключу `key`. Если такие элементы уже есть, они будут проигнорированы.
Если указанного ключа `key` нет, он будет создан, а элементы — добавлены
в новое множество. Если ключ `key` существует, но хранящееся в нем
значение не является множеством, команда вернёт ошибку.

### scard

```sql
SCARD key
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag fast">fast</span>
<span class="tag read">read</span>
<span class="tag set">set</span>

Возвращает мощность множества (количество элементов), хранящегося по
ключу `key`.

### sdiff

```sql
SDIFF key [key ...]
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag read">read</span>
<span class="tag set">set</span>
<span class="tag slow">slow</span>

Работает аналогично [SDIFFSTORE](#sdiffstore), но вместо записи
результирующего множества выводит его клиенту.

### sdiffstore

```sql
SDIFFSTORE destination key [key ...]
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag set">set</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Вычисляет разницу между первым и последующими множествами (хранящимися в
соответствующих ключах `key`) и записывает его в `destination`. Команда
выводит количество элементов в результирующем множестве. Несуществующий
ключ обрабатывается как ключ, содержащий пустое множество. Если целевое
множество в `destination` уже существует, оно будет перезаписано.

Примеры:

```sql
127.0.0.1:7379> SADD key1 "a"
(integer) 1
127.0.0.1:7379> SADD key1 "b"
(integer) 1
127.0.0.1:7379> SADD key1 "c"
(integer) 1
127.0.0.1:7379> SADD key2 "c"
(integer) 1
127.0.0.1:7379> SADD key2 "d"
(integer) 1
127.0.0.1:7379> SADD key2 "e"
(integer) 1
127.0.0.1:7379> SDIFFSTORE key key1 key2
(integer) 2
127.0.0.1:7379> SMEMBERS key
1) "a"
2) "b"
127.0.0.1:7379>
```

### sinter

```sql
SINTER key [key ...]
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag read">read</span>
<span class="tag set">set</span>
<span class="tag slow">slow</span>

Работает аналогично [SINTERSTORE](#sinterstore), но вместо записи
результирующего множества выводит его клиенту.

### sintercard

```sql
SINTERCARD numkeys key [key ...] [LIMIT limit]
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag read">read</span>
<span class="tag set">set</span>
<span class="tag slow">slow</span>

Работает аналогично [SINTER](#sinter), но выводит клиенту не само
множество, а только его мощность. Если указан несуществующий ключ `key`,
то он будет обработан как пустое множество. Если из нескольких указанных
ключей хотя бы один будет содержать пустое множество, то и
результирующее пересечение также будет пустым.

Дополнительный параметр `LIMIT` позволяет ограничить показатель мощности
явно заданным числом. По умолчанию, ограничение не используется (`LIMIT`
равен 0).

### sinterstore

```sql
SINTERSTORE destination key [key ...]
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag set">set</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Вычисляет пересечение элементов из двух или более множеств,
хранящихся по указанным ключам (`key`) в виде нового
множества и записывает его в `destination`. Команда
выводит количество элементов в результирующем множестве.

Пример:

```sql
127.0.0.1:7379> SADD key1 "a"
(integer) 1
127.0.0.1:7379> SADD key1 "b"
(integer) 1
127.0.0.1:7379> SADD key1 "c"
(integer) 1
127.0.0.1:7379> SADD key2 "c"
(integer) 1
127.0.0.1:7379> SADD key2 "d"
(integer) 1
127.0.0.1:7379> SADD key2 "e"
(integer) 1
127.0.0.1:7379> SINTERSTORE key key1 key2
(integer) 1
127.0.0.1:7379> SMEMBERS key
1) "c"
127.0.0.1:7379>
```

### sismember

```sql
SISMEMBER key member
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag fast">fast</span>
<span class="tag read">read</span>
<span class="tag set">set</span>

Возвращает признак присутствия элемента в указанном множестве. В выводе
будет `1` или `0`, соответственно.

### smembers

```sql
SMEMBERS key
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag read">read</span>
<span class="tag set">set</span>
<span class="tag slow">slow</span>

Возвращает список всех элементов, хранящихся в указанном множестве.

### smove

```sql
SMOVE source destination member
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag fast">fast</span>
<span class="tag set">set</span>
<span class="tag write">write</span>

Перемещает элемент из исходного множества (`source`) в целевое
(`destination`). Операция является атомарной. В любой момент времени
элемент будет отображаться как элемент источника или назначения для
других клиентов. Если исходное множество не существует или не содержит
указанный элемент, операция не выполняется и возвращается значение 0. В
противном случае элемент удаляется из исходного множества и добавляется
в целевое. Если указанный элемент уже существует в целевом множестве, он
удаляется из исходного множества.

### spop

```sql
SPOP key [count]
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag fast">fast</span>
<span class="tag set">set</span>
<span class="tag write">write</span>

Извлекает один или несколько элементов (согласно числу, указанному в
`count`), хранящихся в множестве по указанному ключу. Если `count` не
указан, то по умолчанию команда извлечёт один элемент.

### srandmember

```sql
SRANDMEMBER key [count]
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag read">read</span>
<span class="tag set">set</span>
<span class="tag slow">slow</span>

Возвращает случайный элемент из множества, хранящегося по ключу `key`.
Дополнительный параметр `count` позволяет указать количество выводимых
элементов.
Если `count` положителен, команда возвращает массив различных элементов.
Длина массива равна `count` или мощности множества ([SCARD](#scard)), в
зависимости от того, какое из этих значений меньше.
При вызове с отрицательным значением `count` поведение меняется, и
команда может возвращать один и тот же элемент несколько раз. В этом
случае количество возвращаемых элементов равно абсолютному значению
указанного `count`.

### srem

```sql
SREM key member [member ...]
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag fast">fast</span>
<span class="tag set">set</span>
<span class="tag write">write</span>

Удаляет указанные элементы из множества, хранящегося по ключу `key`.
Если указанный элемент отсутствует в множестве, то такой элемент
игнорируется. Если ключ `key` существует, но хранящееся в нем значение
не является множеством, команда вернёт ошибку.

### sscan

```sql
SSCAN key cursor [MATCH pattern] [COUNT count]
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag read">read</span>
<span class="tag set">set</span>
<span class="tag slow">slow</span>

См. [SCAN](#scan)

### sunion

```sql
SUNION key [key ...]
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag read">read</span>
<span class="tag set">set</span>
<span class="tag slow">slow</span>

Работает аналогично [SUNIONSTORE](#sunionstore), но вместо записи
результирующего множества выводит его клиенту.

### sunionstore

```sql
SUNIONSTORE destination key [key ...]
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag set">set</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Вычисляет пересечение элементов из двух или более множеств,
хранящихся по указанным ключам (`key`) в виде нового
множества и записывает его в `destination`. Команда
выводит количество элементов в результирующем множестве. Если множество
в `destination` уже существует, оно будет перезаписано.

Пример:

```sql
127.0.0.1:7379> SADD key1 "a"
(integer) 1
127.0.0.1:7379> SADD key1 "b"
(integer) 1
127.0.0.1:7379> SADD key1 "c"
(integer) 1
127.0.0.1:7379> SADD key2 "c"
(integer) 1
127.0.0.1:7379> SADD key2 "d"
(integer) 1
127.0.0.1:7379> SADD key2 "e"
(integer) 1
127.0.0.1:7379> SUNIONSTORE key key1 key2
(integer) 5
127.0.0.1:7379> SMEMBERS key
1) "a"
2) "b"
3) "c"
4) "d"
5) "e"
```

## Команды для сортированных множеств {: #ordered_sets }

### bzmpop

```sql
BZMPOP timeout numkeys key [key ...] <MIN | MAX> [COUNT count]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag blocking">blocking</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Вариант команды [ZMPOP](#zmpop) с блокировкой. Ведёт себя аналогично
[ZMPOP](#zmpop) в ситуации:

- когда хотя бы в одном из сортированных множеств, хранящихся по
  указанным ключам (`key`), есть элементы
- при использовании внутри блока [MULTI](#multi) или [EXEC](#exec).

Если все сортированные множества пусты, то Radix заблокирует соединение до
тех пор, пока другой клиент не добавит значение хотя бы к одному множеству в
указанных ключах `key`, либо не истечёт время таймаута `timeout`. Если
таймаут установить в `0`, то блокировка будет бесконечной.

### bzpopmax

```sql
BZPOPMAX key [key ...] timeout
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag blocking">blocking</span>
<span class="tag fast">fast</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Вариант команды [ZPOPMAX](#zpopmax) с блокировкой. Ведёт себя так же как
`ZPOPMAX`, но при отсутствии элементов во всех сортированных множествах,
хранящихся по ключам `key`, блокирует соединение. В остальных случаях
возвращает один элемент с наивысшей оценкой из первого непустого ключа
из переданных в команду. Блокировка истекает после таймаута `timeout`.
Если таймаут установить в `0`, то блокировка будет бесконечной.

### bzpopmin

```sql
BZPOPMIN key [key ...] timeout
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag blocking">blocking</span>
<span class="tag fast">fast</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Вариант команды [ZPOPMIN](#zpopmin) с блокировкой. Ведёт себя так же как
`ZPOPMIN`, но при отсутствии элементов во всех сортированных множествах,
хранящихся по ключам `key`, блокирует соединение. Возвращает один
элемент с наименьшей оценкой из первого непустого ключа из переданных в
команду. Блокировка истекает после таймаута `timeout`. Если таймаут
установить в `0`, то блокировка будет бесконечной.

### zadd

```sql
ZADD key [NX | XX] [GT | LT] [CH] [INCR] score member [score member ...]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Добавляет указанные элементы (`member`) и соответствующие им оценки
(`score`) к сортированному множеству, хранящемуся по ключу `key`. Если
указанного ключа `key` нет, он будет создан, а элементы — добавлены в
новое сортированное множество. Если ключ `key` существует, но в нем нет
сортированного множества, команда вернёт ошибку. Если указанный элемент
уже есть в сортированном множестве, то он будет вставлен повторно на ту
же позицию с обновленной оценкой.

Дополнительные параметры:

- `NX` — только добавить новые элементы (существующие не обновлять)
- `XX` — только обновить существующие элементы (новые не добавлять)
- `GT` — обновить существующие элементы только если их новые оценки
  выше, а также добавить новые элементы (если указаны)
- `LT` — обновить существующие элементы только если их новые оценки
  ниже, а также добавить новые элементы (если указаны)
- `CH` — учитывать в выводе не только новые элементы, но и измененные. В
  таком случае команда вернёт число, отражающее сумму новых элементов и
  тех существующих элементов, для которых была обновлена оценка. Если
  указать в команде существующие элементы с их текущей оценкой, то они
  не будут учтены.
- `INCR` — заставляет `ZADD` вести себя как [ZINCRBY](#zincrby). В этом
  режиме можно указать только одну пару оценка/элемент.

??? warning "Примечание"
    Параметры `GT`,`LT` и `NX` можно использовать
    только по отдельности, не сочетая друг с другом.

### zcard

```sql
ZCARD key
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag read">read</span>
<span class="tag sortedset">sortedset</span>

Возвращает мощность множества (количество элементов в сортированном
множестве), хранящегося по ключу `key`.

### zcount

```sql
ZCOUNT key min max
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag read">read</span>
<span class="tag sortedset">sortedset</span>

Возвращает мощность множества (количество элементов в сортированном
множестве), хранящегося по ключу `key`, с оценкой в диапазоне от `min` до
`max`. Поведение аргументов `min` и `max` такое же, как в
[ZREMRANGEBYSCORE](#zremrangebyscore).

### zdiff

```sql
ZDIFF numkeys key [key ...] [WITHSCORES]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>

Работает аналогично [ZDIFFSTORE](#zdiffstore), но вместо записи
результирующего сортированного множества выводит его клиенту.

### zdiffstore

```sql
ZDIFFSTORE destination numkeys key [key ...]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Вычисляет разницу между первым и последующими сортированными множествами
(хранящимися в соответствующих ключах `key`) и записывает его в
`destination`. Перед списком ключей необходимо указать их количество
(`numkeys`). Команда выводит количество элементов в результирующем
множестве. Несуществующий ключ обрабатывается как ключ, содержащий пустое
сортированное множество. Если целевое множество в `destination` уже
существует, оно будет перезаписано.

Примеры:

```sql
127.0.0.1:7379> ZADD zset1 1 "one"
(integer) 1
127.0.0.1:7379> ZADD zset1 2 "two"
(integer) 1
127.0.0.1:7379> ZADD zset1 3 "three"
(integer) 1
127.0.0.1:7379> ZADD zset2 1 "one"
(integer) 1
127.0.0.1:7379> ZADD zset2 2 "two"
(integer) 1
127.0.0.1:7379> ZDIFFSTORE out 2 zset1 zset2
(integer) 1
127.0.0.1:7379> ZRANGE out 0 -1 WITHSCORES
1) "three"
2) "3"
```

### zincrby

```sql
ZINCRBY key increment member
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Увеличивает оценку элемента `member` в сортированном множестве,
хранящемся по ключу `key`, на величину `increment`. Если указанный
элемент в множестве отсутствует, то он будет создан с оценкой, равной
`increment`. Если указанного ключа `key` нет, он будет создан, и элемент
добавлен в новое сортированное множество. Если ключ `key` существует, но
в нем нет сортированного множества, команда вернёт ошибку. Величина
`increment` может быть отрицательной (в таком случае оценка будет
понижена).

### zinter

```sql
ZINTER numkeys key [key ...] [WEIGHTS weight [weight ...]]
  [AGGREGATE <SUM | MIN | MAX>] [WITHSCORES]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>

Работает аналогично [ZINTERSTORE](#zinterstore), но вместо записи
результирующего сортированного множества выводит его клиенту.

### zintercard

```sql
ZINTERCARD numkeys key [key ...] [LIMIT limit]
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>

Работает аналогично [ZINTER](#zinter), но вместо результирующего
сортированного множества выводит только его мощность.

### zinterstore

```sql
ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight
  [weight ...]] [AGGREGATE <SUM | MIN | MAX>]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Вычисляет пересечение элементов из двух или более сортированных множеств,
хранящихся по указанным ключам (`key`) в виде нового сортированного
множества и записывает его в `destination`. Перед
списком ключей необходимо указать их количество (`numkeys`). Команда
выводит количество элементов в результирующем множестве.

По умолчанию, результирующая оценка элемента является суммой оценок
этого элемента во всех исходных множествах, где он присутствует.

Дополнительные параметры `WEIGHTS` и `AGGREGATE` ведут себя так же, как
в команде [ZUNIONSTORE](#zunionstore).

Пример:

```sql
127.0.0.1:7379> ZADD zset1 1 "one"
(integer) 1
127.0.0.1:7379> ZADD zset1 2 "two"
(integer) 1
127.0.0.1:7379> ZADD zset2 1 "one"
(integer) 1
127.0.0.1:7379> ZADD zset2 2 "two"
(integer) 1
127.0.0.1:7379> ZADD zset2 3 "three"
(integer) 1
127.0.0.1:7379> ZINTERSTORE out 2 zset1 zset2 WEIGHTS 2 3
(integer) 2
127.0.0.1:7379> ZRANGE out 0 -1 WITHSCORES
1) "one"
2) "5"
3) "two"
4) "10"
```

### zlexcount

```sql
ZLEXCOUNT key min max
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag read">read</span>
<span class="tag sortedset">sortedset</span>

Возвращает количество всех элементов из сортированного множества,
хранящегося по ключу `key`, в лексикографическом диапазоне от `min` до
`max`. Аргументы `min` и `max` применяются так же, как в команде
[ZRANGEBYLEX](#zrangebylex).

### zmpop

```sql
ZMPOP numkeys key [key ...] <MIN | MAX> [COUNT count]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Извлекает один или несколько элементов, составляющих пары
оценка/элемент, из первого непустого сортированного множества на основании
указанного набора ключей `key`.

Модификатор `MIN` позволяет выводить элементы с наименьшей оценкой,
`MAX` — с наивысшей. Параметр `COUNT` ограничивает число элементов (по
умолчанию — 1).

### zmscore

```sql
ZMSCORE key member [member ...]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag read">read</span>
<span class="tag sortedset">sortedset</span>

Возвращает оценки указанных элементов (`member`) сортированных множеств,
хранящихся по указанному ключу `key`. Если элемент отсутствует в
множестве, то для него будет выведена оценка `nil`.

### zpopmax

```sql
ZPOPMAX key [count]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Извлекает указанное в `count` число элементов с наивысшей оценкой из
сортированного множества, хранящегося по указанному ключу. По умолчанию
`count` равен 1. Если указанный `count` больше мощности множества, то
ошибки не будет. Команда выводит элементы с сортировкой по убыванию
оценки.

### zpopmin

```sql
ZPOPMIN key [count]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Извлекает указанное в `count` число элементов с наименьшей оценкой из
сортированного множества, хранящегося по указанному ключу. По умолчанию
`count` равен 1. Если указанный `count` больше мощности множества, то
ошибки не будет. Команда выводит элементы с сортировкой по возрастанию
оценки.

### zrandmember

```sql
ZRANDMEMBER key [count [WITHSCORES]]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>

Возвращает случайный элемент из сортированного множества, хранящегося по
ключу `key`.

Дополнительный параметр `count` позволяет указать количество выводимых
элементов. Если добавить `WITHSCORES`, то в вывод будут включены оценки
элементов. Если указанный `count` положителен, то будет выведен массив
элементов размером либо с `count`, либо мощность множества (смотря какое
значение ниже). Если указанный `count` отрицателен, то поведение команды
меняется: один и тот же элемент может быть возвращен несколько раз.
Размер итогового массива при этом будет равняться абсолютному значению
`count`.

### zrange

```sql
ZRANGE key start stop [BYSCORE | BYLEX] [REV] [LIMIT offset count]
  [WITHSCORES]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>

Возвращает указанный набор элементов из сортированного множества,
хранящегося по ключу `key`. Команда может выполнять разные типы запросов,
выводя наборы элементов: по индексу, по оценке, в лексикографическом
порядке.

Следующие параметры меняют поведение команды:

- `BYSCORE` — сортировка элементов по возрастанию их оценок. Элементы с
  одинаковыми оценками сортируются лексикографически
- `BYLEX` — лексикографическая сортировка элементов с одинаковой оценкой
- `REV` — оценка элементов множества в обратном порядке

Дополнительный параметр `LIMIT` позволяет ограничить результат явно
заданными рамками (`offset` — смещение, `count` — число элементов).
Отрицательное значение `count` выведет все элементы после `offset`.
Дополнительный параметр `WITHSCORES` позволяет включить в вывод оценки
элементов.

**Диапазоны индексов**

По умолчанию команда выполняет запрос на основе индексов. Отрезок от
`start` до `stop` позволяет ограничить вывод элементов и обрабатывается
включительно (`0` соответствует первому элементу). Например, команда
`ZRANGE myzset 0 1 ` выведет только первый и второй элемент из множества
в ключе `myzset`. Отрицательные значения обозначают позицию относительно
конца множества (`-1` — последний элемент). Индексы, выходящие за
пределы диапазона, не вызывают ошибку. Если `start` больше конечного
индекса сортированного множества или `stop`, возвращается пустой список.
Если `stop` больше конечного индекса сортированного множества, команда
будет использовать последний элемент сортированного множества.

**Диапазоны оценок**

Если указан параметр `BYSCORE`, команда ведёт себя как
[ZRANGEBYSCORE](#zrangebyscore) и возвращает диапазон элементов из
сортированного множества, имеющих оценки, равные или лежащие между
`start` и `stop`.

`start` и `stop` могут быть _-inf_ и _+inf_, обозначая отрицательную и
положительную бесконечность соответственно. Это означает, что вам не
нужно знать наивысшую или наименьшую оценку в сортированном множестве,
чтобы получить все элементы с определённой оценкой или выше.

По умолчанию интервалы оценок, указанные с помощью `start` и `stop`,
являются замкнутыми. Можно указать открытый интервал, добавив перед
оценкой символ (.

Например:

```sql title="элементы с оценкой > 1 и <= 5"
ZRANGE zset (1 5 BYSCORE
```

```sql title="элементы с оценкой > 5 и < 10"
ZRANGE zset (5 (10 BYSCORE
```

**Обратные диапазоны**

Использование параметра `REV` обращает сортированное множество, при этом
индекс `0` будет относиться к элементу с наивысшей оценкой.

По умолчанию, чтобы вернуть какие-либо результаты, значение `start`
должно быть меньше или равно `stop`. Однако, если использован параметр
`BYSCORE` или `BYLEX`, значение `start` является наивысшей оценкой,
которую следует учитывать, а `stop` — наименьшей. Поэтому, чтобы вернуть
какие-либо результаты, значение `start` должно быть больше или равно
`stop`.

Например:

```sql title="элементы между индексами 5 и 10 в обратном порядке"
ZRANGE zset 5 10 REV
```

```sql title="элементы с оценками меньше 10 и больше 5"
ZRANGE zset 10 5 REV BYSCORE
```

**Лексикографические диапазоны**

При использовании параметра `BYLEX` команда ведёт себя как
[ZRANGEBYLEX](#zrangebylex) и возвращает диапазон элементов из
сортированного множества между лексикографическими закрытыми интервалами
диапазона `start` и `stop`.

Обратите внимание, что лексикографическая сортировка ожидает, что оценки
у всех элементов множества будут одинаковыми. Если элементы имеют разные
оценки, то ответ может быть любым.

Допустимые значения `start` и `stop` должны начинаться с ( или [, чтобы
указать, является ли интервал диапазона открытым или замкнутым,
соответственно.

Специальные значения `+` или `-` для `start` и `stop` означают
положительные и отрицательные бесконечные строки, соответственно,
поэтому, например, команда `ZRANGE myzset - + BYLEX` гарантированно
возвращает все элементы в сортированном множестве (при условии, что все
элементы имеют одинаковую оценку).

Параметр `REV` меняет порядок элементов `start` и `stop`, где `start`
должен быть лексикографически больше `stop`, чтобы получить непустой
результат.

**Лексикографическое сравнение строковых значений**

Строки сравниваются как двоичный массив байтов. В случае с набором
символов ASCII сравнение происходит обычным словарным способом.

Приложение сохраняет регистр символов, но не учитывает его при
сравнении. Для сравнения используются строки, приведенные к нижнему
регистру, для вывода результата — исходные значения.

Двоичная природа сравнения позволяет использовать сортированные
множества в качестве индекса общего назначения, например, первая часть
элемента может быть 64-разрядным числом в формате big-endian. Поскольку
в числах big-endian наиболее значимые байты находятся в начальных
позициях, двоичное сравнение будет соответствовать числовому сравнению
чисел. Это можно использовать для реализации запросов по диапазону на
64-разрядных значениях. Как показано в примере ниже, после первых 8 байт
мы можем хранить значение индексируемого элемента.

Пример:

```sql
> ZADD myzset 1 "one" 2 "two" 3 "three"
(integer) 3
> ZRANGE myzset 0 -1
1) "one"
2) "two"
3) "three"
> ZRANGE myzset 2 3
1) "three"
> ZRANGE myzset -2 -1
1) "two"
2) "three"
```

Дополнительные примеры:

```sql
127.0.0.1:7379> ZADD myzset 1 "one" 2 "two" 3 "three"
(integer) 3
127.0.0.1:7379> ZRANGE myzset 0 -1
1) "one"
2) "two"
3) "three"
127.0.0.1:7379> ZRANGE myzset 0 3 BYSCORE
1) "one"
2) "two"
3) "three"
127.0.0.1:7379> ZRANGE myzset 0 3 REV BYSCORE
1) "three"
2) "two"
3) "one"
127.0.0.1:7379> ZRANGE myzset 0 3 BYLEX
(empty array)
127.0.0.1:7379> ZRANGE myzset 0 3 BYSCORE LIMIT 1 1
1) "two"
2) "three"
```

### zrangebylex

```sql
ZRANGEBYLEX key min max [LIMIT offset count]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>

Работает аналогично [ZRANGE](#zrange) c параметром вывода `BYLEX`.

??? warning "Примечание"
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN radix 1.1.1 SET radix.redis_compatibility = '{ "enforce_one_slot_transactions": true, "push_result_includes_popped_items": true, "include_radix_section_in_info_by_default": true, "disable_scatter_gather": true, "enabled_deprecated_commands": ["zrangebylex" ] }';
    ```

### zrangebyscore

```sql
ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>

Работает аналогично [ZRANGE](#zrange) c параметром вывода `BYSCORE`.

??? warning "Примечание"
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN radix 1.1.1 SET radix.redis_compatibility = '{ "enforce_one_slot_transactions": true, "push_result_includes_popped_items": true, "include_radix_section_in_info_by_default": true, "disable_scatter_gather": true, "enabled_deprecated_commands": ["zrangebyscore" ] }';
    ```

### zrangestore

```sql
ZRANGESTORE destination src min max [BYSCORE | BYLEX] [REV] [LIMIT offset count]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Работает аналогично [ZRANGE](#zrange), но вместо вывода
результирующего сортированного множества записывает его в `destination`.

### zrank

```sql
ZRANK key member [WITHSCORE]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag read">read</span>
<span class="tag sortedset">sortedset</span>

Возвращает позиции элемента (`member`) в сортированном множестве,
хранящемся по указанному ключу `key`, с сортировкой по возрастанию
оценки. Отсчёт начинается с `0`.
Дополнительный параметр `WITHSCORE` добавляет в вывод команды сами оценки.

Для вывода позиций элементов по возрастанию оценки (включая в вывод сами
оценки) используйте [ZREVRANK](#zrevrank).

### zrem

```sql
ZREM key member [member ...]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Удаляет указанные элементы из сортированного множества, хранящегося по
ключу `key`. Если указанный элемент отсутствует в множестве, то такой
элемент игнорируется. Если ключ `key` существует, но в нем нет
сортированного множества, команда вернёт ошибку.

### zremrangebylex

```sql
ZREMRANGEBYLEX key min max
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Удаляет все элементы из сортированного множества, хранящегося по ключу
`key`, в лексикографическом диапазоне от `min` до `max`. Аргументы `min`
и `max` применяются так же, как в команде [ZRANGEBYLEX](#zrangebylex).


### zremrangebyrank

```sql
ZREMRANGEBYRANK key start stop
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Удаляет все элементы из сортированного множества, хранящегося по
ключу `key`, с позиции в диапазоне от `start` до `stop`. Значение `0 `—
наиболее низкая позиция, `-1` — наивысшая позиция, `-2` — вторая после
наивысшей и т.д.

### zremrangebyscore

```sql
ZREMRANGEBYSCORE key min max
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Удаляет все элементы из сортированного множества, хранящегося по
ключу `key`, с оценкой в диапазоне от `min` до `max` включительно.

### zrevrange

```sql
ZREVRANGE key start stop [WITHSCORES]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>

Работает аналогично [ZRANGE](#zrange) c параметром вывода `REV`.

??? warning "Примечание"
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN radix 1.1.1 SET radix.redis_compatibility = '{ "enforce_one_slot_transactions": true, "push_result_includes_popped_items": true, "include_radix_section_in_info_by_default": true, "disable_scatter_gather": true, "enabled_deprecated_commands": ["zrevrange" ] }';
    ```

### zrevrangebylex

```sql
ZREVRANGEBYLEX key max min [LIMIT offset count]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>

Работает аналогично [ZRANGE](#zrange) c параметрами вывода `REV` и `BYLEX`.

??? warning "Примечание"
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN radix 1.1.1 SET radix.redis_compatibility = '{ "enforce_one_slot_transactions": true, "push_result_includes_popped_items": true, "include_radix_section_in_info_by_default": true, "disable_scatter_gather": true, "enabled_deprecated_commands": ["zrevrangebylex" ] }';
    ```

### zrevrangebyscore

```sql
ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>

Работает аналогично [ZRANGE](#zrange) c параметрами вывода `REV` и `BYSCORE`.

??? warning "Примечание"
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN radix 1.1.1 SET radix.redis_compatibility = '{ "enforce_one_slot_transactions": true, "push_result_includes_popped_items": true, "include_radix_section_in_info_by_default": true, "disable_scatter_gather": true, "enabled_deprecated_commands": ["zrevrangebyscore" ] }';
    ```

### zrevrank

```sql
ZREVRANK key member [WITHSCORE]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag read">read</span>
<span class="tag sortedset">sortedset</span>

Возвращает позиции элемента (`member`) в сортированном множестве,
хранящемся по указанному ключу `key`, с сортировкой по убыванию оценки.
Отсчёт начинается с `0`.
Дополнительный параметр `WITHSCORE` добавляет в
вывод команды сами оценки.

Для вывода позиций элементов по возрастанию оценки (включая в вывод сами
оценки) используйте [ZRANK](#zrank).

### zscan

```sql
ZSCAN key cursor [MATCH pattern] [COUNT count]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>

См. [SCAN](#scan)

### zscore

```sql
ZSCORE key member
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag fast">fast</span>
<span class="tag read">read</span>
<span class="tag sortedset">sortedset</span>

Возвращает оценку элемента `member` в сортированном множестве, хранящемся
по ключу `key`.

### zunion

```sql
ZUNION numkeys key [key ...] [WEIGHTS weight [weight ...]]
  [AGGREGATE <SUM | MIN | MAX>] [WITHSCORES]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>

Работает аналогично [ZUNIONSTORE](#zunionstore), но вместо записи
результирующего сортированного множества выводит его клиенту.

### zunionstore

```sql
ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight
  [weight ...]] [AGGREGATE <SUM | MIN | MAX>]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag slow">slow</span>
<span class="tag sortedset">sortedset</span>
<span class="tag write">write</span>

Объединяет элементы двух или более сортированных множеств, хранящихся по
указанным ключам (`key`) в новое сортированное множество и записывает его в
`destination`. Объединение происходит на основе оценок элементов,
которые встречаются в исходных множествах. Перед списком ключей необходимо
указать их количество (`numkeys`). Команда выводит количество элементов
в результирующем множестве.

Дополнительный параметр `WEIGHTS` позволяет указать "вес" для каждого
исходного множества. Это число будет использовано как мультипликатор для
оценок в множестве.

Дополнительный параметр `AGGREGATE` позволяет указать способ объединения.
По умолчанию, это суммирование (`SUM`), однако можно указать запись
минимальной (`MIN`) или максимальной (`MAX`) оценки элемента из всех
исходных множеств, где он встречается.

Примеры:

```sql
127.0.0.1:7379> ZADD zset1 1 "one"
(integer) 1
127.0.0.1:7379> ZADD zset1 2 "two"
(integer) 1
127.0.0.1:7379> ZADD zset2 2 "two"
(integer) 1
127.0.0.1:7379> ZADD zset2 3 "three"
(integer) 1
127.0.0.1:7379> ZUNIONSTORE zsetout11 2 zset1 zset2 WEIGHTS 1 1
(integer) 3
127.0.0.1:7379> ZUNIONSTORE zsetout23 2 zset1 zset2 WEIGHTS 2 3
(integer) 3
127.0.0.1:7379> ZUNIONSTORE zsetout34 2 zset1 zset2 WEIGHTS 2 3 AGGREGATE MIN
(integer) 3
127.0.0.1:7379> ZRANGE zsetout11 0 1000 WITHSCORES
1) "one"
2) "1"
3) "three"
4) "3"
5) "two"
6) "4"
127.0.0.1:7379> ZRANGE zsetout23 0 1000 WITHSCORES
1) "one"
2) "2"
3) "three"
4) "9"
5) "two"
6) "10"
127.0.0.1:7379> ZRANGE zsetout34 0 1000 WITHSCORES
1) "one"
2) "2"
3) "two"
4) "4"
5) "three"
6) "9"
```

## Команды для списков {: #lists }

### blmove

```sql
BLMOVE source destination <LEFT | RIGHT> <LEFT | RIGHT> timeout
```
<span class="tag">поддерживается с версии 0.4.0</span>
<span class="tag blocking">blocking</span>
<span class="tag list">list</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Работает аналогично [LMPOP](#lmpop), но с использованием блокировки.
Если исходный список (`source`) пуст, то команда будет ждать наполнения
списка в течение указанного в `timeout` времени (в секундах), и в случае
неудачи вернёт ошибку. Если таймаут установить в `0`, то блокировка
будет бесконечной.

### blmpop

```sql
BLMPOP timeout numkeys key [key ...] <LEFT | RIGHT> [COUNT count]
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag blocking">blocking</span>
<span class="tag list">list</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Работает аналогично [LMOVE](#lmove), но с использованием блокировки.
Если все указанные списки ключей пусты, то команда будет ждать
наполнения любого из них в течение указанного в `timeout` времени (в
секундах), и в случае неудачи вернёт ошибку. Если таймаут установить в
`0`, то блокировка будет бесконечной.

### blpop

```sql
BLPOP key [key ...] timeout
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag blocking">blocking</span>
<span class="tag list">list</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Работает аналогично [LPOP](#lpop), но с использованием блокировки.
Если указанный список пуст, то команда будет ждать его наполнения
в течение указанного в `timeout` времени (в секундах), и в случае
неудачи вернёт ошибку. Если таймаут установить в `0`, то блокировка
будет бесконечной.

### brpoplpush

```sql
BRPOPLPUSH source destination timeout
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag blocking">blocking</span>
<span class="tag list">list</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Работает аналогично [RPOPLPUSH](#rpoplpush), но с использованием
блокировки. При использовании внутри блока [MULTI](#multi) или
[EXEC](#exec) данная команда ведёт себя полностью идентично
[RPOPLPUSH](#rpoplpush).

??? warning "Примечание"
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN radix 1.1.1 SET radix.redis_compatibility = '{ "enforce_one_slot_transactions": true, "push_result_includes_popped_items": true, "include_radix_section_in_info_by_default": true, "disable_scatter_gather": true, "enabled_deprecated_commands": ["brpoplpush" ] }';
    ```

### brpop

```sql
BRPOP key [key ...] timeout
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag blocking">blocking</span>
<span class="tag list">list</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Работает аналогично [RPOP](#lpop), но с использованием блокировки.
Поведение механизма блокировки аналогично таковому для [BLPOP](#blpop).

### lindex

```sql
LINDEX key index
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag list">list</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>

Возвращает элемент с указанным индексом (`index`) из списка, хранящегося
по указанному ключу `key`. Индекс `0` означает первый элемент списка,
`-1` — последний и т.д.

Примеры:

```sql
127.0.0.1:7379> LPUSH mylist "World"
(integer) 1
127.0.0.1:7379> LPUSH mylist "Hello"
(integer) 2
127.0.0.1:7379> LINDEX mylist 0
"Hello"
127.0.0.1:7379> LINDEX mylist -1
"World"
127.0.0.1:7379> LINDEX mylist 3
(nil)
127.0.0.1:7379>
```

### linsert

```sql
LINSERT key <BEFORE | AFTER> pivot element
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag list">list</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Вставляет в список, хранящийся по ключу `key`, элемент (`element`) до
(`BEFORE`) или после (`AFTER`) указанного другого элемента (`pivot`).
Если указан несуществующий ключ, то команда ничего не сделает. Если по
указанному ключу нет списка, то команда вернёт ошибку.

Примеры:

```sql
127.0.0.1:7379> RPUSH mylist "Hello"
(integer) 1
127.0.0.1:7379> RPUSH mylist "World"
(integer) 2
127.0.0.1:7379> LINSERT mylist BEFORE "World" "There"
(integer) 3
127.0.0.1:7379> LRANGE mylist 0 -1
1) "Hello"
2) "There"
3) "World"
127.0.0.1:7379>
```

### llen

```sql
LLEN key
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag fast">fast</span>
<span class="tag list">list</span>
<span class="tag read">read</span>

Возвращает длину (количество элементов) списка, хранящегося по ключу
`key`. Если указан несуществующий ключ, то команда вернёт `0`. Если по
указанному ключу нет списка, то команда вернёт ошибку.

Примеры:

```sql
127.0.0.1:7379> LPUSH mylist "World"
(integer) 1
127.0.0.1:7379> LPUSH mylist "Hello"
(integer) 2
127.0.0.1:7379> LLEN mylist
(integer) 2
```

### lmove

```sql
LMOVE source destination <LEFT | RIGHT> <LEFT | RIGHT>
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag list">list</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Перемещает первый/последний элемент первого списка (`source`) в
начало/конец второго списка (`destination`).

Примеры:

```sql
127.0.0.1:7379> RPUSH mylist "one"
(integer) 1
127.0.0.1:7379> RPUSH mylist "two"
(integer) 2
127.0.0.1:7379> RPUSH mylist "three"
(integer) 3
127.0.0.1:7379> LMOVE mylist myotherlist RIGHT LEFT
"three"
127.0.0.1:7379> LMOVE mylist myotherlist LEFT RIGHT
"one"
127.0.0.1:7379> LRANGE mylist 0 -1
1) "two"
127.0.0.1:7379> LRANGE myotherlist 0 -1
1) "three"
2) "one"
127.0.0.1:7379>
```

### lmpop

```sql
LMPOP numkeys key [key ...] <LEFT | RIGHT> [COUNT count]
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag list">list</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Извлекает (и удаляет) один или несколько (`count`) элементов в начале
(`LEFT`) или в конце (`RIGHT`) из первого непустого
списка ключей (`key`) в перечне списков ключей.

Примеры:

```sql
127.0.0.1:7379> LMPOP 2 non1 non2 LEFT COUNT 10
(nil)
127.0.0.1:7379> LPUSH mylist "one" "two" "three" "four" "five"
(integer) 5
127.0.0.1:7379> LMPOP 1 mylist LEFT
1) "mylist"
2) 1) "five"
127.0.0.1:7379> LRANGE mylist 0 -1
1) "four"
2) "three"
3) "two"
4) "one"
127.0.0.1:7379> LMPOP 1 mylist RIGHT COUNT 10
1) "mylist"
2) 1) "one"
   2) "two"
   3) "three"
   4) "four"
127.0.0.1:7379> LPUSH mylist "one" "two" "three" "four" "five"
(integer) 5
127.0.0.1:7379> LPUSH mylist2 "a" "b" "c" "d" "e"
(integer) 5
127.0.0.1:7379> LMPOP 2 mylist mylist2 right count 3
1) "mylist"
2) 1) "one"
   2) "two"
   3) "three"
127.0.0.1:7379> LRANGE mylist 0 -1
1) "five"
2) "four"
127.0.0.1:7379> LMPOP 2 mylist mylist2 right count 5
1) "mylist"
2) 1) "four"
   2) "five"
127.0.0.1:7379> LMPOP 2 mylist mylist2 right count 10
1) "mylist2"
2) 1) "a"
   2) "b"
   3) "c"
   4) "d"
   5) "e"
127.0.0.1:7379> EXISTS mylist mylist2
(integer) 0
127.0.0.1:7379>
```

### lpop

```sql
LPOP key [count]
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag fast">fast</span>
<span class="tag list">list</span>
<span class="tag write">write</span>

Извлекает (и удаляет) указанное число (`count`) первых элементов,
хранящихся в списке по адресу ключа `key`. Без аргумента `count` команда
извлекает один первый элемент в начале списка.

Примеры:

```sql
127.0.0.1:7379> RPUSH mylist "one" "two" "three" "four" "five"
(integer) 5
127.0.0.1:7379> LPOP mylist
"one"
127.0.0.1:7379> LPOP mylist 2
1) "two"
2) "three"
127.0.0.1:7379> LRANGE mylist 0 -1
1) "four"
2) "five"
```

### lpos

```sql
LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag list">list</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>

Возвращает индекс найденного в списке, хранящегося по ключу `key`,
элемента (`element`). Без дополнительных аргументов эта команда
просканирует список слева направо и вернёт индекс первого найденного
элемента. Нумерация элементов начинается с `0`.

Пример:

```sql
> RPUSH mylist a b c 1 2 3 c c
> LPOS mylist c
2
```

Параметр `RANK` позволяет вывести другой (по счёту `rank`) найденный
элемент в случае, если их несколько. Отрицательное значение `rank`
означает, что нумерация результата будет вестись справа налево.

Примеры:

```sql
> LPOS mylist c RANK 2
6
> LPOS mylist c RANK -1
7
```

Параметр `COUNT` позволяет вывести позиции всех (по счёту `num-matches`)
найденных элементов.

Пример:

```sql
> LPOS mylist c COUNT 2
[2,6]
```

При совместном использовании `COUNT` и `RANK` можно изменить точку
отсчёта, с которой будет производиться поиск совпадений.

Пример:

```sql
> LPOS mylist c RANK -1 COUNT 2
[7,6]
```

### lpush

```sql
LPUSH key element [element ...]
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag fast">fast</span>
<span class="tag list">list</span>
<span class="tag write">write</span>

Вставляет указанные элементы в начало списка, хранящегося по ключу
`key`.

Примеры:

```sql
127.0.0.1:7379> LPUSH mylist "world"
(integer) 1
127.0.0.1:7379> LPUSH mylist "hello"
(integer) 2
127.0.0.1:7379> LRANGE mylist 0 -1
1) "hello"
2) "world"
```

### lpushx

```sql
LPUSHX key element [element ...]
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag fast">fast</span>
<span class="tag list">list</span>
<span class="tag write">write</span>

Работает аналогично [LPUSH](#lpush), но проверяет, что указанный ключ
`key` существует. В противном случае команда ничего не делает (в отличие
от `LPUSH`).

### lrange

```sql
LRANGE key start stop
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag list">list</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>

Возвращает диапазон элементов списка, хранящегося по ключу `key`.
Позиция `start` обозначает начало диапазона, `stop` — его конец. При
указании отрицательных значений можно использовать диапазон, отсчитанный
справа налево. Нумерация элементов списка начинается с нуля.
Некорректный диапазон будет воспринят либо как пустой список (если
`start` превышает максимальный номер элемента), либо как корректный с
отсечением пустой части (если `stop` превышает максимальный номер
элемента). Следует учитывать, что при прямом отсчёте элементов слева
направо значение `stop` будет включено в состав элементов. То есть,
диапазон `LRANGE list 0 10` будет содержать 11 элементов.

Примеры:

```sql
127.0.0.1:7379> RPUSH mylist "one"
(integer) 1
127.0.0.1:7379> RPUSH mylist "two"
(integer) 2
127.0.0.1:7379> RPUSH mylist "three"
(integer) 3
127.0.0.1:7379> LRANGE mylist 0 0
1) "one"
127.0.0.1:7379> LRANGE mylist -3 2
1) "one"
2) "two"
3) "three"
127.0.0.1:7379> LRANGE mylist -100 100
1) "one"
2) "two"
3) "three"
127.0.0.1:7379> LRANGE mylist 5 10
(empty array)
```

### lrem

```sql
LREM key count element
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag list">list</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Удаляет из списка, хранящегося по ключу `key`, указанное количество
(`count`) найденных элементов (`element`). Положительное значение `count`
означает поиск слева направо, отрицательное — справа налево. При
значении `0` будут удалены все найденные элементы.

Примеры:

```sql
127.0.0.1:7379> RPUSH mylist "hello"
(integer) 1
127.0.0.1:7379> RPUSH mylist "hello"
(integer) 2
127.0.0.1:7379> RPUSH mylist "foo"
(integer) 3
127.0.0.1:7379> RPUSH mylist "hello"
(integer) 4
127.0.0.1:7379> LREM mylist -2 "hello"
(integer) 2
127.0.0.1:7379> LRANGE mylist 0 -1
1) "hello"
2) "foo"
127.0.0.1:7379>
```

### lset

```sql
LSET key index element
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag list">list</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Устанавливает индекс (`index`) для добавляемого элемента (`element`).
Таким образом можно затереть один элемент списка и заменить его новым
значением.

Примеры:

```sql
127.0.0.1:7379> RPUSH mylist "one"
(integer) 1
127.0.0.1:7379> RPUSH mylist "two"
(integer) 2
127.0.0.1:7379> RPUSH mylist "three"
(integer) 3
127.0.0.1:7379> LSET mylist 0 "four"
"OK"
127.0.0.1:7379> LSET mylist -2 "five"
"OK"
127.0.0.1:7379> LRANGE mylist 0 -1
1) "four"
2) "five"
3) "three"
127.0.0.1:7379>
```

### ltrim

```sql
LTRIM key start stop
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag list">list</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Обрезает список, хранящийся по ключу `key`, задавая его размер с помощью
диапазона. При указании отрицательных значений можно использовать
диапазон, отсчитанный справа налево. Нумерация элементов списка
начинается с нуля. Некорректный диапазон будет воспринят либо как пустой
список (если `start` превышает максимальный номер элемента) c удалением
ключей, либо как корректный с увеличением границ списка (если `stop`
превышает максимальный номер элемента).

Типичное применение `LTRIM`:

```sql
LPUSH mylist someelement
LTRIM mylist 0 99
```

Эти команды добавят в список значение `someelement` и при этом установят
емкость списка равной 100 элементам.

Дополнительные примеры:

```sql
127.0.0.1:7379> RPUSH mylist "one"
(integer) 1
127.0.0.1:7379> RPUSH mylist "two"
(integer) 2
127.0.0.1:7379> RPUSH mylist "three"
(integer) 3
127.0.0.1:7379> LTRIM mylist 1 -1
"OK"
127.0.0.1:7379> LRANGE mylist 0 -1
1) "two"
2) "three"
127.0.0.1:7379>
```

### rpop

```sql
RPOP key [count]
```
<span class="tag">поддерживается с версии 0.4.0</span>
<span class="tag fast">fast</span>
<span class="tag list">list</span>
<span class="tag write">write</span>

Извлекает (и удаляет) указанное число (`count`) последних элементов,
хранящихся в списке по адресу ключа `key`. Без аргумента `count` команда
извлекает один первый элемент в начале списка.

Примеры:

```sql
127.0.0.1:7379> RPUSH mylist "one" "two" "three" "four" "five"
(integer) 5
127.0.0.1:7379> RPOP mylist
"five"
127.0.0.1:7379> RPOP mylist 2
1) "four"
2) "three"
127.0.0.1:7379> LRANGE mylist 0 -1
1) "one"
2) "two"
```

### rpoplpush

```sql
RPOPLPUSH source destination
```
<span class="tag">поддерживается с версии 0.13.0</span>
<span class="tag list">list</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Извлекает один последний элемент из множества, хранящегося в `source` и
добавляет его в начало множества, хранящегося в `destination`. Если
`source` не существует, команда вернёт `nil` и ничего не переместит.
Если в качестве `source` и `destination` указать одно и то же множество,
то команда переместит элемент из его конца в его начало.

??? warning "Примечание"
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN radix 1.1.1 SET radix.redis_compatibility = '{ "enforce_one_slot_transactions": true, "push_result_includes_popped_items": true, "include_radix_section_in_info_by_default": true, "disable_scatter_gather": true, "enabled_deprecated_commands": ["rpoplpush" ] }';
    ```

### rpush

```sql
RPUSH key element [element ...]
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag fast">fast</span>
<span class="tag list">list</span>
<span class="tag write">write</span>

Работает аналогично [LPUSH](#lpush), но добавляет элементы в конец
списка.

### rpushx

```sql
RPUSHX key element [element ...]
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag fast">fast</span>
<span class="tag list">list</span>
<span class="tag write">write</span>

Работает аналогично [RPUSH](#rpush), но проверяет существование ключа
`key` и то, что этот ключ содержит список. В противном случае команда
ничего не делает.

## Команды управления подпиской (Pub/Sub) {: #pubsub }

Pub/Sub — механизм для отправки сообщений между клиентами через каналы.

### psubscribe

```sql
PSUBSCRIBE pattern [pattern ...]
```
<span class="tag">поддерживается с версии 0.2</span>
<span class="tag pubsub">pubsub</span>
<span class="tag slow">slow</span>

Подписывает клиента на получение данных согласно указанному шаблону (`pattern`). Примеры шаблонов:

- `h?llo` подписывает на _hello_, _hallo_ и _hxllo_
- `h*llo` подписывает на _hllo_ и _heeeello_
- `h[ae]llo` подписывает на _hello_ и _hallo_, но не _hillo_

### publish

```sql
PUBLISH channel message
```
<span class="tag">поддерживается с версии 0.2</span>
<span class="tag fast">fast</span>
<span class="tag pubsub">pubsub</span>

Размещает сообщение (`message`) в указанном канале (`channel`).
Сообщение будет доступно клиентам вне зависимости от того, к какому узлу
кластера они подключены.

### pubsub channels {: #pubsub_channels }

```sql
PUBSUB CHANNELS [pattern]
```
<span class="tag">поддерживается с версии 0.2</span>
<span class="tag pubsub">pubsub</span>
<span class="tag slow">slow</span>

Выводит список активных каналов. Канал считается активным, если на него
есть хотя бы один подписчик (подписка на шаблоны (`pattern`) не
считается). Если в команде не указан шаблон (`pattern`), то будут
выведены все активные каналы. В противном случае будут выведены только
те активные каналы, которые соответствуют шаблону.

### pubsub numpat {: #pubsub_numpat }

```sql
PUBSUB NUMPAT
```
<span class="tag">поддерживается с версии 0.2</span>
<span class="tag pubsub">pubsub</span>
<span class="tag slow">slow</span>

Выводит список уникальных шаблонов, на которые были произведены подписки
со стороны клиентов (с помощью команды [PSUBSCRIBE](#psubscribe)). Не
следует путать вывод этой команды с общим числом клиентов.

### pubsub numsub {: #pubsub_numsub }

```sql
PUBSUB NUMSUB [channel [channel ...]]
```
<span class="tag">поддерживается с версии 0.2</span>
<span class="tag pubsub">pubsub</span>
<span class="tag slow">slow</span>

Выводит список всех подписчиков указанных каналов. Подписчики на шаблоны
(`pattern`) не считаются.

### pubsub shardchannels {: #pubsub_shardchannels}

```sql
PUBSUB SHARDCHANNELS [pattern]
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag pubsub">pubsub</span>
<span class="tag slow">slow</span>

Выводит список активных шард-каналов (каналов Redis, работающих в рамках
отдельных репликасетов). Параметр `pattern` позволяет отфильтровать
список, указав необходимый шаблон.

### pubsub shardnumsub {: #pubsub_shardnumsub}

```sql
PUBSUB SHARDNUMSUB [shardchannel [shardchannel ...]]
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag pubsub">pubsub</span>
<span class="tag slow">slow</span>

Выводит количество подписчиков для указанных шард-каналов (каналов
Redis, работающих в рамках отдельных репликасетов).

### punsubscribe

```sql
PUNSUBSCRIBE [pattern [pattern ...]]
```
<span class="tag">поддерживается с версии 0.2</span>
<span class="tag pubsub">pubsub</span>
<span class="tag slow">slow</span>

Отписывает клиента от указанных шаблонов. Если ни один канал (`pattern`)
не указан, то клиент будет отписан от всех шаблонов.

### spublish

```sql
SPUBLISH shardchannel message
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag fast">fast</span>
<span class="tag pubsub">pubsub</span>

Размещает сообщение (`message`) в указанном шард-канале
(`shardchannel`). Сообщение будет доступно клиентам на всех репликах,
входящих в состав репликасета, на котором создан шард-канал.

### ssubscribe

```sql
SSUBSCRIBE shardchannel [shardchannel ...]
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag pubsub">pubsub</span>
<span class="tag slow">slow</span>

Подписывает клиента на получение данных из указанных шард-каналов
(`shardchannel`). Все шард-каналы, указанные в команде, должны
относиться к одному репликасету. См. также [PSUBSCRIBE](#psubscribe).


### subscribe

```sql
SUBSCRIBE channel [channel ...]
```
<span class="tag">поддерживается с версии 0.2</span>
<span class="tag pubsub">pubsub</span>
<span class="tag slow">slow</span>

Подписывает клиента на получение данных из указанных каналов
(`channel`). См. также [PSUBSCRIBE](#psubscribe).

### sunsubscribe

```sql
SUNSUBSCRIBE [shardchannel [shardchannel ...]]
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag pubsub">pubsub</span>
<span class="tag slow">slow</span>

Отписывает клиента от указанных шард-каналов. Если ни один шард-канал
(`shardchannel`) не указан, то клиент будет отписан от всех
шард-каналов.

### unsubscribe

```sql
UNSUBSCRIBE [channel [channel ...]]
```
<span class="tag">поддерживается с версии 0.2</span>
<span class="tag pubsub">pubsub</span>
<span class="tag slow">slow</span>

Отписывает клиента от указанных каналов. Если ни один канал (`channel`)
не указан, то клиент будет отписан от всех каналов.

## Команды для строк {: #string }

### append

```sql
APPEND key value
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag fast">fast</span>
<span class="tag string">string</span>
<span class="tag write">write</span>

Добавляет значение `value` к строке, хранящейся по ключу `key`. Если
указанный ключ не существует, то он будет создан, и тогда данная команда
сработает аналогично [SET](#set).

Пример:

```sql
> APPEND mykey "Hello"
(integer) 5
> APPEND mykey " World"
(integer) 11
> GET mykey
"Hello World"
```

### get

```sql
GET key
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag fast">fast</span>
<span class="tag read">read</span>
<span class="tag string">string</span>

Получает значение ключа `key`. Если ключ не существует, возвращается
специальное значение `nil`. Если значение, хранящееся в ключе, не
является строкой, возвращается ошибка, поскольку `GET` работает только
со строковыми значениями.

### getdel

```sql
GETDEL key
```
<span class="tag">поддерживается с версии 1.0.0</span>
<span class="tag fast">fast</span>
<span class="tag string">string</span>
<span class="tag write">write</span>

Получает значение ключа `key` и удаляет его. Команда действует подобно
[GET](#get), но удаляет ключ только в том случае, если в нём
действительно хранятся строковые значения.

### getrange

```sql
GETRANGE key start end
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>
<span class="tag string">string</span>

Возвращает подстроку из значения, хранящегося по указанному ключу.
Границы подстроки определяют аргументами `start` и `end`.

### incr

```sql
INCR key
```
<span class="tag">поддерживается с версии 0.4.3</span>
<span class="tag fast">fast</span>
<span class="tag string">string</span>
<span class="tag write">write</span>

Увеличивает значение, хранящееся по указанному ключу, на `1.` Если
указанный ключ не существует, то его значение
принимается за `0`.

### incrby

```sql
INCRBY key increment
```
<span class="tag">поддерживается с версии 0.4.3</span>
<span class="tag fast">fast</span>
<span class="tag string">string</span>
<span class="tag write">write</span>

Увеличивает значение, хранящееся по указанному ключу, на величину
`increment`. Если указанный ключ не существует, то его значение
принимается за `0`.

### incrbyfloat

```sql
INCRBYFLOAT key increment
```
<span class="tag">поддерживается с версии 0.4.3</span>
<span class="tag fast">fast</span>
<span class="tag string">string</span>
<span class="tag write">write</span>

Увеличивает значение, хранящееся по указанному ключу, на величину
`increment`, но при этом поддерживает дробные и отрицательные значения.
Если указанный ключ не существует, то его значение принимается за `0`.

### mget

```sql
MGET key [key ...]
```
<span class="tag">поддерживается с версии 0.12.0</span>
<span class="tag fast">fast</span>
<span class="tag read">read</span>
<span class="tag string">string</span>

Возвращает значения всех указанных ключей. Если ключ не существует, или
не содержит значения, то для него команда вернёт `nil`. Благодаря этому,
команда никогда не возвращает ошибку.

### mset

```sql
MSET key value [key value ...]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag slow">slow</span>
<span class="tag string">string</span>
<span class="tag write">write</span>

Сохраняет строковые значения в ключах в соответствующих парах.
Существующие значения при этом перезаписываются (аналогично
[SET](#set)). Команда работает атомарно, устанавливая все значения за один
проход, без возможности отследить, какие ключи были изменены, а какие
нет.

### psetex

```sql
PSETEX key milliseconds value
```
<span class="tag">поддерживается с версии 0.7.0</span>
<span class="tag slow">slow</span>
<span class="tag string">string</span>
<span class="tag write">write</span>

Устанавливает значение и срок жизни (таймаут) для ключа `key` подобно
[SETEX](#setex), но в миллисекундах.

??? warning "Примечание"
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN radix 1.1.1 SET radix.redis_compatibility = '{ "enforce_one_slot_transactions": true, "push_result_includes_popped_items": true, "include_radix_section_in_info_by_default": true, "disable_scatter_gather": true, "enabled_deprecated_commands": ["psetex" ] }';
    ```

### set

```sql
SET key value [NX | XX] [GET] [EX seconds | PX milliseconds |
  EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]
```
<span class="tag">поддерживается с версии 0.1.0</span>
<span class="tag slow">slow</span>
<span class="tag string">string</span>
<span class="tag write">write</span>

Сохраняет строковое значение в ключе. Если ключ уже содержит значение,
оно будет перезаписано, независимо от его типа. Любое предыдущее
ограничение таймаута, связанное с ключом, отменяется при успешном
выполнении операции `SET`.

Параметры:

- `EX` — установка указанного времени истечения срока действия в
  секундах (целое положительное число)
- `PX` — установка указанного времени истечения в миллисекундах (целое
  положительное число)
- `EXAT` — установка указанного времени Unix, в которое истекает срок
  действия ключа, в секундах (целое положительное число)
- `PXAT` — установка указанного времени Unix, по истечении которого срок
  действия ключа истечёт, в миллисекундах (целое положительное число)
- `NX` — установка значения ключа только в том случае, если он ещё
  не существует
- `XX` — установка значения ключа только в том случае, если он уже
  существует
- `KEEPTTL` — сохранить время жизни, связанное с ключом
- `GET` — возвращает старую строку, хранящуюся по адресу ключа, или
  `nil`, если ключ не существовал. Возвращается ошибка и `SET`
  прерывается, если значение, хранящееся по адресу ключа `key`, не
  является строкой.

### setex

```sql
SETEX key seconds value
```
<span class="tag">поддерживается с версии 0.7.0</span>
<span class="tag slow">slow</span>
<span class="tag string">string</span>
<span class="tag write">write</span>

Устанавливает для ключа `key` значение `value` и срок жизни (таймаут) в секундах.
Аналогичный результат достигается так:

```sql
SET key value EX seconds
```

??? warning "Примечание"
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN radix 1.1.1 SET radix.redis_compatibility = '{ "enforce_one_slot_transactions": true, "push_result_includes_popped_items": true, "include_radix_section_in_info_by_default": true, "disable_scatter_gather": true, "enabled_deprecated_commands": ["setex" ] }';
    ```

Установка некорректного значения вернёт ошибку.

### setnx

```sql
SETNX key value
```
<span class="tag">поддерживается с версии 0.7.0</span>
<span class="tag fast">fast</span>
<span class="tag string">string</span>
<span class="tag write">write</span>

Устанавливает для ключа `key` значение `value` только если такого ключа
ранее не было.

??? warning "Примечание"
    Данная команда отнесена в Redis в разряд
    устаревших и по умолчанию отключена в Radix. Для включения
    используйте следующий SQL-запрос:
    ```sql
    ALTER PLUGIN radix 1.1.1 SET radix.redis_compatibility = '{ "enforce_one_slot_transactions": true, "push_result_includes_popped_items": true, "include_radix_section_in_info_by_default": true, "disable_scatter_gather": true, "enabled_deprecated_commands": ["setnx" ] }';
    ```

### strlen

```sql
STRLEN key
```
<span class="tag">поддерживается с версии 0.3.0</span>
<span class="tag fast">fast</span>
<span class="tag read">read</span>
<span class="tag string">string</span>

Возвращает длину текстового значения, хранящегося по
указанному ключу.

## Команды для получения информации о Sentinel {: #sentinel }

Radix поддерживает необходимый минимум команд для того, чтобы приложения
могли получать адреса серверов Picodata, если эти приложения написаны
с поддержкой Sentinel.

По умолчанию перечисленные ниже команды отключены. Для того, чтобы их
включить, используйте запрос:

```sql
ALTER PLUGIN radix 1.1.1 SET radix.sentinel_enabled = 'true';
```
<span class="tag">поддерживается с версии 0.10.0</span>

### sentinel get-master-addr-by-name {: #sentinel-get-master-addr-by-name }

```sql
SENTINEL GET-MASTER-ADDR-BY-NAME <replicaset name>
```
<span class="tag">поддерживается с версии 0.10.0</span>

Возвращает адрес Radix для заданного репликасета.

### sentinel master {: #sentinel-master }

```sql
SENTINEL MASTER <replicaset name>
```
<span class="tag">поддерживается с версии 0.10.0</span>

Выводит мастера для заданного репликасета. Radix возвращает мастера
репликасета с соответствующим именем.

### sentinel masters {: #sentinel-masters }

```sql
SENTINEL MASTERS
```
<span class="tag">поддерживается с версии 0.10.0</span>

Возвращает список репликасетов, которые есть в системе.

### sentinel myid {: #sentinel-myid }

```sql
SENTINEL MYID
```
<span class="tag">поддерживается с версии 0.10.0</span>

Возвращает id текущего инстанса

### sentinel replicas {: #sentinel-replicas }

```sql
SENTINEL REPLICAS <replicaset name>
```
<span class="tag">поддерживается с версии 0.10.0</span>

Показывает список реплик для заданного репликасета.

### sentinel sentinels {: #sentinel-sentinels }

```sql
SENTINEL SENTINELS <replicaset name>
```
<span class="tag">поддерживается с версии 0.10.0</span>

Показывает список сентинелей для заданного репликасета. Radix
возвращает мастера репликасета с соответствующим именем.

## Команды для скриптов {: #scripting }

Radix поддерживает следующие команды для работы с Lua-скриптами:

### eval

```sql
EVAL script numkeys [key [key ...]] [arg [arg ...]]
```
<span class="tag">поддерживается с версии 0.5.0</span>
<span class="tag scripting">scripting</span>
<span class="tag slow">slow</span>

Вызывает Lua-скрипт. Первый аргумент — исходный код Lua-скрипта
(`script`). Следующий за ним аргумент — количество передаваемых ключей
(`numkeys`) и далее сами ключи и их аргументы.

Пример:

```sql
> EVAL "return ARGV[1]" 0 hello
"hello"
```

Поддерживаемые скриптовые функции для `EVAL`:

- `redis.call(command) `— вызов команды Redis и вывод её результата (при его наличии)
- `redis.pcall(command)` — аналог `redis.call()`, но с гарантированным возвратом ответа, что удобно для анализа ошибок команд
- `redis.log(level, message)` — запись в журнал инстанса сообщения с указанием уровня важности. Например, `redis.log(redis.LOG_WARNING, 'Something is terribly wrong')`
- `redis.sha1hex(x)` — возврат шестнадцатеричного SHA1-хэша для указанного числа _x_
- `redis.status_reply(x)` — возврат статуса состояния _x_ в виде строки
- `redis.error_reply` — возврат состояния _x_ в виде строки
- `redis.REDIS_VERSION` — возврат текущей версии Redis в виде строки в формате Lua
- `redis.REDIS_VERSION_NUM `— возврат текущей версии Redis в виде номера

### evalro

```sql
EVAL_RO script numkeys [key [key ...]] [arg [arg ...]]
```
<span class="tag">поддерживается с версии 0.5.0</span>
<span class="tag scripting">scripting</span>
<span class="tag slow">slow</span>

Вызывает Lua-скрипт аналогично [eval](#eval), но в режиме "только
чтение", т.е. без модификации данных БД.

### evalsha

```sql
EVALSHA sha1 numkeys [key [key ...]] [arg [arg ...]]
```
<span class="tag">поддерживается с версии 0.5.0</span>

Вызывает Lua-скрипт аналогично [eval](#eval), но в качестве аргумента
принимает не сам скрипт, а его SHA1-хэш из кеша скриптов.

### evalsharo

```sql
EVALSHA_RO sha1 numkeys [key [key ...]] [arg [arg ...]]
```
<span class="tag">поддерживается с версии 0.5.0</span>
<span class="tag scripting">scripting</span>
<span class="tag slow">slow</span>

Вызывает Lua-скрипт аналогично [evalsha](#evalsha), но в режиме "только
чтение", т.е. без модификации данных БД.

### script exists {: #script_exists }

```sql
SCRIPT EXISTS sha1 [sha1 ...]
```
<span class="tag">поддерживается с версии 0.6.0</span>
<span class="tag scripting">scripting</span>
<span class="tag slow">slow</span>

Возвращает информацию о существовании скрипта с указанным хэшем SHA1 в
кеше скриптов.

### script flush {: #script_flush }

```sql
SCRIPT FLUSH [ASYNC | SYNC]
```
<span class="tag">поддерживается с версии 0.11.0</span>
<span class="tag scripting">scripting</span>
<span class="tag slow">slow</span>

Очищает кеш Lua-скриптов. По умолчанию, операция производится в
синхронном режиме. Пользователь может указать режим явно:

- `ASYNC` — очистить кеш асинхронно
- `SYNC` — очистить кеш синхронно

### script load {: #script_load }

```sql
SCRIPT LOAD script
```
<span class="tag">поддерживается с версии 0.6.0</span>
<span class="tag scripting">scripting</span>
<span class="tag slow">slow</span>

Загружает скрипт в кеш скриптов. Работает идемпотентно (т.е.
подразумевая, что такой скрипт уже есть в хранилище).

## Команды для транзакций {: #transactions }

### discard

```sql
DISCARD
```
<span class="tag">поддерживается с версии 0.6.0</span>
<span class="tag fast">fast</span>
<span class="tag transaction">transaction</span>

Удаляет все команды из очереди исполнения

### exec

```sql
EXEC
```
<span class="tag">поддерживается с версии 0.6.0</span>
<span class="tag slow">slow</span>
<span class="tag transaction">transaction</span>

Исполняет все команды в очереди в рамках единой транзакции.

### multi

```sql
MULTI
```
<span class="tag">поддерживается с версии 0.6.0</span>
<span class="tag fast">fast</span>
<span class="tag transaction">transaction</span>

Обозначает момент блокировки транзакции. Последующие команды будут
исполняться одна за другой при помощи [exec](#exec).

### unwatch

```sql
UNWATCH key [key ...]
```
<span class="tag">поддерживается с версии 0.6.0</span>
<span class="tag fast">fast</span>
<span class="tag transaction">transaction</span>

Удаляет все ключи из списка наблюдения [watch](#watch).

### watch

```sql
WATCH key [key ...]
```
<span class="tag">поддерживается с версии 0.6.0</span>
<span class="tag fast">fast</span>
<span class="tag transaction">transaction</span>

Включает проверку значений указанных ключей для последующих транзакций.

## Команды управления и диагностики {: #server }

### flushall

```sql
FLUSHALL [ASYNC | SYNC]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag dangerous">dangerous</span>
<span class="tag keyspace">keyspace</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Очищает все базы данных.

- `SYNC`: синхронно, т.е. команда вернёт управление только после полной очистки БД.
- `ASYNC`: асинхронно, команда вернёт управление быстрее, данные очистятся в фоне.

Если ни одна из опций не указана, используется режим SYNC

### flushdb

```sql
FLUSHDB [ASYNC | SYNC]
```
<span class="tag">поддерживается с версии 0.10.0</span>
<span class="tag dangerous">dangerous</span>
<span class="tag keyspace">keyspace</span>
<span class="tag slow">slow</span>
<span class="tag write">write</span>

Очищает текущую базу данных.

- `SYNC`: синхронно — команда вернёт управление только после полной очистки БД
- `ASYNC`: асинхронно — команда вернёт управление быстрее, данные очистятся в фоне

Если ни одна из опций не указана, используется режим SYNC

### info

```sql
INFO [section [section ...]]
```
<span class="tag">поддерживается с версии 0.4.0</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Возвращает информацию о сервере, подключенных клиентах, нагрузке на
текущий узел и прочую статистику. Параметр `section` позволяет уточнить
запрос, ограничив его нужной секцией. Доступные секции:

- `server`
- `clients`
- `memory`
- `persistence`
- `stats`
- `replication`
- `cpu`
- `modules`
- `errorstats`
- `cluster`
- `keyspace`
- `commandstats`
- `latencystats`
- `sentinel`
- `radix`

!!! note "Примечание"
    Секция `radix` отображается в выводе команды `info` только
    при включённом параметре `include_radix_section_in_info_by_default = true`, либо
    при явном вызове типа `INFO radix clients`.

??? example "Образец вывода полного набора сведений"
    ```
    127.0.0.1:6379> info
    # Server
    redis_version:8.0.0
    redis_git_sha1:aa3d2882602f094b1941e32c297a5260880ba985
    redis_git_dirty:1
    redis_build_id:
    redis_mode:standalone
    os:AlmaLinux 7.2.4-200.fc44.x86_64 x86_64
    arch_bits:64
    monotonic_clock:POSIX clock_gettime with CLOCK_MONOTONIC
    multiplexing_api:epoll
    atomicvar_api:c11-builtin
    gcc_version:rustc 1.98.1 (48a229cea 2026-09-01)
    process_id:1
    process_supervised:no
    run_id:8642b760e02946f9b2cf5df9caedc627
    tcp_port:6379
    server_time_usec:1789051928765325000
    uptime_in_seconds:22
    uptime_in_days:0
    hz:3200
    configured_hz:0
    lru_clock:0
    executable:/usr/bin/picodata
    config_file:
    io_threads_active:1

    # Clients
    connected_clients:1
    cluster_connections:0
    maxclients:10000
    client_recent_max_input_buffer:8192
    client_recent_max_output_buffer:8192
    blocked_clients:0
    tracking_clients:0
    pubsub_clients:0
    watching_clients:0
    clients_in_timeout_table:0
    total_watched_keys:0
    total_blocking_keys:0
    total_blocking_keys_on_nokey:0

    # Memory
    used_memory:130023424
    used_memory_human:124.00M
    used_memory_rss:65708032
    used_memory_rss_human:62.66M
    used_memory_peak:130023424
    used_memory_peak_human:124.00M
    used_memory_peak_perc:100.00
    used_memory_overhead:96468992
    used_memory_startup:130023424
    used_memory_dataset:33554432
    used_memory_dataset_perc:25.81
    allocator_allocated:130023424
    allocator_active:130023424
    allocator_resident:65708032
    total_system_memory:33281540096
    total_system_memory_human:31.00G
    used_memory_lua:7453438
    used_memory_vm_eval:7453438
    used_memory_lua_human:7.11M
    used_memory_scripts_eval:0
    number_of_cached_scripts:0
    number_of_functions:0
    number_of_libraries:0
    used_memory_vm_functions:0
    used_memory_vm_total:7453438
    used_memory_vm_total_human:7.11M
    used_memory_functions:0
    used_memory_scripts:0
    used_memory_scripts_human:0B
    maxmemory:268435456
    maxmemory_human:256.00M
    maxmemory_policy:noeviction
    allocator_frag_ratio:12.50
    allocator_frag_bytes:33554432
    allocator_muzzy:0
    allocator_rss_ratio:NaN
    allocator_rss_bytes:0
    mem_not_counted_for_evict:0
    mem_replication_backlog:0
    mem_total_replication_buffers:0
    mem_fragmentation_ratio:NaN
    mem_fragmentation_bytes:0
    mem_clients_normal:24576
    mem_allocator:slab
    active_defrag_running:0
    lazyfree_pending_objects:0
    lazyfreed_objects:0

    # Persistence
    loading:0
    async_loading:0

    # Stats
    total_connections_received:2
    total_commands_processed:4
    instantaneous_ops_per_sec:0
    total_net_input_bytes:98
    total_net_output_bytes:718
    total_net_repl_input_bytes:0
    total_net_repl_output_bytes:0
    instantaneous_input_kbps:0.00
    instantaneous_output_kbps:0.00
    instantaneous_input_repl_kbps:0.00
    instantaneous_output_repl_kbps:0.00
    rejected_connections:0
    sync_full:0
    sync_partial_ok:0
    sync_partial_err:0
    expired_keys:0
    evicted_keys:0
    total_eviction_exceeded_time:0
    current_eviction_exceeded_time:0
    keyspace_hits:0
    keyspace_misses:0
    pubsub_channels:0
    pubsub_patterns:0
    latest_fork_usec:0
    migrate_cached_sockets:0
    unexpected_error_replies:0
    total_error_replies:2
    total_reads_processed:6
    total_writes_processed:4
    client_query_buffer_limit_disconnections:0
    client_output_buffer_limit_disconnections:0
    reply_buffer_expands:0
    reply_buffer_shrinks:0
    request_buffer_expands:0
    request_buffer_shrinks:0
    acl_access_denied_auth:0
    acl_access_denied_cmd:0
    acl_access_denied_key:0
    acl_access_denied_channel:0
    watching_clients:0
    clients_in_timeout_table:0
    total_watched_keys:0

    # Replication
    role:master
    connected_slaves:0
    master_failover_state:no-failover
    master_replid:153a0c8e-5a07-4792-8995-a8a3e5f1c3be
    master_replid2:153a0c8e-5a07-4792-8995-a8a3e5f1c3be
    master_repl_offset:34845
    second_repl_offset:34845
    repl_backlog_active:0
    repl_backlog_size:0
    repl_backlog_first_byte_offset: 0
    repl_backlog_histlen:0

    # CPU
    used_cpu_sys:0.010062
    used_cpu_user:0.029083
    used_cpu_sys_children:0.000000
    used_cpu_user_children:0.000000
    used_cpu_sys_main_thread:0.009573
    used_cpu_user_main_thread:0.028856

    # Modules

    # Errorstats
    errorstat_UNKNOWN_COMMAND:count=2
    # Cluster
    cluster_enabled:1

    # Keyspace

    # Commandstats
    cmdstat_info:calls=1,usec=59,usec_per_call=59,rejected_calls=0,failed_calls=0
    cmdstat_ping:calls=1,usec=198,usec_per_call=198,rejected_calls=0,failed_calls=0
    # Latencystats
    latency_percentiles_usec_info:p50.0=59,p99.0=59,p99.9=59
    latency_percentiles_usec_ping:p50.0=198,p99.0=198,p99.9=198
    # Sentinel
    sentinel_masters:1
    sentinel_tilt:0
    sentinel_tilt_since_seconds:0
    sentinel_running_scripts:0
    sentinel_scripts_queue_length:0
    sentinel_simulate_failure_flags:0

    # Radix
    radix_version:1.1.1
    picodata_version:26.1.6
    picodata_cluster_name:radix-docker-standalone
    picodata_cluster_uuid:9f0005c4-b3f1-42b4-abf0-772299e563be
    picodata_instance_name:default_1_1
    picodata_instance_uuid:153a0c8e-5a07-4792-8995-a8a3e5f1c3be
    slab_info_items_size:16272
    slab_info_items_used:160
    slab_info_items_used_ratio:0.98
    slab_info_quota_size:268435456
    slab_info_quota_used:33554432
    slab_info_quota_used_ratio:12.5
    slab_info_arena_size:33554432
    slab_info_arena_used:49312
    slab_info_arena_used_ratio:0.1
    ```

### latency graph {: #latency_graph }

```sql
LATENCY GRAPH event
```
<span class="tag">поддерживается с версии 1.0.5</span>
<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Строит ASCII-график для указанного события задержки. График помогает быстро
оценить тренд задержек без разбора сырых данных из [LATENCY
HISTORY](#latency_history) или внешних инструментов.

Пример:

```text
127.0.0.1:6379> latency reset command
(integer) 0
127.0.0.1:6379> latency graph command
command - high 500 ms, low 101 ms (all time high 500 ms)
--------------------------------------------------------------------------------
   #_
  _||
 _|||
_||||
11186
542ss
sss
```

Подробности:

- Вертикальные подписи под столбцами графика показывают, сколько секунд, минут, часов или дней назад произошло событие. Например, `15s` означает, что первое показанное событие произошло 15 секунд назад.
- График нормализуется по шкале min-max: символ `_` в нижней строке соответствует минимальному значению, а символ `#` в верхней строке — максимальному.

### latency help {: #latency_help }

```sql
LATENCY HELP
```
<span class="tag">поддерживается с версии 1.0.5</span>
<span class="tag slow">slow</span>

Возвращает справку с описанием подкоманд `LATENCY`.

### latency histogram {: #latency_histogram }

```sql
LATENCY HISTOGRAM [command [command ...]]
```
<span class="tag">поддерживается с версии 1.0.5</span>
<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Возвращает кумулятивное распределение задержек команд в формате гистограммы.

Параметры и варианты использования:

- `command [command ...]` — одна или несколько команд, для которых нужно вернуть гистограммы задержек. Если аргумент не указан, возвращаются гистограммы для всех доступных команд.

Подробности:

- Каждая гистограмма содержит имя команды, общее количество вызовов этой команды и карту временных корзин.
- Каждая корзина представляет диапазон задержек и покрывает в два раза больший диапазон, чем предыдущая.
- Пустые корзины не включаются в ответ.
- Отслеживаются задержки от 1 наносекунды примерно до 1 секунды. Все, что выше 1 секунды, считается `+Inf`.
- Максимальное количество корзин — `log2(1,000,000,000) = 30`.
- Для работы команды должна быть включена расширенная статистика задержек. По умолчанию она включена. Чтобы включить ее явно, используйте `CONFIG SET latency-tracking yes`.
- Чтобы удалить данные гистограмм задержек, используйте команду `CONFIG RESETSTAT`.

Пример:

```text
127.0.0.1:6379> LATENCY HISTOGRAM set
1# "set" =>
   1# "calls" => (integer) 100000
   2# "histogram_usec" =>
      1# (integer) 1 => (integer) 99583
      2# (integer) 2 => (integer) 99852
      3# (integer) 4 => (integer) 99914
      4# (integer) 8 => (integer) 99940
      5# (integer) 16 => (integer) 99968
      6# (integer) 33 => (integer) 100000
```

### latency history {: #latency_history }

```sql
LATENCY HISTORY event
```
<span class="tag">поддерживается с версии 1.0.5</span>
<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Возвращает сырые данные временного ряда всплесков задержки для события
`event`. Команда возвращает до 160 пар «метка времени — задержка» для
указанного события.

Пример:

```text
127.0.0.1:6379> latency history command
1) 1) (integer) 1405067822
   2) (integer) 251
2) 1) (integer) 1405067941
   2) (integer) 1001
```

### latency latest {: #latency_latest }

```sql
LATENCY LATEST
```
<span class="tag">поддерживается с версии 1.0.5</span>
<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Возвращает последние зарегистрированные события задержки.

Подробности:

Каждое событие в ответе содержит следующие поля:

- имя события;
- Unix-метка времени последнего всплеска задержки для события;
- задержка последнего события в миллисекундах;
- максимальная задержка этого события за все время.

Значение «за все время» означает максимальную задержку с момента запуска
экземпляра или с момента сброса событий командой [LATENCY
RESET](#latency_reset).

Пример:

```text
127.0.0.1:6379> latency latest
1) 1) "command"
   2) (integer) 1405067976
   3) (integer) 251
   4) (integer) 1001
```

### latency reset {: #latency_reset }

```sql
LATENCY RESET [event [event ...]]
```
<span class="tag">поддерживается с версии 1.0.5</span>
<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Сбрасывает временные ряды всплесков задержки для всех событий или только для
указанных событий.

Параметры и варианты использования:

- `event [event ...]` — одно или несколько событий задержки, которые нужно сбросить. Если аргумент не указан, сбрасываются все события.

### memory usage {: #memory_usage }

```sql
MEMORY USAGE key [SAMPLES count]
```
<span class="tag">поддерживается с версии 0.6.0</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>

Показывает объём ОЗУ, занимаемый указанным ключом `key`. Параметр
`SAMPLES` позволяет указать число дочерних элементов ключа (если такие
имеются), объём которых также будет учтён. По умолчанию, значение
`SAMPLES` равно 5. Для учёта всех дочерних элементов следует указать
`SAMPLES 0`.

### slowlog get {: #slowlog_get }

```sql
SLOWLOG GET [count]
```
<span class="tag">поддерживается с версии 1.0.5</span>
<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Возвращает записи из медленного журнала в хронологическом порядке.

Параметры и варианты использования:

- `count` — количество последних записей медленного журнала, которые нужно вернуть. Значение `-1` возвращает все записи. По умолчанию возвращается 10 записей.

Подробности:

- Медленный журнал фиксирует запросы, время выполнения которых превысило заданный порог.
- Время выполнения не включает операции ввода-вывода: взаимодействие с клиентом, отправку ответа и подобные действия. Учитывается только время фактического выполнения команды, то есть участок, на котором поток заблокирован и не может обслуживать другие запросы.
- Новая запись добавляется, когда команда превышает порог, заданный параметром конфигурации `slowlog-log-slower-than`.
- Максимальное количество записей в журнале задается параметром `slowlog-max-len`.
- Каждая запись содержит уникальный последовательный идентификатор, Unix-метку времени выполнения команды, длительность выполнения в микросекундах, массив аргументов команды, IP-адрес и порт клиента, а также имя клиента, если оно задано командой [CLIENT SETNAME](#client_setname).
- Уникальный идентификатор записи можно использовать, чтобы не обрабатывать одну и ту же запись несколько раз. Идентификатор не сбрасывается во время работы сервера и сбрасывается только при его перезапуске.

### slowlog help {: #slowlog_help }

```sql
SLOWLOG HELP
```
<span class="tag">поддерживается с версии 1.0.5</span>
<span class="tag slow">slow</span>

Возвращает справку с описанием подкоманд `SLOWLOG`.

### slowlog len {: #slowlog_len }

```sql
SLOWLOG LEN
```
<span class="tag">поддерживается с версии 1.0.5</span>
<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Возвращает текущее количество записей в медленном журнале.

Подробности:

- Новая запись добавляется в медленный журнал, когда команда превышает порог времени выполнения, заданный параметром `slowlog-log-slower-than`.
- Максимальное количество записей в журнале задается параметром `slowlog-max-len`.
- Когда журнал достигает максимального размера, самая старая запись удаляется при добавлении новой.
- Очистить медленный журнал можно командой [SLOWLOG RESET](#slowlog_reset).

### slowlog reset {: #slowlog_reset }

```sql
SLOWLOG RESET
```
<span class="tag">поддерживается с версии 1.0.5</span>
<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag slow">slow</span>

Очищает медленный журнал, удаляя из него все записи. После удаления эти
сведения восстановить нельзя.

### object encoding {: #object_encoding }

```sql
OBJECT ENCODING key
```
<span class="tag">_поддерживается с версии 0.14.0_</span>
<span class="tag keyspace">keyspace</span>
<span class="tag read">read</span>
<span class="tag slow">slow</span>

Возвращает способ кодирования объекта, хранящегося по ключу `key`.
Варианты кодирования:

- `raw` — стандартное кодирование текстовых строк
- `quicklist` — способ кодирования списков, совместимый с типами `linkedlist`, `ziplist` и `listpack` в Redis
- `hashtable` — стандартное кодирование множеств
- `skiplist` — стандартное кодирование сортированных множеств

## Команды для отладки {: #pico_debug }

Команды с префиксом `pico` предназначены для отладки и диагностики
кластера в том случае, если отсутствует возможность подключиться к
Picodata другим способом.

!!! warning "Внимание!"
    Никаких гарантий на форматы вывода нет; любые
    попытки использования данных команд в работе приложения не
    поддерживаются!

### pico status  {: #pico_status }

<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag pico">pico</span>

Отображает состояние команд для отладки (включено/выключено).

### pico enable {: #pico_enable }

<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag pico">pico</span>

Включает использование команд для отладки.

### pico disable {: #pico_disable }

<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag pico">pico</span>

Выключает использование команд для отладки.

### pico sql {: #pico_sql }

<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag pico">pico</span>

Позволяет выполнить SQL-запрос в Redis-консоли.

Пример:

```sql
127.0.0.1:7379> pico sql "SELECT * FROM _pico_user WHERE schema_version=4"
1) "+----+------+----------------+------------------------------------------------+-------+------+"
2) "| id | name | schema_version | auth                                           | owner | type |"
3) "+============================================================================================+"
4) "| 33 | andy | 4              | ['md5', 'md59ee8b0076cf18219f9b2f585f57d4d0c'] | 1     | user |"
5) "+----+------+----------------+------------------------------------------------+-------+------+"
6) "(1 rows)"
```

### pico lua {: #pico_lua}

<span class="tag admin">admin</span>
<span class="tag dangerous">dangerous</span>
<span class="tag pico">pico</span>

Позволяет выполнить Lua-запрос в Redis-консоли.

Пример:

```sql
127.0.0.1:7379> pico lua "return box.cfg.memtx_dir"
"data"
```
