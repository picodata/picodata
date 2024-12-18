#  Изменение конфигурации Picodata

# Проблема

*Есть конфигурационные опции тарантула (memtx, vinyl), есть*
*конфигурационные параметры picodata (рафт параметры, конфигурация
тиров, параметры сети, …), есть параметры, которые валидны только
на старте, есть, которые можно менять между рестартами, а есть,
которые можно менять динамически (без рестарта). Есть параметры
единые для всего кластера, есть параметры, которые можно
задавать для каждой ноды*
*отдельно. Нам нужна одна удобная система которая
позволяла бы задавать и менять параметры.*

Основные артефакты документа - представлены классы параметров
и предложен метод присвоения класса
новому параметру.

# Предлагаемое решение

Дальше по тексту параметры, которые задаются через config file,
или env, или cli на старте/рестарте будем называть задающимися
через рестарт.

## Классы параметров

Предлагается два класса параметров:

- Задающиеся через config file/env/cli
- Задающиеся через ALTER SYSTEM
<!-- - Задающиеся через config file, но
которые можно неперсистентно изменить с помощь
`picodata command_name_tba instance_id memtx.memory=0` -->

Каждый параметр лежит ровно в одном классе.

Тем не менее, для некоторых параметров
имеется возможность временно переопределить значение:
- ALTER SYSTEM
  - параметры из connection string из pgproto, на время сессии
  - параметры из option части sql запроса, на время исполнения запроса
  - параметры, поддержанные в [new cli]
- config file/env/restart
  - параметры, которые могут меняться динамически
<!-- Под предсказуемостью здесь понимается следующее:
может ли операции закончится неудачей?
Например, изменение memtx.memory может
неудачно закончиться, а memtx.checkpoint_count нет-->

### CLI для изменения параметров в рантайме

Original: https://git.picodata.io/core/picodata/-/issues/1163

#### Мотивация

В условиях предложенного дизайна
необходимость такой команды обусловлена следующим:
 - мы бы хотели менять какие параметры со скоупом `instance` в рантайме
 - ALTER SYSTEM не работает со скоупом `instance`, а также требует кворума
 - мы не хотим реализовывать механизм перегрузки конфига.
В целом мы считаем что конфиг предоставляет лишь начальные настройки
 - мы не хотим чтобы пользователи знали про Lua, т.к. завтра его может не быть

Что за задачи решает:
  - отключить постгресовый порт
  - подрезать (добавить) инстансу памяти


#### Требования

Изменения должны носить неперсистентный характер, т.е после рестарта значение параметра
берется из config file/ALTER SYSTEM.

Изменять параметры можно как из класса ALTER SYSTEM, так и из config file.

#### Реализация

```bash
picodata instance pg_listen=null               # параметр из конфиг файла
picodata instance memtx.memory=16gb            # параметр из конфиг файла
picodata instance memtx.checkpoint_interval=1  # параметр из ALTER SYSTEM
```

название команды `instance` не утверждено.


### ALTER SYSTEM

Предлагается новое измерение \- скоуп параметра.
Неформально есть 3 скоупа \- instance, tier, cluster:
- instance \- параметры задающиеся на рестарте
- tier \- параметры для которых общее значение на всех инстансах
тира имеет какое-то значение, также помечены оранжевым цветом в таблице
- cluster \- параметры, значение которых уникально в рамках кластера, также помечены красным в таблице

Параметры ALTER SYSTEM обладают только скоупом tier и cluster.



Значение параметра, обладающего скоупом tier, может быть
выставлено как для конкретного тира, так и
для всех тиров(глобально, для всего кластера).

Для всех параметров есть значение
по умолчанию. Эти значения
[задаются константами](https://git.picodata.io/core/picodata/-/blob/master/src/config.rs?ref_type=heads#L1425) в коде пикодаты и возможности изменить их в обход SQL
нет.

Синтаксис скоупов в ALTER SYSTEM:

- Параметры со скоупом cluster:
  ```sql
  ALTER SYSTEM SET parameter_name = parameter_value
  ```


- Параметры со скоупом tier:

  - для кокретного тира
    ```sql
    ALTER SYSTEM SET parameter_name = parameter_value
    FOR TIER tier_name
    ```
  - для всех тиров

    ```sql
    ALTER SYSTEM SET parameter_name = parameter_value
    FOR ALL TIERS
    ```

### Рестарт параметры

Поведение рестарт параметров не меняется.

Параметры, влияющие на сборку кластера, на наличие кворума и т.п.
предлагается вынести в конфиг.

### Таблица параметров

#### Неизменяемые параметры со скоупом `cluster`

| Название параметра                                                         | Реконфигурация |
| :------------------------------------------------------------------------: | :------------: |
| name                                                                       | Нет            |
| tiers                                                                      | Нет            |
| shredding                                                                  | Нет            |                                                                                                                            |

#### Неизменяемые параметры со скоупом `tier`

| Название параметра                                                         | Реконфигурация |
| :------------------------------------------------------------------------: | :------------: |
| name                                                                       | Нет            |
| replication_factor                                                         | Нет            |
| can_vote                                                                   | Нет            |

#### Неизменяемые параметры со скоупом `instance`

| Название параметра                                                         | Реконфигурация |
| :------------------------------------------------------------------------: | :------------: |
| name                                                                       | Нет            |
| failure_domain                                                             | Нет            |

#### Изменяемые параметры со скоупом `cluster`

| Название параметра                                                         | Реконфигурация      | Новое название параметра                                                                                                   |
| :------------------------------------------------------------------------: | :-----------------: | :------------------------------------------------------------------------------------------------------------------------: |
|                                                                            |                     |                                                                                                                            |
| password_min_length                                                        | alter system        | auth_password_length_min                                                                                                   |
| password_enforce_uppercase                                                 | alter system        | auth_password_enforce_uppercase                                                                                            |
| password_enforce_lowercase                                                 | alter system        | auth_password_enforce_lowercase                                                                                            |
| password_enforce_digits                                                    | alter system        | auth_password_enforce_digits                                                                                               |
| password_enforce_specialchars                                              | alter system        | auth_password_enforce_specialchars                                                                                         |
| max_login_attempts                                                         | alter system        | auth_login_attempt_max                                                                                                     |
| max_pg_statements                                                          | alter system        | pg_statement_max                                                                                                           |
| max_pg_portals                                                             | alter system        | pg_portal_max                                                                                                              |
| snapshot_chunk_max_size                                                    | alter system        | raft_snapshot_chunk_size_max                                                                                               |
| vtable_max_rows                                                            | alter system        | sql_motion_row_max                                                                                                         |
| vdbe_max_steps                                                             | alter system        | sql_vdbe_opcode_max                                                                                                        |
| raft_op_timeout                                                            | alter system + cli  | governor_raft_op_timeout                                                                                                   |
| common_rpc_timeout                                                         | alter system + cli  | governor_common_rpc_timeout                                                                                                |
| plugin_rpc_timeout                                                         | alter system + cli  | governor_plugin_rpc_timeout                                                                                                |
| auto_offline_timeout                                                       | alter system + cli  | governor_auto_offline_timeout                                                                                              |
| snapshot_read_view_close_timeout                                           | alter system + cli  | raft_snapshot_read_view_close_timeout                                                                                      |
| raft_tick                                                                  | alter system + cli  | not implemented https://git.picodata.io/core/picodata/-/issues/1243                                                        |

#### Изменяемые параметры со скоупом `tier`

| Название параметра                                                         | Реконфигурация | Статус                                                                                                                     |
| :------------------------------------------------------------------------: | :------------: | :------------------------------------------------------------------------------------------------------------------------: |
| iproto.max_concurrent_messages                                             | alter system   | НОВОЕ НАЗВАНИЕ iproto_net_msg_max                                                                                          |
| memtx.checkpoint_count                                                     | alter system   | НОВОЕ НАЗВАНИЕ memtx_checkpoint_count                                                                                      |
| memtx.checkpoint_interval                                                  | alter system   | НОВОЕ НАЗВАНИЕ memtx_checkpoint_interval                                                                                   |

#### Изменяемые параметры со скоупом `instance`

| Название параметра                                                         | Реконфигурация       | Статус                                                                                                                     |
| :------------------------------------------------------------------------: | :------------------: | :------------------------------------------------------------------------------------------------------------------------: |
| admin_socket                                                               | config/restart       |                                                                                                                            |
| listen                                                                     | config/restart       | НОВОЕ НАЗВАНИЕ iproto_listen                                                                                               |
| http_listen                                                                | config/restart       |                                                                                                                            |
| pg.listen                                                                  | config/restart + cli | ТАКЖЕ С ПОМОЩЬЮ [new CLI](#cli-для-изменения-параметров-в-рантайме)                                                        |
| pg.ssl                                                                     | config/restart       |                                                                                                                            |
| advertise_address                                                          | config/restart       | НОВОЕ НАЗВАНИЕ iproto_advertise                                                                                            |
| audit                                                                      | config/restart       |                                                                                                                            |
| plugin_dir                                                                 | config/restart       |                                                                                                                            |
| data_dir                                                                   | config/restart       |                                                                                                                            |
| service-password-file                                                      | config/restart       | НОВОЕ НАЗВАНИЕ iproto_password_file https://git.picodata.io/core/picodata/-/issues/1249                                    |
| log.destination                                                            | config/restart       |                                                                                                                            |
| log.format                                                                 | config/restart       |                                                                                                                            |
| log.level                                                                  | config/restart       |                                                                                                                            |
| memtx.memory                                                               | config/restart + cli | ТАКЖЕ С ПОМОЩЬЮ [new CLI](#cli-для-изменения-параметров-в-рантайме)                                                        |
| vinyl.cache                                                                | config/restart + cli | ТАКЖЕ С ПОМОЩЬЮ [new CLI](#cli-для-изменения-параметров-в-рантайме)                                                        |
| vinyl.memory                                                               | config/restart + cli | ТАКЖЕ С ПОМОЩЬЮ [new CLI](#cli-для-изменения-параметров-в-рантайме)                                                        |

### Приоритеты применения параметров

Приоритеты следующие:

* statement options ([параметры](https://docs.picodata.io/picodata/stable/reference/sql/non_block/#examples) из option части запроса)
* connect options (сессионные параметры, пока что
задаются только в [connection string](https://git.picodata.io/core/picodata/-/merge_requests/1443) для pgproto)
* ALTER SYSTEM
* cli
* env
* config file

Не все параметры из конфиг файла можно
переопределить с помощью env/cmdline.



# Недостатки

Почти любое измененеие параметра не из ALTER
SYSTEM это рестарт инстанса.

ALTER SYSTEM требует кворума, поэтому
в случае его утраты не совсем ясно что делать.
В оправдание можно сказать, что параметры которые
имеют значение в таких ситуациях вынесены в config file.

В конфиг файле остаются некоторые параметры,
которые было бы круто уметь менять например на всем тире -
memtx.memory, vinyl.memory.

Возможна ситуация, когда на всех инстансах выставлены разные важных
параметров, типо таймаутов. Мы это не контролируем.

# Другие предложения

## Переименование названия конфига

Название конфига `config.yaml` -> `picodata.yaml`.

## Переименование параметров

Naming guideline:

- min, max, size, count указываются в конце имени
- единицы измерения не пишем, а считаем по умолчанию в секундах/байтах
- даем возможность задать юниты в значении переменной
- используем префиксное дерево в именовании переменных, т.е. имя системы_имя подсистемы_значение
- используем подчеркивания в качестве разделителя, для cmdline и то, и другое (дефис и подчеркивание)


## Ограничения на значения

Каждый конфигурационный параметр имеет допустимый ренж значений, а некоторые из параметров допускают только монотонные изменения (например memtx\_memory можно только повышать). Эти условия должны задаваться на этапе компиляции и проверяться в рантайме для любого изменения параметра.
[https://git.picodata.io/core/picodata/-/issues/907](https://git.picodata.io/core/picodata/-/issues/907)

# Рассмотренные альтернативы

## Упразднение env, cli, _pico_db_config переменных в пользу конфигурационного файла

Конфигурация происходит через config file.
Таблица _pico_db_config упраздняется, поэтому
ALTER SYSTEM тоже пишет в файл.
Так делают в TiKV.

## Иметь параметры задающиеся и через рестарт и изменяющиеся, и через ALTER SYSTEM

Сложно следить за консистентностью.

## Разделить конфиг на bootstrap и mutable части

Усложняет основной пользовательский сценарий. Разрушит
существующую ansible роль.


# Существующие решения

## TiKV

TiKV позволяет изменять с помощью sql значения из конфиг файла, в таком случае сам конфиг файл перезаписывается с новым значением:
*After dynamically changing TiKV configuration items, the TiKV configuration file is automatically updated.*

При модификации через sql также можно выбрать изменить на конкретном инстансе или на всех.

Links:

1. [Modifying through SQL](https://docs.pingcap.com/tidb/stable/dynamic-config#modify-tikv-configuration-dynamically)
2. [Config file params](https://tikv.org/docs/7.1/deploy/configure/tikv-configuration-file/)

## Clickhouse

Глобальные серверные настройки задаются только через файл.

*There are two main groups of ClickHouse settings:*

- *Global server settings*
- *Query-level settings / User-level settings*

*The main distinction between global server settings and query-level settings is that global server settings must be set in configuration files, while query-level settings can be set in configuration files or with SQL queries.*
[Link](https://clickhouse.com/docs/en/operations/settings)

[Пользовательские настройки](https://clickhouse.com/docs/en/operations/settings/query-level) могут задаваться через файл, при создании пользователя и на уровне сессии \- для настроек в файле [указывается их приоритет](https://clickhouse.com/docs/en/operations/configuration-files#merging)

## Cockroach

*In contrast to cluster-wide settings, node-level settings apply to a single node. They are defined by flags passed to the cockroach start command when starting a node and cannot be changed without stopping and restarting the node.*

Node level настройки задаются при старте ноды и не меняются до её перезапуска. Кластерные настройки задаются через sql.
[Link](https://www.cockroachlabs.com/docs/v24.3/cluster-settings)

### Others:

[https://www.postgresql.org/docs/current/sql-show.html](https://www.postgresql.org/docs/current/sql-show.html)
[https://ydb.tech/docs/ru/maintenance/manual/dynamic-config-volatile-config](https://ydb.tech/docs/ru/maintenance/manual/dynamic-config-volatile-config)
