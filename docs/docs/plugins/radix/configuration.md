# Настройка плагина {: #configuration }

Для настройки плагина используйте файл конфигурации, который можно
применить к плагину с помощью команды [`picodata plugin
configure`] или [инвентарного файла Ansible].

Пример файла конфигурации:

```yaml
radix:
  clients: # ограничения клиентских соединений
    max_clients: 10000
    max_input_buffer_size: 1073741824
    max_output_buffer_size: 1073741824
  redis_compatibility: # совместимость с разными версиями Redis
    enabled_deprecated_commands: []
    enforce_one_slot_transactions: false
    push_result_includes_popped_items: true
    disable_scatter_gather: true
    include_radix_section_in_info_by_default: true
  cluster_mode: true
  sentinel_enabled: false
  max_defer_actions_per_iteration: 100
  authorization_mode: # авторизация
    state: enabled
    default_user_name: default
  eviction: # вытеснение ключей
    policy: volatile-ttl
    max_samples: 5
    lfu_decay_time: 60
    lfu_log_factor: 10
    watermark: 0.81
    tenacity: 10
  latency_monitor_threshold_ms: 0 # порог мониторинга задержек (мс), 0 = выключено
  slowlog_log_slower_than_us: 10000 # порог slowlog (мкс), -1 = выключено
  slowlog_max_len: 128 # макс. записей в slowlog
  debug: # отладка
    pico_commands_enabled: true
```

[`picodata plugin configure`]: ../../reference/cli.md#plugin_configure
[инвентарного файла Ansible]: ../../admin/deploy_ansible.md#plugin_management

Пример команды, применяющей файл конфигурации:

```shell
picodata plugin configure --peer andy@127.0.0.1:3001 --service-password-file radix/secret.txt radix 1.1.1 radix/plugin_config.yaml
```

## Авторизация и управление доступом {: #auth_and_access_control}

Radix поддерживает авторизацию под пользователем по умолчанию с заранее
заданным паролем (аналог `requirepass` в Redis). Указать пользователя по
умолчанию можно в секции `authorization_mode` в файле конфигурации
плагина. Пользователь по умолчанию имеет полный набор прав на работу с
объектами СУБД.

!!! warning "Внимание!"
    В версии Radix 1.0.5 имеются ограничения: для пользователя по умолчанию
    обязательно должен быть установлен [пустой пароль](#empty_password).

Для управления правами пользователей в Radix применяется ACL (Access
Control List) — система прав пользователей, в которой можно задать, к
каким командам или категориям команд, ключам и каналам имеет доступ
каждый пользователь.

Настройка пользователя происходит в два этапа:

- создание пользователя на стороне Picodata и выдача необходимых прав на доступ к данным Radix
- задание прав на стороне Radix с помощью системы ACL в Redis.

### Предустановленные роли {: #auth_roles }

После выполнения [миграций](install.md#plugin_migrate) Radix в SQL-интерфейсе Picodata становятся
доступны следующие роли::

- Глобальные:
    - `radix_reader` — доступ на чтение ко всем данным,
    - `radix_writer` — доступ на запись ко всем данным.
- Локальные для каждой БД:
    - `radix_reader_0` … `radix_reader_15`
    - `radix_writer_0` … `radix_writer_15`

### Настройка пользователя для авторизации {: #default_user_name }

В Radix 1.0.6 и новее необходимо указать пользователя и задать ему пароль,
удовлетворяющий определённым [требованиям]. Укажите пользователя по умолчанию в
секции `authorization_mode` в файле конфигурации плагина.

!!! caution "Внимание!"
    В версии Radix 1.0.5 пользователь по умолчанию без пароля
    обязан существовать, однако он может не иметь прав на доступ к каким-либо
    данным. См. раздел [Пользователи с пустыми паролями](#empty_password).

[требованиям]: ../../admin/access_control.md#allowed_passwords

Для указания пользователя по умолчанию используйте параметр
`default_user_name` в конфигурации плагина. От
имени этого пользователя будут выполняться команды без введенной команды
авторизации (аналог пользователя `default` в Redis). Кроме того, если
команда авторизации вызвана в формате формате `AUTH <password>`
(например, при работе старых клиентов) будет выполнена попытка
авторизации этого пользователя с паролем из команды.

По умолчанию значение `default_user_name` задано в файле `manifest.yaml`
плагина как `default` и может быть переопределено в файле
конфигурации плагина:

```yaml title="Фрагмент файла конфигурации Radix"
services:
  - name: radix
    description: Redis protocol implementation
    default_configuration:
      ...
      authorization_mode:
        state: enabled
        default_user_name: default
```

Пользователь должен существовать в Picodata и иметь возможность
использовать [нужные роли](#auth_roles):

```sql
CREATE USER IF NOT EXISTS default WITH PASSWORD 'S0mePass' OPTION(TIMEOUT=1200);
GRANT radix_reader TO default;
GRANT radix_writer TO default;
```

Теперь авторизация включена. Можно войти с помощью `AUTH <password>`:

```shell
$ redis-cli -p 7301
127.0.0.1:7301> set other-key 123
(error) NOAUTH Authentication required
127.0.0.1:7301> auth S0mePass
OK
127.0.0.1:7301> get other-key
(nil)
127.0.0.1:7301> set other-key 321
OK
127.0.0.1:7301> get other-key
"321"
```

И с помощью `AUTH <user_name> <password>`:

```shell
$ redis-cli -p 7301
127.0.0.1:7301> get some-key
(error) NOAUTH Authentication required
127.0.0.1:7301> auth default S0mePass
OK
127.0.0.1:7301> get some-key
(nil)
```

#### Пользователи с пустыми паролями {: #empty_password }

Для того, чтобы выполнять команды без предварительного вызова команды
`AUTH` (аналог пользователя `default` в Redis без задания пароля в
параметре `requirepass` его конфигурации), можно создать необходимого
пользователя с пустым паролем с помощью следующей последовательности
действий:

1. Временно отключите политики безопасности пароля Picodata:
  ```sql
  ALTER SYSTEM SET auth_password_length_min TO 0;
  ALTER SYSTEM SET auth_password_enforce_uppercase TO FALSE;
  ALTER SYSTEM SET auth_password_enforce_lowercase TO FALSE;
  ALTER SYSTEM SET auth_password_enforce_digits TO FALSE;
  ```

2. Создайте необходимого пользователя и задайте ему права использования
   нужных ролей (Radix должен быть установлен, но необязательно
   запущен):
  ```sql
  CREATE USER IF NOT EXISTS default WITH PASSWORD '';
  GRANT radix_reader TO default;
  GRANT radix_writer TO default;
  ```

3. Верните политики безопасности пароля Picodata:
  ```sql
  ALTER SYSTEM SET auth_password_enforce_digits TO TRUE;
  ALTER SYSTEM SET auth_password_enforce_lowercase TO TRUE;
  ALTER SYSTEM SET auth_password_enforce_uppercase TO TRUE;
  ALTER SYSTEM SET auth_password_length_min TO 8;
  ```

!!! warning "Внимание!"
    В версии Radix 1.0.5 пользователю по умолчанию можно задать
    только пустой пароль. Для других пользователей такого ограничения нет.

### Переход на Radix при использовании ACL {: #migrate_acl }

Если у вас есть инсталляция Redis с настроенными правами (ACL), которые
необходимо перенести в Radix, то для этого потребуется отдельный набор
действий.

Для начала убедитесь, что на данном этапе в кластере Picodata уже
выполнены необходимые предварительные настройки для Radix, включая:

- [Добавление плагина в кластере](install.md#plugin_add)
- [Добавление сервиса и установка параметров](install.md#plugin_enable_details)
- [Запуск миграции](install.md#plugin_migrate)

Предположим, что сам плагин Radix ещё не включён. Выполните следующие шаги:

1. Создайте файл с правами ACL в Redis до начала миграции и отредактируйте его
1. [Создайте пользователей], указанных в этом файле, в Picodata
1. Задайте расположение этого файла в параметре `aclfile` в конфигурации плагина Radix
1. Запустите Radix
1. Загрузите файл с правами с помощью команды `ACL LOAD`

[Создайте пользователей]: ../../admin/access_control.md#create_user

Разберём эти шаги подробнее.

**Создание файла с правами**

Пусть файл с ACL-правами лежит по пути `/tmp/users.acl`. Он получен командой `ACL SAVE` в Redis.

**Проверка и редактирование файла с правами**

Посмотрим на содержание файла с правами:

```acl title="Пример файла `/tmp/users.acl`"
user cache off sanitize-payload ~cache:* resetchannels -@all +get +set (~some-key resetchannels -@all +set)
user default on nopass sanitize-payload ~* &* +@all
user epic-user off sanitize-payload #42a9798b99d4afcec9995e47a1d246b98ebc96be7a732323eee39d924006ee1d ~this-key resetchannels -@all +get
user pubsub on sanitize-payload #4cb5461c1904e3be5b941a47fba6c3afdf97a6f8f2712207eb8226443fa62f81 resetchannels &channels:* -@all +subscribe
```

Так как пользователи создаются на уровне Picodata, этот файл нужно
проверить и отредактировать:

- Пользователю `default` в Redis соответствует пользователь, указанный в
  параметре `authorization_mode.default_user_name` [конфигурации
  Radix](#default_user_name) (по умолчанию это также `default`). Если вы
  меняли значение `authorization_mode.default_user_name` на собственное,
  то нужно будет указать его вместо `default` в файле с правами.
- из файла с правами надо удалить всё, что связано с паролями (строки с
  префиксами `>`, `<`, `#`, `!` и строка `nopass`). Radix рассчитан на
  применение в корпоративной среде и не позволяет передавать пароли пользователей
  через незашифрованный файл на диске. Для управления пользователями и их паролями
  рекомендуется использовать централизованное хранилище (например, LDAP, для
  синхронизации с которым Picodata предлагает плагин [Argus](../argus.md)).
- Метки включения пользователя (`off` и `on`) задаются в Picodata — их
  тоже нужно убрать из файла с правами.

Если пробовать загрузить этот файл без необходимых правок,
Radix будет возвращать ошибки c подсказками по их устранению:

???+ example "Пользователь не создан"
    ```
    127.0.0.1:7301> acl load
    (error) ERR users.acl:1: user cache not found
    ```

???+ example "Не удалены спецификаторы пароля"
    ```
    127.0.0.1:7301> acl load
    (error) ERR users.acl:1: change user password is prohibited, use Picodata SQL for it (https://docs.picodata.io/picodata/stable/reference/sql/ter_user/#examples)
    ```

???+ example "Не удалены метки включения"
    ```
    127.0.0.1:7301> acl load
    (error) ERR users.acl:1: change user status is prohibited, use Picodata SQL for it (https://docs.picodata.io/picodata/stable/reference/sql/ter_user/#examples)
    ```

После исправлений файл `/tmp/users.acl` станет выглядеть так:

```
user cache sanitize-payload ~cache:* resetchannels -@all +get +set (~some-key resetchannels -@all +set)
user default sanitize-payload ~* &* +@all
user epic-user sanitize-payload ~this-key resetchannels -@all +get
user pubsub sanitize-payload resetchannels &channels:* -@all +subscribe
```

Теперь создайте пользователей в Picodata и выдайте им необходимые роли:

```sql
CREATE USER cache WITH PASSWORD 'cachePassword123' OPTION(TIMEOUT=1200);
GRANT radix_reader TO cache OPTION(TIMEOUT=1200);
GRANT radix_writer TO cache OPTION(TIMEOUT=1200);
-- пользователь выключен (`user cache off ...`) в users.acl, выключим его и здесь;
ALTER USER cache NOLOGIN;

CREATE USER default WITH PASSWORD 'S0mePass' OPTION(TIMEOUT=1200);
GRANT radix_reader TO default OPTION(TIMEOUT=1200);
GRANT radix_writer TO default OPTION(TIMEOUT=1200);

CREATE USER "epic-user" WITH PASSWORD 'Password8' OPTION(TIMEOUT=1200);
GRANT radix_reader TO "epic-user" OPTION(TIMEOUT=1200);
GRANT radix_writer TO "epic-user" OPTION(TIMEOUT=1200);

CREATE USER pubsub WITH PASSWORD 'PUBSUBPass0' OPTION(TIMEOUT=1200);
GRANT radix_reader TO pubsub OPTION(TIMEOUT=1200);
GRANT radix_writer TO pubsub OPTION(TIMEOUT=1200);
```

**Включение авторизации**

Включите в Radix авторизацию, указав путь к файлу с правами и имя пользователя по умолчанию:

```sql
ALTER PLUGIN radix 1.1.1 SET radix.authorization_mode='{ "state": "enabled", "aclfile": "/tmp/users.acl", "default_user_name": "default" }';
```

**Включение плагина**

Включите плагин Radix в Picodata:

```sql
ALTER PLUGIN radix 1.1.1 ENABLE;
```

При первом запуске заданному по умолчанию пользователю выдается ACL на
доступ ко всем командам Radix, всем ключам и каналам (`+@all ~* &*`).
Этот пользователь имеет возможность загрузить права из файла в кластере
с включённой авторизацией.

**Загрузка прав**

Подключитесь к Radix и загрузите права:

```shell
$ redis-cli -p 7301127.0.0.1:7301> auth S0mePass
OK
127.0.0.1:7301> acl whoami
"default"
127.0.0.1:7301> acl list
1) "user cache off sanitize-payload resetchannels -@all"
2) "user default on sanitize-payload ~* &* +@all"
3) "user epic-user on sanitize-payload resetchannels -@all"
4) "user pubsub on sanitize-payload resetchannels -@all"
127.0.0.1:7301> acl load
OK
127.0.0.1:7301> acl list
1) "user cache off sanitize-payload ~cache:* resetchannels -@all +get +set (~some-key resetchannels -@all +set)"
2) "user default on sanitize-payload ~* &* +@all"
3) "user epic-user on sanitize-payload ~this-key resetchannels -@all +get"
4) "user pubsub on sanitize-payload &channels:* -@all +subscribe"
```

По выводу первого `ACL LIST` можно увидеть, что пользователи, кроме
пользователя по умолчанию, создаются без прав, а после `ACL LOAD` права
соответствуют настройкам из `users.acl`.

Проверьте права пользователей (для их обновления нужно закончить текущую
сессию и запустить новую):

```shell
$ redis-cli -p 7301
127.0.0.1:7301> get this-key
(error) NOAUTH Authentication required
127.0.0.1:7301> auth default S0mePass
OK
127.0.0.1:7301> get this-key
(nil)
127.0.0.1:7301> set this-key hello!
OK
127.0.0.1:7301> get this-key
"hello!"
127.0.0.1:7301> auth epic-user Password8
OK
127.0.0.1:7301> get this-key
"hello!"
127.0.0.1:7301> get other-key
(error) NOPERM No permissions to access a key
127.0.0.1:7301> subscribe channels:123
(error) NOPERM User epic-user has no permissions to run the 'subscribe' command
127.0.0.1:7301> auth cache cachePassword123
(error) WRONGPASS invalid username-password pair or user is disabled.
127.0.0.1:7301> auth pubsub PUBSUBPass0
OK
127.0.0.1:7301> subscribe wrong-chan:123
(error) NOPERM No permissions to access a channel
127.0.0.1:7301> subscribe channels:123
1) "subscribe"
2) "channels:123"
3) (integer) 1
Reading messages... (press Ctrl-C to quit or any key to type command)
```

Права пользователей хранятся в Picodata, так что после перезапуска не
нужно будет выставлять их заново, как в Redis. Поэтому опциональный
параметр `aclfile` можно в дальнейшем опустить.

### Разделение доступов по БД {: #access_separation }

Представим, что одно приложение пишет и читает какие-то
данные в БД №0 Redis и два других приложения, которые имеют отдельные
БД (например, №2 и №5) в Redis, но при этом читают данные БД №0:

- Приложение 1:
  - БД 0, чтение и запись.
- Приложение 2:
  - БД 0, чтение.
  - БД 2, чтение и запись.
- Приложение 3:
  - БД 0, чтение.
  - БД 5, чтение и запись.

Будем считать, что [пользователем по умолчанию](#default_user_name) является `default` с полным доступом.

```sql title="создайте пользователя для Приложения 1"
CREATE USER app_1_user WITH PASSWORD 'S0m1Str2ngP3ssword-1' OPTION(TIMEOUT=1200);
GRANT radix_reader_0 TO app_1_user OPTION(TIMEOUT=1200);
GRANT radix_writer_0 TO app_1_user OPTION(TIMEOUT=1200);
```

```sql title="создайте пользователя для Приложения 2"
CREATE USER app_2_user WITH PASSWORD 'S0m1Str2ngP3ssword-2' OPTION(TIMEOUT=1200);
GRANT radix_reader_0 TO app_2_user OPTION(TIMEOUT=1200);
GRANT radix_reader_2 TO app_2_user OPTION(TIMEOUT=1200);
GRANT radix_writer_2 TO app_2_user OPTION(TIMEOUT=1200);
```

```sql title="создайте пользователя для Приложения 3"
create USER app_3_user WITH PASSWORD 'S0m1Str2ngP3ssword-3' OPTION(TIMEOUT=1200);
GRANT radix_reader_0 TO app_3_user OPTION(TIMEOUT=1200);
GRANT radix_reader_5 TO app_3_user OPTION(TIMEOUT=1200);
GRANT radix_writer_5 TO app_3_user OPTION(TIMEOUT=1200);
```

Выдайте права пользователям:

```shell
$ redis-cli -p 7301
127.0.0.1:7301> auth default S0mePass
OK
127.0.0.1:7301> acl setuser app_1_user +@all ~cache:*
OK
127.0.0.1:7301> acl setuser app_2_user -@all (+get %R~cache:*) (+@all %RW~app2:*)
OK
127.0.0.1:7301> acl setuser app_3_user +@all %R~cache:* %RW~app3:*
OK
```

Проверьте, что приложение 1 может читать и писать в свою БД:

```shell
$ redis-cli -p 7301
127.0.0.1:7301> auth app_1_user S0m1Str2ngP3ssword-1
OK
127.0.0.1:7301> set cache:12345 'hi from user 1!'
OK
127.0.0.1:7301> get cache:12345
"hi from user 1!"
127.0.0.1:7301> select 2
(error) INSUFFICIENT PRIVILEGES no required rights assigned
127.0.0.1:7301> select 5
(error) INSUFFICIENT PRIVILEGES no required rights assigned
127.0.0.1:7301> lrange cache:list-key 0 -1
(empty array)
```

Как видно, у приложения имеется полный доступ к своим данным, но нет доступа к данным других приложений. Посмотрим на приложение 2:

```shell
$ redis-cli -p 7301
127.0.0.1:7301> auth app_2_user S0m1Str2ngP3ssword-2
OK
127.0.0.1:7301> get cache:12345
"hi from user 1!"
127.0.0.1:7301> set cache:12345 'hello, user 1, this is user 2!'
(error) NOPERM No permissions to access a key
127.0.0.1:7301> get cache:12345
"hi from user 1!"
127.0.0.1:7301> lrange cache:list-key 0 -1
(error) NOPERM No permissions to access a key
127.0.0.1:7301> select 2
OK
127.0.0.1:7301[2]> get cache:12345
(nil)
127.0.0.1:7301[2]> set app2:data my_data
OK
127.0.0.1:7301[2]> get app2:data
"my_data"
127.0.0.1:7301[2]> select 5
(error) INSUFFICIENT PRIVILEGES no required rights assigned
```

Второе приложение может прочитать данные первого, свои данные, но не
может прочитать данные третьего приложения. Попытка записи в данные
первого падает с ошибкой, а в свои — нет. Для третьего пользователя
результат будет аналогичным.

### Примеры управления доступом {: #auth_examples }

#### Выдача прав пользователю в Radix {: #grant_rights }

```shell
$ redis-cli -p 7301
127.0.0.1:7301> acl setuser custom_user ~* &* +@all
OK
```

#### Изменение конфигурации Radix через SQL-запросы в Picodata {: #setup_radix_with_sql }

```sql
ALTER PLUGIN radix 1.1.1 SET radix.authorization_mode='{"state": "enabled", "default_user_name": "custom_user"}';
```

#### Использование LDAP {: #ldapuser }

```sql
CREATE USER custom_user USING ldap;
GRANT radix_reader TO custom_user;
GRANT radix_writer TO custom_user;
ALTER PLUGIN radix 1.1.1 SET radix.authorization_mode = '{ "state": "enabled", "default_user_name": "custom_user" }';
```

#### Использование Argus для синхронизации пользователей {: #argus }

```yaml
argus:
  searches:
    - role: "radix_reader"
      base: "dc=example,dc=org"
      filter: "<filter>"
      attr: "cn"
    - role: "radix_writer"
      base: "dc=example,dc=org"
      filter: "<filter>"
      attr: "cn"
```

## Настройка адресов и TLS {: #tls_settings }

Начиная с версии 1.0.0 настройка адресов производится в [конфигурационном файле] инстанса Picodata:

```yaml
instance:
  plugin:
    radix:
      service:
        radix:
          listener:
            enabled: true
            listen: "0.0.0.0:7379"                 # Radix откроет сокет по указанному адресу и будет его слушать
            advertise: "localhost:7379"            # Radix будет использовать этот адрес в кластерных и sentinel-командах
            tls:
              enabled: false
```

!!! warning "Внимание!"
    Убедитесь, что на каждом узле кластера для слушающего сокета Radix
    указаны разные значения. Это необходимо для того, чтобы избежать конфликтов
    портов.

Пример с включённым TLS:

```yaml
instance:
  plugin:
    radix:
      service:
        radix:
          listener:
            enabled: true
            listen: "0.0.0.0:7379"
            advertise: "localhost:7379"
            tls:
              enabled: true
              cert_file: tls/server.crt
              key_file: tls/server.key
              ca_file: tls/ca.crt
```
[конфигурационном файле]: ../../reference/config.md

Для генерации сертификатов воспользуйтесь [инструкцией]. По умолчанию будет включён mTLS, то есть клиенту необходимо предоставлять клиентские сертификаты тоже:

```shell
redis-cli --tls --cert tls/client.crt --key tls/client.key --cacert tls/ca.crt -p 7379 incr asdf2
```

[инструкцией]: ../../admin/ssl.md/#create_certs_and_keys

Если вы хотите просто использовать TLS без клиентских сертификатов, то
следует **не указывать** `ca_file` в файле конфигурации Picodata и
**указывать** его в конфигурации клиента.

```yaml
instance:
  plugin:
    radix:
      service:
        radix:
          listener:
            enabled: true
            listen: "0.0.0.0:7379"
            advertise: "localhost:7379"
            tls:
              enabled: true
              cert_file: tls/server.crt
              key_file: tls/server.key
              # ca_file: tls/ca.crt
```

```shell
redis-cli --tls --cacert tls/ca.crt -p 7379 incr asdf2
```
