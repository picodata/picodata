# Работа с сертифицированной версией {: #certified }

Данный раздел описывает особенности установки и настройки версий плагина
Radix, сертифицированных ФСТЭК. Информация, изложенная для этих версий
ниже, является приоритетной по сравнению с остальными разделами
документации Radix.

## Radix 1.0.5 {: #1.0.5 }

### Установка {: #1.0.5_install }

Особенность данной версии состоит в том, что перед включением плагина
нужно создать пользователя, который указан в настройке
`authorization_mode.default_user_name`. По умолчанию этот пользователь
называется `default`. Если его не создать, то плагин не сможет
включиться. Эта проблема была исправлена в более поздних версиях. После
включения плагина, пользователю нужно будет выдать роли `radix_reader` и
`radix_writer`.

Рассмотрим эти шаги подробнее. Для примера используем роль [Ansible для
Picodata](https://github.com/picodata/picodata-ansible).

Первым делом убедитесь что вы используете роль Ansible версии 26.2.1 или более новую.

Обратите внимание на изменения в роли:

- параметр `sql_file` переименован в  `sql_file_pre_plugins`. В этом
параметре указан файл с SQL-командами, выполняемыми до активации
плагинов
- появился параметр `sql_file_post_plugins` — с файлом, выполняемым
после включения плагинов.

Если необходимо поменять имя пользователя c `default` на, к примеру,
`radixuser1`, то его нужно будет поменять в
`files/radix_plugin_config.yml`и `files/pre_plugin.sql`. В данном
примере оставлено имя по умолчанию.

Другая особенность 1.0.5 — пользователь по умолчанию должен иметь
**пустой пароль**. Чтобы создать такого пользователя, нужно временно
ослабить парольные политики. После создания нужно вернуть их обратно.
Если требуется, чтобы доступ в Radix всё-таки был защищён паролем — нужно
создать ещё одного пользователя, после включения плагина
выдать новому пользователю все права, а у пользователя по умолчанию их изъять.

Рассмотрим содержимое файлов, которые будут использоваться для развёртывания Radix.

`files/pre_plugin.sql` — создаём пользователя _до_ включения плагина

```sql
  ALTER SYSTEM SET auth_password_length_min TO 0;
  ALTER SYSTEM SET auth_password_enforce_uppercase TO FALSE;
  ALTER SYSTEM SET auth_password_enforce_lowercase TO FALSE;
  ALTER SYSTEM SET auth_password_enforce_digits TO FALSE;
  CREATE USER IF NOT EXISTS default WITH PASSWORD '' OPTION(TIMEOUT=1200);
  ALTER SYSTEM SET auth_password_enforce_digits TO TRUE;
  ALTER SYSTEM SET auth_password_enforce_lowercase TO TRUE;
  ALTER SYSTEM SET auth_password_enforce_uppercase TO TRUE;
  ALTER SYSTEM SET auth_password_length_min TO 8;
```

`files/post_plugin.sql` — назначаем нужные права пользователю по умолчанию в Radix

```sql
GRANT radix_reader TO default;
GRANT radix_writer TO default;
```

`files/radix_plugin_config.yml` — указываем имя пользователя по умолчанию для Radix

```yaml
radix:
  authorization_mode:
    state: enabled
    default_user_name: default
```

Содержимое инвентарного файла:

```yaml

    sql_file_pre_plugins: files/pre_plugin.sql
    sql_file_post_plugins: files/post_plugin.sql
    tiers:
      default:
        replicaset_count: 1
        replication_factor: 3
        can_vote: true
        config:
          memtx:
            memory: 2G
        bucket_count: 16384 # обязательная опция для radix

    plugins:
      radix:
        path: files/radix_1.0.5-astra_1.8_x86-64.tar.gz
        config: files/radix_plugin_config.yml
        tiers:
          - default
        services:
          radix:
            tiers:
              - default:
                  listener:
                    enabled: true
                    listen: "0.0.0.0:73<INSTANCE_NUM>"
                    advertise: "<INSTANCE_ADDR>:73<INSTANCE_NUM>"
                    tls:
                      enabled: false

        migration_context:
          tier_for_db_0: 'default'
          tier_for_db_1: 'default'
          tier_for_db_2: 'default'
          tier_for_db_3: 'default'
          tier_for_db_4: 'default'
          tier_for_db_5: 'default'
          tier_for_db_6: 'default'
          tier_for_db_7: 'default'
          tier_for_db_8: 'default'
          tier_for_db_9: 'default'
          tier_for_db_10: 'default'
          tier_for_db_11: 'default'
          tier_for_db_12: 'default'
          tier_for_db_13: 'default'
          tier_for_db_14: 'default'
          tier_for_db_15: 'default'
          unlogged: ''
```

После установки с этим инвентарным файлом можно создать нового пользователя для Radix и ограничить пользователя `default`. Для примера назовём нового пользователя `radix_default`.

Выполните следующие команды в SQL-консоли Picodata:

```sql
CREATE USER IF NOT EXISTS radix_default WITH PASSWORD 'mypassword1' OPTION(TIMEOUT=1200);
GRANT radix_reader TO radix_default;
GRANT radix_writer TO radix_default;
```

Теперь через `redis-cli` выдайте все права `radix_default` и заберите их у `default`:

```shell
redis-cli -h localhost -p 7300

acl setuser radix_default +@all ~* &*
acl setuser default -@all resetkeys resetchannels
```

### Аутентификация клиентов {: #1.0.5_auth }

При аутентификации в Radix 1.0.5 присутствует ошибка, которая была исправлена в более новых версиях `1.0.6` и `1.1.0`.

**Симптом:** Клиент передаёт имя пользователя и пароль при подключении, но команда выполняется от имени `default`:

```
$ redis-cli -h 127.0.0.1 -p 7300 --user myuser1 -a 'mypassword1' get key1
AUTH failed: INSUFFICIENT PRIVILEGES no required rights assigned
(error) NOPERM User default has no permissions to run the 'get' command
```

Вместо `INSUFFICIENT PRIVILEGES` может быть `WRONGPASS invalid
username-password pair or user is disabled.` Ошибка в данном случае
выдаётся из-за того, что у пользователя `default` отобраны все права.

**Причина:** В 1.0.5 команда `AUTH` отклоняется, если это **первая
команда в соединении**. Если до `AUTH` была любая другая команда, `AUTH`
работает. Ошибка исправлена в версии 1.0.6.

**Кого затрагивает:**

- `redis-cli` с параметрами `--user` / `-a`: он отправляет `AUTH` сразу
  после подключения
- приложения и библиотеки, которые отправляют `AUTH` первой командой

Ошибка не затрагивает интерактивный `redis-cli` и клиентов, которые при
подключении сначала отправляют `HELLO` (go-redis, lettuce, node-redis и
др.).

**Обход на 1.0.5:** Сначала отправить любую команду, потом `AUTH`, например в интерактивном `redis-cli`:

```shell
$ redis-cli -h 127.0.0.1 -p 7300
127.0.0.1:7300> auth myuser1 mypassword1
OK
127.0.0.1:7300> get key1
"value1"
```
