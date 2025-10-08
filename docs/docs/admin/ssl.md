# Безопасность кластера {: #cluster_security }

## Использование SSL {: #enable_ssl }

При подключении к Picodata рекомендуется использовать протокол [SSL],
который обеспечивает безопасную коммуникацию между клиентом (окном
терминала или клиентским API) и сервером ([инстансом] Picodata).

[SSL]: https://ru.wikipedia.org/wiki/SSL
[инстансом]: ../overview/glossary.md#instance

Picodata поддерживает SSL при подключении по протоколу PostgreSQL.
Безопасное соединение задействуется автоматически если оно
поддерживается (и [настроено](#create_certs_and_keys)) на стороне
инстанса Picodata.

!!! note "Примечание"
    Если в конфигурации инстанса включен параметр
    [instance.pg.ssl], то небезопасные соединения (без SSL) при этом
    становятся запрещены.

Пример строки подключения с явным запросом безопасного режима:

```shell
psql postgres://admin:qazWSX123@192.168.101.166:5001?sslmode=require
```

См. также:

- [Режимы sslmode](../dev/connectors/jdbc.md#sslmode)

## Настройки при LDAP-аутентификации {: #enable_ssl_for_ldap }

При использовании аутентификации [LDAP] рекомендуется включить SSL
для того, чтобы пароль пользователя не передавался по сети в открытом
виде.

[LDAP]: ldap.md
[instance.pg.ssl]: ../reference/config.md#instance_pg_ssl

Чтобы настроить использование SSL, выполните следующие шаги:

1. Задайте в [файле конфигурации](../reference/config.md#instance_pg_ssl)
    параметр `instance.pg.ssl: true`

1. Добавьте в [рабочую директорию инстанса](../reference/cli.md#run_instance_dir)
    `<INSTANCE_DIR>` SSL-сертификат и ключ `server.crt`, `server.key`

1. (опционально) Для включения [mTLS] добавьте в
    [рабочую директорию инстанса](../reference/cli.md#run_instance_dir) `<INSTANCE_DIR>`
    SSL-сертификат `ca.crt`. В результате PostgreSQL-сервер в Picodata будет
    принимать подключения только в том случае, если клиент предоставит сертификат,
    подписанный с помощью `ca.crt`.

## Создание сертификатов и ключей {: #create_certs_and_keys }

Ниже показаны примеры команд для генерации самоподписанного сертификата
(публичного ключа) и закрытого ключа как для сервера (Picodata), так и
для клиентов, которые хотят подключиться к серверу. Для этих команд
используется консольное приложение `openssl` из одноименного пакета.
Серверный и клиентский сертификаты должны быть подписаны одним и тем же
корневым сертификатом (CA).

```bash title="Шаг 1. Создание директории для сертификатов для пользователя picouser"
mkdir -p /home/picouser/certs && cd /home/picouser/certs
```

```bash title="Шаг 2. Создание корневого сертификата (CA)"
openssl genrsa -out ca.key 2048
openssl req -x509 -new -nodes -key ca.key -sha256 -days 365 -out ca.crt -subj "/CN=RootCA"
```

```bash title="Шаг 3. Создание серверного закрытого ключа"
openssl genrsa -out server.key 2048
```

```bash title="Шаг 4. Создание запроса на подпись серверного сертификата (CSR)"
openssl req -new -key server.key -out server.csr -subj "/CN=Server"
```

```bash title="Шаг 5. Подпись серверного сертификата с помощью корневого сертификата (CA)"
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 365 -sha256
```

```bash title="Шаг 6. Создание клиентского закрытого ключа"
openssl genrsa -out client.key 2048
```

```bash title="Шаг 7. Создание запроса на подпись клиентского сертификата (CSR)"
openssl req -new -key client.key -out client.csr -subj "/CN=Client"
```

```bash title="Шаг 8. Подпись клиентского сертификата с помощью корневого сертификата (CA)"
 openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -days 365 -sha256
```

```bash title="Шаг 9. Конвертация клиентского ключа в формат PKCS#8"
openssl pkcs8 -topk8 -inform PEM -outform DER -in client.key -out client.pk8 -nocrypt
```

```bash title="Шаг 10. Установка прав на файлы"
chmod 600 ca.key server.key client.key client.pk8
```

!!! note "Примечание"
    Если клиентский ключ планируется использовать от лица
    текущего пользователя, назначьте права `600` (u=rw), если от root —
    `640` (u=rw,g=r).

Если требуется использовать хранилище в формате `PKCS#12`, то следует:

- объединить сертификат и ключ клиента в один файл (`cat client.key client.crt > store.txt`)
- конвертировать получившийся файл в формат `PKCS#12` (`openssl pkcs12 -export -in store.txt -out store.pkcs12 -name myAlias -noiter -nomaciter`)

[mTLS]: https://en.wikipedia.org/wiki/Mutual_authentication

## Настройка mTLS для защиты кластера {: #enable_mtls }

Для защиты коммуникации внутри кластера по протоколу Iproto используйте
безопасный режим mutual TLS (mTLS). Он работает следующим образом:

- Администратор создает корневой сертификат (CA), набор серверных и
клиентских сертификатов и ключей и подписывает их с помощью CA (см.
[выше](#create_certs_and_keys)).

- На стороне сервера (инстанса Picodata) администратор
[включает](../reference/config.md#instance_iproto_tls) режим TLS и
предоставляет пути к CA, а также серверному сертификату и закрытому
ключу. CA должен быть единым для всех инстансов кластера; содержимое серверных
сертификатов и закрытого ключа должно быть уникальным на каждом инстансе.

- После запуска инстансов кластера внутренняя коммуникация по протоколу
Iproto будет происходить в зашифрованном виде. Администратор может
подключиться к любому инстансу кластера, предоставив CA, клиентский
сертификат и клиентский закрытый ключ. Если указан флаг `--tls-auth`, то при успешной проверке
сертификатов и ключа на стороне сервера (инстанса), администратор
получит доступ к кластеру без пароля и соответствующей ему проверки
аутентификации (`md5`, `chap-sha1`, `ldap`). Подключение
[возможно](../reference/cli.md#tls) в командной строке с помощью
команд [picodata status], [picodata expel] и [picodata plugin configure].

[picodata status]: ../reference/cli.md#status
[picodata expel]: ../reference/cli.md#expel
[picodata plugin configure]: ../reference/cli.md#plugin_configure
