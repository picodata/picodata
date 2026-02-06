# Безопасность кластера {: #cluster_security }

## Подключение по протоколу PostgreSQL в безопасном режиме {: #connect_pgproto_ssl}

Picodata поддерживает [TLS/SSL] при подключении по протоколу PostgreSQL,
которое обеспечивает безопасную коммуникацию между клиентом (окном
терминала или клиентским API) и сервером ([инстансом] Picodata).

В частности, при проверке подлинности с помощью пароля, использование
TLS/SSL позволяет не передавать его по сети в открытом виде.

Безопасное соединение задействуется автоматически если оно
поддерживается (и [настроено](#create_certs_and_keys)) на стороне
инстанса Picodata.

!!! note "Примечание"
    Если в конфигурации инстанса включен параметр
    [instance.pg.ssl], то небезопасные соединения (без TLS/SSL) при этом
    становятся запрещены.

См. также:

- [Режимы sslmode](../dev/connectors/jdbc.md#sslmode)

[TLS/SSL]: https://ru.wikipedia.org/wiki/SSL
[instance.pg.ssl]: ../reference/config.md#instance_pg_ssl
[инстансом]: ../overview/glossary.md#instance

### Настройка на стороне сервера {: #setup_pgproto_ssl }

Безопасный режим — это подключение после проверки сертификатов сервера
(инстанса Picodata) и клиента (например, `psql`). Возможна настройка в
двух режимах:

- односторонняя проверка сервера клиентом (режим TLS/SSL). Требуется
  пара сертификат + закрытый ключ на сервере (`pg.ssl.cert_file` и
  `pg.ssl.key_file`) и корневой сертификат на клиенте (`ca.crt`)
- двусторонняя проверка сервера и клиента друг другом (режим mTLS). На
  обеих сторонах нужны по 3 файла: `pg.ssl.cert_file`, `pg.ssl.key_file`
  и `pg.ssl.ca_file` на сервере, и `client.crt`, `client.key` и `ca.crt`
  на клиенте. В результате PostgreSQL-сервер в Picodata будет принимать
  подключения только в том случае, если клиент предоставит сертификат,
  подписанный с помощью такого же `ca.crt`.

Для включения [TLS/SSL] на стороне Picodata используйте в [файле
конфигурации](../reference/config.md#instance_pg_ssl) параметр
`instance.pg.ssl: true` и положите на сервер сертификат и ключ. Укажите
пути к ним через параметры командной строки или конфигурационный файл.
Пример:

```yaml
pg:
    ssl: true
    cert_file: tls/custom_server.crt
    key_file: tls/custom_server.key
```

Если нужно включить проверку клиентского сертификата ([mTLS]), добавьте
параметр `instance.pg.ssl.ca_file` с путем до CA-сертификата, который
может верифицировать клиентские сертификаты. Пример:

```yaml
pg:
    ssl: true
    cert_file: tls/custom_server.crt
    key_file: tls/custom_server.key
    ca_file: tls/custom_ca.crt
```

!!! note "Примечание"
    Помимо явного указания путей через параметры конфигурации, можно
    положить файлы с предопределенными именами в [рабочую директорию
    инстанса](../reference/cli.md#run_instance_dir) <INSTANCE_DIR>:
    - SSL-сертификат: `server.crt`
    - ключ сертификата: `server.key`
    - CA-сертификат (для [mTLS]): `ca.crt`
    Рекомендуется использовать явные настройки в конфигурационном файле.

### Настройка на стороне клиента {: #client_pgproto_ssl }

При использовании приложения `psql` в режиме [TLS/SSL] нужно
предоставить приложению CA-сертификат, соответствующий сертификату в
Picodata (параметр `pg.ssl.cert_file`). По умолчанию, `psql` будет
читать его из файла `~/.postgresql/root.crt`. Подробнее см. в
[документации
libpq-ssl](https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-FILEUSAGE).

Примеры команд для подключения:

```shell title="в формате URI"
psql postgres://admin@192.168.0.22:5001?sslmode=verify-ca
```

```shell title="в формате conninfo"
psql 'host=192.168.0.22 port=5001 user=admin sslmode=verify-ca'
```
```shell title="с указанием пути к CA-файлу"
psql 'host=192.168.0.22 port=5001 user=admin sslmode=verify-ca sslrootcert=/etc/picodata.ca.crt`
```

Для режима [mTLS] понадобятся файлы с клиентским сертификатом и ключом.
Сертификат должен проходить валидацию с помощью корневого сертификата
`pg.ssl.ca_file`, настроенного в Picodata.

Пример подключения с явным указанием всех необходимых файлов:

```shell
psql "user=admin host=192.168.0.22 port=5001 sslrootcert=/etc/certs/ca.crt sslcert=/etc/certs/myserver1.int.crt sslkey=/etc/certs/myserver1.int.key"
```

## Создание сертификатов и ключей {: #create_certs_and_keys }

Ниже показаны примеры команд для генерации самоподписанного сертификата
(публичного ключа) и закрытого ключа как для сервера (Picodata), так и
для клиентов, которые хотят подключиться к серверу. Для этих команд
используется консольное приложение `openssl` из одноименного пакета.
Серверный и клиентский сертификаты должны быть подписаны одним и тем же
корневым сертификатом (CA). Сертификаты должны содержать валидные имена
в поле `Subject Alternative Name (SAN)`. В примере ниже клиент находится
на узле с именем `client.int`, а Picodata — на узле с именем
`server.int`.

```shell title="Шаг 1. Создание директории для сертификатов для пользователя picouser"
mkdir -p /home/picouser/certs && cd /home/picouser/certs
```

```shell title="Шаг 2. Создание корневого сертификата (CA)"
openssl genrsa -out ca.key 2048
openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 -out ca.crt -subj "/CN=RootCA"
```

```shell title="Шаг 3. Создание серверного закрытого ключа"
openssl genrsa -out server.key 2048
```

```shell title="Шаг 4. Создание запроса на подпись серверного сертификата (CSR)"
openssl req -new -key server.key -out server.csr -subj "/CN=server.int"
```

```shell title="Шаг 5. Подпись серверного сертификата с помощью корневого сертификата (CA)"
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 3650 -sha256 -extfile <(echo subjectAltName="DNS:server.int")
```

```shell title="Шаг 6. Создание клиентского закрытого ключа"
openssl genrsa -out client.key 2048
```

```shell title="Шаг 7. Создание запроса на подпись клиентского сертификата (CSR)"
openssl req -new -key client.key -out client.csr -subj "/CN=client.int"
```

```shell title="Шаг 8. Подпись клиентского сертификата с помощью корневого сертификата (CA)"
 openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -days 3650 -sha256 -extfile <(echo subjectAltName="DNS:client.int")
```

```shell title="Шаг 9. Конвертация клиентского ключа в формат PKCS#8"
openssl pkcs8 -topk8 -inform PEM -outform DER -in client.key -out client.pk8 -nocrypt
```

```shell title="Шаг 10. Установка прав на файлы"
chmod 600 ca.key server.key client.key client.pk8
```

!!! note "Примечание"
    Если клиентский ключ планируется использовать от лица
    текущего пользователя, назначьте права `600` (u=rw), если от root —
    `640` (u=rw,g=r).

Если требуется использовать хранилище в формате `PKCS#12`, то следует:

- объединить сертификат и ключ клиента в один файл (`cat client.key client.crt > store.txt`)
- конвертировать получившийся файл в формат `PKCS#12` (`openssl pkcs12 -export -in store.txt -out store.pkcs12 -name myAlias -noiter -nomaciter`)

### Настройки для mTLS-коммуникации внутри кластера {: #iproto_certs_and_keys }

Каждому инстансу Picodata нужен сертификат для подключения к другим
членам кластера под учетной записью `pico_service`.

Допустим наш кластер состоит из трех серверов:

- `server1.int`
- `server2.int`
- `server3.int`

Эти же имена мы указываем в конфигурациях инстансов Picodata для
параметра `instance.iproto_advertise`.

Сгенерируйте CA-сертификат как описано [выше](#create_certs_and_keys).
Далее создайте 3 сертификата с указанием учетной записи `pico_service` и
корректного имени каждого сервера.

```shell
# server1.int
openssl genrsa -out server1.int.key 2048
openssl req -new -key server1.int.key -out server1.int.csr -subj "/CN=pico_service@server1.int"
openssl x509 -req -in server1.int.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server1.int.crt -days 3650 -sha256 < <(echo subjectAltName="DNS:server1.int")

# server2.int
openssl genrsa -out server2.int.key 2048
openssl req -new -key server2.int.key -out server2.int.csr -subj "/CN=pico_service@server2.int"
openssl x509 -req -in server2.int.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server2.int.crt -days 3650 -sha256 < <(echo subjectAltName="DNS:server2.int")

# server3.int
openssl genrsa -out server3.int.key 2048
openssl req -new -key server3.int.key -out server3.int.csr -subj "/CN=pico_service@server3.int"
openssl x509 -req -in server3.int.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server3.int.crt -days 3650 -sha256 < <(echo subjectAltName="DNS:server3.int")
```

!!! note "Примечание"
    Если в `instance.iproto_advertise` указывается
    IP-адрес сервера, а не DNS-имя, то при создании сертификата нужно
    указывать IP-адрес сервера в параметре `subjectAltName`. Например,
    `subjectAltName="IP:192.168.1.1"`.

Вместо этого также можно выпустить один шаблонный сертификат, который
будет годиться для любого сервера в домене `.int`:

```shell
# *.int
openssl genrsa -out wildcard.int.key 2048
openssl req -new -key wildcard.int.key -out wildcard.int.csr -subj "/CN=pico_service"
openssl x509 -req -in wildcard.int.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out wildcard.int.crt -days 3650 -sha256 <(echo subjectAltName="DNS:*.int")
```

[mTLS]: https://en.wikipedia.org/wiki/Mutual_authentication

## Настройка mTLS для коммуникации внутри кластера {: #setup_iproto_mtls }

Для защиты коммуникации внутри кластера по протоколу IPROTO используйте
безопасный режим mutual TLS (mTLS). Он работает следующим образом:

- Администратор создает корневой сертификат (CA), набор серверных и
клиентских сертификатов и ключей и подписывает их с помощью CA (см.
[выше](#create_certs_and_keys)).

- Администратор настраивает поля сертификата `server.crt`
(`instance.iproto_tls.cert_file`):
    - При взаимодействии инстансов друг с
    другом `server.crt` выступает одновременно как серверный, и клиентский сертификат.
    Поэтому, при добавлении в сертификат поля `Extended Key Usage`, в нем
    должны быть оба значения: и `TLS Web Client Authentication`, и `TLS Web
    Server Authentication`.

    - Поле `subjectAltName` сертификата `instance.iproto_tls.cert_file` должно
    соответствовать строке, указанной в параметре
    `instance.iproto_advertise`. Например, если в настройке инстанса
    указано:
    ```yaml
    instance:
    iproto_advertise: myserver1.int:13001
    ```
    то в поле `subjectAltName` сертификата должно быть значение
    `myserver1.int` или wildcard `*.int`.

    - Поле `CN` должно содержать значение `pico_service` — служебного
      пользователя для авторизации запросов внутри кластера. Допустимы
      значения `CN=pico_service `или в виде
      `CN=pico_service@myserver1.int`.

- На стороне сервера (инстанса Picodata) администратор
[включает](../reference/config.md#instance_iproto_tls) режим TLS и
предоставляет пути к CA, а также серверному сертификату и закрытому
ключу. CA должен быть единым для всех инстансов кластера; содержимое серверных
сертификатов и закрытого ключа должно быть идентичным на каждом инстансе.

    Пример настройки mTLS в файле конфигурации:

    ```yaml
    iproto_tls:
        enabled: true
        cert_file: tls/server.crt
        key_file: tls/server.key
        ca_file: tls/ca.crt
    ```
    !!! note "Примечание"
        mTLS должен быть включен на всех инстансах кластера

- После запуска инстансов кластера внутренняя коммуникация по протоколу
IPROTO будет происходить в зашифрованном виде и в режиме проверки TLS
`verify-full`. Администратор может подключиться к любому инстансу
кластера, предоставив CA, клиентский сертификат и клиентский закрытый
ключ. Если указан набор флагов `--tls-auth`, `--tls-cert` `--tls-key` и
`--tls-ca`, то при успешной проверке сертификатов и ключа на стороне
сервера (инстанса), администратор получит доступ к кластеру без пароля.
Подключение [возможно](../reference/cli.md#tls) в командной строке с
помощью команд [picodata status], [picodata expel] и [picodata plugin
configure].

[picodata status]: ../reference/cli.md#status
[picodata expel]: ../reference/cli.md#expel
[picodata plugin configure]: ../reference/cli.md#plugin_configure

## Настройка HTTPS для безопасного доступа к метрикам и веб-интерфейсу {: #setup_https }

Для безопасного доступа к [метрикам] и [веб-интерфейсу] Picodata
настройте шифрование протокола HTTP. Для этого следует указать в
конфигурационных файлах инстансов соответствующий блок настроек:

```yaml
https:
    enabled: true
    cert_file: cert.pem
    key_file: key.pem
```

[метрикам]: monitoring.md/#enable_metrics
[веб-интерфейсу]: ../tutorial/webui.md

Для генерации необходимых сертификатов и ключей используйте приведенные
[выше](#create_certs_and_keys) инструкции.

См. также:

- [Настройка HTTPS в файле конфигурации](../reference/config.md#instance_https)
