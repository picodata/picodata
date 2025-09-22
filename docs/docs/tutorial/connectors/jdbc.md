# JDBC

В данном разделе приведено описание [JDBC-драйвера] для работы с СУБД Picodata.

## Общие сведения {: #intro }

Драйвер предоставляет [JDBC API][jdbc_api] для работы с
Picodata и служит коннектором к СУБД Picodata из приложений,
поддерживающих JDBC-подключения.

[JDBC-драйвера]: https://git.picodata.io/picodata/picodata/picodata-jdbc
[jdbc_api]: https://docs.picodata.io/apidocs/jdbc/latest/

## Подключение {: #enabling }

Для подключения коннектора следует добавить его в проект в качестве
зависимости. Пример для Maven:

```xml
<dependency>
    <groupId>io.picodata</groupId>
    <artifactId>picodata-jdbc</artifactId>
    <version>LATEST</version>
</dependency>
```

Вместо `LATEST` можно также указать и конкретную версию коннектора.

Также в `pom.xml` вашего приложения или в глобальные настройки Maven
необходимо будет добавить репозиторий Picodata:

```xml
    <repositories>
        <repository>
            <id>binary.picodata.io</id>
            <url>https://binary.picodata.io/repository/maven-releases/</url>
        </repository>
    </repositories>
```

## Поддерживаемые возможности {: #features }

JDBC-драйвер для Picodata использует протокол PGPROTO и поддерживает некоторые настройки подключения драйвера
[PgJDBC](https://jdbc.postgresql.org/documentation/use/#system-properties).
Реализован класс `io.picodata.Driver`, имплементирующий `java.sql.Driver`. В
качестве адреса для подключения следует использовать формат
`jdbc:picodata://host:port/?user=sqluser,password=P@ssw0rd`.

Полный список поддерживаемых возможностей приведен в документации API JDBC-драйвера:

- [Все поддерживаемые опции драйвера](https://docs.picodata.io/apidocs/jdbc/latest/io/picodata/jdbc/PicodataProperty.html)
- [настройка источника данных PicodataClusterAwareDataSource](https://docs.picodata.io/apidocs/jdbc/latest/io/picodata/jdbc/datasource/PicodataClusterAwareDataSource.html)

## Использование шифрования {: #enable_tls }

JDBC-драйвер для Picodata и источник данных
`PicodataClusterAwareDataSource` поддерживают безопасный режим работы и
могут быть настроены на использование шифрования mTLS в рамках всего
кластера Picodata. Поддерживаемые варианты SSL и хранилища сертификатов:

- `PKCS#8` (контейнер с одним ключом и одним сертификатом)
- `PKCS#12 `(контейнер с поддержкой нескольких ключей и сертификатов)
- глобальное хранилище, используемое в `sslFactory=io.picodata.jdbc.ssl.DefaultJavaSSLSocketFactory`

### Создание сертификатов и ключей {: #create_certs_and_keys}

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
chmod 640 ca.key server.key client.key client.pk8
```

Если требуется использовать хранилище в формате `PKCS#12`, то следует:

- объединить сертификат и ключ клиента в один файл (`cat client.key client.csr > store.txt`)
- конвертировать получившийся файл в формат `PKCS#12` (`openssl pkcs12 -export -in store.txt -out store.pkcs12
-name myAlias -noiter -nomaciter`)

### Параметры подключения {: #connection_params}

Для безопасного подключения используйте класс `PicodataSSLSocketFactory`:

```
java.lang.Object
    javax.net.SocketFactory
        javax.net.ssl.SSLSocketFactory
            io.picodata.jdbc.ssl.PicodataSSLSocketFactory
```

Основные параметры подключения:

- `user` — имя пользователя
- `password` — пароль пользователя
- `sslMode` — режим подключения (см. [ниже](#sslModes)). По умолчанию
  используется режим `require`
- `sslPassword` — единый пароль для всех хранилищ в том случае, если
  хотя бы одно из них защищено паролем
- `sslCert` — полный путь к клиентскому сертификату или хранилищу. Если
  параметр не указан, будет использовано значение [SSL_CERT] из класса
  PicodataProperty
- `sslKey` — полный путь к клиентскому закрытому ключу или хранилищу.
  Если параметр не указан, будет использовано значение [SSL_KEY] из
  класса PicodataProperty
- `sslRootCert` — полный путь к корневому сертификата или хранилищу для
  проверки серверного сертификата

[SSL_CERT]: https://docs.picodata.io/apidocs/jdbc/latest/io/picodata/jdbc/PicodataProperty.html#SSL_CERT
[SSL_KEY]: https://docs.picodata.io/apidocs/jdbc/latest/io/picodata/jdbc/PicodataProperty.html#SSL_KEY

Дополнительные параметры подключения:

- `sslPasswordCallback` — полное имя класса собственной реализации
  CallbackHandler. Если хотя бы одно из хранилищ защищено паролем и при
  этом пароль и класс callback не предоставлены, то пароль будет
  запрошен в интерактивном режиме (при использовании в скриптах это может заблокировать работу
  приложения)
- `sslHostnameVerifier` — полное имя класса собственной реализации
  HostnameVerifier, используемой для проверки сетевого имени хоста в
  режиме `verify-full`. По умолчанию проверка сетевого имени производится
  средствами JDBC-драйвера

### Режимы SslMode {: #sslModes }

- `allow` — сначала попробовать незашифрованное соединение, затем
  зашифрованное
- `disable` — не использовать зашифрованное соединение
- `prefer` — сначала попробовать зашифрованное соединение, при неудаче
  откатиться к незашифрованному
- `require` — требовать зашифрованное соединение
- `verify-ca` — убедиться, что соединение зашифровано и клиент доверяет
  сертификату, выданному сервером
- `verify-full` — то же, что `verify-ca`, но с проверкой того, что
  сетевое имя сервера указано в сертификате, выданном сервером

### Пример подключения {: #connect_example }

Ниже показан пример подключения по JDBC с принудительной проверкой безопасного режима (`verify-ca`):

```java
public class PicoJdbc {
    public void checkConnectionSsl(String url, String username, String password, Map<String, String> extraProps) {
        Properties props = new Properties();
        props.putAll(extraProps);
        props.put("user", "your_username");
        props.put("password", "your_password");
        props.put("sslMode", "verify-ca");
        props.put("sslPassword", "your_ssl_password");
        props.put("sslCert", "/path/to/sslcert");
        props.put("rootCert", "/path/to/rootcert");

        try (Connection connection = DriverManager.getConnection(url, props)) {
            if (!connection.isClosed()) {
                connection.close();
            }
            System.out.println("Connection was successful");
        } catch (SQLException e) {
            System.out.println("Connection failed");
        }
    }

    // ...
}
```

!!! note title "Примечание"
    Если пароль для расшифровки SSL-ключа не был задан, то при
    подключении следует передать пустое значение (`("sslPassword", "")`)

## Проверка работы {: #testing }

Мы предоставляем [тестовое Java-приложение][example], которое создает и
заполняет таблицу в Picodata посредством [JDBC-драйвера].

Для проверки работы тестового приложения потребуется JDK (например,
[OpenJDK](https://openjdk.org)) версии 11 или новее, и Docker.

!!! note "Примечание"
    Проверить наличие необходимой версии JDK можно командой `./mvnw
    --version` (строка Java version) в директории проекта `picodata-jdbc`.

[example]:
    https://git.picodata.io/picodata/picodata/examples/-/tree/master/picodata-jdbc-example

Порядок действий:

1.&nbsp;Склонируйте репозиторий [тестового приложения][example] и
соберите его:

```shell
git clone https://git.picodata.io/picodata/picodata/examples/-/tree/master/picodata-jdbc-example
```

```shell
./mvnw install
```

2.&nbsp;Перейдите в директорию `src/main/resources` и запустите
   контейнеры с тестовым кластером Picodata:

```shell
docker-compose up -d
```

3.&nbsp;Создайте отдельного пользователя для подключения по JDBC и выдайте ему права на создание таблиц:

```shell
docker-compose exec picodata-1 bash -c "echo -ne \"CREATE USER \\\"sqluser\\\" WITH PASSWORD 'P@ssw0rd' USING md5;\nGRANT CREATE TABLE TO \\\"sqluser\\\";\" | picodata admin /home/picouser/picodata-1/admin.sock"
```

4.&nbsp;Вернитесь в исходную директорию `picodata-jdbc-example` и
запустите тестовое приложение.


Для версии JDK 17 и выше:

```shell
_JAVA_OPTIONS="--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" ./mvnw exec:java
```

Для более старых версий:

```shell
./mvnw exec:java
```

Результатом успешной работы будет вставка строки в тестовую таблицу и
вывод содержимого таблицы:

```shell
19:09:22.473 [io.picodata.PicodataJDBCExample.main()] INFO  io.picodata.PicodataJDBCExample - Connected to the Picodata server successfully.
19:09:22.491 [io.picodata.PicodataJDBCExample.main()] INFO  io.picodata.PicodataJDBCExample - Executed file before.sql
19:09:22.608 [io.picodata.PicodataJDBCExample.main()] INFO  io.picodata.PicodataJDBCExample - 1 rows was deleted
19:09:22.640 [io.picodata.PicodataJDBCExample.main()] INFO  io.picodata.PicodataJDBCExample - 1 rows was inserted
19:09:22.674 [io.picodata.PicodataJDBCExample.main()] INFO  io.picodata.PicodataJDBCExample - Id is 1, name is Dima
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
```

## Структура приложения {: #app_tree }

Ниже показано дерево файлов минимального тестового приложения:

```
    ├── pom.xml
    └── src
        └── main
            ├── java
            │   └── io
            │       └── picodata
            │           └── PicodataJDBCExample.java
            └── resources
                ├── docker-compose.yaml
                └── logback.xml
```

Содержимое файлов тестового приложения:

??? example "pom.xml"
    ```
    <project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
        <modelVersion>4.0.0</modelVersion>

        <groupId>io.picodata</groupId>
        <artifactId>picodata-jdbc-example</artifactId>
        <version>1.0.0-SNAPSHOT</version>

        <dependencies>
            <dependency>
                <groupId>io.picodata</groupId>
                <artifactId>picodata-jdbc</artifactId>
                <version>1.0.0</version>
            </dependency>
            <dependency>
                <groupId>org.slf4j</groupId>
                <artifactId>slf4j-api</artifactId>
                <version>2.0.16</version>
            </dependency>
            <dependency>
                <groupId>ch.qos.logback</groupId>
                <artifactId>logback-classic</artifactId>
                <version>1.3.4</version>
            </dependency>
        </dependencies>

        <build>
            <plugins>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-resources-plugin</artifactId>
                    <version>3.2.0</version>
                </plugin>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-compiler-plugin</artifactId>
                    <version>3.11.0</version>
                    <configuration>
                        <fork>true</fork>
                        <debug>true</debug>
                        <optimize>true</optimize>
                        <showDeprecation>true</showDeprecation>
                        <showWarnings>true</showWarnings>
                        <source>17</source>
                        <target>17</target>
                        <excludes>
                            <exclude>**/package-info.java</exclude>
                        </excludes>
                    </configuration>
                </plugin>
                <plugin>
                    <groupId>org.codehaus.mojo</groupId>
                    <artifactId>exec-maven-plugin</artifactId>
                    <version>3.4.0</version>
                    <executions>
                        <execution>
                            <goals>
                                <goal>java</goal>
                            </goals>
                        </execution>
                    </executions>
                    <configuration>
                        <mainClass>io.picodata.PicodataJDBCExample</mainClass>
                    </configuration>
                </plugin>
            </plugins>
        </build>

        <repositories>
            <repository>
                <id>binary.picodata.io</id>
                <url>https://binary.picodata.io/repository/maven-releases/</url>
            </repository>
        </repositories>
    </project>
    ```

??? example "PicodataJDBCExample.java"
    ```
    package io.picodata;

    import java.io.IOException;
    import java.nio.file.Files;
    import java.nio.file.Paths;
    import java.sql.Connection;
    import java.sql.DriverManager;
    import java.sql.SQLException;
    import java.util.Properties;

    import org.slf4j.Logger;
    import org.slf4j.LoggerFactory;

    public class PicodataJDBCExample {
        private static Logger logger = LoggerFactory.getLogger(PicodataJDBCExample.class);

        public static void main(String[] args) {
            var props = new Properties();
            props.setProperty("user", "sqluser");
            props.setProperty("password", "P@ssw0rd");
            props.setProperty("sslmode", "disable");

            var connstr = "jdbc:picodata://localhost:4327/";
            try (Connection conn = DriverManager.getConnection(connstr, props)) {
                logger.info("Connected to the Picodata server successfully.");

                var stmt = conn.createStatement();

                try {
                    Files.readAllLines(Paths.get("before.sql")).stream().forEach(statement -> {
                        try {
                            stmt.execute(statement);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    logger.info("Executed file before.sql");
                } catch (RuntimeException e) {
                    logger.error("Failed to execute before.sql", e);
                    System.exit(1);
                } catch (IOException e) {
                    logger.error("Failed to open file before.sql", e);
                    System.exit(1);
                }

                var deleteQuery = "DELETE FROM \"warehouse\";";
                var preparedStmt = conn.prepareStatement(deleteQuery);
                var deleteRows = preparedStmt.executeUpdate();
                logger.info("{} rows were deleted", deleteRows);


                var insertQuery = "INSERT INTO \"warehouse\" VALUES (?, ?);";
                preparedStmt = conn.prepareStatement(insertQuery);
                preparedStmt.setInt(1, 1);
                preparedStmt.setString(2, "Dima");
                var insertedRows = preparedStmt.executeUpdate();
                logger.info("{} rows were inserted", insertedRows);

                var selectQuery = "SELECT * FROM \"warehouse\" WHERE id = ?;";
                preparedStmt = conn.prepareStatement(selectQuery);
                preparedStmt.setInt(1, 1);
                var res = preparedStmt.executeQuery();
                while (res.next()) {
                    logger.info("Id is {}, name is {}", res.getInt(1), res.getString(2));
                }

            } catch (SQLException e) {
                logger.error("Unexpected error: ", e);
                System.exit(1);
            }
        }
    }
    ```

??? example "docker-compose.yml"
    ```
    ---
    version: '3'

    services:
    picodata-1:
        image: docker-public.binary.picodata.io/picodata:24.4.1
        container_name: picodata-1
        hostname: picodata-1
        environment:
        PICODATA_INSTANCE_NAME: picodata-1
        PICODATA_INSTANCE_DIR: picodata-1
        PICODATA_IPROTO_LISTEN: picodata-1:3301
        PICODATA_IPROTO_ADVERTISE: picodata-1:3301
        PICODATA_PEER: picodata-1:3301
        PICODATA_PG_LISTEN: picodata-1:4327
        PICODATA_PG_SSL: "false"
        ports:
        - "3301:3301"
        - "4327:4327"

    picodata-2:
        image: docker-public.binary.picodata.io/picodata:24.4.1
        container_name: picodata-2
        hostname: picodata-2
        depends_on:
        - picodata-1
        environment:
        PICODATA_INSTANCE_NAME: picodata-2
        PICODATA_INSTANCE_DIR: picodata-2
        PICODATA_IPROTO_LISTEN: picodata-2:3302
        PICODATA_IPROTO_ADVERTISE: picodata-2:3302
        PICODATA_PEER: picodata-1:3301
        ports:
        - "3302:3302"


    picodata-3:
        image: docker-public.binary.picodata.io/picodata:24.4.1
        container_name: picodata-3
        hostname: picodata-3
        depends_on:
        - picodata-1
        environment:
        PICODATA_INSTANCE_NAME: picodata-3
        PICODATA_INSTANCE_DIR: picodata-3
        PICODATA_IPROTO_LISTEN: picodata-3:3303
        PICODATA_IPROTO_ADVERTISE: picodata-3:3303
        PICODATA_PEER: picodata-1:3301
        ports:
        - "3303:3303"
    ```

??? example "logback.xml"
    ```
    <configuration debug="true">
        <variable name="logLevel" value="${logging.logLevel}"/>

        <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
            <encoder>
                <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
            </encoder>
        </appender>

        <root level="${logLevel:-INFO}">
            <appender-ref ref="STDOUT"/>
        </root>
    </configuration>
    ```
