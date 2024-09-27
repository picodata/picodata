# picodata-jdbc

В данном разделе приведено описание [picodata-jdbc] — JDBC-драйвера для
работы с СУБД Picodata.

## Общие сведения {: #intro }

Драйвер предоставляет [JDBC API][jdbc_api] для работы с
Picodata и служит коннектором к СУБД Picodata из приложений,
поддерживающих JDBC-подключения.

[picodata-jdbc]: https://git.picodata.io/picodata/picodata/picodata-jdbc
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

JDBC-драйвер для Picodata поддерживает настройки подключения
[PgJDBC](https://jdbc.postgresql.org/documentation/use/#system-properties).
Реализован драйвер `java.sql.Driver` с классом `io.picodata.Driver`. В
качестве адреса для подключения следует использовать формат
`jdbc:picodata://host:port/?user=sqluser,password=P@ssw0rd`.

## Проверка работы {: #testing }

Мы предоставляем [тестовое Java-приложение][example], которое создает и
заполняет таблицу в Picodata посредством коннектора [picodata-jdbc].

Для проверки работы тестового приложения потребуются JDK (например,
[OpenJDK](https://openjdk.org)) версии 1.11 или новее, и Docker.

!!! note "Примечание"
    Проверить наличие необходимой версии JDK можно командой `./mvnw
    --version` (строка Java version) в директории проекта [picodata-jdbc].

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

            var connstr = "jdbc:picodata://localhost:5432/";
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
        PICODATA_INSTANCE_ID: picodata-1
        PICODATA_DATA_DIR: picodata-1
        PICODATA_LISTEN: picodata-1:3301
        PICODATA_ADVERTISE: picodata-1:3301
        PICODATA_PEER: picodata-1:3301
        PICODATA_PG_LISTEN: picodata-1:5432
        PICODATA_PG_SSL: "false"
        ports:
        - "3301:3301"
        - "5432:5432"

    picodata-2:
        image: docker-public.binary.picodata.io/picodata:24.4.1
        container_name: picodata-2
        hostname: picodata-2
        depends_on:
        - picodata-1
        environment:
        PICODATA_INSTANCE_ID: picodata-2
        PICODATA_DATA_DIR: picodata-2
        PICODATA_LISTEN: picodata-2:3302
        PICODATA_ADVERTISE: picodata-2:3302
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
        PICODATA_INSTANCE_ID: picodata-3
        PICODATA_DATA_DIR: picodata-3
        PICODATA_LISTEN: picodata-3:3303
        PICODATA_ADVERTISE: picodata-3:3303
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
