# picodata-java

В данном разделе приведено описание [picodata-java] — Java-драйвера для
работы с СУБД Picodata.

## Общие сведения {: #intro }

Драйвер предоставляет [Java API][java_api] для работы с низкоуровневым Picodata API
и служит коннектором к СУБД Picodata из Java-приложений. Драйвер
совместим с кластерами Picodata, использующими библиотеку
шардирования [vshard].

[vshard]: ../../overview/glossary.md#vshard
[picodata-java]: https://git.picodata.io/picodata/picodata/picodata-java
[java_api]: https://docs.picodata.io/apidocs/java-api/latest/

## Подключение {: #enabling }

Для подключения коннектора следует добавить его в проект в качестве
зависимости. Пример для Maven:

```xml
<dependency>
  <groupId>io.picodata</groupId>
  <artifactId>picodata-java</artifactId>
</dependency>
```

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

## Проверка работы {: #testing }

Мы предоставляем [тестовое Java-приложение][example], которое создает и
заполняет таблицу в Picodata посредством коннектора [picodata-java].

Для проверки работы тестового приложения потребуются JDK (например,
[OpenJDK](https://openjdk.org)) версии 1.8 или новее, и Docker.

!!! note "Примечание"
    Проверить наличие необходимой версии JDK можно командой `./mvnw
    --version` (строка Java version) в директории проекта [picodata-java].

[example]:
    https://git.picodata.io/picodata/picodata/examples/-/tree/master/picodata-java-example

Порядок действий:

1.&nbsp;Склонируйте репозиторий [тестового приложения][example] и
соберите его:

```shell
git clone https://git.picodata.io/picodata/picodata/examples/-/tree/master/picodata-java-example
```

```shell
./mvnw install
```

2.&nbsp;Перейдите в директорию `src/main/resources` и запустите
   контейнеры с тестовым кластером Picodata:

```shell
docker-compose up -d
```

3.&nbsp;Настройте авторизацию для Picodata в контейнере:

```shell
docker-compose exec picodata-1 bash -c "echo -ne \"\\set language sql\nALTER USER \\\"admin\\\" WITH PASSWORD 'P@ssw0rd';\" | picodata admin /home/picouser/picodata-1/admin.sock"
```

4.&nbsp;Вернитесь в исходную директорию `picodata-java-example` и
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

```
[nioEventLoopGroup-2-1] INFO  i.p.d.c.c.AbstractTarantoolConnectionManager - Connected to Tarantool server at /127.0.0.1:3301
Insert result:
row_count 1
Select all characters after insert:
metadata [{name=id, type=integer}, {name=name, type=string}, {name=year, type=integer}]
rows [[2, Vasya, 2000]]
[io.picodata.PicodataExample.main()] INFO  i.p.d.c.c.AbstractTarantoolConnectionManager - Disconnected from /127.0.0.1:3301

```

## Структура приложения {: #app_tree }

Ниже показано дерево файлов минимального тестового приложения:

```
├── pom.xml
├── src
│   └── main
│       ├── java
│       │   └── com
│       │       └── example
│       │           └── PicodataExample.java
│       └── resources
│           ├── docker-compose.yml
│           └── logback.xml
```

Содержимое файлов тестового приложения:

??? example "pom.xml"
    ```
    <project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
        <modelVersion>4.0.0</modelVersion>

        <groupId>io.picodata</groupId>
        <artifactId>picodata-java-example</artifactId>
        <version>1.0.0-SNAPSHOT</version>

        <dependencies>
            <dependency>
                <groupId>io.picodata</groupId>
                <artifactId>picodata-java</artifactId>
                <version>1.0.2</version>
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
                    <version>3.8.0</version>
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
                        <mainClass>io.picodata.PicodataExample</mainClass>
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

??? example "PicodataExample.java"
    ```
    package io.picodata;

    import java.util.Arrays;
    import java.util.HashMap;
    import java.util.Map;

    import io.picodata.driver.api.TarantoolClient;
    import io.picodata.driver.api.TarantoolClientFactory;
    import io.picodata.driver.api.TarantoolResult;
    import io.picodata.driver.api.tuple.DefaultTarantoolTupleFactory;
    import io.picodata.driver.api.tuple.TarantoolTuple;
    import io.picodata.driver.api.tuple.TarantoolTupleFactory;
    import io.picodata.driver.mappers.factories.DefaultMessagePackMapperFactory;


    public class PicodataExample {

        private static final DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
        private static final TarantoolTupleFactory tupleFactory =
                new DefaultTarantoolTupleFactory(mapperFactory.defaultComplexTypesMapper());

        static TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> setupClient() {
            return TarantoolClientFactory.createClient()
                // If any addresses or an address provider are not specified,
                // the default host 127.0.0.1 and port 3301 are used
                .withAddress("127.0.0.1", 3301)
                // For connecting to a Cartridge application, use the value of cluster_cookie parameter in the init.lua file
                .withCredentials("admin", "P@ssw0rd")
                // you may also specify more client settings, such as:
                // timeouts, number of connections, custom MessagePack entities to Java objects mapping, etc.
                .build();
        }

        public static void main(String[] args) throws Exception {
            try (TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = setupClient()) {
                client.callForSingleResult(
                    "pico.sql",
                    Arrays.asList(
                        "CREATE TABLE \"characters\" (" +
                        "\"id\" INT PRIMARY KEY, \"name\" TEXT, \"year\" INT" +
                        ") distributed by (\"id\")"
                    ),
                    HashMap.class
                ).get();

                client.callForSingleResult(
                    "pico.sql",
                    Arrays.asList("DELETE FROM \"characters\" WHERE 1=1"),
                    HashMap.class
                ).get();

                String query = "INSERT INTO \"characters\" VALUES (?, ?, ?)";
                TarantoolTuple queryTuple = tupleFactory.create(Arrays.asList(2, "Vasya", 2000));

                Map<String, Object> result =
                    client.callForSingleResult(
                        "pico.sql",
                        Arrays.asList(query, queryTuple),
                        HashMap.class
                    ).get();

                String query2 = "SELECT * FROM \"characters\" WHERE \"id\" = ?";
                TarantoolTuple queryTuple2 = tupleFactory.create(Arrays.asList(2));

                Map<String, Object> result2 =
                    client.callForSingleResult(
                        "pico.sql",
                        Arrays.asList(query2, queryTuple2),
                        HashMap.class
                    ).get();

                System.out.println("Insert result:");
                result.forEach((key, value) -> System.out.println(key + " " + value));
                System.out.println("Select all characters after insert:");
                result2.forEach((key, value) -> System.out.println(key + " " + value));
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                System.exit(0);
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
        ports:
          - "3301:3301"

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
