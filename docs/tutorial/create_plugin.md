# Создание плагина

В данном разделе приведено руководство для написания плагинов к Picodata
с помощью команд языка SQL.

## Возможности плагинов {: #plugins_capabilities }

С помощью [плагинов][plugin] разработчик может добавить практически
любую дополнительную функциональность к распределенной СУБД Picodata.
Условием выступает лишь написание валидного Rust-кода. В свою очередь,
Picodata предоставляет API плагинов — фреймворк для создания
распределенных приложений, работающих во всем кластере СУБД.

Технически, реализация плагина — это написание набора callback'ов,
которые представлены трейтом Service. Реализовав этот трейт, разработчик
получает "строительные кирпичики" плагина — сервисы. Их стоит
рассматривать как классические web-микросервисы, из которых можно
построить законченную систему.

[plugin]: ../overview/glossary.md#plugin

## Пример разработки тестового плагина {: #develop_plugin }

### Функции тестового плагина {: #plugin_features }

В данном руководстве мы попробуем создать плагин, который будет
использовать Picodata в роли кэширующей БД, хранящей данные о погоде. Мы
предоставим HTTP API, который позволит обрабатывать запросы к публичному
сервису OpenWeather, получая от него текущую температуру по
географическим координатам. При этом температуру по заданным координатам
мы будем кэшировать и сохранять в базе данных и, если к нам придет еще
один запрос с такими же координатами, мы не будем совершать еще один
запрос к OpenWeather, а отдадим кешированное значение. Для упрощения
нашего примера мы не будем инвалидировать кеш.

### Разработка компонентов плагина {: #plugin_parts }

### Сервис {: #service }

Основой плагина является [сервис][service], с которого мы и начнем.
Сервис для плагина необходимо написать на Rust, поэтому первым делом
инициализируем крейт:

```shell
mkdir weather_cache && cd weather_cache
cargo init --lib
```

[service]: ../overview/glossary.md#service

Обратите внимание на параметр `--lib` — мы будем собирать библиотеку вида
`*.so` или `*.dylib`, а значит и крейт сразу нужно инициализировать
соответствующим образом. Также сразу добавим строки для сборки
`*.so`-файла в `Cargo.toml`:

```
[lib]
crate-type = ["lib", "cdylib"]
```

Далее добавим наш SDK в проект:

```shell
cargo add picodata-plugin
```

Выполняемая сервисом логика будет находиться в файле
`src/lib.rs`:

```rust
use picodata_plugin::plugin::prelude::*;

struct WeatherService;

impl Service for WeatherService {
    type Config = ();
    fn on_config_change(
        &mut self,
        _ctx: &PicoContext,
        new_cfg: Self::Config,
        _old_cfg: Self::Config,
    ) -> CallbackResult<()> {
        println!("I got a new config: {new_cfg:?}");
        Ok(())
    }

    fn on_start(&mut self, _ctx: &PicoContext, cfg: Self::Config) -> CallbackResult<()> {
        println!("I started with config: {cfg:?}");
        Ok(())
    }

    fn on_stop(&mut self, _ctx: &PicoContext) -> CallbackResult<()> {
        println!("I stopped!");
        Ok(())
    }

    /// Called after replicaset master is changed
    fn on_leader_change(&mut self, _ctx: &PicoContext) -> CallbackResult<()> {
        println!("Leader has changed!");
        Ok(())
    }
}

impl WeatherService {
    pub fn new() -> Self {
        WeatherService {}
    }
}

#[service_registrar]
pub fn service_registrar(reg: &mut ServiceRegistry) {
    reg.add("weather_service", "0.1.0", WeatherService::new);
}
```

Посмотрим на этот код подробнее, а именно на реализацию трейта `Service`.

Первое на что нужно обратить внимание — это тип `Config`. Он позволит
описать конфигурацию сервиса — например, адреса внешних узлов или
значения таймаутов. Мы пока не хотим настраивать кэширование,
поэтому определим тип пустым.

Перейдем к функциям сервиса, которые необходимо реализовать:

- сервис начинает свою жизнь с вызова `on_start`. Эта функция будет
вызвана каждым узлом, где включается сервис, или при вводе нового узла,
где он должен быть запущен. При [изменении конфигурации
плагина](https://docs.picodata.io/picodata/devel/reference/sql/alter_plugin/)
будет вызвана функция `on_config_change`, а старая и новые
конфигурации будут переданы в качестве параметров.

- о смене лидера в репликасете информирует вызов функции
`on_leader_change`, что позволяет реагировать и запускать /
останавливать фоновые операции. Корректное завершение работы (`gracefull
shutdown`) мы можем отработать в функции `on_stop`.

Пока что мы оставим тут заглушки и проверим, что наш минимальный плагин
загружается и пишет что-то в журнал.

### Манифест {: #manifest }

Теперь нам необходимо как-то описать для Picodata правила загрузки
плагина. Это делается при помощи [манифеста][manifest] — файла, который
описывает составляющие части плагина и предоставляет Picodata необходимую
для установки и запуска метаинформацию. Можно представить, что это
аналог `cargo.toml` в пакетном менеджере `Cargo` или `package.json` в
`npm`.

[manifest]: ../overview/glossary.md#manifest

Создадим файл манифеста:

```yaml title="manifest.yaml"
# Имя плагина
name: weather_cache
# Описание плагина. Это метаданные, которые сейчас не используются Picdata, но могут администратору системы при установке или изучении установленных в кластер плагинов
description: That one is created as an example of Picodata's plugin
# Версия плагина в формате semver. Picodata следит за установленными версиями плагинов и плагины с разными версиями — это разные объекты для Picodata
version: 0.1.0
# Список сервисов. Так как наш плагин не слишком сложный, нам хватит одного сервиса для реализации задуманного.
services:
    # Имя сервиса
  — name: weather_service
    # Описание сервиса. Не используется внутри Picodata, но могут помочь администраторам системы
    description: This service provides HTTP route for a throughput weather cache
    # Конфигурация сервиса по умолчанию
    default_configuration:
```

### Пробный запуск {: #plugin_test_run }

Соберем сервис:

```shell
cargo build
```

Теперь нужно правильно организовать размещение файлов плагина и
манифеста. Picodata выполняет поиск плагина в [plugin_path] с учетом его
имени и версии, используя следующую структуру пути:
`<share-dir>/<plugin-name>/<plugin-version>`. Для нашего плагина это
будет выглядеть как `<share-dir>/weather_cache/0.1.0`.

```shell
mkdir -p build/weather_cache/0.1.0
cp target/debug/libweather_cache.so build/weather_cache/0.1.0
cp manifest.yaml build/weather_cache/0.1.0
```

Должна получиться такая структура:

```
build
└── weather_cache
    └── 0.1.0
        ├── libweather_cache.so
        └── manifest.yaml
```

Теперь запустим Picodata с поддержкой плагинов и затем попробуем
запустить наш плагин.

Запуск Picodata:

```shell
picodata run --iproto-listen 127.0.0.1:3301 --iproto-advertise 127.0.0.1:3301 --peer 127.0.0.1:3301 --http-listen 127.0.0.1:8081 --instance-dir i1 --share-dir build
```

Запуск плагина:

```sql
$ picodata admin i1/admin.sock
Connected to admin console by socket path "i1/admin.sock"
type '\help' for interactive help
(admin) sql> CREATE PLUGIN weather_cache 0.1.0;
1
(admin) sql> ALTER PLUGIN weather_cache 0.1.0 ADD SERVICE weather_service TO TIER default;
1
(admin) sql> ALTER PLUGIN weather_cache 0.1.0 ENABLE;
1
```

После этого в журнале Picodata появится строка о том,
что наш плагин ожил и запустился.

```
I started with config: ()
```

Попробуем выключить и удалить плагин:

```sql
(admin) sql> ALTER PLUGIN weather_cache 0.1.0 DISABLE;
1
(admin) sql> DROP PLUGIN weather_cache 0.1.0;
1
```

### Добавление миграций {: #add_migrations }

Вернемся к задаче кэширования (наша цель — сохранять результаты запросов
к внешнему сервису). Так как Picodata — это, в первую очередь, СУБД, нам
стоит создать для этого таблицу. Система плагинов в Picodata позволяет
создавать необходимые служебные таблицы для каждого плагина при помощи
механизма [миграций][migration]. Для этого надо написать SQL-команды,
которые необходимо выполнить при установке плагина, а также те, которые
необходимы для удаления этих таблиц (при удалении плагина). У нас
получится файл `0001_weather.sql`:

[migration]: ../overview/glossary.md#migration

```sql title="0001_weather.sql"
-- pico.UP

CREATE TABLE "weather" (
    id UUID NOT NULL,
    latitude INTEGER NOT NULL,
    longitude INTEGER NOT NULL,
    temperature INTEGER NOT NULL,
    PRIMARY KEY (id)
)
USING memtx
DISTRIBUTED BY (latitude, longitude);

-- pico.DOWN
DROP TABLE "weather";
```

В этом файле есть специальные аннотации — `-- pico.UP` и `-- pico.DOWN`.
Именно они помечают, какие команды выполнять на установке (`UP`) и
удалении (`DOWN`). Теперь необходимо положить файл миграций в директорию
с плагином и добавить его в манифест, чтобы Picodata могла знать, откуда
его загрузить:

!!! note "Примечание"
    Не забудьте также отредактировать манифест в [plugin_path], а не
    только в репозитории


```yaml title="manifest.yaml"
name: weather_cache
description: That one is created as an example of Picodata's plugin
version: 0.1.0
services:
  — name: weather_service
    description: This service provides HTTP route for a throughput weather cache
    default_configuration:
      openweather_timeout: 5
migration:
  — 0001_weather.sql
```

Добавим файл миграций в [plugin_path]:

```shell
cp 0001_weather.sql build/weather_cache/0.1.0
```

[plugin_path]: ../overview/glossary.md#plugin_path

Теперь еще раз установим плагин, но в этот раз также запустим добавленные
миграции:

```shell
$ picodata admin i1/admin.sock
Connected to admin console by socket path "i1/admin.sock"
type '\help' for interactive help
(admin) sql> CREATE PLUGIN weather_cache 0.1.0;
1
(admin) sql> ALTER PLUGIN weather_cache 0.1.0 ADD SERVICE weather_service TO TIER default;
1
(admin) sql> ALTER PLUGIN weather_cache MIGRATE TO 0.1.0;
1
(admin) sql> ALTER PLUGIN weather_cache 0.1.0 ENABLE;
1
```

Убедимся, что была создана таблица `weather`:

```sql
(admin) sql> SELECT * FROM weather;

+----+----------+-----------+-------------+
| id | latitude | longitude | temperature |
+=========================================+
+----+----------+-----------+-------------+
(0 rows)
```

Снова очистим кластер, но на этот раз также удалим созданные
миграциями таблицы:

```sql
(admin) sql> ALTER PLUGIN weather_cache 0.1.0 DISABLE;
1
(admin) sql> DROP PLUGIN weather_cache 0.1.0 WITH DATA;
1
```

Убедимся в успешности `DOWN`-миграции:

```sql
(admin) sql> SELECT * FROM weather;
sbroad: table with name "weather" not found
```

Теперь попробуем поднять HTTP-сервер.
Чтобы не писать код для FFI между Lua и Rust, давайте возьмем
готовое решение — библиотеку `shors`.
Это библиотека позволит нам создать HTTP-сервер в плагине,
инкапсулируя связывание двух разных языков внутри:

```shell
cargo add shors@0.12.1 --features picodata
```

И добавим код для инициализации HTTP endpoint, который вернет нам
`Hello, World!` в callback `on_start`:

```rust
fn on_start(&mut self, _ctx: &PicoContext, _cfg: Self::Config) -> CallbackResult<()> {
    println!("I started with config: {_cfg:?}");

    let endpoint = Builder::new()
        .with_method("GET")
        .with_path("/hello")
        .build(
            |_ctx: &mut Context, _: Request| -> Result<_, Box<dyn Error>> {
                Ok("Hello, World!".to_string())
            },
        );

    let s = server::Server::new();
    s.register(Box::new(endpoint));

    Ok(())
}
```

Далее добавим endpoint, который будет осуществлять запрос к Openweather.
Для этого мы воспользуемся еще одной библиотекой, которая предоставит
нам HTTP клиент — `fibreq`. Мы не можем использовать популярные HTTP-клиенты,
например, `reqwest`, из-за особенностей однопоточной среды выполнения Picodata —
необходимо использовать библиотеки со специальной реализацией
асинхронного ввода/вывода.

```shell
cargo add fibreq@0.1.8 --features picodata
```

Также нам понадобится `serde` и `serde-json` для сериализации JSON в HTTP-запросах:

```shell
cargo add serde
cargo add serde-json
```

Добавим запрос к OpenWeather в отдельном файле `openweather.rs`:

```rust
use std::{error::Error, time::Duration};
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Clone, Debug)]
struct CurrentWeather {
    temperature_2m: f64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]

pub(crate) struct WeatherInfo {
    latitude: f64,
    longitude: f64,
    current: CurrentWeather,
}

static METEO_URL: once_cell::sync::Lazy<String> = once_cell::sync::Lazy::new(|| {
    std::env::var("METEO_URL").unwrap_or(String::from("https://api.open-meteo.com"))
});

pub fn weather_request(latitude: f64, longitude: f64, request_timeout: u64) -> Result<WeatherInfo, Box<dyn Error>> {
    let http_client = fibreq::ClientBuilder::new().build();
    let http_req = http_client
        .get(format!(
            "{url}/v1/forecast?\
            latitude={latitude}&\
            longitude={longitude}&\
            current=temperature_2m",
            url = METEO_URL.as_str(),
            latitude = latitude,
            longitude = longitude
        ))?;

    let mut http_resp = http_req.request_timeout(Duration::from_secs(request_timeout)).send()?;

    let resp_body = http_resp.text()?;
    let info = serde_json::from_str(&resp_body)?;

    return Ok(info)
}
```

и изменим код нашего сервиса следующим образом:

```rust
fn on_start(&mut self, _ctx: &PicoContext, _cfg: Self::Config) -> CallbackResult<()> {
    println!("I started with config: {_cfg:?}");

    let hello_endpoint = Builder::new()
        .with_method("GET")
        .with_path("/hello")
        .build(
            |_ctx: &mut Context, _: Request| -> Result<_, Box<dyn Error>> {
                Ok("Hello, World!".to_string())
            },
        );


    #[derive(Serialize, Deserialize)]
    pub struct WeatherReq {
        latitude: i8,
        longitude: i8,
    }

    let weather_endpoint = Builder::new()
        .with_method("POST")
        .with_path("/weather")
        .build(
            |_ctx: &mut Context, request: Request| -> Result<_, Box<dyn Error>> {
                let req: WeatherReq = request.parse()?;
                let res = openweather::weather_request(req.latitude, req.longitude, 3)?;
                Ok(res)
            },
        );

    let s = server::Server::new();
    s.register(Box::new(hello_endpoint));
    s.register(Box::new(weather_endpoint));

    Ok(())
}
```

Запустим наш сервис и проверим его работоспособность следующим запросом:

```shell
curl --location '127.0.0.1:8081/weather' \
    --header 'Content-Type: application/json' \
    --data '{
        "latitude": 55,
        "longitude": 55
    }'
```

Теперь необходимо добавить кеш. Здесь мы воспользуемся `Picodata` как СУБД.
Для начала добавим структуру, которая хранится в БД:

```rust
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Weather {
    latitude: f64,
    longitude: f64,
    temperature: f64,
}
```

Теперь напишем запрос извлечения ее из БД:

```rust
let SELECT_QUERY: &str = r#"
SELECT * FROM "weather"
WHERE
    (latitude < (? + 0.5) AND latitude > (? - 0.5))
    AND
    (longitude < (? + 0.5) AND longitude > (? - 0.5));
"#;
let cached: Vec<Weather> = picodata_plugin::sql::query(&SELECT_QUERY)
    .bind(latitude)
    .bind(latitude)
    .bind(longitude)
    .bind(longitude)
    .fetch::<Weather>()
    .map_err(|err| format!("failed to retrieve data: {err}"))?;
```

```rust
let select_query: &str = r#"
SELECT * FROM "weather"
WHERE
    (latitude < (? + 0.5) AND latitude > (? - 0.5))
    AND
    (longitude < (? + 0.5) AND longitude > (? - 0.5));
"#;
let res = picoplugin::sql::query(&select_query)
    .bind(latitude)
    .bind(latitude)
    .bind(longitude)
    .bind(longitude)
    .fetch::<StoredWeatherInfo>()
    .unwrap();
```

!!! note "Примечание"
    При запросе мы сравниваем широту и долготу
    с ограниченной точностью, так как это числа с плавающей точкой

Аналогично напишем запрос на вставку в кэш после получения данных:

```rust
let INSERT_QUERY: &str = r#"
INSERT INTO "weather"
VALUES(?, ?, ?)
"#;

let _ = picodata_plugin::sql::query(&INSERT_QUERY)
    .bind(resp.latitude)
    .bind(resp.longitude)
    .bind(resp.temperature)
    .execute()
    .map_err(|err| format!("failed to retrieve data: {err}"))?;
```

После этого необходимо добавить только проверку — нашли ли мы необходимые данные
БД или необходимо запросить данные с OpenWeather. Наш `on_start` будет выглядеть так:

```rust
fn on_start(&mut self, _ctx: &PicoContext, _cfg: Self::Config) -> CallbackResult<()> {
    println!("I started with config: {_cfg:?}");

    let hello_endpoint = Builder::new().with_method("GET").with_path("/hello").build(
        |_ctx: &mut Context, _: Request| -> Result<_, Box<dyn Error>> {
            Ok("Hello, World!".to_string())
        },
    );

    #[derive(Serialize, Deserialize)]
    pub struct WeatherReq {
        latitude: f64,
        longitude: f64,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct Weather {
        latitude: f64,
        longitude: f64,
        temperature: f64,
    }

    let weather_endpoint = Builder::new()
        .with_method("POST")
        .with_path("/weather")
        .build(
            |_ctx: &mut Context, request: Request| -> Result<_, Box<dyn Error>> {
                let req: WeatherReq = request.parse()?;
                let latitude = req.latitude;
                let longitude = req.longitude;

                let cached: Vec<Weather> = picodata_plugin::sql::query(&SELECT_QUERY)
                    .bind(latitude)
                    .bind(latitude)
                    .bind(longitude)
                    .bind(longitude)
                    .fetch::<Weather>()
                    .map_err(|err| format!("failed to retrieve data: {err}"))?;
                if !cached.is_empty() {
                    let resp = cached[0].clone();
                    return Ok(resp);
                }
                let openweather_resp =
                    openweather::weather_request(req.latitude, req.longitude, 3)?;
                let resp: Weather = Weather {
                    latitude: openweather_resp.latitude,
                    longitude: openweather_resp.longitude,
                    temperature: openweather_resp.current.temperature_2m,
                };

                let _ = picodata_plugin::sql::query(&INSERT_QUERY)
                    .bind(resp.latitude)
                    .bind(resp.longitude)
                    .bind(resp.temperature)
                    .execute()
                    .map_err(|err| format!("failed to retrieve data: {err}"))?;

                Ok(resp)
            },
        );

    let s = server::Server::new();
    s.register(Box::new(hello_endpoint));
    s.register(Box::new(weather_endpoint));

    Ok(())
}
```

Разработка тестового плагина завершена.
Его [исходный код](https://git.picodata.io/core/plugin-example) можно найти на нашем Gitlab.

См. также:

- [Механизм плагинов](../architecture/plugins.md)


## Разработка плагина с помощью Pike {: #using_pike}

Для более удобной разработки плагинов воспользуйтесь [Pike] — созданной в
Picodata консольной утилитой, упрощающей многие операции с плагинами.
Pike представляет собой [крейт для Cargо], позволяющий:

- быстро создавать шаблоны плагинов Picodata
- автоматизировать создание архива с файлами плагина для его последующего деплоя
- удобно запускать локальный кластер Picodata для тестирования плагина

[крейт для Cargо]: https://crates.io/crates/picodata-pike

[Pike]: https://github.com/picodata/pike


### Установка Pike {: #install_pike}

Введите следующую команду для установки крейта `picodata-pike`:

```shell
cargo install picodata-pike
```

### Создание плагина {: #create_plugin_with_pike}

Рассмотрим процесс разработки плагина с помощью Pike.

Для создания нового проекта плагина из шаблона введите следующую команду:

```shell
cargo pike plugin new name_of_the_new_plugin
```

В указанной директории автоматически инициализируется проект git.

Дополнительные параметры:

- `--without-git` — отключение автоматической инициализации git-репозитория
- `--workspace` — создание проекта плагина как рабочего пространства

В директории проекта плагина появится следующая структура файлов:

```shell
├── build.rs
├── Cargo.toml
├── manifest.yaml.template
├── migrations
│   └── 0001_init.sql
├── picodata.yaml
├── plugin_config.yaml
├── rust-toolchain.toml
├── src
│   ├── config.rs
│   ├── lib.rs
│   └── service.rs
├── tests
│   ├── helpers
│   │   └── mod.rs
│   └── metrics.rs
├── tmp
└── topology.toml

```

В директории `src` находятся файлы собирающегося и устанавливающегося
плагина. Проект плагина сразу готов для сборки с помощью Cargo.

<!--
В `tests` предоставляется набор встроенных тестов для создания, сборки и
проверки работы плагина в кластере.
 -->

### Сборка и упаковка плагина {: #build_plugin_with_pike}

Для сборки плагина в архив `.tar.gz` для доставки на
сервера посредством роли Ansible или Helm-чарта используйте следующую
команду:

```shell
cargo pike plugin pack
```

Команда `plugin pack` упакует release-версию плагина в новый архив в
директории `target` проекта.

Дополнительные параметры:

- `--debug` — сборка и упаковка debug-версии плагина
- `--target-dir <TARGET_DIR>` — директория собранных бинарных файлов. Значение по умолчанию: `target`
- `--plugin-path` — путь до директории плагина. Значение по умолчанию: `./`

Для сборки плагина без упаковки используйте команду `cargo build`. По
умолчанию будет собрана debug-версию плагина (используйте параметр `-r`
для сборки release-версии).

### Тестирование и отладка плагина {: #run_and_test_plugin_with_pike}

#### Запуск тестового кластера {: #pike_test_run}

Для тестирования и отладки плагина в Pike предусмотрен удобный запуск локального
кластера Picodata.

!!! danger "Внимание"
    Запуск кластера с помощью Pike предназначен
    исключительно для отладки плагина на локальном узле! Для полноценного
    развертывания кластера Picodata см. раздел [Создание
    кластера](deploy.md)

Параметры кластера и входящих в него [инстансов] задаются в
файле `topology.toml`.

??? example "Пример файла topology.toml"
    ```yaml
    [tier.default]
    replicasets = 2
    replication_factor = 2

    [plugin.super_plugin]
    migration_context = [
        { name = "example_name", value = "example_value" },
    ]

    [plugin.super_plugin.service.main]
    tiers = ["default"]
    ```

[инстансов]: ../overview/glossary.md#instance

Для запуска кластера введите команду:

```shell
cargo pike run
```

Вариант с явным указанием файла топологии и рабочей директории:

```shell
cargo pike run --topology topology.toml --data-dir ./tmp
```

Кластер будет запущен уже с установленным плагином.

Соответственно, для остановки кластера нажмите ++ctrl+c++  или введите в
отдельном окне терминала команду `cargo pike stop`.

Дополнительные параметры командной строки при запуске `cargo pike run`:

- `-t, --topology <TOPOLOGY>` — путь к файлу топологии. Значение по
  умолчанию: `topology.toml`
- `--data-dir <DATA_DIR>` — путь к директории хранения файлов кластера.
  Значение по умолчанию: `./tmp`
- `--disable-install-plugins` — отключение автоматической установки
  плагинов
- `--base-http-port <BASE_HTTP_PORT>` — базовый http-порт, с которого
  начнут открываться http-порты отдельных инстансов. Значение по
  умолчанию: `8000`
- `--base-pg-port <BASE_PG_PORT>` — базовый порт протокола PostgreSQL, с
  которого начнут открываться порты отдельных инстансов. Значение по
  умолчанию: `5432`
- `--picodata-path <BINARY_PATH>` — путь до исполняемого файла Picodata.
  Значение по умолчанию: `picodata`
- `--release` — сборка и запуск release-версии плагина
- `--target-dir <TARGET_DIR>` — директория собранных бинарных файлов.
  Значение по умолчанию: `target`
- `-d, --daemon` — запуск кластера в режиме службы
- `--disable-colors` — отключает раскрашивание имен инстансов в разные
  цвета в журнале
- `--plugin-path` — путь до директории плагина. Значение по умолчанию:
  `./`

#### Топология тестового кластера {: #pike_topology}

Управление топологией тестового кластера производится через изменение
файла `topology.toml`:

!!! example "topology.toml"
    ```yaml
    # описание количества репликасетов и фактора репликации тира
    # фактор репликации отвечает за количество инстансов в одном репликасете
    # в примере используется тир default, указано два репликасета и фактор репликации 2,
    # следовательно, будет создано всего четыре инстанса
    [tier.default]
    replicasets = 2
    replication_factor = 2

    # настройки плагинов
    [plugin.sp] # в примере настройки для плагина sp
    # переменные, которые будут подставлены в миграции
    # подробнее тут: https://docs.picodata.io/picodata/stable/architecture/plugins/#use_plugin_config
    migration_context = [
        { name = "example_name", value = "example_value" },
    ]

    # настройки сервисов плагинов
    [plugin.sp.service.main] # в примере настройка сервиса main плагина sp
    tiers = ["default"] # тиры, на которых должен работать сервис

    # переменные окружения, которые будут переданы каждому инстансу Picodata
    # в значении переменной можно указать liquid-шаблон, в таком случае
    # переменная будет динамически вычислена для каждого инстанса отдельно
    # подробнее про liquid-шаблоны: https://shopify.dev/docs/api/liquid
    [environment]
    SP_CONST_VAR = "const" # такое значение будет передано каждому инстансу без изменений
    # здесь мы используем переменную из контекста шаблонов,
    # для первого, например, инстанса значение будет "1"
    SP_LIQUID_VAR = "{{ instance_id }}"
    # здесь используется переменная из контекста и стандартная функция plus
    # результатом для первого, например, инстанса будет "4243"
    SP_LIQUID_VAR2 = "{{ instance_id | plus: 4242 }}"
    ```

Доступные переменные контекста в шаблонах:

- `instance_id` — порядковый номер инстанса при запуске, начинается с 1

#### Конфигурация Picodata {: #pike_picodata_config}

Pike позволяет использовать файл конфигурации Picodata (`picodata.yaml`)
вместе с запущенным кластером. Пример файла сразу генерируется командами
`new` и `init`.

См. также:

- [Описание файла конфигурации](../reference/config.md)


#### Настройка нескольких тиров {: #pike_tiers}

Для настройки необходимо добавить нужные тиры в конфигурацию кластера
(файл `picodata.yaml`) и затем указать их в файле топологии
`topology.toml`.

Пример добавления тира `compute`:

!!! example "picodata.yaml"
    ```yaml
    cluster:
    tier:
        default:
        can_vote: true
        compute: # новый тир
        can_vote: true
    ```

!!! example "topology.toml"
    ```yaml
    # ...

    [tier.compute] # новый тир
    replicasets = 1
    replication_factor = 1
    ```

### Дополнительные команды Pike {: #pike_commands}

#### Остановка кластера {: #pike_cluster_stop}

Остановите кластер комбинацией клавиш ++ctrl+c++ в терминале, где
вызывалась команда `cargo pike run`, либо в другом окне следующей
командой:

```shell
cargo pike stop --data-dir ./tmp
```

При помощи параметра `--data-dir` укажите путь до директории с файлами
кластера (значение по умолчанию: `./tmp`)

Вывод:

```shell
[*] stopping picodata cluster, data folder: ./tmp
[*] stopping picodata instance: i1
[*] stopping picodata instance: i2
[*] stopping picodata instance: i3
[*] stopping picodata instance: i4
```

Дополнительные параметры:

- `--data-dir <DATA_DIR>` — путь к директории хранения файлов кластера.
  Значение по умолчанию: `./tmp`
- `--plugin-path` — путь до директории плагина. Значение по умолчанию:
  `./`

#### Очистка данных кластера {: #pike_cluster_reset}

Для очистки директорий с данными кластера Picodata введите команду:

```shell
cargo pike clean
```

Дополнительные параметры:

- `--data-dir <DATA_DIR>` — путь к директории хранения файлов кластера.
  Значение по умолчанию: `./tmp`

####  Применение конфигурации к запущенному плагину {: #pike_plugin_config_apply}

Для применения конфигурации сервисов плагина к запущенному командой `run` кластеру Picodata введите команду:

```shell
cargo pike config apply
```

Пример файла конфигурации сервисов:

!!! example "plugin_config.yaml"
    ```yaml
    main: # имя сервиса
    value: changed # пример параметра конфигурации
    ```

Дополнительные параметры:

- `-c, --config-path <CONFIG>` — путь к файлу конфигурации. Значение по умолчанию: `plugin_config.yaml`
- `--data-dir <DATA_DIR>` — путь к директории хранения файлов кластера. Значение по умолчанию: `./tmp`


См. также:

- [Создание кластера](deploy.md)
