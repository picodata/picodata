# Создание плагина

В данном разделе приведено руководство для написания плагинов к Picodata
с помощью команд языка SQL.

## Возможности плагинов {: #plugins_capabilities }

С помощью [плагинов][plugin] разработчик может добавить практически
любую дополнительную функциональность к распределенной СУБД Picodata.
Условием выступает лишь написание валидного Rust-кода. В свою очередь,
Picodata предоставляет API плагинов — фреймворк для создания
распределенных приложений, поддерживающих работу во всем кластере СУБД.

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

Обратите внимание на флаг `--lib` — мы будем собирать библиотеку вида
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
# Версия плагина в формате semver. Picodata следит за установленными версиями плагинов и плагины с разными версиями — это разные объекты для Пикодаты
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
`<plugin-dir>/<plugin-name>/<plugin-version>`. Для нашего плагина это
будет выглядеть как `<plugin-dir>/weather_cache/0.1.0`.

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
picodata run -l 127.0.0.1:3301 --advertise 127.0.0.1:3301 --peer 127.0.0.1:3301 --http-listen 127.0.0.1:8081 --instance-dir i1 --plugin-dir build
```

Запуск плагина:

```sql
$ picodata admin i1/admin.socket
Connected to admin console by socket path "i1/admin.socket"
type '\help' for interactive help
picodata> CREATE PLUGIN weather_cache 0.1.0;
1
picodata> ALTER PLUGIN weather_cache 0.1.0 ADD SERVICE weather_service TO TIER default;
1
picodata> ALTER PLUGIN weather_cache 0.1.0 ENABLE;
1
```

После этого в журнале Picodata появится строка о том,
что наш плагин ожил и запустился.

```
I started with config: ()
```

Попробуем выключить и удалить плагин:

```sql
picodata> ALTER PLUGIN weather_cache 0.1.0 DISABLE;
1
picodata> DROP PLUGIN weather_cache 0.1.0;
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
    latitude NUMBER NOT NULL,
    longitude NUMBER NOT NULL,
    temperature NUMBER NOT NULL,
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
$ picodata admin i1/admin.socket
Connected to admin console by socket path "i1/admin.socket"
type '\help' for interactive help
picodata> CREATE PLUGIN weather_cache 0.1.0;
1
picodata> ALTER PLUGIN weather_cache 0.1.0 ADD SERVICE weather_service TO TIER default;
1
picodata> ALTER PLUGIN weather_cache MIGRATE TO 0.1.0;
1
picodata> ALTER PLUGIN weather_cache 0.1.0 ENABLE;
1
```

Убедимся, что была создана таблица `weather`:

```sql
picodata> SELECT * FROM weather;

+----+----------+-----------+-------------+
| id | latitude | longitude | temperature |
+=========================================+
+----+----------+-----------+-------------+
(0 rows)
```

Снова очистим кластер, но на этот раз также удалим созданные
миграциями таблицы:

```sql
picodata> ALTER PLUGIN weather_cache 0.1.0 DISABLE;
1
picodata> DROP PLUGIN weather_cache 0.1.0 WITH DATA;
1
```

Убедимся в успешности `DOWN`-миграции:

```sql
picodata> SELECT * FROM weather;
sbroad: table with name "weather" not found
```

Теперь попробуем поднять HTTP-сервер.
Чтобы не писать код для FFI между Lua и Rust, давайте возьмем
готовое решение — библиотеку `shors`.
Это библиотека позволит нам создать HTTP-сервер в плагине,
инкапсулируя связывание двух разных языков внутри:

```bash
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

```bash
cargo add fibreq@0.1.8 --features picodata
```

Также нам понадобится `serde` и `serde-json` для сериализации JSON в HTTP-запросах:

```bash
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

```bash
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
Его [исходный код](https://git.picodata.io/picodata/plugin/example) можно найти на нашем Gitlab.

См. также:

- [Механизм плагинов](../architecture/plugins.md)
