status: accepted
decision-makers: @vifley, @d.rodionov, @funbringer, @kostja
consulted: @d.rodionov, @funbringer, @kostja

--------------------------------

# Инфраструктура для открытия сокетов в пикодате и хранения этой конфигурации

## Мотивация

Клиентам требуется TLS в radix и других плагинах.

У нас уже есть TLS для pgproto и https.

Также кластерным системам типа Radix требуется хранить advertise адреса узлов, для чего есть [#1771](https://git.picodata.io/core/picodata/-/issues/1771).

Для дискавери и отладки будет полезно иметь разделение listen/advertise также для других плагинов и для HTTP.

## Решение

В Пикодате должно появиться апи для открытия сокета на заданные listen/advertise адреса, опционально с TLS/mTLS.

### Конфигурация инстанса

#### Текущее состояние

```yaml
instance:
  http_listen: <URI>
  https:
    enabled: true
    cert_file: cert.pem
    key_file: key.pem
    password_file: pass.txt

  iproto_advertise: <URI>
  iproto_listen: <URI>
  iproto_tls:
    enabled: true
    cert_file: tls/server.crt
    key_file: tls/server.key
    ca_file: tls/ca.crt

  pg:
    listen: <URI>
    advertise: <URI>
    ssl: true
    cert_file: tls/server.crt
    key_file: tls/server.key
    ca_file: tls/ca.crt
```

Как мы видим, конфигурация разнообразна и неконсистентна: http не имеет `advertise`, `iproto` - не группа и т.д. Предлагается унифицировать эти настройки (оставив старые для совместимости):

#### Решение для настройки портов инстанса

##### Порты инстанса

```yaml
instance:
  http:
    enabled: <bool>
    advertise: <URI>
    listen: <URI>
    tls:
      enabled: <bool>
      cert_file: tls_http/server.crt
      key_file: tls_http/server.key
      ca_file: tls_http/ca.crt
      password_file: tls_http/pass.txt

  iproto:
    enabled: <bool>
    advertise: <URI>
    listen: <URI>
    tls:
      cert_file: tls_http/server.crt
      key_file: tls_http/server.key
      password_file: tls_http/pass.txt

  pgproto:
    enabled: <bool>
    advertise: <URI>
    listen: <URI>
    tls:
      # выключаем TLS на пг
      enabled: false
```

Важные изменения по сравнению с текущей историей: явное указание — включен протокол или нет. Валидность проверяется отдельно в коде приложения (например, мы можем запретить запускать пикодату без `iproto`). Необходимо изменить `picodata config default`, чтобы генерило новый конфиг правильно.

##### Дефолтное состояние

###### iproto

| Старые ключи iproto       | Новая секция iproto                       | Результат                                                                                         |
|---------------------------|-------------------------------------------|---------------------------------------------------------------------------------------------------|
| Присутствует хотя бы один | Присутствует                              | Пикодата не запускается с ошибкой: "Указаны новые и старые настройки iproto, укажите только одни" |
| Присутствует хотя бы один | Отсутствует                               | Пикодата запускается с настройками, указанными в старых ключах                                    |
| Отсутствуют               | Отсутствует                               | Пикодата запускается с настройками iproto по умолчанию                                            |
| Отсутствуют               | Присутствует, но ключ enabled отсутствует | Пикодата не запускается с ошибкой: "Необходимо явно указать iproto включен или нет"               |
| Отсутствуют               | Присутствует, enabled == false            | Пикодата не запускается с ошибкой: "iproto необходим для работы пикодаты"                         |
| Отсутствуют               | Присутствует, enabled == true             | Пикодата запускается с указанными настройками, пропущенные ключи имеют значения по умолчанию.     |

###### pgproto

| Старая секция pg | Новая секция pgproto                      | Результат                                                                                                                                 |
|------------------|-------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| Присутствует     | Присутствует                              | Пикодата не запускается с ошибкой: "Указаны новые и старые настройки pg, укажите только одни"                                             |
| Присутствует     | Отсутствует                               | Пикодата запускается с настройками, указанными в старой секции                                                                            |
| Отсутствует      | Отсутствует                               | Пикодата запускается с настройками pg по умолчанию                                                                                        |
| Отсутствует      | Присутствует, но ключ enabled отсутствует | Пикодата не запускается с ошибкой: "Необходимо явно указать pgproto включен или нет"                                                      |
| Отсутствует      | Присутствует                              | Пикодата запускается с указанными настройками, пропущенные ключи имеют значения по умолчанию. Можно явно включать или выключать протокол. |

##### http

| Старые ключи http  | Новая секция http                         | Результат                                                                                                                                 |
|--------------------|-------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| Присутствуют       | Присутствует                              | Пикодата не запускается с ошибкой: "Указаны новые и старые настройки http, укажите только одни"                                           |
| Присутствуют       | Отсутствует                               | Пикодата запускается с настройками, указанными в старых ключах                                                                            |
| Отсутствуют        | Отсутствует                               | Пикодата запускается с настройками http по умолчанию                                                                                      |
| Отсутствуют        | Присутствует, но ключ enabled отсутствует | Пикодата не запускается с ошибкой: "Необходимо явно указать http включен или нет"                                                         |
| Отсутствуют        | Присутствует                              | Пикодата запускается с указанными настройками, пропущенные ключи имеют значения по умолчанию. Можно явно включать или выключать протокол. |

##### Порты плагинов

В конфигурацию инстанса добавляется конфигурация плагинов. Этот конфиг читается один раз при старте инстанса и записывается в системные таблицы (пока что только в `_pico_peer_address`). Пока допустим лишь один параметр, `listener` для настройки слушающего сокета.

```yaml
plugin:
  plugin-name:
    service:
      service-name:
        listener:
          enabled: <bool>
          advertise: <URI>
          listen: <URI>
          tls:
            enabled: <bool>
            cert_file: tls_2/server.crt
            key_file: tls_2/server.key
            ca_file: tls_2/ca.crt
            password_file: tls_2/pass.txt
```

Например:

```yaml
plugin:
  radix:
    service:
      radix:
        listener:
          enabled: bool
          advertise: <URI>
          listen: <URI>
          tls:
            enabled: bool
            cert_file: tls_2/server.crt
            key_file: tls_2/server.key
            ca_file: tls_2/ca.crt
            password_file: tls_2/pass.txt
```

Мы осознанно ограничиваем общей конфигурацией каждый сервис одним сокетом. Плагинов, которым требуется больше одного сокета, мы пока не видели. Авторы таких плагинов могут создать несколько сервисов или же открывать сокеты вручную.

Если включен `tls` (tls.enabled == true), то пикодата обязана проверить наличие сертификата, приватного ключа (см. https://docs.picodata.io/picodata/stable/admin/ssl/), корневого сертификата (если задан) и то, что пароль (если задан), подходит к закрытому ключу.

#### Расширение таблицы `_pico_peer_address`

Колонка `connection_type`: enum заменяется на строку (`http`, `pgproto`, `iproto` для системных сокетов и `plugin_name.service_name` для плагинных).

#### Чтение конфига

Чтение происходит на этапе запуска инстанса. Вне зависимости от фактически установленных плагинов, инстанс пикодаты прописывает в `pico_peer_address` строки: `<my_raft_id>, <URI>, <plugin_name.service_name>`.

Если содержимое конфига не совпадает со значениями в `pico_peer_address` для нашего `raft_id`, то инстанс может запуститься только в том случае, если добавлены новые или удалены полностью секции для конкретных `plugin/service`. Если они изменились, то тогда инстанс должен упасть с ошибкой о неверной конфигурации.

### Растовое апи для плагина.

В модуль [transport](https://docs.rs/picodata-plugin/25.5.2/picodata_plugin/transport/) добавляем подмодуль `listener` с API, примерно описанным ниже.

```rust
#[derive(Error, Debug)]
pub enum TlsHandshakeError {
    #[error("setup failure: {0}")]
    SetupFailure(ErrorStack),

    #[error("handshake error: {0}")]
    Failure(SslError),
}

pub struct PicoListener {}

pub struct PicoStream<S>
    where S: io::Read + io::Write {
  inner: PicoStreamImpl<S>
}

enum PicoStreamImpl<S>
    where S: io::Read + io::Write
{
    Plain(S),
    Tls(SslStream<S>)
}

impl<S> io::Read for PicoStream<S>
    where S: io::Read + io::Write
{
}

impl<S> io::Write for PicoStream<S>
    where S: io::Read + io::Write
{
}

pub enum PicoStreamError {
    Config,
    Io(io::Error),
    Tls(TlsHandshakeError)
}

impl PicoListener {
    /// Возвращает ошибку:
    /// - если для данного сервиса (описанного в PicoContext) не указаны конфигурация сокета (или она невалидна)
    /// - если сокет слушать не получается (например, new вызвали дважды или доступа нет)
    /// - если включен tls, но прочитать сертификаты не удалось
    /// - если listener выключен в конфиге (`enabled` - `false`)
    pub fn bind(context: &'_ PicoContext) -> Result<Self, PicoListenerError> {}

    /// Возвращает стандартные io ошибки или же ошибки TLS (скопировано из PGProto)
    pub fn accept<S>(&self) -> Result<(PicoStream<S>, SocketAddr), PicoStreamError>
        where S: io::Read + io::Write
    { }
}
```

#### Использование из плагина

```rust
impl prelude::Service for Plugin {
    type Config = Config;

    fn on_start(&mut self, ctx: &prelude::PicoContext, cfg: Self::Config) -> interface::CallbackResult<()> {
        let listener = PicoListener::new(ctx);
        loop {
            let connection = listener.accept()?;
            connection.read();
            connection.write();
        }
    }
```

### Инвентарь для плейбуки

#### Текущая ситуация

Сейчас интересующие нас секции выглядят вот так:

```yaml
    listen_address: '{{ ansible_fqdn }}'     # адрес, который будет слушать инстанс. Для IP указать {{ansible_default_ipv4.address}}
    pg_address: '{{ listen_address }}'       # адрес, который будет слушать PostgreSQL-протокол инстанса

    first_bin_port: 13301     # начальный бинарный порт для первого инстанса
    first_http_port: 18001    # начальный http-порт для первого инстанса для веб-интерфейса
    first_pg_port: 15001      # начальный номер порта для PostgreSQL-протокола инстансов кластера
```

Мы не можем указать разные listen и advertise.

#### Предлагаемое решение

Добавляем иерархию.

```yaml
    tls:
      cert_file: "{{ cert_dir }}/{{ cluster_name }}/{{ cert_file | basename }}"
      key_file: "{{ cert_dir }}/{{ cluster_name }}/{{ key_file | basename }}"
      ca_file: "{{ cert_dir }}/{{ cluster_name }}/{{ ca_file | basename }}"
      password_file: "{{ cert_dir }}/{{ cluster_name }}/{{ password_file | basename }}"

    iproto:
      listen: '{{ ansible_fqdn }}'     # адрес, который будет слушать инстанс. Для IP указать {{ansible_default_ipv4.address}}
      advertise: '{{ ansible_fqdn }}'  # адрес, который будет сервер сообщать остальным
      first_port: 13301                # начальный бинарный порт
      tls:
        enabled: "true/false"

    pg:
      enabled: true                    # включен пгпрото или нет
      listen: '{{ ansible_fqdn }}'     # адрес, который будет слушать инстанс. Для IP указать {{ansible_default_ipv4.address}}
      advertise: '{{ ansible_fqdn }}'  # адрес, который будет сервер сообщать остальным
      first_port: 15001                # начальный порт для PostgreSQL-протокола
      tls:
        enabled: "true/false"
        cert_file: "{{ cert_dir }}/{{ cluster_name }}/pg/{{ cert_file | basename }}"
        key_file: "{{ cert_dir }}/{{ cluster_name }}/pg/{{ key_file | basename }}"
        ca_file: "{{ cert_dir }}/{{ cluster_name }}/pg/{{ ca_file | basename }}"
        password_file: "{{ cert_dir }}/{{ cluster_name }}/pg/{{ password_file | basename }}"

    http:
      enabled: true                    # включен хттп или нет
      listen: '{{ ansible_fqdn }}'     # адрес, который будет слушать инстанс. Для IP указать {{ansible_default_ipv4.address}}
      advertise: '{{ ansible_fqdn }}'  # адрес, который будет сервер сообщать остальным
      first_port: 18001                # начальный порт для HTTP-протокола
      tls:
        enabled: "true/false"
        cert_file: "{{ cert_dir }}/{{ cluster_name }}/pg/{{ cert_file | basename }}"
        key_file: "{{ cert_dir }}/{{ cluster_name }}/pg/{{ key_file | basename }}"
        password_file: "{{ cert_dir }}/{{ cluster_name }}/pg/{{ password_file | basename }}"
```

Изменения в коде довольно прозрачны будут. Параметр надо проксировать напрямую, иерархию параметрам пикодата выстроит сама.

У плагинов уже сейчас есть настройки в роли. Мы их расширим.

```yaml
    plugins:
      example:                                                  # плагин
        path: '../plugins/weather_0.1.0-ubuntu-focal.tar.gz'    # путь до пакета плагина
        config: '../plugins/weather-config.yml'                 # путь до файла с настройками плагина
        services:
          weather_service:
            tiers:                                                  # список тиров, в которые устанавливается сервис плагина
              - default                                             # указано значение по умолчанию (default)
            listener:
              enabled: true                    # включен листенер или нет
              listen: '{{ ansible_fqdn }}'     # адрес, который будет слушать инстанс. Для IP указать {{ansible_default_ipv4.address}}
              advertise: '{{ ansible_fqdn }}'  # адрес, который будет сервер сообщать остальным
              first_port: 7401                 # начальный порт для плагина
              tls:
                enabled: "true/false"
                cert_file: "{{ cert_dir }}/{{ cluster_name }}/pg/{{ cert_file | basename }}"
                key_file: "{{ cert_dir }}/{{ cluster_name }}/pg/{{ key_file | basename }}"
                password_file: "{{ cert_dir }}/{{ cluster_name }}/pg/{{ password_file | basename }}"
```

### Как происходит ротация сертификатов

Так как сертификаты читаются при открытии сокета на слушание, то замена сертификатов проводится в два этапа на каждом инстансе:

1. Обновляется сертификат
2. Перезапускается инстанс.

## Резюме

[#1771](https://git.picodata.io/core/picodata/-/issues/1771) становится ненужным.
