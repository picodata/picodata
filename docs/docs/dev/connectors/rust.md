# Rust

В данном разделе приведено описание [Rust-драйвера] для работы с СУБД Picodata.

## Общие сведения {: #intro }

Драйвер предоставляет [Rust API][rust_api] для работы с
Picodata и служит коннектором к СУБД Picodata из приложений, написанных
на языке Rust. Драйвер основан на крейте `tokio_postgres`.

[rust_api]: https://docs.rs/picodata-rust/1.0.0/picodata_rust/
[Rust-драйвера]: https://crates.io/crates/picodata-rust

## Подключение {: #enabling }

Добавьте следующий блок в ваш файл `Cargo.toml`:

```toml
[dependencies]
picodata-rust = { version = "1.0.0" }
```

При необходимости, включите SSL/TLS:

```toml
[dependencies]
picodata-rust = { version = "1.0.0", features = ["tls"] }
```

## Пример использования {: #usage_example }

```rust
use std::time::Duration;
use picodata_rust::{Config, connect_cfg, Client, Error};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut pg_cfg = pg::Config::new();
    pg_cfg.host("127.0.0.1");
    pg_cfg.user("user");
    pg_cfg.password("password");
    pg_cfg.dbname("mydb");

    let mut cfg = Config {
        postgres: pg_cfg,
        pool_max_size: 10,
        discorery_interval: Duration::from_secs(1),
    };

    let client: Client = connect_cfg(cfg).await?;

    let rows = client.query(
        "SELECT id, name FROM users WHERE active = $1",
        &[&true]
    ).await?;
    for row in rows {
        let id: i32 = row.get("id");
        let name: &str = row.get("name");
        println!("User {}: {}", id, name);
    }

    client.close().await?;
    Ok(())
}
```

## Настройка SSL/TLS и mTLS {: #ssl_config}

Если вы используете драйвер с включённой в файле `Cargo.toml` функцией
SSL/TLS, то достаточно указать пути к сертификатам в структуре `Config`:

```rust
use std::time::Duration;
use picodata_rust::{Config, connect_cfg, Client, Error};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut pg_cfg = pg::Config::new();
    pg_cfg.host("127.0.0.1");
    pg_cfg.user("user");
    pg_cfg.password("password");
    pg_cfg.dbname("mydb");

    let mut cfg = Config {
        postgres: pg_cfg,
        pool_max_size: 10,
        discorery_interval: Duration::from_secs(1),
        ssl_cert_file: Some("/path/to/client.crt".into()),
        ssl_key_file: Some("/path/to/client.key".into()),
        ssl_ca_file: Some("/path/to/ca-cert.crt".into()),
    };

    let client: Client = connect_cfg(cfg).await?;
    client.close().await?;
    Ok(())
}
```

!!! note "Примечание"
    Наличие корневого сертификата (`ca-cert.crt`)
    необходимо для двусторонней проверки подлинности (mutual TLS, mTLS).
    Без него режим SSL/TLS также будет работать, но только в значении
    проверки сервера клиентом.

## Изменение стратегии балансировки {: #balancing }

По умолчанию, для балансировки используется стратегия RoundRobin,
однако, вы можете реализовать альтернативную стратегию, используя трейт
`Strategy`:

```rust title="пример для стратегии Random"
use picodata_rust::{Config, client::Client, strategy::{Random, Strategy}};

let client = Client::new_with_strategy(cfg, Random).await?;
```
