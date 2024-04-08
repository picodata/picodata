# Сервер для получения метрик из пикодаты

#[derive(Debug, Parser, Clone)] #[command(author, version, about, long_about = None)]
struct Args { #[arg(long, default_value = "localhost")]
host: String, #[arg(long, default_value = "3301")]
port: String, #[arg(long, default_value = "pico_service")]
username: String, #[arg(long, default_value = "")]
password: String, #[arg(long, default_value = "127.0.0.1:4401")]
addr: String,
}

## Запуск

- host - хост пикодаты
- port - порт пикодаты
- username - юзер пикодаты
- password - пароль юзера пикодаты
- addr - адрес сервера

```shell
./target/debug/gostech-metrics --host localhost --port 3301 --username pico_service --password pwd --addr localhost:4401
```

## Получить метрики

```shell
curl --location 'http://<addr>/metrics'
```
