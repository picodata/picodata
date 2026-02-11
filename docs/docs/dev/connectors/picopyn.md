# Picopyn

В данном разделе приведено описание [Python-драйвера] для работы с СУБД Picodata.

## Общие сведения {: #intro }

Драйвер Picopyn представляет собой пакет для Python, реализующий подключение и
работу с СУБД Picodata из Python-приложений. Picopyn основан на пакете [asyncpg].

[Python-драйвера]: https://git.picodata.io/core/drivers/picopyn
[asyncpg]: https://github.com/MagicStack/asyncpg

## Поддерживаемые функции {: #features }

Драйвер Picopyn обеспечивает следующую функциональность:

- поддержка пула подключений, возможность настраивать размер пула
- опциональное автоматическое обнаружение узлов кластера Picodata
- возможность выбора стратегии балансировки подключений
- полностью асинхронный API

## Подключение {: #enabling }

Установите драйвер из исходного кода:

```shell
git clone https://git.picodata.io/core/drivers/picopyn.git
cd picopyn
make install
```

## Пример использования {: #usage_example }

```python
import asyncio
from picopyn import Client

async def main():
    # create and connect client to the Picodata cluster
    client = Client(dsn="postgresql://admin:pass@localhost:5432")
    await client.connect()

    # execute DDL operations
    await client.execute('''
        CREATE TABLE "warehouse" (id INTEGER NOT NULL, item TEXT NOT NULL, PRIMARY KEY (id)) USING memtx DISTRIBUTED BY (id) OPTION (TIMEOUT = 3.0);
    ''')

    # execute DML operations
    await client.execute('INSERT INTO \"warehouse\" VALUES ($1::int, $2::varchar)', 1, "test")
    rows = await client.fetch('SELECT * FROM \"warehouse\"')
    print(rows)

    await client.close()

asyncio.run(main())
```

## Изменение параметров {: #configure }

### Параметры клиента {: #client_settings }

Используйте следующие параметры для класса `Client`:

- `dsn` (_str_) — имя источника данных (data source name) в формате
  `postgresql://user:pass@host:port`
- `balance_strategy` (_callable, optional_) — стратегия балансировки
  подключений. По умолчанию используется `round-robin`
- `pool_size` — размер используемого пула подключений
- `connect_kwargs` — дополнительные параметры подключения в формате
  `ключ = значение`

Пример использования параметров:

```python
>>> client = Client(
...     dsn="postgresql://admin:pass@localhost:5432",
...     balance_strategy=random_strategy,
...     pool_size=4
... )
```

Задайте стратегию балансировки подключений в блоке следующего вида:

```python
def random_strategy(connections):
...     import random
...     return random.choice(connections)
```

### Параметры пула подключений {: #pool_settings }

Используйте следующие параметры для класса `Pool`:

- `dsn` (_str_) — имя источника данных (data source name) в формате
  `postgresql://user:pass@host:port`
- `balance_strategy` (_callable, optional_) — стратегия балансировки
  нагрузки. По умолчанию используется `round-robin`
- `max_size` (_int_) — максимальное число подключений в пуле. Значение
  не может быть меньше 1
- `enable_discovery` (_bool_) — признак автоматического обнаружения
  узлов кластера Picodata. При значении `True` драйвер будет искать
  доступные узлы кластера, при `False` — использовать только указанный в
  `dsn` узел
- `connect_kwargs` — дополнительные параметры подключения в формате
  `ключ = значение`

Пример использования параметров:

```python
>>> pool = Pool(
...     dsn="postgresql://admin:pass@localhost:5432",
...     balance_strategy=random_strategy,
...     max_size=10,
...     enable_discovery=True
... )
```
