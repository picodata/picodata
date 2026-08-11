# Picopyn

В данном разделе приведено описание [Python-драйвера] для работы с СУБД Picodata.

## Общие сведения {: #intro }

Драйвер Picopyn представляет собой пакет для Python, реализующий
подключение и работу с СУБД Picodata из Python-приложений.

Picopyn предоставляет как асинхронный, так и синхронный интерфейс.

Дополнительные примеры и подробности реализации можно найти на
[странице Readthedocs для Picopyn](https://picopyn.readthedocs.io).

Версии драйвера Picopyn требуют определённых версий СУБД Picodata.
Ниже показана таблица совместимости версий:

| Picopyn         | Picodata          |
|-----------------|-------------------|
| 0.1.1           | >=25.2.1, <25.4.4 |
| 0.2.0           | >=25.4.4, <25.5.1 |
| 1.0.0           | >=25.5.1, <26.1.x |
| ⚠️ В разработке | >=26.1.1, <26.2.x |


[Python-драйвера]: https://git.picodata.io/core/drivers/picopyn

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
pip install -e .
```

## Пример использования {: #usage_example }

```python
import asyncio
from picopyn import Client

async def main():
    # create and connect client to the picodata cluster
    client = Client(dsn="postgresql://admin:pass@localhost:5432")
    await client.connect()

    # execute DDL operations
    await client.execute('''
        CREATE TABLE "warehouse" (id INTEGER NOT NULL, item TEXT NOT NULL, PRIMARY KEY (id)) USING memtx DISTRIBUTED BY (id) OPTION (TIMEOUT = 3.0);
    ''')

    # execute DML/DQL operations
    await client.execute('INSERT INTO "warehouse" VALUES ($1::int, $2::varchar)', 1, "test")
    rows = await client.fetch('SELECT * FROM "warehouse"')
    print(rows)

    await client.close()

asyncio.run(main())
```

### Использование синхронного драйвера {: #sync_driver }

```python
from picopyn.synchronous import connect

# create and connect to the picodata cluster
with connect("postgresql://admin:pass@localhost:5432") as conn:
    cur = conn.cursor()

    # execute DDL operations
    cur.execute('''
        CREATE TABLE "warehouse" (id INTEGER NOT NULL, item TEXT NOT NULL, PRIMARY KEY (id)) USING memtx DISTRIBUTED BY (id) OPTION (TIMEOUT = 3.0);
    ''')

    # execute DML/DQL operations
    cur.execute('INSERT INTO "warehouse" VALUES (%s, %s)', (1, "test"))
    cur.execute('SELECT * FROM "warehouse"')
    print(cur.fetchall())
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
