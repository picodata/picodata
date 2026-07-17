# Picopyn

В данном разделе приведено описание [Python-драйвера] для работы с СУБД Picodata.

## Общие сведения {: #intro }

Драйвер Picopyn представляет собой пакет для Python, реализующий подключение и работу с СУБД Picodata из Python-приложений.

Picopyn предоставляет как асинхронный, так и синхронный инерфейс.

Дополнительные примеры и подробности реализации можно найти в [документации](https://picopyn.readthedocs.io).

Версионирование:

| Версия Picopyn | Версия Picodata    | Статус                     |
|----------------|--------------------|----------------------------|
| <=1.0.0        | >=25.4.x, < 26.2.x | ✅ Полностью поддерживается |
| -              | >=26.2.x           | ⚠️ Ещё не протестирована    |

[Python-драйвера]: https://git.picodata.io/core/drivers/picopyn
[PYPI]: https://pypi.org/project/picopyn/
[asyncpg]: https://github.com/MagicStack/asyncpg
[psycopg]: https://www.psycopg.org/

## Поддерживаемые функции {: #features }

Драйвер Picopyn обеспечивает следующую функциональность:

- поддержка пула подключений, возможность настраивать размер пула
- опциональное автоматическое обнаружение узлов кластера Picodata
- возможность выбора стратегии балансировки подключений
- асинхронный API, основанный на [asyncpg]
- синхронный [DBAPI-совместимый](https://peps.python.org/pep-0249/) API, основанный на [psycopg]

## Подключение {: #enabling }

Установите драйвер из [PYPI]:

```shell
pip install picopyn
```

Или из исходного кода:

```shell
git clone https://git.picodata.io/core/drivers/picopyn.git
cd picopyn
make install
```

## Примеры использования {: #usage_example }

### Использование асинхронного драйвера {: #async_driver }

```python
import asyncio
from picopyn.asynchronous import Client

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

### Использование синхронного драйвера {: #sync_driver }

```python
from picopyn.synchronous import Connection

# create and connect to the picodata cluster
conn = Connection("postgresql://admin:pass@localhost:5432")
conn.connect()
cur = conn.cursor()

# execute DDL operations
cur.execute('''
    CREATE TABLE "warehouse" (id INTEGER NOT NULL, item TEXT NOT NULL, PRIMARY KEY (id)) USING memtx DISTRIBUTED BY (id) OPTION (TIMEOUT = 3.0);
''')

# execute DML/DQL operations
cur.execute('INSERT INTO "warehouse" VALUES (%s, %s)', (1, "test"))
cur.execute('SELECT * FROM "warehouse"')
print(cur.fetchall())

conn.close()
```

## Изменение параметров {: #configure }

### Параметры асинхронного клиента {: #client_settings }

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

### Параметры асинхронного пула подключений {: #pool_settings }

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
