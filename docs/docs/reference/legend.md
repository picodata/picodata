# Тестовые таблицы

Некоторые примеры использования SQL-команд на сайте документации включают
в себя запросы к тестовым таблицам. Чтобы пользователь смог самостоятельно
воспроизвести эти примеры, здесь опубликованы команды для создания
и заполнения тестовых таблиц.

## Описание {: #description }

В качестве тестовых используются четыре шардированные таблицы:

1. WAREHOUSE — перечень и тип строительных материалов на складе
1. ITEMS — перечень и количество строительных материалов
1. ORDERS — список закупок
1. DELIVERIES — учет новых поступлений

## Создание таблиц {: #create_test_tables }

```sql title="Создание таблицы WAREHOUSE"
CREATE TABLE warehouse (
    id INTEGER NOT NULL,
    item TEXT NOT NULL,
    type TEXT NOT NULL,
    PRIMARY KEY (id))
USING memtx DISTRIBUTED BY (id)
OPTION (TIMEOUT = 3.0);
```

``` sql title="Создание таблицы ITEMS"
CREATE TABLE items (
    id INTEGER NOT NULL,
    name TEXT NOT NULL,
    stock INTEGER,
    PRIMARY KEY (id))
USING memtx DISTRIBUTED BY (id)
OPTION (TIMEOUT = 3.0);
```

```sql title="Создание таблицы ORDERS"
CREATE TABLE orders (
    id INTEGER NOT NULL,
    item TEXT NOT NULL,
    amount INTEGER,
    since DATETIME,
    PRIMARY KEY (id))
USING memtx DISTRIBUTED BY (id)
OPTION (TIMEOUT = 3.0);
```

```sql title="Создание таблицы DELIVERIES"
CREATE TABLE deliveries (
    nmbr INTEGER NOT NULL,
    product TEXT NOT NULL,
    quantity INTEGER,
    PRIMARY KEY (nmbr))
USING vinyl DISTRIBUTED BY (product)
OPTION (TIMEOUT = 3.0);
```

## Заполнение таблиц {: #populate_test_tables }

```sql title="Заполнение таблицы WAREHOUSE"
INSERT INTO warehouse VALUES
    (1, 'bricks', 'heavy'),
    (2, 'bars', 'light'),
    (3, 'blocks', 'heavy'),
    (4, 'piles', 'light'),
    (5, 'panels', 'light');
```

```sql title="Заполнение таблицы ITEMS"
INSERT INTO items VALUES
    (1, 'bricks', 1123),
    (2, 'panels', 998),
    (3, 'piles', 177),
    (4, 'bars', 90211),
    (5, 'blocks', 16);
```

```sql title="Заполнение таблицы ORDERS"
INSERT INTO orders VALUES
    (1, 'metalware', 5000, to_date('2024-02-13 12:43:24', '')),
    (2, 'adhesives', 350, to_date('2024-01-29 15:41:22', '')),
    (3, 'moldings', 900, to_date('2023-11-11 13:01:56', '')),
    (4, 'bars', 100, to_date('2024-05-11 18:59:01', '')),
    (5, 'blocks', 20000, to_date('2024-04-01 00:00:01', ''));
```

```sql title="Заполнение таблицы DELIVERIES"
INSERT INTO deliveries VALUES
    (1, 'metalware', 2000),
    (2, 'adhesives', 300),
    (3, 'moldings', 100),
    (4, 'bars', 5),
    (5, 'blocks', 15000);
```
