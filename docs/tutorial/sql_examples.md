# Работа с данными SQL

В данном разделе приведены примеры команд для работы с данными в
Picodata с помощью команд языка SQL.

## Создание таблицы {: #creating_table }

Для [создания таблицы](../reference/sql/create_table.md) в Picodata следует сначала
[подключиться](connecting.md) к интерактивной консоли инстанса.

Пользователям доступны функции для работы как с глобальными, так и
шардированными таблицами (в последнем случае реализованы возможности
[распределенного SQL](../architecture/distributed_sql.md)).

Для примера создадим таблицу со списком товаров на складе. В ней будут
две колонки: идентификатор товара и его название:

```sql
CREATE TABLE warehouse (
			id INTEGER NOT NULL,
			item TEXT NOT NULL,
			PRIMARY KEY (id))
USING memtx DISTRIBUTED BY (id)
OPTION (TIMEOUT = 3.0);
```

Помимо двух колонок, в примере указаны:

- первичный ключ таблицы (колонка `id`);
- [движок хранения данных](../overview/glossary.md#db_engine) in-memory (`memtx`);
- тип таблицы (шардированный, `distributed by`);
- ключ шардирования таблицы (колонка `id`);
- таймаут перед возвращением управления пользователю.

Для того чтобы создать такую же, но глобальную таблицу, следует указать
соответствующий тип:

```sql
CREATE TABLE warehouse_global (
			id INTEGER NOT NULL,
			item TEXT NOT NULL,
			PRIMARY KEY (id))
USING memtx DISTRIBUTED GLOBALLY
OPTION (TIMEOUT = 3.0);
```

Подробнее о типах таблиц см. в [глоссарии](../overview/glossary.md#table).

## Запись данных в таблицу {: #writing_to_table }

Запись данных, т.е. вставка строк, в таблицу происходит с помощью
команды `INSERT` в SQL-запросе. В данный момент поддерживается запись
только в шардированные таблицы. Можно использовать обычный запрос с
прямой передачей значений:

```sql
INSERT INTO warehouse (id, item) VALUES (1, 'bricks');
```

Либо параметризированный запрос, но в Lua-режиме (`\s l lua`):

```sql
pico.sql(
	[[INSERT INTO warehouse (id, item) VALUES (?, ?)]],
	{1, 'bricks'}
);
```

См. [подробнее](../reference/sql/insert.md) о различиях в `INSERT`-запросах.

## Чтение данных из таблицы {: #reading_from_table }

Для чтения всех данных из таблицы подойдет команда:

```sql
SELECT * FROM warehouse;
```

Можно вывести отдельно строку по известному полю:

```sql
SELECT * FROM warehouse WHERE id = 1;
```

См. [подробнее](../reference/sql/select.md) о вариантах чтения данных в SQL.

## Удаление данных {: #deleting_from_table }

Удаление строки с известным `id`:

```sql
DELETE FROM warehouse WHERE id = 1;
```

В консоли будет выведено количество удаленных строк (в данном случае, это `1`).

Приведенный выше пример поможет сделать первые шаги в работе с данными в Picodata.
Подробнее о внутренней архитектуре кластера Picodata см. в разделе
[Жизненный цикл инстанса](../architecture/instance_lifecycle.md).

Параметры запуска из командной строки описаны в разделе [Аргументы командной строки Picodata](../reference/cli.md).
