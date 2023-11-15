# Работа с данными SQL
В данном разделе приведены примеры команд для работы с данными в
Picodata с помощью языка [SQL-запросов](../reference/sql_queries.md).

## Создание таблицы {: #creating-table }

Для создания таблицы в Picodata следует сначала
[подключиться](connecting.md#accessing-console) к
интерактивной консоли инстанса. Для ввода команд можно использовать
как формат Lua, так и язык SQL напрямую, в зависимости от
[выбранного](../reference/sql_queries.md#available_langs) языка консоли. В примерах
ниже использован формат Lua.

Пользователям доступны функции для работы как с глобальными, так и
шардированными таблицами (в последнем случае реализованы возможности
[распределенного SQL](../architecture/distributed_sql.md)).

Для примера создадим шаблон списка друзей Свинки Пеппы,
котором будет два поля: идентификатор записи и имя друга:

```sql
pico.sql([[
	create table "friends_of_peppa" (
    	        "id" integer,
                "name" text not null,
    	        primary key ("id")
	) using memtx distributed by ("id")
	option (timeout = 3.0)
]])
```

Помимо двух колонок, в примере указаны:

- первичный ключ таблицы (колонка `"id"`);
- [движок хранения данных](../overview/glossary.md#db-engine) in-memory (`memtx`);
- тип таблицы (шардированный, `distributed by`);
- ключ шардирования таблицы (колонка `"id"`);
- таймаут перед возвращением управления пользователю.

Для того чтобы создать такую же, но глобальную таблицу, следует указать
соответствующий тип:

```sql
pico.sql([[
	create table "friends_of_peppa" (
    	        "id" integer,
                "name" text not null,
    	        primary key ("id")
	) using memtx distributed globally
	option (timeout = 3.0)
]])
```

Подробнее о типах таблиц см. в [глоссарии](../overview/glossary.md#table).
Описание команд SQL приведено в разделе [Команды SQL](../reference/sql_queries.md).

## Запись данных в таблицу {: #writing-to-table }
Запись данных, т.е. вставка строк, в таблицу происходит с помощью
команды `INSERT` в SQL-запросе. В данный момент поддерживается запись
только в шардированные таблицы. Можно использовать обычный запрос с
прямой передачей значений:

```sql
pico.sql(
	[[insert into "friends_of_peppa" ("id", "name") values (1, "Suzy")]]
)
```

Либо параметризированный запрос:

```sql
pico.sql(
	[[insert into "friends_of_peppa" ("id", "name") values (?, ?)]],
	{1, "Suzy"}
)
```

См. [подробнее](../reference/sql_queries.md#insert) о различиях в `INSERT`-запросах.

## Чтение данных из таблицы {: #reading-from-table }
Для чтения всех данных из таблицы подойдёт команда:

```
pico.sql([[select * from "friends_of_peppa"]])
```

Можно вывести отдельно строку по известному полю:

```
pico.sql([[select * from "friends_of_peppa" where "id" = 1]])
```

См. [подробнее](../reference/sql_queries.md#select) о вариантах чтения данных в SQL.

## Удаление данных {: #deleting-from-table }

Удаление строки с известным `id`:

```sql
picodata> pico.sql([[delete from "friends_of_peppa" where "id" = 1]])
```

В консоли будет выведено количество удаленных строк (в данном случае, это `1`).

Приведенный выше пример поможет сделать первые шаги в работе с данными в Picodata.
Подробнее о внутренней архитектуре кластера Picodata см. в разделе
[Жизненный цикл инстанса](../architecture/instance_lifecycle.md).

Параметры запуска из командной строки описаны в разделе [Аргументы командной строки Picodata](../reference/cli.md).
