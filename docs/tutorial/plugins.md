# Управление плагинами

В данном разделе приведены примеры команд для управления плагинами в
Picodata с помощью команд языка SQL.

## Общие сведения {: #intro }

Плагин — набор кода на языке Rust для добавления новой функциональности к
Picodata. Плагин имеет собственный жизненный цикл, т.е. может быть
независимо от остальных плагинов подключен, отключен, и использован
вместе с другими плагинами.

Управлять плагинами может только [Администратор СУБД]. В свою очередь,
плагины также работают с административными правами и имеют полный доступ
к системе.

[Администратор СУБД]: ../admin/access_control.md#admin

## Запуск Picodata с поддержкой плагинов {: #run_picodata_with_plugins }

Для использования плагинов запустите Picodata c параметром
[`picodata run --share-dir`](../reference/cli.md#run_share_dir)

В этой директории Picodata будет искать плагины. Требуется, чтобы
структура директорий плагина соответствовали схеме:

```
корень share-dir
    └── имя_плагина
        └── версия_плагина
```

В качестве примера рассмотрим установку и использование тестового
плагина [weather_cache], который позволяет узнать температуру воздуха по
заданным координатам. Плагин принимает на вход долготу и широту,
проверяет погоду в указанных координатах с помощью публичного сервиса
OpenWeather и возвращает температуру в градусах Цельсия. Данные
кэшируются: при повторных запросах они будут прочитаны из Picodata.

[weather_cache]: https://git.picodata.io/picodata/core/plugin-example

!!! note "Примечание"
	Здесь и далее приведен список шагов для создания плагина вручную. Эти
 	операции можно также автоматизировать с помощью консольной утилиты
 	[Pike](create_plugin.md#using_pike)

## Подготовка плагина {: #prepare_plugin }

Исходный код плагина требуется сначала скомпилировать, для чего
понадобятся Rust и Cargo 1.85 или новее. Используйте следующую команду:

```shell
make artifacts
```

Результат сборки появится в директории `build`. Перенесите файлы в
поддиректорию плагина так, чтобы получилась следующая структура:

```
└── weather_cache
    └── 0.1.0
        ├── manifest.yaml
        ├── migrations
        │   └── 0001_weather.db
        └── weather_cache.so
```

## Структура плагина {: #plugin_structure }

### Манифест {: #manifest }

Файл манифеста (`manifest.yaml`) содержит описание плагина, список его
сервисов и их конфигурации, а также список необходимых миграций.
Содержимое файла в тестовом плагине:

```yaml
name: weather_cache
description: That one is created as an example of Picodata's plugin
version: 0.1.0
services:
  - name: weather_service
    description: This service provides HTTP route for a throughput weather cache
    default_configuration:
      openweather_timeout: 5
migration:
  - migrations/0001_weather.db
```

### Сервисы {: #services }

Тестовый плагин содержит один сервис `weather_service`, для которого
установлена конфигурация по умолчанию (таймаут ожидания ответа от службы
OpenWeather).

### Миграции {: #migrations }

Миграция представляет собой набор SQL-команд, которые необходимы для
создания нужных плагину объектов (`pico.UP`) и их удаления
 (`pico.DOWN`). Файлы миграций должны располагаться внутри поддиректории
 с версией плагина (путь к каждому файлу миграции прописывается в
 манифесте отдельно).

??? example "Содержимое файла `0001_weather.db` тестового плагина"

	```sql
	-- pico.UP

	CREATE TABLE "weather" (
		id UUID NOT NULL,
		latitude INTEGER NOT NULL,
		longitude INTEGER NOT NULL,
		temperature INTEGER NOT NULL,
		PRIMARY KEY (id)
	)
	USING memtx
	DISTRIBUTED BY (latitude, longitude);

	-- pico.DOWN
	DROP TABLE "weather";
	```

## Добавление плагина {: #add_plugin }

Добавление плагина означает его регистрацию в кластере. [Подключитесь к
консоли администратора](connecting.md#admin_console) инстанса Picodata,
запущенного с поддержкой плагинов, и введите SQL-команду [CREATE
PLUGIN](../reference/sql/create_plugin.md):

```sql
CREATE PLUGIN weather_cache 0.1.0;
```

После успешного добавления в системных таблицах появятся записи о новом плагине.

В таблице [`_pico_plugin`] со списком добавленных плагинов:

```sql
(admin) sql> SELECT * FROM _pico_plugin;
+-------------+---------+-------------+---------+-------------+------------+
| name        | enabled | services    | version | description | migration_ |
|             |         |             |         |             | list       |
+==========================================================================+
| "weather_ca | false   | ["weather_s | "0.1.0" | "That one   | ["migratio |
| che"        |         | ervice"]    |         | is created  | ns/0001_we |
|             |         |             |         | as an       | ather.db"] |
|             |         |             |         | example of  |            |
|             |         |             |         | Picodata's  |            |
|             |         |             |         | plugin"     |            |
+-------------+---------+-------------+---------+-------------+------------+
(1 rows)
```

В таблице [`_pico_plugin_config`] с конфигурацией сервисов плагина:

```sql
(admin) sql> SELECT * FROM _pico_plugin_config;
+-----------------+---------+-------------------+------------------+-------+
| plugin          | version | entity            | key              | value |
+==========================================================================+
| "weather_cache" | "0.1.0" | "weather_service" | "openweather_tim | 5     |
|                 |         |                   | eout"            |       |
+-----------------+---------+-------------------+------------------+-------+
(1 rows)
```

В таблице [`_pico_service`] со списком сервисов плагина (но пока без
привязки к [тиру][tier]):

```sql
(admin) sql> SELECT * FROM _pico_service;
+-----------------+-------------------+---------+-------+------------------+
| plugin_name     | name              | version | tiers | description      |
+==========================================================================+
| "weather_cache" | "weather_service" | "0.1.0" | []    | "This service    |
|                 |                   |         |       | provides HTTP    |
|                 |                   |         |       | route for a      |
|                 |                   |         |       | throughput       |
|                 |                   |         |       | weather cache"   |
+-----------------+-------------------+---------+-------+------------------+
(1 rows)
```

!!! note "Примечание"
	В кластере может быть не более двух версий одного и того же плагина.
	Чтобы добавить новую версию, может понадобиться удалить одну из старых.

## Запуск миграции плагина {: #migrate_plugin }

Процесс миграции означает создание или изменение необходимой для плагина
схемы данных. На данном этапе таблица `weather`, указанная в файле
`0001_weather.db` еще не создана, т.к. миграции не были запущены. Для их
запуска введите следующую команду [ALTER
PLUGIN](../reference/sql/alter_plugin.md):

```sql
ALTER PLUGIN weather_cache MIGRATE TO 0.1.0;
```

Успешная миграция означает, что в БД появилась новая таблица `weather`:

```sql
(admin) sql> SELECT * FROM weather;
+----+----------+-----------+-------------+
| id | latitude | longitude | temperature |
+=========================================+
+----+----------+-----------+-------------+
(0 rows)
```

Данные плагина в системных таблицах и таблицах, созданных в результате
миграции `pico.UP`, персистентны и сохраняются между циклами
включения/отключения плагина.

## Включение сервисов плагина {: #enable_plugin_services }

Основная функциональность плагина реализуется через его сервисы, которые
должны быть включены в явном виде. Сервисы работают с привязкой
к [тирам][tier]: если конфигурация тиров в кластере не была явно задана,
то в примере с тестовым плагином укажите тир по умолчанию `default` в
команде [ALTER PLUGIN](../reference/sql/alter_plugin.md):

```sql
ALTER PLUGIN weather_cache 0.1.0 ADD SERVICE weather_service TO TIER default;
```

После успешного включения плагина в системной таблице [`_pico_service`]
появится привязка сервиса к указанному тиру:

```sql
(admin) sql> SELECT * FROM _pico_service;
+----------------+----------------+---------+-------------+----------------+
| plugin_name    | name           | version | tiers       | description    |
+==========================================================================+
| "weather_cache | "weather_servi | "0.1.0" | ["default"] | "This service  |
| "              | ce"            |         |             | provides HTTP  |
|                |                |         |             | route for a    |
|                |                |         |             | throughput     |
|                |                |         |             | weather cache" |
+----------------+----------------+---------+-------------+----------------+
(1 rows)
```

[`_pico_plugin`]: ../architecture/system_tables.md#_pico_plugin
[`_pico_plugin_config`]: ../architecture/system_tables.md#_pico_plugin_config
[`_pico_service`]: ../architecture/system_tables.md#_pico_service
[tier]: ../overview/glossary.md#tier

## Включение и отключение плагина {: #enable_or_disable_plugin}

Включите плагин следующей командой:

```sql
ALTER PLUGIN weather_cache 0.1.0 ENABLE;
```

После успешного включения в колонке `enabled` в системной таблице
[`_pico_plugin`] установится значение `true`:

```sql
(admin) sql> SELECT * FROM _pico_plugin;
+-------------+---------+-------------+---------+-------------+------------+
| name        | enabled | services    | version | description | migration_ |
|             |         |             |         |             | list       |
+==========================================================================+
| "weather_ca | true    | ["weather_s | "0.1.0" | "That one   | ["migratio |
| che"        |         | ervice"]    |         | is created  | ns/0001_we |
|             |         |             |         | as an       | ather.db"] |
|             |         |             |         | example of  |            |
|             |         |             |         | Picodata's  |            |
|             |         |             |         | plugin"     |            |
+-------------+---------+-------------+---------+-------------+------------+
(1 rows)
```

!!! note "Примечание"
	Нельзя включить больше одной версии плагина в кластере

Для отключения плагина используйте следующую команду:

```sql
ALTER PLUGIN weather_cache 0.1.0 DISABLE;
```

## Конфигурация плагина {: #configure_plugin}

Конфигурация плагина хранится в [манифесте](#manifest) и предполагает
настройку входящих в него сервисов. Например, для изменения таймаута для
вызова OpenMeteo в тестовом плагине введите следующую команду [ALTER
PLUGIN](../reference/sql/alter_plugin.md):

```sql
ALTER PLUGIN weather_cache 0.1.0 SET weather_service.openweather_timeout='7';
```

См. также:

- [Конфигурация плагинов](../architecture/plugins.md#plugin_config)

## Проверка работы плагина {: #check_plugin}

Запустите отдельное окно терминала и введите следующую команду для
проверки работы тестового плагина:

```shell
curl "localhost:8081/api/v1/weather?longitude=55&latitude=66"
```

## Удаление плагина {: #drop_plugin}

Перед удалением плагина его необходимо [отключить](#enable_or_disable_plugin).

Введите команду [DROP PLUGIN](../reference/sql/drop_plugin.md) для
удаления плагина:

```sql
DROP PLUGIN weather_cache 0.1.0;
```

При удалении плагина с помощью указанной выше команды его схема данных
(записи в системных таблицах), а также таблицы, созданные ранее
миграцией `pico.UP`, остаются в кластере. Для очистки этих данных
следует при удалении плагина использовать параметр `WITH DATA`,
например:

```sql
DROP PLUGIN weather_cache 0.1.0 WITH DATA;
```

В таком случае будут запущена миграция `pico.DOWN`, а также удалены записи
плагина и его сервисов из системных таблиц.


См. также:

- [Разработка плагина с помощью Pike](create_plugin.md#using_pike)
- [Управление плагинами с помощью роли Ansible](deploy_ansible.md#plugin_management)
