# CREATE PLUGIN

[DDL](ddl.md)-команда `CREATE PLUGIN` используется для регистрации
[плагина](../../overview/glossary.md#plugin) в кластере.

## Синтаксис {: #syntax }

![CREATE PLUGIN](../../images/ebnf/create_plugin.svg)

## Параметры {: #params }

* **PLUGIN** — имя плагина. Соответствует правилам имен для всех
  [объектов](object.md) в кластере. Версия плагина указывается в формате
  semver.
* **IF NOT EXISTS** — проверка наличия плагина в кластере. Если плагин уже есть,
  то никакое действие не будет выполнено.

## Примеры {: #examples }

```sql
CREATE PLUGIN weather_cache 0.1.0
```
