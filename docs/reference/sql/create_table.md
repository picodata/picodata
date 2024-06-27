# CREATE TABLE

[DDL](ddl.md)-команда `CREATE TABLE` используется для создания новой
шардированной или глобальной таблицы.

## Синтаксис {: #syntax }

![Create table](../../images/ebnf/create_table.svg)

### Тип {: #type }

??? note "Диаграмма"
    ![Type](../../images/ebnf/type.svg)

## Параметры {: #params }

* **TABLE** — название таблицы. Соответствует правилам имен для всех [объектов](object.md)
  в кластере.

* **PRIMARY KEY** — первичный ключ обеспечивает уникальность и сортировку данных только
  в рамках одного экземпляра кластера. Глобальную уникальность записи он не дает.

* **DISTRIBUTED GLOBALLY** — глобальное распределение таблицы. В результате, данные в
  таблице идентичны на всех экземплярах кластера и синхронизируются через Raft-журнал.
  Поддерживают только `memtx` движок хранения данных.

* **DISTRIBUTED BY** — шардирование таблицы по набору колонок. В результате, каждый
  экземпляр содержит только часть данных в таблице.

* **MEMTX** — [движок хранения данных](../../overview/glossary.md#db_engine) в памяти.

* **VINYL** — дисковый движок хранения данных, использующий LSM-деревья (Log Structured
  Merge Tree).

## Примеры {: #examples }

Создание шардированной таблицы с использованием движка хранения `memtx`:

```sql
CREATE TABLE warehouse (
            id INTEGER NOT NULL,
            item TEXT NOT NULL,
            type TEXT NOT NULL,
            PRIMARY KEY (id))
USING memtx DISTRIBUTED BY (id)
OPTION (TIMEOUT = 3.0);
```
