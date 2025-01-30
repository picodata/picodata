# Data Manipulation Language

Data Manipulation Language — команды языка SQL, изменяющие данные в
таблицах кластера. Поддерживаются операции с шардированными, глобальными
и системными таблицами.

См. также:

- [Таблицы](../../overview/glossary.md#table)

## Синтаксис {: #syntax }

![DML](../../images/ebnf/dml.svg)

## Параметры {: #params }

* **SQL_VDBE_OPCODE_MAX** — ограничение на максимальное количество
  [инструкций ](https://www.sqlite.org/opcode.html) при исполнении
  локального плана ([VDBE](https://www.sqlite.org/vdbe.html)) на узле
  кластера.

* **SQL_MOTION_ROW_MAX** — ограничение на максимальное число строк в
  результирующей виртуальной таблице, собирающей результаты отдельных
  локальных запросов.
