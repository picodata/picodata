# Data Manipulation Language

Data Manipulation Language — команды языка SQL, изменяющие данные в
таблицах кластера (как в пользовательских, так и в системных).

## Синтаксис {: #syntax }

![DML](../../images/ebnf/dml.svg)

## Параметры {: #params }

* **SQL_VDBE_MAX_STEPS** — ограничение на максимальное количество
  [инструкций ](https://www.sqlite.org/opcode.html) при исполнении
  локального плана ( [VDBE](https://www.sqlite.org/vdbe.html)) на узле
  кластера.

* **TABLE_MAX_ROWS** — ограничение на максимальное число строк в
  результирующей виртуальной таблице, собирающей результаты отдельных
  локальных запросов.
