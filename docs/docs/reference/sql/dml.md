# Data Manipulation Language

Data Manipulation Language — команды языка SQL, изменяющие данные в
таблицах кластера. К ним относятся команды `CALL`, `DELETE`, `INSERT`,
`UPDATE`. Поддерживаются операции с шардированными, глобальными
и системными таблицами.

См. также:

- [Таблицы](../../overview/glossary.md#table)

## Синтаксис {: #syntax }

![DML](../../images/ebnf/dml.svg)

## Общие опции и параметры {: #common_options }

Для DML-запросов применяются следующие общие параметры и опции:

* [SQL_VDBE_OPCODE_MAX](common_options.md#vdbe)
* [SQL_MOTION_ROW_MAX](common_options.md#vdbe)
* [FORWARD](common_options.md#forward)
