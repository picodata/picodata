# ALTER SYSTEM

[DDL](ddl.md)-команда `ALTER SYSTEM` используется для изменения
системных параметров кластера.

## Синтаксис {: #syntax }

![ALTER SYSTEM](../../images/ebnf/alter_system.svg)

## Параметры {: #params }

* **SET** — установка значения параметра.

* **RESET** — сброс параметра до значения по умолчанию.

* **RESET ALL** — сброс всех параметров до их значений по умолчанию.

## Примеры {: #examples }

Установка параметра:

```sql
ALTER SYSTEM SET password_enforce_digits to false;
```

Сброс параметра:

```sql
ALTER SYSTEM RESET password_enforce_digits;
```

Сброс всех параметров:

```sql
ALTER SYSTEM RESET all;
```

См. также:

 — [Параметры конфигурации СУБД](../system_parameters.md)
