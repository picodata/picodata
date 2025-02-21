# ALTER SYSTEM

[DDL](ddl.md)-команда `ALTER SYSTEM` используется для изменения
[параметров конфигурации СУБД](../../reference/db_config.md). Команда требует
привилегий [Администратора СУБД](../../tutorial/access_control.md#admin)
(`admin`).

## Синтаксис {: #syntax }

![ALTER SYSTEM](../../images/ebnf/alter_system.svg)

## Параметры {: #params }

* **SET** — установка значения параметра.

* **RESET** — сброс параметра до значения по умолчанию.

* **RESET ALL** — сброс всех параметров до их значений по умолчанию.

* **FOR TIER** / **FOR ALL TIERS** — применение действия для конкретного
  тира, или глобально для всех тиров. По умолчанию параметры
  устанавливаются глобально.

## Примеры {: #examples }

Установка параметра:

```sql
ALTER SYSTEM SET auth_password_enforce_digits to false;
```

Сброс параметра:

```sql
ALTER SYSTEM RESET auth_password_enforce_digits;
```

Сброс всех параметров:

```sql
ALTER SYSTEM RESET all;
```

Получить текущее значение параметра:

```sql
SELECT * FROM _pico_db_config WHERE key = 'auth_password_enforce_digits';
```
