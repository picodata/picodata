# TO_CHAR

Функция `TO_CHAR` преобразует объект *expression* типа [DATETIME] в строку
типа [TEXT] согласно формату *format*.

Функция является [строгой].

Значение *format* должно соответствовать спецификации [strftime].

## Синтаксис {: #syntax }

![TO_CHAR](../../images/ebnf/to_char.svg)

## Выражение {: #expression }

??? note "Диаграмма"
    ![Expression](../../images/ebnf/expression.svg)

## Литерал {: #literal }

??? note "Диаграмма"
    ![Literal](../../images/ebnf/literal.svg)

## Примеры {: #examples }

??? example "Подготовка тестового окружения"
    Примеры использования команд включают в себя запросы к [тестовым
    таблицам](../legend.md).

```sql title="Преобразование объектов DATETIME в строковые литералы заданного формата"
sql> SELECT to_char(since, 'In stock since: %d %b %Y') FROM orders;
+-------------------------------+
| COL_1                         |
+===============================+
| "In stock since: 13 Feb 2024" |
|-------------------------------|
| "In stock since: 29 Jan 2024" |
|-------------------------------|
| "In stock since: 11 Nov 2023" |
|-------------------------------|
| "In stock since: 11 May 2024" |
|-------------------------------|
| "In stock since: 01 Apr 2024" |
+-------------------------------+
(5 rows)
```

[TEXT]: ../sql_types.md#text
[DATETIME]: ../sql_types.md#datetime
[strftime]: https://man.freebsd.org/cgi/man.cgi?query=strftime
[строгой]: ../../overview/glossary.md#strict_function
