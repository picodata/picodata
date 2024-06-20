# TO_CHAR {: #to_char }

Функция `TO_CHAR` преобразует объект *expression* типа [DATETIME] в строку
типа [TEXT] согласно формату *format*.

Значение *format* должно соответствовать спецификации [strftime].

[TEXT]: ../sql_types.md#text
[DATETIME]: ../sql_types.md#datetime
[strftime]: https://man.freebsd.org/cgi/man.cgi?query=strftime

## Синтаксис {: #syntax }

![TO_CHAR](../../images/ebnf/to_char.svg)

### Выражение {: #expression }

<details><summary>Диаграмма</summary><p>
![Expression](../../images/ebnf/expression.svg)
</p></details>

### Литерал {: #literal }

<details><summary>Диаграмма</summary><p>
![Literal](../../images/ebnf/literal.svg)
</p></details>

## Примеры {: #examples }

??? example "Тестовые таблицы"
    Примеры использования команд включают в себя запросы к [тестовым
    таблицам](../legend.md).

```sql title="Преобразование объектов DATETIME в строковые литералы заданного формата"
picodata> SELECT to_char(since, 'In stock since: %d %b %Y') FROM orders;
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
