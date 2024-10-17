# LOWER

Выражение `LOWER` используется в [SELECT](select.md)-запросах для
получения [строковых данных](../sql_types.md#text) в нижнем регистре.

## Синтаксис {: #syntax }

![LOWER](../../images/ebnf/lower.svg)

## Пример использования {: #using_example }

Для проверки `LOWER` используем таблицу, содержащую строковые
значения как в верхнем, так и в нижнем регистре:

```sql
CREATE TABLE str(n string primary key)
INSERT INTO str VALUES ('PRODUCT'), ('Product'), ('prod_1')
```

Следующая команда выведет все строки, конвертируя значения в нижний
регистр:

```sql
SELECT lower(n) FROM str
```

??? note "Результат"
    ```shell
    +-----------+
    | col_1     |
    +===========+
    | "product" |
    |-----------|
    | "product" |
    |-----------|
    | "prod_1"  |
    +-----------+
    (3 rows)
    ```
