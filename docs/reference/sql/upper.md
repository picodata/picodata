# UPPER

Выражение `UPPER` используется в [SELECT](select.md)-запросах для
получения [строковых данных](../sql_types.md#text) в верхнем регистре.

## Синтаксис {: #syntax }

![UPPER](../../images/ebnf/upper.svg)

## Пример использования {: #using_example }

Для проверки `UPPER` используем таблицу, содержащую строковые
значения как в верхнем, так и в нижнем регистре:

```sql
CREATE TABLE str(n string primary key)
INSERT INTO str VALUES ('PRODUCT'), ('Product'), ('prod_1')
```

Следующая команда выведет все строки, конвертируя значения в верхний
регистр:

```sql
SELECT upper(n) FROM str
```

??? note "Результат"
    ```shell
    +-----------+
    | col_1     |
    +===========+
    | "PRODUCT" |
    |-----------|
    | "PRODUCT" |
    |-----------|
    | "PROD_1"  |
    +-----------+
    (3 rows)
    ```
