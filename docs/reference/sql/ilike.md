# ILIKE

Выражение `ILIKE` используется в [SELECT](select.md)-запросах для
проверки соответствия строк шаблону символов без учета регистра. При
совпадении возвращается `true`, в противном случае — `false`. Для
проверки подходят только столбцы, имеющие [строковый тип
данных](../sql_types.md#text).

См. также:

- [Выражение LIKE](like.md)

## Синтаксис {: #syntax }

![LIKE](../../images/ebnf/ilike.svg)

### Выражение {: #expression }

??? note "Диаграмма"
    ![Expression](../../images/ebnf/expression.svg)

### Литерал {: #literal }

??? note "Диаграмма"
    ![Literal](../../images/ebnf/literal.svg)

## Примеры использования {: #using_examples }

Для проверки `ILIKE` потребуется таблица, содержащая похожие строковые
значения в верхнем и нижнем регистре:

```sql
CREATE TABLE str(n string primary key)
INSERT INTO str VALUES ('PRODUCT'), ('Product'), ('prod_1')
```

Следующая команда выведет все строки таблицы:

```sql
SELECT n FROM str WHERE n ilike 'prod%'
```

??? note "Результат"
    ```shell
    +-----------+
    | n         |
    +===========+
    | "Product" |
    |-----------|
    | "PRODUCT" |
    |-----------|
    | "prod_1"  |
    +-----------+
    (3 rows)
    ```

Более подробно об использовании подстановочных символов и экранировании
см. в описании выражения [LIKE](like.md).
