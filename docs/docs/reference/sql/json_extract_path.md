# JSON_EXTRACT_PATH

Функция `JSON_EXTRACT_PATH` извлекает данные из поля в формате JSON
согласно указанным компонентам пути.

## Синтаксис {: #syntax }

![JSON_EXTRACT_PATH](../../images/ebnf/json_extract_path.svg)

Первый аргумент функции -- поле типа JSON, из которого извлекаются данные.

Второй и последующие аргументы функции -- компоненты пути, по которому извлекаются данные из JSON.
Представляют собой результаты выражений типа [TEXT](../sql_types.md#text).

### Выражение {: #expression }

??? note "Диаграмма"
    ![Expression](../../images/ebnf/expression.svg)

### Литерал {: #literal }

??? note "Диаграмма"
    ![Literal](../../images/ebnf/literal.svg)

## Пример использования {: #using_example }

```sql
CREATE TABLE t (id INT, col JSON, PRIMARY KEY (id));
```

??? note "Таблица"
    ```shell
    +----+-------------------+
    | id | col               |
    +========================+
    | 1  | {"a": {"b": "c"}} |
    +----+-------------------+
    ```

```sql
SELECT json_extract_path(col, 'a', 'b') FROM t;
```

??? note "Результат"
    ```shell
    +-------+
    | col_1 |
    +=======+
    | c     |
    +-------+
    (1 rows)
    ```
