# Поддерживаемые запросы и их синтаксис
Функциональность компонента SQL Broadcaster обеспечивает поддержку распределенных запросов SELECT и INSERT. Ниже на схеме показаны базовые варианты этих запросов.


![Query](ebnf/query.svg)

## Запрос SELECT

Cхема возможных запросов SELECT показана ниже

![Select](ebnf/select.svg)


### Примеры запросов
```
SELECT "identification_number", "product_code" FROM "hash_testing"
        WHERE "identification_number" = 1"
```

```
SELECT "identification_number", "product_code"
        FROM "hash_testing"
        WHERE "identification_number" = 1 AND "product_code" = '1'
        OR "identification_number" = 2 AND "product_code" = '2'
```

```
SELECT *
        FROM
            (SELECT "identification_number", "product_code"
            FROM "hash_testing"
            WHERE "sys_op" = 1
            UNION ALL
            SELECT "identification_number", "product_code"
            FROM "hash_testing_hist"
            WHERE "sys_op" > 1) AS "t3"
        WHERE "identification_number" = 1
```


Используется в:

* EXPRESSION
* INSERT
* QUERY
* SELECT

### **COLUMN**

![Column](ebnf/column.svg)



Используется в:

* Select

### **EXPRESSION**

![Expression](ebnf/expression.svg)



Используется в:

* Cast
* Column
* Expression
* Select

### **GROUP BY**

![GroupBy](ebnf/groupby.svg)



Используется в:

* Select

### **REFERENCE**

![Reference](ebnf/reference.svg)



Используется в:

* Expression
* GroupBy
* Insert

### **VALUE**

![Value](ebnf/value.svg)



Используется в:

* Expression
* Values

### **CAST**

![Cast](ebnf/cast.svg)



Используется в:

* GroupBy

### **TYPE**

![Type](ebnf/type.svg)



Используется в:

* Cast

## Запрос INSERT

**INSERT**

![Insert](ebnf/insert.svg)

### Пример запроса
```
INSERT INTO "t" VALUES(1, 2, 3, 4)
```

Используется в:

* Query


### **VALUES**

![Values](ebnf/values.svg)



Используется в:

* Insert
* Query

Читать далее: [Перечень поддерживаемых типов данных](../sql_datatypes)
