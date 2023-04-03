# Справочник команд SQL Broadcaster
Справочник команд предоставлят основные варианты использования команд SQL в Picodata при работе с распределенной СУБД.

Функциональность компонента SQL Broadcaster обеспечивает поддержку распределенных запросов SELECT и INSERT. Ниже на схеме показаны базовые варианты этих запросов.


![Query](ebnf/query.svg)

## Запрос SELECT

Запрос `SELECT` используется для получения информации из указанной таблицы в базе данных. Он возвращает 0 или более строк из таблицы согласно поисковому запросу. 
В контексте распределенной системы, запрос `SELECT` в `SQL Broadcaster` получает информацию из всех сегментов таблицы, которая может храниться на нескольких узлах кластера.

Cхема возможных распределенных запросов `SELECT` показана ниже.

![Select](ebnf/select.svg)


### Примеры запросов
Ниже показаны некоторые примеры работающих SQL-запросов.

Простой запрос строки из таблицы по известному ID:

```
SELECT "identification_number", "product_code" FROM "hash_testing"
        WHERE "identification_number" = 1"
```
Запрос с двумя вариантами условий, в каждом из которых используется оператор `AND`:
```
SELECT "identification_number", "product_code"
        FROM "hash_testing"
        WHERE "identification_number" = 1 AND "product_code" = '1'
        OR "identification_number" = 2 AND "product_code" = '2'
```

Пример запроса со вложенными подзапросами `SELECT`, объединенными оператором `UNION ALL`:

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

* expression
* insert
* query
* select

### **column**

![Column](ebnf/column.svg)



Используется в:

* select

### **expression**

![Expression](ebnf/expression.svg)



Используется в:

* cast
* column
* expression
* select

### **group by**

![GroupBy](ebnf/groupby.svg)



Используется в:

* select

### **reference**

![Reference](ebnf/reference.svg)



Используется в:

* expression
* groupby
* insert

### **value**

![Value](ebnf/value.svg)



Используется в:

* expression
* values

### **cast**

![Cast](ebnf/cast.svg)



Используется в:

* groupby

### **type**

![Type](ebnf/type.svg)



Используется в:

* cast

## Запрос INSERT
Запрос `INSERT` используется для помещения (записи) данных в одну или несколько таблиц. 

Схема возможных запросов `INSERT` показана ниже.


![Insert](ebnf/insert.svg)

### Пример запроса
Для примера вставим строки значений из таблицы `t2`в таблицу `t1` с использоваением подзапроса `SELECT`:
```
INSERT INTO "t1" FROM (SELECT id, id2 FROM t2)
```

Используется в:

* query


## Использование VALUES
Команда `VALUES` представляет собой конструктор строки значений для
использования в запросах `SELECT` и `INSERT`. В некотором смысле,
передаваемые с `VALUES` значения являются временной таблицей, которая
существует только в рамках запроса.

Пример использования с `SELECT`:
```
SELECT id, id2 FROM hash_testing WHERE (id, id2) in (VALUES (1, 2), (2, 3))
```
Здесь в таблицу hash_testing будет вставлены две строки: (1, 2) и (2, 3).

Пример использования со вставкой строки значений в таблицу при помощи команды `INSERT`:

```
INSERT INTO "t" VALUES(1, 2, 3, 4)
```

### **values**

![Values](ebnf/values.svg)

Используется в:

* insert
* query

Читать далее: [Перечень поддерживаемых типов данных](../sql_datatypes)
