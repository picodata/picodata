# Возможности кластерного SQL

## Основная информация об SQL Broadcaster
Данный раздел содержит описание возможностей распределенного SQL, реализованного в виде компонента [SQL Broadcaster](https://git.picodata.io/picodata/picodata/sbroad) в составе Picodata.
SQL Broadcaster представляет собой библиотеку, предоставляющую функции планировщика и модуля исполнения SQL-запросов в рамках распределенного кластера СУБД Tarantool. Подробности архитектуры планировщика доступны в отдельной [PDF-презентации](https://git.picodata.io/picodata/picodata/sbroad/-/blob/main/doc/design/sbroad.pdf).

## Принцип работы распределенных запросов
SQL Broadcaster — это динамическая библиотека, которая работает на всех экземплярах Tarantool в кластере. SQL-запросы выполняются на узлах, исполняющих роль маршрутизаторов (роутеров), и получают данные с узлов хранения (storages). Поскольку в кластере может быть много как узлов хранения, так и роутеров, каждый распределенный запрос разбивается на части для опроса всех узлов. Собранные данные затем консолидируются и возвращаются пользователю. 

На схеме ниже показан общий принцип работы распределенного SQL-запроса в кластере с одним роутером.

![Distributed query](sbroad-curves.svg "general distributed query flow")

На схеме <span style="color:#ff0000ff">красным</span> показан исходный пользовательский запрос, <span style="color:#fcc501ff">желтым</span> — план запроса (IR, intermediate representation), <span style="color:#39cb00ff">зеленым</span> — собранные фрагменты ответов, <span style="color:#00c8e5ff">голубым</span> — консолидированный ответ на пользовательский запрос в виде списка кортежей, обработанного функцией MapReduce.

## Поддерживаемые запросы и их синтаксис
Функциональность компонента SQL Broadcaster обеспечивает поддержку распределенных запросов SELECT, VALUES и INSERT. Ниже на схеме показаны базовые варианты этих запросов.


![Query](ebnf/query.svg)

## Запрос SELECT

Cхема возможных запросов SELECT показана ниже

![Select](ebnf/select.svg)


Примеры запросов:
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

**COLUMN**

![Column](ebnf/column.svg)



Используется в:

* Select

**EXPRESSION**

![Expression](ebnf/expression.svg)



Используется в:

* Cast
* Column
* Expression
* Select

**GROUP BY:**

![GroupBy](ebnf/groupby.svg)



Используется в:

* Select

**REFERENCE**

![Reference](ebnf/reference.svg)



Используется в:

* Expression
* GroupBy
* Insert

**VALUE**

![Value](ebnf/value.svg)



Используется в:

* Expression
* Values

**CAST**

![Cast](ebnf/cast.svg)



Используется в:

* GroupBy

**TYPE**

![Type](ebnf/type.svg)



Используется в:

* Cast

## Запрос INSERT

**INSERT**

![Insert](ebnf/insert.svg)



Используется в:

* Query

## Запрос VALUES

**VALUES**

![Values](ebnf/values.svg)



Используется в:

* Insert
* Query


## Установка и пример использования SQL Broadcaster
### Общие сведения для установки
На данный момент компонент SQL Broadcaster работает только для приложений, использующих Tarantool Cartridge. Однако, при этом требуется использовать версию Tarantool, которая поставляется вместе с программным продуктом Picodata. Подробная информация об установке SQL Broadcaster приведена в описании его [Git-репозитория](https://git.picodata.io/picodata/picodata/sbroad). Установка возможна либо путём компиляции исходного кода, либо посредством утилиты `tarantoolctl`. В обоих случаях требуется выполнять команды из директории Cartridge-приложения. Для ознакомления и тестирования удобно воспользоваться [тестовым приложением](https://git.picodata.io/picodata/picodata/sbroad/-/tree/main/sbroad-cartridge/test_app), поставляемым вместе с кодом SQL Broadcaster.

### Пример установки для CentOS 7
Ниже показан пример установки сборочных зависимостей и компиляции исходного кода SQL Broadcaster в операционной системе CentOS 7. Шаги для других дистрибутивов Linux могут отличаться. Для начала требуется подключить репозиторий с пакетами Picodata. Этот этап документирован для [страницы загрузки Picodata](https://picodata.io/download/). Далее предлагается выполнить в терминале следующие команды:
````bash
sudo yum --disablerepo="*" --enablerepo="picodata" install -y tarantool-picodata tarantool-picodata-devel
sudo yum install cartridge-cli
sudo yum groupinstall -y "Development Tools"
sudo yum install rust cargo cmake3
sudo ln -s /usr/bin/cmake3 /usr/local/bin/cmake
git clone https://git.picodata.io/picodata/picodata/sbroad.git
cd sbroad/sbroad-cartridge/test_app
````
Сборка SQL Broadcaster, запуск приложения, настройка тестового шардирования:
````bash
cd .. && make build_integration
cd test_app
cartridge start -d
cartridge replicasets setup --bootstrap-vshard
````
### Пример использования
Перед тем как применять команды SQL Broadcaster, требуется загрузить в тестовое приложение корректную схему данных. Для этого откройте веб-страницу Cartridge (`localhost:8081`), перейдите в раздел Code и вставьте в поле файла `schema.yml` следующее содержимое:
<details>
  <summary>Показать:</summary>

````bash
spaces:
    arithmetic_space:
      format:
      - type: integer
        name: id
        is_nullable: false
      - type: integer
        name: a
        is_nullable: false
      - type: integer
        name: b
        is_nullable: false
      - type: integer
        name: c
        is_nullable: false
      - type: integer
        name: d
        is_nullable: false
      - type: integer
        name: e
        is_nullable: false
      - type: integer
        name: f
        is_nullable: false
      - type: boolean
        name: boolean_col
        is_nullable: false
      - type: string
        name: string_col
        is_nullable: false
      - type: number
        name: number_col
        is_nullable: false
      - type: unsigned
        name: bucket_id
        is_nullable: true
      temporary: false
      engine: memtx
      is_local: false
      sharding_key:
      - id
      indexes:
      - unique: true
        parts:
        - path: id
          type: integer
          is_nullable: false
        name: id
        type: TREE
      - unique: false
        parts:
        - path: bucket_id
          type: unsigned
          is_nullable: true
        name: bucket_id
        type: TREE
    arithmetic_space2:
      format:
      - type: integer
        name: id
        is_nullable: false
      - type: integer
        name: a
        is_nullable: false
      - type: integer
        name: b
        is_nullable: false
      - type: integer
        name: c
        is_nullable: false
      - type: integer
        name: d
        is_nullable: false
      - type: integer
        name: e
        is_nullable: false
      - type: integer
        name: f
        is_nullable: false
      - type: boolean
        name: boolean_col
        is_nullable: false
      - type: string
        name: string_col
        is_nullable: false
      - type: number
        name: number_col
        is_nullable: false
      - type: unsigned
        name: bucket_id
        is_nullable: true
      temporary: false
      engine: memtx
      is_local: false
      sharding_key:
      - id
      indexes:
      - unique: true
        parts:
        - path: id
          type: integer
          is_nullable: false
        name: id
        type: TREE
      - unique: false
        parts:
        - path: bucket_id
          type: unsigned
          is_nullable: true
        name: bucket_id
        type: TREE
    broken_hot:
      format:
      - is_nullable: false
        name: id
        type: number
      - is_nullable: false
        name: reqId
        type: number
      - is_nullable: false
        name: name
        type: string
      - is_nullable: false
        name: department
        type: string
      - is_nullable: false
        name: manager
        type: string
      - is_nullable: false
        name: salary
        type: number
      - is_nullable: false
        name: sysOp
        type: number
      - is_nullable: false
        name: bucket_id
        type: unsigned
      temporary: false
      engine: memtx
      is_local: false
      indexes:
      - unique: true
        parts:
        - path: id
          is_nullable: false
          type: number
        name: id
        type: TREE
      - unique: false
        parts:
        - path: bucket_id
          is_nullable: false
          type: unsigned
        name: bucket_id
        type: TREE
      sharding_key:
      - id
    cola_accounts_history:
      format:
      - type: integer
        name: id
        is_nullable: false
      - type: integer
        name: cola
        is_nullable: false
      - type: integer
        name: colb
        is_nullable: true
      - type: integer
        name: sys_from
        is_nullable: false
      - type: integer
        name: sys_to
        is_nullable: false
      - type: unsigned
        name: bucket_id
        is_nullable: true
      temporary: false
      engine: memtx
      is_local: false
      sharding_key:
      - cola
      indexes:
      - unique: true
        parts:
        - path: id
          type: integer
          is_nullable: false
        name: id
        type: TREE
      - unique: false
        parts:
        - path: cola
          type: integer
          is_nullable: false
        name: cola
        type: TREE
      - unique: false
        parts:
        - path: bucket_id
          type: unsigned
          is_nullable: true
        name: bucket_id
        type: TREE
    col1_col2_transactions_num_actual:
      format:
      - type: number
        name: col1
        is_nullable: false
      - type: integer
        name: col2
        is_nullable: false
      - type: integer
        name: amount
        is_nullable: true
      - type: integer
        name: account_id
        is_nullable: true
      - type: integer
        name: sys_from
        is_nullable: false
      - type: unsigned
        name: bucket_id
        is_nullable: true
      temporary: false
      engine: memtx
      is_local: false
      sharding_key:
      - col1
      - col2
      indexes:
      - unique: true
        parts:
        - path: col1
          type: number
          is_nullable: false
        - path: col2
          type: integer
          is_nullable: false
        name: col1
        type: TREE
      - unique: false
        parts:
        - path: bucket_id
          type: unsigned
          is_nullable: true
        name: bucket_id
        type: TREE
    col1_transactions_actual:
      format:
      - type: integer
        name: col1
        is_nullable: false
      - type: integer
        name: amount
        is_nullable: true
      - type: integer
        name: account_id
        is_nullable: true
      - type: integer
        name: sys_from
        is_nullable: false
      - type: unsigned
        name: bucket_id
        is_nullable: true
      temporary: false
      engine: memtx
      is_local: false
      sharding_key:
      - col1
      indexes:
      - unique: true
        parts:
        - path: col1
          type: integer
          is_nullable: false
        name: col1
        type: TREE
      - unique: false
        parts:
        - path: bucket_id
          type: unsigned
          is_nullable: true
        name: bucket_id
        type: TREE
    space_simple_shard_key:
      format:
      - type: integer
        name: id
        is_nullable: false
      - type: string
        name: name
        is_nullable: true
      - type: integer
        name: sysOp
        is_nullable: false
      - type: unsigned
        name: bucket_id
        is_nullable: true
      temporary: false
      engine: memtx
      is_local: false
      sharding_key:
      - id
      indexes:
      - unique: true
        parts:
        - path: id
          type: integer
          is_nullable: false
        name: id
        type: TREE
      - unique: false
        parts:
        - path: bucket_id
          type: unsigned
          is_nullable: true
        name: bucket_id
        type: TREE
    cola_colb_accounts_actual:
      format:
      - type: integer
        name: id
        is_nullable: false
      - type: integer
        name: cola
        is_nullable: false
      - type: integer
        name: colb
        is_nullable: false
      - type: integer
        name: sys_from
        is_nullable: false
      - type: unsigned
        name: bucket_id
        is_nullable: true
      temporary: false
      engine: memtx
      is_local: false
      sharding_key:
      - cola
      - colb
      indexes:
      - unique: true
        parts:
        - path: id
          type: integer
          is_nullable: false
        name: id
        type: TREE
      - unique: false
        parts:
        - path: cola
          type: integer
          is_nullable: false
        - path: colb
          type: integer
          is_nullable: false
        name: cola
        type: TREE
      - unique: false
        parts:
        - path: bucket_id
          type: unsigned
          is_nullable: true
        name: bucket_id
        type: TREE
    col1_col2_transactions_actual:
      format:
      - type: integer
        name: col1
        is_nullable: false
      - type: integer
        name: col2
        is_nullable: false
      - type: integer
        name: amount
        is_nullable: true
      - type: integer
        name: account_id
        is_nullable: true
      - type: integer
        name: sys_from
        is_nullable: false
      - type: unsigned
        name: bucket_id
        is_nullable: true
      temporary: false
      engine: memtx
      is_local: false
      sharding_key:
      - col1
      - col2
      indexes:
      - unique: true
        parts:
        - path: col1
          type: integer
          is_nullable: false
        - path: col2
          type: integer
          is_nullable: false
        name: col1
        type: TREE
      - unique: false
        parts:
        - path: bucket_id
          type: unsigned
          is_nullable: true
        name: bucket_id
        type: TREE
    t:
      format:
      - type: integer
        name: id
        is_nullable: false
      - type: number
        name: a
        is_nullable: false
      - type: unsigned
        name: bucket_id
        is_nullable: true
      temporary: false
      indexes:
      - unique: true
        parts:
        - path: id
          is_nullable: false
          type: integer
        name: id
        type: TREE
      - unique: false
        parts:
        - path: bucket_id
          is_nullable: true
          type: unsigned
        name: bucket_id
        type: TREE
      is_local: false
      sharding_key:
      - id
      engine: memtx
    testing_space:
      format:
      - type: integer
        name: id
        is_nullable: false
      - type: string
        name: name
        is_nullable: false
      - type: integer
        name: product_units
        is_nullable: false
      - type: unsigned
        name: bucket_id
        is_nullable: true
      temporary: false
      engine: memtx
      is_local: false
      sharding_key:
      - id
      - name
      indexes:
      - unique: true
        parts:
        - path: id
          type: integer
          is_nullable: false
        name: id
        type: TREE
      - unique: false
        parts:
        - path: bucket_id
          type: unsigned
          is_nullable: true
        name: bucket_id
        type: TREE
    col1_col2_transactions_history:
      format:
      - type: integer
        name: id
        is_nullable: false
      - type: integer
        name: col1
        is_nullable: false
      - type: integer
        name: col2
        is_nullable: false
      - type: integer
        name: amount
        is_nullable: true
      - type: integer
        name: account_id
        is_nullable: true
      - type: integer
        name: sys_from
        is_nullable: false
      - type: integer
        name: sys_to
        is_nullable: false
      - type: unsigned
        name: bucket_id
        is_nullable: true
      temporary: false
      engine: memtx
      is_local: false
      sharding_key:
      - col1
      - col2
      indexes:
      - unique: true
        parts:
        - path: id
          type: integer
          is_nullable: false
        name: id
        type: TREE
      - unique: false
        parts:
        - path: col1
          type: integer
          is_nullable: false
        - path: col2
          type: integer
          is_nullable: false
        name: col1
        type: TREE
      - unique: false
        parts:
        - path: bucket_id
          type: unsigned
          is_nullable: true
        name: bucket_id
        type: TREE
    col1_col2_transactions_num_history:
      format:
      - type: number
        name: id
        is_nullable: false
      - type: number
        name: col1
        is_nullable: false
      - type: integer
        name: col2
        is_nullable: false
      - type: integer
        name: amount
        is_nullable: true
      - type: integer
        name: account_id
        is_nullable: true
      - type: integer
        name: sys_from
        is_nullable: false
      - type: integer
        name: sys_to
        is_nullable: false
      - type: unsigned
        name: bucket_id
        is_nullable: true
      temporary: false
      engine: memtx
      is_local: false
      sharding_key:
      - col1
      - col2
      indexes:
      - unique: true
        parts:
        - path: id
          type: number
          is_nullable: false
        name: id
        type: TREE
      - unique: false
        parts:
        - path: col1
          type: number
          is_nullable: false
        - path: col2
          type: integer
          is_nullable: false
        name: col1
        type: TREE
      - unique: false
        parts:
        - path: bucket_id
          type: unsigned
          is_nullable: true
        name: bucket_id
        type: TREE
    testing_space_bucket_in_the_middle:
      format:
      - type: integer
        name: id
        is_nullable: false
      - type: unsigned
        name: bucket_id
        is_nullable: true
      - type: string
        name: name
        is_nullable: false
      - type: integer
        name: product_units
        is_nullable: false
      temporary: false
      engine: memtx
      is_local: false
      sharding_key:
      - id
      - name
      indexes:
      - unique: true
        parts:
        - path: id
          type: integer
          is_nullable: false
        name: id
        type: TREE
      - unique: false
        parts:
        - path: bucket_id
          type: unsigned
          is_nullable: true
        name: bucket_id
        type: TREE
    BROKEN:
      format:
      - is_nullable: false
        name: id
        type: number
      - is_nullable: false
        name: reqId
        type: number
      - is_nullable: false
        name: name
        type: string
      - is_nullable: false
        name: department
        type: string
      - is_nullable: false
        name: manager
        type: string
      - is_nullable: false
        name: salary
        type: number
      - is_nullable: false
        name: sysOp
        type: number
      - is_nullable: false
        name: bucket_id
        type: unsigned
      temporary: false
      engine: memtx
      is_local: false
      sharding_key:
      - id
      indexes:
      - unique: true
        parts:
        - path: id
          is_nullable: false
          type: number
        name: id
        type: TREE
      - unique: false
        parts:
        - path: bucket_id
          is_nullable: false
          type: unsigned
        name: bucket_id
        type: TREE
    space_t1:
      format:
      - type: integer
        name: a
        is_nullable: false
      - type: integer
        name: b
        is_nullable: false
      - type: unsigned
        name: bucket_id
        is_nullable: true
      temporary: false
      engine: memtx
      is_local: false
      sharding_key:
      - a
      indexes:
      - unique: true
        parts:
        - path: a
          type: integer
          is_nullable: false
        name: a
        type: TREE
      - unique: false
        parts:
        - path: bucket_id
          type: unsigned
          is_nullable: true
        name: bucket_id
        type: TREE
    space_simple_shard_key_hist:
      format:
      - type: integer
        name: id
        is_nullable: false
      - type: string
        name: name
        is_nullable: true
      - type: integer
        name: sysOp
        is_nullable: false
      - type: unsigned
        name: bucket_id
        is_nullable: true
      temporary: false
      engine: memtx
      is_local: false
      sharding_key:
      - id
      indexes:
      - unique: true
        parts:
        - path: id
          type: integer
          is_nullable: false
        name: id
        type: TREE
      - unique: false
        parts:
        - path: bucket_id
          type: unsigned
          is_nullable: true
        name: bucket_id
        type: TREE
    col1_transactions_history:
      format:
      - type: integer
        name: id
        is_nullable: false
      - type: integer
        unique: false
        name: col1
        is_nullable: false
      - type: integer
        name: amount
        is_nullable: true
      - type: integer
        name: account_id
        is_nullable: true
      - type: integer
        name: sys_from
        is_nullable: false
      - type: integer
        name: sys_to
        is_nullable: false
      - type: unsigned
        name: bucket_id
        is_nullable: true
      temporary: false
      engine: memtx
      is_local: false
      sharding_key:
      - col1
      indexes:
      - type: TREE
        parts:
        - path: id
          type: integer
          is_nullable: false
        name: id
        unique: true
      - unique: false
        parts:
        - path: col1
          type: integer
          is_nullable: false
        name: col1
        type: TREE
      - unique: false
        parts:
        - path: bucket_id
          type: unsigned
          is_nullable: true
        name: bucket_id
        type: TREE
    cola_colb_accounts_history:
      format:
      - type: integer
        name: id
        is_nullable: false
      - type: integer
        name: cola
        is_nullable: false
      - type: integer
        name: colb
        is_nullable: false
      - type: integer
        name: sys_from
        is_nullable: false
      - type: integer
        name: sys_to
        is_nullable: false
      - type: unsigned
        name: bucket_id
        is_nullable: true
      temporary: false
      engine: memtx
      is_local: false
      sharding_key:
      - cola
      - colb
      indexes:
      - unique: true
        parts:
        - path: cola
          type: integer
          is_nullable: false
        - path: colb
          type: integer
          is_nullable: false
        name: cola
        type: TREE
      - unique: false
        parts:
        - path: bucket_id
          type: unsigned
          is_nullable: true
        name: bucket_id
        type: TREE
    testing_space_hist:
      format:
      - type: integer
        name: id
        is_nullable: false
      - type: string
        name: name
        is_nullable: false
      - type: integer
        name: product_units
        is_nullable: false
      - type: unsigned
        name: bucket_id
        is_nullable: true
      temporary: false
      engine: memtx
      is_local: false
      sharding_key:
      - id
      - name
      indexes:
      - unique: true
        parts:
        - path: id
          type: integer
          is_nullable: false
        name: id
        type: TREE
      - unique: false
        parts:
        - path: bucket_id
          type: unsigned
          is_nullable: true
        name: bucket_id
        type: TREE
    space_for_breake_cache:
      format:
      - type: integer
        name: id
        is_nullable: false
      - type: number
        name: field1
        is_nullable: false
      - type: number
        name: field2
        is_nullable: false
      - type: string
        name: field3
        is_nullable: false
      - type: boolean
        name: field4
        is_nullable: false
      - type: integer
        name: field5
        is_nullable: false
      - type: integer
        name: field6
        is_nullable: false
      - type: integer
        name: field7
        is_nullable: false
      - type: integer
        name: field8
        is_nullable: false
      - type: integer
        name: field9
        is_nullable: false
      - type: string
        name: field10
        is_nullable: false
      - type: string
        name: field11
        is_nullable: false
      - type: integer
        name: field12
        is_nullable: false
      - type: number
        name: field13
        is_nullable: false
      - type: unsigned
        name: bucket_id
        is_nullable: true
      temporary: false
      indexes:
      - unique: true
        parts:
        - path: id
          is_nullable: false
          type: integer
        name: id
        type: TREE
      - unique: false
        parts:
        - path: bucket_id
          is_nullable: true
          type: unsigned
        name: bucket_id
        type: TREE
      is_local: false
      sharding_key:
      - id
      engine: vinyl
    cola_accounts_actual:
      format:
      - type: integer
        name: id
        is_nullable: false
      - type: integer
        name: cola
        is_nullable: false
      - type: integer
        name: colb
        is_nullable: true
      - type: integer
        name: sys_from
        is_nullable: false
      - type: unsigned
        name: bucket_id
        is_nullable: true
      temporary: false
      engine: memtx
      is_local: false
      sharding_key:
      - cola
      indexes:
      - unique: true
        parts:
        - path: cola
          type: integer
          is_nullable: false
        name: cola
        type: TREE
      - unique: false
        parts:
        - path: bucket_id
          type: unsigned
          is_nullable: true
        name: bucket_id
        type: TREE
    dtm__marketing__sales_and_stores_history:
      format:
      - type: integer
        name: id
        is_nullable: false
      - type: string
        name: region
        is_nullable: false
      - type: unsigned
        name: bucket_id
        is_nullable: false
      - type: number
        name: sys_from
        is_nullable: false
      - type: number
        name: sys_to
        is_nullable: true
      - type: number
        name: sys_op
        is_nullable: false
      temporary: false
      indexes:
      - unique: true
        parts:
        - path: id
          is_nullable: false
          type: integer
        - path: region
          is_nullable: false
          type: string
        - path: sys_from
          is_nullable: false
          type: number
        type: TREE
        name: id
      - unique: false
        parts:
        - path: sys_from
          is_nullable: false
          type: number
        type: TREE
        name: x_sys_from
      - unique: false
        parts:
        - path: sys_to
          is_nullable: true
          type: number
        - path: sys_op
          is_nullable: false
          type: number
        type: TREE
        name: x_sys_to
      - unique: false
        parts:
        - path: bucket_id
          is_nullable: false
          type: unsigned
        type: TREE
        name: bucket_id
      is_local: false
      engine: memtx
      sharding_key:
      - id
    dtm__marketing__sales_and_stores_actual:
      format:
      - type: integer
        name: id
        is_nullable: false
      - type: string
        name: region
        is_nullable: false
      - type: unsigned
        name: bucket_id
        is_nullable: false
      - type: number
        name: sys_from
        is_nullable: false
      - type: number
        name: sys_to
        is_nullable: true
      - type: number
        name: sys_op
        is_nullable: false
      temporary: false
      indexes:
      - unique: true
        parts:
        - path: id
          is_nullable: false
          type: integer
        - path: region
          is_nullable: false
          type: string
        - path: sys_from
          is_nullable: false
          type: number
        type: TREE
        name: id
      - unique: false
        parts:
        - path: sys_from
          is_nullable: false
          type: number
        type: TREE
        name: x_sys_from
      - unique: false
        parts:
        - path: bucket_id
          is_nullable: false
          type: unsigned
        type: TREE
        name: bucket_id
      is_local: false
      engine: memtx
      sharding_key:
      - id
````
</details>

Не забудьте нажать кнопку `Apply`.

### Запись и чтение данных
Пример вставки кортежа данных в таблицу посредством SQL Broadcaster:
```
sbroad.execute([[insert into "testing_space" ("id", "name", "product_units") values (?, ?, ?), (?, ?, ?)]], {1, "123", 1, 2, "123", 2})
```
Пример чтения данных из таблицы посредством SQL Broadcaster:
```
sbroad.execute([[select "name" from "testing_space" where "id" = 1]], {})
```

## Поддерживаемые функции стандарта SQL
Приведенный ниже перечень функциональности планировщика отражает соответствие SQL Broadcaster в Picodata требованиями стандарта SQL:2016, а именно ISO/IEC 9075 «Database Language SQL» (Язык баз данных SQL).

### E011. Числовые типы данных
### E011-01. Типы данных INTEGER и SMALLINT (включая все варианты написания).
Подпункт 5.3, Установка знака для беззнакового целого
### E011-02. Типы данных REAL, DOUBLE PRECISION и FLOAT
Подпункт 5.3, Установка знака для приблизительного числового литерала
Подпункт 6.1, Округление числового типа
### E011-03. Типы данных DECIMAL и NUMERIC
Подпункт 5.3, Литералы точных чисел
### E011-05. Числовые сравнения
Подпункт 8.2, Поддержка числовых типов данных, без учета табличного подзапроса и без поддержки Feature F131, «Групповые операции»
### E011-06. Неявное приведение числовых типов данных
Подпункт 8.2, Значения любых числовых типов данных можно сравнивать друг с другом; такие значения сравниваются по отношению к их алгебраическим значениям

Подпункт 9.1, «Назначение получения» и Подпункт 9.2, «Назначение хранения»: Значение одного числового типа можно назначать другому типу с возможным округлением, отсечением и учётом условий вне диапазона.
### E021. Типы символьных строк
### E021-01. Символьный тип данных (включая все варианты написания)
Подпункт 6.1, Соответствие типа CHARACTER лишь одному символьному типу 
Подпункт 13.5, Использование символьного типа CHARACTER для всех поддерживаемых языков
### E021-03. Символьные литералы
Подпункт 5.3
### E021-07. Конкатенация символов
Подпункт 6.29, Выражение конкатенации
### E021-12. Сравнение символов
Подпункт 8.2, Поддержка типов CHARACTER и CHARACTER VARYING, не включая поддержку табличных подзапросов и без поддержки Feature F131, «Групповые операции»
### E031. Идентификаторы
### E031-02. Идентификаторы в нижнем регистре
Подпункт 5.2, Алфавитный символ в обычном идентификаторе может быть как строчным, так и прописным (т.е. идентификаторы без разделителей не обязательно должны содержать только буквы верхнего регистра)
### E031-03. Символ нижнего подчеркивания в конце
Подпункт 5.2, Последний символ в обычном идентификаторе может быть символом подчеркивания.
### E051. Спецификация базовых запросов
### E051-05. Элементы в списке выборки можно переименовывать
Подпункт 7.12, «Спецификация запросов»: как в заголовке пункта
### E051-07. Допускается использование * в квалификаторе для списка выборки
Подпункт 7.12, «Спецификация запросов»: символ умножения
### E051-08. Корреляционные имена в предложении FROM
Подпункт 7.6, «Ссылка на таблицу»: [ AS ] в качестве корреляционного имени
### E061. Базовые предикаты и условия поиска
### E061-01. Предикат сравнения
Подпункт 8.2, «Предикат сравнения»: Для поддерживаемых типов данных, без поддержки табличных подзапросов
### E061-02. Предикат BETWEEN
Подпункт 8.3, «Предикат BETWEEN»
### E061-03. Предикат IN со списком значений
Подпункт 8.4, «Предикат `in`»: Без поддержки табличных подзапросов
### E061-06. Предикат NULL
Подпункт 8.8, «Предикат `null`»: Не включая Feature F481, «Расширенный предикат NULL»
### E061-09. Подзапросы в предикате сравнения
Подпункт 8.2, «Предикат сравнениe»: Включая поддержку табличных подзапросов
### E061-11. Подзапросы в предикате IN
Подпункт 8.4, «Предикат `in`»: Включая поддержку табличных подзапросов
### E061-14. Условие поиска
Подпункт 8.21, «Условие поиска»
### E071. Базовые выражения с запросами
### E071-02. Табличный оператор UNION ALL
Подпункт 7.13, «Выражение с запросом»: Включая поддержку UNION [ ALL ]
### E071-03. Табличный оператор EXCEPT DISTINCT
Подпункт 7.13, «Выражение с запросом»: Включая поддержку EXCEPT [ DISTINCT ]
### E071-05. Столбцы, совмещенные с помощью табличных операторов, необязательно должны иметь идентичный тип данных
Подпункт 7.13, «Выражение с запросом»: Столбцы, совмещенные с помощью UNION и EXCEPT, необязательно должны иметь идентичный тип данных
### E071-06. Табличные операторы в подзапросах
Подпункт 7.13, «Выражение с запросом»: В табличных подзапросах можно указывать UNION и EXCEPT 
### E101. Базовая обработка данных
### E101-01. Инструкция INSERT
Подпункт 14.11, «Инструкция INSERT»: Когда конструктор контекстно типизированного табличного значения может состоять не более чем из одного контекстно типизированного выражения значения строки
### E111. Инструкция SELECT, возвращающая одну строку
Подпункт 14.7, «Инструкция `select`: одна строка»: Не включая поддержку Feature F131, «Групповые операции»
### E131. Поддержка значения NULL (NULL вместо значений)
Подпункт 4.13, «Столбцы, поля и атрибуты»: Способность обнуления 

Подпункт 6.5, «Спецификация контекстно типизированного значения»: Спецификация нулевого значения
### F041. Базовое объединение таблиц
### F041-01. Операция inner join, но необязательно с ключевым словом INNER
Подпункт 7.6, «Ссылка на таблицу»: Раздел про объединения таблиц, но не включая поддержки подфункций с ### F041-02 по ### F041-08
### F041-02. Ключевое слово INNER
Подпункт 7.7, «Объединенная таблица»: тип объединения INNER 
### F041-05.  Вложенные outer join
Подпункт 7.7, «Объединенная таблица»: Подфункция ### F041-01 расширена таким образом, что ссылка на таблицу в объединенной таблице сама может быть объединенной таблицей:
### F041-08. Все операторы сравнения поддерживаются (помимо обычного =.
Подпункт 7.7, «Объединенная таблица»: Подфункция ### F041-01 расширена таким образом, что условие соединения не ограничивается предикатом сравнения с оператором сравнения
### F201. Функция CAST
Подпункт 6.13, «Спецификация CAST» для всех типов данных

Подпункт 6.26, «Выражение значения» для спецификации CAST
### F471. Скалярные значения подзапросов
Подпункт 6.26, «Выражение значения»: Первичное выражение значения может быть скалярным подзапросом.
### T631. Предикат IN с одним элементом списка
Подпункт 8.4, «Предикат `in`»: Список значений 'in' содержит ровно одно выражение значения строки.
### E101-03. Инструкция UPDATE с поиском
### E101-04. Инструкция DELETE с поиском
### E011-04. Арифметические операторы
### E071-01. Табличный оператор UNION DISTINCT
### E071-03. Табличный оператор EXCEPT DISTINCT
### F041-03. LEFT OUTER JOIN.

Подробнее о внутренней архитектуре кластера Picodata см. в разделе [Общая схема инициализации кластера](../clustering). Параметры запуска из командной строки описаны в разделе [Описание параметров запуска](../cli).
