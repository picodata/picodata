# Установка и пример использования SQL Broadcaster
## Общие сведения для установки
На данный момент компонент SQL Broadcaster работает только для приложений, использующих Tarantool Cartridge. Однако, при этом требуется использовать версию Tarantool, которая поставляется вместе с программным продуктом Picodata. Подробная информация об установке SQL Broadcaster приведена в описании его [Git-репозитория](https://git.picodata.io/picodata/picodata/sbroad). Установка возможна либо путём компиляции исходного кода, либо посредством утилиты `tarantoolctl`. В обоих случаях требуется выполнять команды из директории Cartridge-приложения. Для ознакомления и тестирования удобно воспользоваться [тестовым приложением](https://git.picodata.io/picodata/picodata/sbroad/-/tree/main/sbroad-cartridge/test_app), поставляемым вместе с кодом SQL Broadcaster.

## Пример установки для CentOS 7
Ниже показан пример установки сборочных зависимостей и компиляции исходного кода SQL Broadcaster в операционной системе CentOS 7. Шаги для других дистрибутивов Linux могут отличаться. Для начала требуется подключить репозиторий с пакетами Picodata. Этот этап документирован для [страницы загрузки Picodata](https://picodata.io/download/). Далее предлагается выполнить в терминале следующие команды:

<style>
  code {
    white-space : pre-wrap !important;
    word-break: break-word;
  }
</style>

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
## Пример использования
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

## Запись и чтение данных
Пример вставки кортежа данных в таблицу посредством SQL Broadcaster:
```
sbroad.execute([[insert into "testing_space" ("id", "name", "product_units") values (?, ?, ?), (?, ?, ?)]], {1, "123", 1, 2, "123", 2})
```
Пример чтения данных из таблицы посредством SQL Broadcaster:
```
sbroad.execute([[select "name" from "testing_space" where "id" = 1]], {})
```

См. также: [Справочник поддерживаемых функций стандарта SQL](../sql_reference)
