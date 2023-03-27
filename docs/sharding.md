# Шардирование данных в кластере

В данном разделе рассматривается пример шардирования данных в кластере Picodata.

1. Запустим кластер из двух экземпляров
    ```sh
    picodata run  --listen localhost:3301 --instance-id i1 --cluster-id c1 --peer localhost:3301,localhost:3302,localhost:3303

    picodata run  --listen localhost:3302 --instance-id i2 --cluster-id c1 --peer localhost:3301,localhost:3302,localhost:3303
    ```
1. Создадим шардированную таблицу
    
    Ключевым моментом является создание индекса `bucket_id` в спейсе (его наличие помечает спейс как шардированный). 
    ```lua
    pico.add_migration(1, 'create table t2(a int, "bucket_id" unsigned, primary key (a));')

    pico.add_migration(2, 'create index "bucket_id" on t2 ("bucket_id");')

    pico.migrate(2)
    ```
    На текущий момент в Picodata используется предопределенное количество бакетов - 3000.
1. Заполним шардированный спейс тестовыми данными
    ```lua
    for i=1,3000 do vshard.router.callrw(i, 'box.space.T2:insert', {{i, i}}) end
    ```
1. Проверим, что данные поровну распределились между экземплярами.
    На каждом из них (`i1`, `i2`) выполним запрос:
    ```lua
    vshard.storage.info().bucket
    ```
    В результате мы получим по 1500 бакетов на каждом экземпляре:
    ```
    ---
    - receiving: 0
    active: 1500
    total: 1500
    garbage: 0
    pinned: 0
    sending: 0
    ...
    ```
1. Добавим еще один экземпляр
    ```sh
    picodata run  --listen localhost:3303 --instance-id i3 --cluster-id c1 --peer localhost:3301,localhost:3302,localhost:3303
    ```
1. Запустим ребалансировку данных

    Важный момент, что ребалансер бакетов работает на экземпляре с наименьшим занчением `instance_uuid` из спейса `_picodata_instance`.
    ```sql
    \set language sql

    select "instance_uuid", "instance_id" from "_picodata_instance"

    \set language lua
    ```
    В результате получим:
    ```
    ---
    - metadata:
    - name: instance_uuid
        type: string
    - name: instance_id
        type: string
    rows:
    - ['68d4a766-4144-3248-aeb4-e212356716e4', 'i1']
    - ['24c4ac5f-4981-3441-879c-aee1edb608a6', 'i2']
    - ['5d7a7353-3e82-30fd-af0d-261436544389', 'i3']
    ...
    ```
    В данном случае наименьшее значение UUID имеет экземпляр `i2` - `24c4ac5f-4981-3441-879c-aee1edb608a6`. Поэтому ребалансировка будет запущена на нем (но мы для примера запустим ее вручную).
    ```lua
    vshard.storage.rebalancer_wakeup()
    ```
1. Проверим, что данные перераспределились между тремя экземплярами.
    На каждом из них (`i1`, `i2` и `i3`) выполним запрос:
    ```lua
    vshard.storage.info().bucket
    ```
    В результате мы получим по 1000 бакетов на каждом экземпляре:
    ```
    ---
    - receiving: 0
    active: 1000
    total: 1000
    garbage: 0
    pinned: 0
    sending: 0
    ...
    ```
