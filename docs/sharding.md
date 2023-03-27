# Шардирование данных в кластере

В данном разделе рассматривается пример шардирования данных в кластере Picodata.

1. Запустим кластер из двух экземпляров

    ```sh
    picodata run --data-dir tmp/i1 --listen localhost:3301 --instance-id i1 --peer localhost:3301,localhost:3302
    picodata run --data-dir tmp/i2 --listen localhost:3302 --instance-id i2 --peer localhost:3301,localhost:3302
    ```

1. Создадим шардированную таблицу. На любом инстансе выполним команды

    ```lua
    pico.add_migration(1, 'create table t2(a int, "bucket_id" unsigned, primary key (a));')
    pico.add_migration(2, 'create index "bucket_id" on t2 ("bucket_id");')
    pico.migrate(2)
    ```

    Ключевым моментом является создание индекса `bucket_id` в спейсе
    (его наличие помечает спейс как шардированный). Важно не забыть
    кавычки, т.к. vshard ожидает увидеть его в нижнем регистре.

    На текущий момент в Picodata используется предопределенное количество бакетов - 3000.

1. Заполним шардированный спейс тестовыми данными. На любом инстансе
   выполним команды.

    ```lua
    for i=1,3000 do vshard.router.callrw(i, 'box.space.T2:insert', {{i, i}}) end
    ```

1. Проверим, что данные поровну распределились между экземплярами. На
   каждом из них (`i1`, `i2`) выполним запрос:

    ```lua
    box.space.T2:len()
    ```

    В результате мы получим по 1500 бакетов (и столько же таплов) на
    каждом экземпляре:

    ```yaml
    ---
    - 1500
    ...
    ```

1. Добавим еще один экземпляр

    ```sh
    picodata run --data-dir tmp/i3 --listen localhost:3303 --instance-id i3 --peer localhost:3301,localhost:3302
    ```

1. Проверим, что данные перераспределились между тремя экземплярами. На
   каждом из них (`i1`, `i2` и `i3`) выполним запрос:

    ```lua
    box.space.T2:len()
    ```

    В результате мы получим по 1000 бакетов на каждом экземпляре:

    ```yaml
    ---
    - 1000
    ...
    ```
