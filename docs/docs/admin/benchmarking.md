# Тестирование производительности

В данном разделе приведена информация о тестировании производительности Picodata.

Для замера производительности используется утилита [Pgbench], которая
подходит как для PostgreSQL, так и Picodata. Основой теста выступает
TPC-B, используемый в Pgbench по умолчанию.

## Системные требования {: #benchmark_requirements }

Убедитесь, что в вашей системе установлены следующие компоненты:

- Python 3.12 или новее
- [Poetry] (система управления зависимостями Python)
- [Pgbench] (инструмент тестирования СУБД)

При необходимости установите отсутствующие компоненты с помощью
пакетного менеджера ОС.

[Poetry]: https://python-poetry.org/docs
[Pgbench]: https://www.postgresql.org/docs/current/pgbench.html

## Использование стандартного теста TPC-B {: #benchmark_setup_and_run }

### Предварительные действия {: #prerequisites }

Для установки и запуска стандартного теста TPC-B требуется работающий кластер Picodata.

Читайте далее:

- [Установка Picodata](../tutorial/install.md)
- [Создание кластера](../tutorial/deploy.md)

Также, необходимо иметь в кластере пользователя `postgres`, обладающего правами на работу с таблицами. Пример создания и настройки пользователя:
```sql
CREATE USER postgres WITH PASSWORD 'Passw0rd';
GRANT CREATE TABLE TO postgres;
GRANT READ TABLE TO postgres;
GRANT WRITE TABLE TO postgres;
```

Читайте далее:

- [Управление доступом](access_control.md)

### Установка, инициализация и запуск теста {: #benchmark_run }

Файлы теста находятся в директории `benchmark/tpcb` в [git-репозитории Picodata].

1. Откройте окно/вкладку терминала в корневой директории исходного кода
   Picodata и установите зависимости теста:
```shell
poetry install
```

1. Инициализируйте тест:
```shell
poetry run python benchmark/tpcb/init.py \
    "postgres://postgres:Passw0rd@127.0.0.1:4327?sslmode=disable" \
    --scale 10

```

1. Запустите тест:
```shell
pgbench \
    "postgres://postgres:Passw0rd@127.0.0.1:4327?sslmode=disable" \
    --scale 10 \
    --time 30 \
    --client 200 \
    --protocol prepared \
    --jobs 1 --progress 1 --no-vacuum
```
[git-репозитории Picodata]: https://git.picodata.io/core/picodata/

### Изменение параметров теста {: #benchmark_customize }

При необходимости задайте собственные параметры для приведённой выше команды `pgbench`:

- данные пользователя. Укажите имя и пароль пользователя, под которым вы подключаетесь к инстансу Picodata
- адрес подключения. В примере выше используется инстанс, принимающий подключения по протоколу PostgreSQL по адресу `127.0.0.1:4327`. Убедитесь, что в вашем случае указано актуальное значение этого параметра (см. [instance.pg.listen])
- коэффициент масштабирования (`--scale 10`). Используйте значение, указанное при инициализации теста. При использовании более высокого значения `scale`, в кластере может возникнуть ошибка выделения памяти. Для обхода этой проблемы следует увеличить значение параметра [instance.memtx.memory] или [instance.vinyl.memory] (в зависимости от используемого [движка])
- длительность теста (`--time 30`). Для снижения погрешностей иногда есть смысла увеличить время
- количество одновременных клиентов (`--client 200`). В зависимости от
  сценария, оптимальное значение может меняться в большую или меньшую сторону
- использовать подготовленные запросы (prepared statements, `--protocol prepared`). Этот режим позволяет добиться максимальной производительности теста

!!! note "Примечание"
    Описание всех параметров можно найти в
    [официальной документации
    Pgbench](https://www.postgresql.org/docs/current/pgbench.html)

[instance.pg.listen]: ../reference/config.md#instance_pg_listen
[instance.memtx.memory]: ../reference/config.md#instance_memtx_memory
[instance.vinyl.memory]: ../reference/config.md#instance_vinyl_memory
[движка]: ../overview/glossary.md#db_engine

## Использование собственных тестов {: #custom_benchmarks }

Утилита [Pgbench] позволяет тестировать произвольные SQL-запросы, переданные в параметре `--file`.

См. также:

- [Использование собственных скриптов в Pgbench](https://www.postgresql.org/docs/current/pgbench.html#id-1.9.4.11.9.3)

Пример запуска теста с явным указанием скрипта:

```shell
pgbench \
    "postgres://postgres:Passw0rd@127.0.0.1:4327?sslmode=disable" \
    --file script.sql \
    --scale 10 \
    --time 30 \
    --client 200 \
    --protocol prepared \
    --jobs 1 --progress 1 --no-vacuum
```

Пример скрипта по умолчанию для TPC-B:

```sql title="script.sql"
\set aid random(1, 100000 * :scale)
\set bid random(1, 1 * :scale)
\set tid random(1, 10 * :scale)
\set delta random(-5000, 5000)
BEGIN;
UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
UPDATE pgbench_tellers SET tbalance = tbalance + :delta WHERE tid = :tid;
UPDATE pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid;
INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
END;
```

!!! note "Примечание"
    При использовании собственного скрипта может
    потребоваться изменить содержимое файла `init.py` или предусмотреть
    собственный способ инициализации теста. Файлы поставляемого с
    Picodata теста производительности находятся в директории
    `benchmark/tpcb` в [git-репозитории Picodata].
