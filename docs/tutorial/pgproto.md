# Pgproto

Модуль pgproto реализует протокол PostgreSQL, путем эмуляции сервера PostgreSQL.
Основная цель pgproto - предоставить пользователям возможность
взаимодействовать с picodata с помощью большого числа хорошо знакомых
инструментов и библиотек, написанных для PostgreSQL.


## Basic setup {: #basic_setup }

Для активации pgproto укажите опцию `--pg-listen`. Например,
запустить узел, принимающий подключения PostgreSQL по адресу 
`localhost:5432`, можно следующим оброазом:
```bash
picodata run --pg-listen localhost:5432
```

Затем создайте пользователя с паролем, совместимым с PostgreSQL.
В настоящее время поддерживается только md5:
```sql
CREATE USER "postgres" WITH PASSWORD 'P@ssw0rd' USING md5
```

И разрешите пользователю создавать таблицы:
```sql
GRANT CREATE TABLE TO "postgres"
```

После этого вы можете подключиться к picodata, используя `psql`.
Выполните следующую команду и введите пароль:
```
SSLMODE=disable psql -U postgres -h localhost -p 5432 -W
```
По умолчанию, SSL отключен в pgproto, поэтому его также
следует отключить на стороне клиента, используя `SSLMODE`.

Теперь вы можете начать интерактивую сессию:
```bash
postgres=> CREATE TABLE WAREHOUSE (
    W_ID INTEGER NOT NULL,
    W_NAME VARCHAR(10) NOT NULL,
    W_TAX DOUBLE,
    W_YTD DOUBLE,
    PRIMARY KEY (W_ID)
)
USING MEMTX DISTRIBUTED BY (W_ID);
CREATE TABLE

postgres=> INSERT INTO WAREHOUSE (W_ID, W_NAME) VALUES (1, 'aaaa'), (2, 'aaab'), (3, 'aaac'), (4, 'aaad');
INSERT 0 4

postgres=> SELECT W_ID, W_NAME FROM WAREHOUSE;
 "W_ID" | "W_NAME"
--------+----------
      1 | aaaa
      2 | aaab
      3 | aaac
      4 | aaad
(4 rows)

postgres=> SELECT W_NAME FROM WAREHOUSE WHERE W_ID=1;
 "W_NAME"
----------
 aaaa
(1 row)
```


## Limitations {: #limitations }

 * Поступающие запросы передаются в текстовом виде в picodata без изменений,
 поэтому возможно выполнение только поддерживаемых в picodata запросов.
 * [Postgres catalog](https://www.postgresql.org/docs/current/catalogs.html)
 пока не поддерживается.
 * Pgproto работает в режиме [autocommit](https://www.postgresql.org/docs/current/ecpg-sql-set-autocommit.html)
 так как picodata не поддерживает интерактивные транзакции.
