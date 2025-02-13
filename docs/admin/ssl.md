# Включение протокола SSL {: #enable_ssl }

При подключении к Picodata рекомендуется использовать протокол [SSL],
который обеспечивает безопасную коммуникацию между клиентом (окном
терминала или клиентским API) и сервером ([инстансом] Picodata).

[SSL]: https://ru.wikipedia.org/wiki/SSL
[инстансом]: ../overview/glossary.md#instance

Picodata поддерживает SSL при подключении по протоколу [PostgreSQL].
Безопасное соединение автоматически задействуется если оно
поддерживается (и настроено) на стороне сервера. При использовании
аутентификации [LDAP] также рекомендуется включить SSL для того, чтобы
пароль пользователя не передавался по сети в открытом виде.

[PostgreSQL]: ../tutorial/connecting.md#pgproto
[LDAP]: ldap.md

Чтобы настроить использование SSL, сделайте следующее:

1. Задайте в [файле конфигурации](../reference/config.md#instance_pg_ssl)
    параметр `instance.pg.ssl: true`

1. Добавьте в [рабочую директорию инстанса](../reference/cli.md#run_instance_dir)
    `<DATA_DIR>` SSL-сертификат и ключ `server.crt`, `server.key`

1. (опционально) Для включения [mTLS] добавьте в
    [рабочую директорию инстанса](../reference/cli.md#run_instance_dir) `<DATA_DIR>`
    SSL-сертификат `ca.crt`. В результате PostgreSQL-сервер в Picodata будет
    принимать подключения только в том случае, если клиент предоставит сертификат,
    подписанный с помощью `ca.crt`.


[mTLS]: https://en.wikipedia.org/wiki/Mutual_authentication
