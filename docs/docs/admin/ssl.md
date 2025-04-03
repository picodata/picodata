# Включение протокола SSL {: #enable_ssl }

При подключении к Picodata рекомендуется использовать протокол [SSL],
который обеспечивает безопасную коммуникацию между клиентом (окном
терминала или клиентским API) и сервером ([инстансом] Picodata).

[SSL]: https://ru.wikipedia.org/wiki/SSL
[инстансом]: ../overview/glossary.md#instance

Picodata поддерживает SSL при подключении по протоколу [PostgreSQL].
Безопасное соединение задействуется автоматически если оно
поддерживается (и настроено) на стороне инстанса Picodata ("сервера
PostgreSQL").

!!! note "Примечание"
    Если в конфигурации инстанса включен параметр
    [instance.pg.ssl], то небезопасные соединения (без SSL) при этом
    становятся запрещены.

При использовании аутентификации [LDAP] также рекомендуется включить SSL
для того, чтобы пароль пользователя не передавался по сети в открытом
виде.

[PostgreSQL]: ../tutorial/connecting.md#pgproto
[LDAP]: ldap.md
[instance.pg.ssl]: ../reference/config.md#instance_pg_ssl

Чтобы настроить использование SSL, выполните следующие шаги:

1. Задайте в [файле конфигурации](../reference/config.md#instance_pg_ssl)
    параметр `instance.pg.ssl: true`

1. Добавьте в [рабочую директорию инстанса](../reference/cli.md#run_instance_dir)
    `<INSTANCE_DIR>` SSL-сертификат и ключ `server.crt`, `server.key`

1. (опционально) Для включения [mTLS] добавьте в
    [рабочую директорию инстанса](../reference/cli.md#run_instance_dir) `<INSTANCE_DIR>`
    SSL-сертификат `ca.crt`. В результате PostgreSQL-сервер в Picodata будет
    принимать подключения только в том случае, если клиент предоставит сертификат,
    подписанный с помощью `ca.crt`.


[mTLS]: https://en.wikipedia.org/wiki/Mutual_authentication
