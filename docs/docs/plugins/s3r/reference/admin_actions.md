# Справочник административных действий

Справочник для администратора кластера: для каждого административного действия указаны запрос
[API управления](management_api.md), способ выполнить то же средствами SQL, S3 или конфигурации и
раздел руководства с описанием.

## Как читать таблицы {: #admin_actions_legend }

| Столбец | Что в нём |
| --- | --- |
| [API управления](glossary.md#management_api) | запрос; путь указан относительно базового пути `/s3gateway/api`. Запросы к данным [тенанта](glossary.md#tenant) принимают параметр `tenant_id` |
| SQL и другие способы | команда SQL, параметр плагина, операция S3 или диагностический эндпоинт |
| Раздел | где действие описано |

Значение «нет» означает, что этим способом действие не выполняется.

Какой способ выбрать:

- Для большинства действий рекомендуется API управления. Его запросы проверяют права по ролям
  Picodata, записывают в журнал, кто выполнил изменение, и отклоняют недопустимые значения до их
  сохранения.
- Команды SQL и параметры плагина применяются там, где API управления такого действия не
  предоставляет, а также в установке, где API управления не включён.

Порядок включения API описан в разделе
[Адрес и включение](management_api.md#api_control_address), состав ролей описан в разделе
[Ролевая модель тенантов](../concepts/tenants.md#tenants_role_model).

## Установка и конфигурация {: #admin_actions_install }

| Действие | API управления | SQL и другие способы | Раздел |
| --- | --- | --- | --- |
| Установить и включить плагин | нет | `CREATE PLUGIN`, `ALTER PLUGIN ... MIGRATE TO`, `ALTER PLUGIN ... ENABLE` | [Пошаговая установка](../install/manual.md) |
| Изменить параметр плагина | нет | `ALTER PLUGIN s3r 2.2.0 SET <сервис>.<параметр> = '...'` | [Конфигурация](configuration.md) |
| Включить консоль и API управления | нет | `ALTER PLUGIN s3r 2.2.0 SET s3r_api.control_api = 'true'` | [Адрес и включение](management_api.md#api_control_address) |
| Выбрать [схему избыточности](glossary.md#erasure_shape) | нет | `cluster.tier.<тир>.replication_factor` и `instance.failure_domain` в конфигурации Picodata, `s3r_storage.erasure`, [поддомены](glossary.md#sub_domain) дисков `hba=` | [Схема избыточности](../install/planning.md) |
| Проверить [начальное формирование](glossary.md#formation) | `GET /v2/cluster/servers` | `/-/ready`, `SELECT state, step FROM s3r_cluster` | [Начальное формирование](../concepts/formation.md#initial_formation) |
| Удалить плагин | нет | `ALTER PLUGIN ... DISABLE`, `DROP PLUGIN ... WITH DATA` | [Удаление](../install/upgrade_remove.md#plugin_removal) |

## Тенанты {: #admin_actions_tenants }

| Действие | API управления | SQL и другие способы | Раздел |
| --- | --- | --- | --- |
| Создать тенанта | `POST /v2/tenants` | нет: строка, добавленная командой `INSERT`, обходит счётчик идентификаторов, см. предупреждение по ссылке | [Управление тенантами](../admin/tenants.md) |
| Просмотреть тенантов | `GET /v3/tenants`, `GET /v3/tenants/{id}` | `SELECT ... FROM s3r_tenant` | [Управление тенантами](../admin/tenants.md) |
| Изменить FQDN и сертификат тенанта | `PUT /v2/tenants` | `UPDATE s3r_tenant` | [Сертификат тенанта и выбор по SNI](../admin/tls.md#tenant_cert_and_sni) |
| Задать правила CORS по умолчанию для новых [бакетов](glossary.md#bucket) | `PUT /v2/tenants` с полем `default_cors` | `UPDATE s3r_tenant SET default_cors = ...` (без проверки) | [Правила CORS тенанта по умолчанию](../admin/tenants.md#default_cors) |
| Загрузить цепочку для проверки клиентских сертификатов | `PUT /v2/tenants/{id}/load_mtls_cert` | `UPDATE s3r_tenant SET mtls_ca = ...` | [Сертификат тенанта и выбор по SNI](../admin/tls.md#tenant_cert_and_sni) |
| Включить или выключить требование клиентского сертификата | `PUT /v2/tenants/{id}/set_mtls` | нет | [Сертификат тенанта и выбор по SNI](../admin/tls.md#tenant_cert_and_sni) |
| Разрешить администратору тенанта менять материал TLS | `PUT /v2/tenants/extra_rights` | нет | [Состав пространства](management_api.md#space_components) |
| Удалить тенанта | нет | `UPDATE s3r_tenant SET state = 'draining'` | [Управление тенантами](../admin/tenants.md) |

## Пользователи и ключи доступа {: #admin_actions_users }

| Действие | API управления | SQL и другие способы | Раздел |
| --- | --- | --- | --- |
| Создать пользователя | `POST /v2/users` | `CREATE USER` | [Учётные данные](../usage/credentials.md#storage_credentials) |
| Просмотреть пользователей и их ключи | `GET /v2/users` | `SELECT ... FROM s3r_credential` | [Учётные данные](../usage/credentials.md#storage_credentials) |
| Заблокировать или разблокировать пользователя | `PUT /v2/users` | нет; `ALTER USER ... WITH NOLOGIN` запрещает только вход в СУБД, ключи S3 продолжают действовать | [Учётные данные](../usage/credentials.md#storage_credentials) |
| Удалить пользователя из тенанта | `DELETE /v2/users` | нет; `DROP USER` удаляет пользователя Picodata, но оставляет его ключи и квоты | [Учётные данные](../usage/credentials.md#storage_credentials) |
| Задать пароль для входа по SQL | `PUT /v2/users/password` | `ALTER USER ... WITH PASSWORD` | [Пароль для входа по SQL](../usage/credentials.md#sql_client_password) |
| Выдать ключ доступа | `POST /v2/credentials` | параметр `s3r_api.credentials`, только для тенанта 0 | [Учётные данные](../usage/credentials.md#storage_credentials) |
| Заменить секретный ключ | `PUT /v2/credentials/{access_key}` | нет | [Смена и отзыв ключей](../usage/credentials.md#storage_credentials_revoke) |
| Отозвать ключ доступа | `DELETE /v2/credentials/{access_key}` | нет | [Смена и отзыв ключей](../usage/credentials.md#storage_credentials_revoke) |
| Отозвать токены и сеансы консоли пользователя | `POST /v2/revocations` | `INSERT INTO s3r_token_denylist` | [Отзыв доступа](management_api.md#api_revocation) |
| Просмотреть список отзыва | `GET /v2/revocations` | `SELECT ... FROM s3r_token_denylist` | [Отзыв доступа](management_api.md#api_revocation) |
| Снять отзыв | `DELETE /v2/revocations/{kind}/{value}` | `DELETE FROM s3r_token_denylist` | [Отзыв доступа](management_api.md#api_revocation) |

## Бакеты, объекты и права доступа {: #admin_actions_buckets }

| Действие | API управления | SQL и другие способы | Раздел |
| --- | --- | --- | --- |
| Создать бакет для пользователя | `POST /v2/buckets` | S3: `CreateBucket` от имени пользователя | [Поддерживаемые операции](../usage/operations.md#supported_operations) |
| Просмотреть бакеты | `GET /v2/buckets` | S3: `ListBuckets` | [Поддерживаемые операции](../usage/operations.md#supported_operations) |
| Передать бакет другому владельцу | `PUT /v2/buckets` | нет | [Состав пространства](management_api.md#space_components) |
| Удалить пустой бакет | `DELETE /v2/buckets` | S3: `DeleteBucket` | [Поддерживаемые операции](../usage/operations.md#supported_operations) |
| Просмотреть настройки бакета | `GET /v2/buckets/{bucket}/settings` | S3: операции `Get...` бакета | [Поддерживаемые операции](../usage/operations.md#supported_operations) |
| Включить или приостановить версионирование | `PUT /v2/buckets/{bucket}/versioning` | S3: `PutBucketVersioning` | [Версионирование](../usage/buckets.md#object_versioning) |
| Задать правила CORS | `PUT /v2/buckets/{bucket}/cors` | S3: `PutBucketCors` | [Совместное использование ресурсов между источниками (CORS)](../usage/access.md#resources_sharing) |
| Задать политику бакета | `PUT /v2/buckets/{bucket}/policy` | S3: `PutBucketPolicy` | [Политика бакета](../usage/access.md#bucket_policy) |
| Задать правила жизненного цикла | `PUT /v2/buckets/{bucket}/lifecycle` | S3: `PutBucketLifecycleConfiguration` | [Правила жизненного цикла](../usage/buckets.md#lifecycle_rules) |
| Настроить блокировку публичного доступа | `PUT /v2/buckets/{bucket}/public-access` | S3: `PutPublicAccessBlock` | [Блокировка публичного доступа](../usage/access.md#public_access_block) |
| Включить сжатие | `PUT /v2/buckets/{bucket}/compression` | `UPDATE s3r_bucket SET compress = true`, `UPDATE s3r_tenant SET compress = true` | [Сжатие](../concepts/tenants.md#compression) |
| Включить [дедупликацию](glossary.md#deduplication) | `PUT /v2/buckets/{bucket}/deduplication` | `UPDATE s3r_bucket SET dedup = true` | [Дедупликация](../concepts/tenants.md#deduplication) |
| Выдать пользователю права на бакет | `PUT /v2/acl` | S3: `PutBucketAcl` | [Списки управления доступом](../usage/access.md#acl) |
| Просмотреть [классы хранения](glossary.md#storage_class) кластера | `GET /v2/storage-classes` | `SELECT ... FROM s3r_tier` | [Классы хранения S3 и ярусы](../concepts/storage_classes.md#storage_classes_and_tiers) |
| Просмотреть объекты бакета | `GET /v2/objects` | S3: `ListObjectsV2` | [Поддерживаемые операции](../usage/operations.md#supported_operations) |
| Удалить объекты | `DELETE /v2/objects`, `POST /v2/objects/delete` | S3: `DeleteObject`, `DeleteObjects` | [Массовое удаление](../usage/objects.md#mass_deletion) |
| Получить [заранее подписанную ссылку](glossary.md#presigned_url) | `POST /v2/objects/presign` | `aws s3 presign` | [Заранее подписанные ссылки и анонимный доступ](../usage/access.md#presigned_links) |

## Квоты {: #admin_actions_quotas }

| Действие | API управления | SQL и другие способы | Раздел |
| --- | --- | --- | --- |
| Просмотреть квоты и потребление | `GET /v2/quotas` | `SELECT ... FROM s3r_quota`, `s3r_quota_usage` | [Квоты и ограничение скорости](../admin/quotas.md) |
| Задать или снять квоту тенанта, пользователя или бакета | `PUT /v2/quotas` | `INSERT INTO s3r_quota`, `DELETE FROM s3r_quota` | [Квоты и ограничение скорости](../admin/quotas.md) |
| Задать квоты бакета | `PUT /v2/buckets/{bucket}/quota` | `INSERT INTO s3r_quota` с областью `bucket` | [Квоты и ограничение скорости](../admin/quotas.md) |

## Диски, контроль целостности и сверка {: #admin_actions_storage }

| Действие | API управления | SQL и другие способы | Раздел |
| --- | --- | --- | --- |
| Просмотреть диски | `GET /v2/disks` | `SELECT ... FROM s3r_disk` | [Состояние кластера](../admin/monitoring.md#cluster_state) |
| Просмотреть [группы размещения](glossary.md#placement_group) | `GET /v2/placement-groups` | `SELECT ... FROM s3r_pg` | [Состояние кластера](../admin/monitoring.md#cluster_state) |
| Перевести диск в обслуживание | `POST /v2/disks/{disk_id}/drain` | нет | [Замена исправного диска](../admin/disks.md#healthy_disk_replacement) |
| Добавить диск на работающий [инстанс](glossary.md#instance) | `POST /v2/disks` | `S3R_DISKS` или `s3r_storage.disks` только при первом запуске инстанса | [Действия с дисками](../admin/disks.md) |
| Удалить диск | `DELETE /v2/disks/{disk_id}` | нет | [Действия с дисками](../admin/disks.md) |
| Признать диск вышедшим из строя | `POST /v2/disks/{disk_id}/failed` | нет | [Действия с дисками](../admin/disks.md) |
| Переформатировать диск | `POST /v2/disks/{disk_id}/reformat` | нет | [Действия с дисками](../admin/disks.md) |
| Вернуть группы на позиции плана | `POST /v2/placement-groups/{pg_id}/reform`, `POST /v2/placement-groups/reform` | нет | [Группы не на своих позициях](../concepts/placement_groups.md#asymmetric_groups) |
| Запросить проверку | `POST /v2/repair` | нет | [Запрос проверки](../admin/integrity.md#repair_request) |
| Узнать состояние контроля целостности по областям | `GET /v2/repair` | нет | [Состояние по областям](../admin/integrity.md#repair_areas) |
| Просмотреть группы, ожидающие [ресинхронизации](glossary.md#resync), и повреждённые [полосы](glossary.md#stripe) | `GET /v2/repair/data` | `SELECT ... FROM s3r_scrub_pg`, `s3r_t<N>_damage` | [Контроль целостности](../admin/integrity.md) |
| Просмотреть диски и группы, требующие внимания | `GET /v2/repair/disks`, `GET /v2/repair/groups` | `SELECT ... FROM s3r_disk`, `s3r_pg` | [Состояние по областям](../admin/integrity.md#repair_areas) |
| Приостановить или возобновить фоновый контроль целостности | `PUT /v2/repair/mode` | нет | [Приостановка фонового контроля целостности](../admin/integrity.md#repair_pause) |
| Просмотреть результаты сканирования [каталога](glossary.md#catalog) и изменить его расписание | `GET /v2/repair/catalog`, `PUT /v2/repair/catalog` | `SELECT ... FROM s3r_catalog_pass` | [Расписание сверки каталога](../admin/integrity.md#catalog_schedule) |
| Просмотреть [репликацию между площадками](glossary.md#site_replication) | `GET /v2/replication` | `SELECT count(*) FROM s3r_t<N>_siterepl` | [Наблюдение за репликацией между площадками](../admin/monitoring.md#monitor_placements_replication) |

## Кластер и наблюдение {: #admin_actions_cluster }

| Действие | API управления | SQL и другие способы | Раздел |
| --- | --- | --- | --- |
| Проверить учётные данные и роли | `GET /v2/rights` | нет | [Состав пространства](management_api.md#space_components) |
| Просмотреть ёмкость | `GET /v2/capacity` | `SELECT ... FROM s3r_disk` | [Состояние кластера](../admin/monitoring.md#cluster_state) |
| Просмотреть расход памяти под метаданные | `GET /v2/memory` | нет | [Состав пространства](management_api.md#space_components) |
| Проверить состояние подсистем | `GET /v2/health` | `/-/health` проверяет только работу процесса | [Диагностические эндпоинты](endpoints.md#diagnostic_endpoints) |
| Просмотреть состав кластера | `GET /v2/cluster/servers` | `/-/debug/cluster` | [Диагностические эндпоинты](endpoints.md#diagnostic_endpoints) |
| Добавить или вывести [узел](glossary.md#node) | нет | средства Picodata | [Добавление и вывод узлов](../admin/nodes.md#adding_and_enabling_nodes) |
| Выполнить шаг фонового процесса | нет | `/-/debug/run/<процесс>` | [Фоновые процессы](../concepts/background.md#background_processes) |
