# Обращение к хранилищу

Страница нужна разработчику, который подключает клиент S3 к S3R. Она
описывает, какие клиенты подходят, как задать адрес и подписать запрос, какие гарантии
согласованности действуют и какие операции S3 API поддерживаются.
Отдельные операции разобраны на страницах
[Операции с объектами](objects.md), [Настройки и удаление бакета](buckets.md),
[Управление доступом](access.md) и [Ошибки и повторные запросы](errors.md).

## Совместимость с клиентами {: #clients_compatibility }

Работает любой клиент протокола S3 с подписью версии 4, например `aws-sdk-go`,
`aws-sdk-java`, `boto3`, `aws-sdk-cpp`, `aws-cli`, `s3cmd`, `rclone` и `mc`.
Собственного протокола плагин не вводит. В настройках клиента задаются:

- адрес эндпоинта;
- ключ доступа (см. [Учётные данные](credentials.md));
- адресация в формате пути (см. [Адресация](#address_format)).

## Подпись запросов {: #request_signing }

Запросы подписываются по схеме AWS Signature Version 4. Подпись передаётся:

- в заголовке;
- в параметрах запроса, как в
  [заранее подписанной ссылке](access.md#presigned_links).

Для передачи тела запроса частями поддерживается потоковая подпись
`STREAMING-AWS4-HMAC-SHA256-PAYLOAD` (см.
[Потоковая загрузка](objects.md#streaming_payload)).

## Адресация {: #address_format }

Поддерживаются две формы адреса:

| Форма | Вид адреса | Когда доступна |
| --- | --- | --- |
| Путь (по умолчанию) | `https://s3.example.net/бакет/ключ` | всегда |
| Виртуально-хостовая: имя [бакета](../reference/glossary.md#bucket) входит в доменное имя | `бакет.FQDN` | только для [тенанта](../reference/glossary.md#tenant) с заданным FQDN |

Адресация в формате пути включается в клиенте параметром
`addressing_style: path` или флагом `force_path_style`. На точке входа по
умолчанию, то есть при обращении по IP-адресу или имени [узла](../reference/glossary.md#node), это единственная
поддерживаемая форма.

Тенант с заданным FQDN отвечает и по своему FQDN, и в форме `бакет.FQDN`. Для
второй формы нужна запись DNS с подстановочным знаком на FQDN тенанта. См.
[Тенанты](../concepts/tenants.md).

## Примеры обращения {: #example_requests }

Настройка профиля и работа через AWS CLI:

```shell
aws configure set aws_access_key_id AKIA...
aws configure set aws_secret_access_key ...

aws --endpoint-url https://s3.example.net s3 mb s3://documents
aws --endpoint-url https://s3.example.net s3 cp report.pdf s3://documents/
aws --endpoint-url https://s3.example.net s3 ls s3://documents/
aws --endpoint-url https://s3.example.net s3 rm s3://documents/report.pdf
```

Обращение из Python:

```python
import boto3
from botocore.config import Config

s3 = boto3.client(
    "s3",
    endpoint_url="https://s3.example.net",
    aws_access_key_id="AKIA...",
    aws_secret_access_key="...",
    config=Config(s3={"addressing_style": "path"}),
)
s3.put_object(Bucket="documents", Key="report.pdf", Body=data)
```

!!! note "Сокращение в примерах"
    Далее в примерах вместо `aws --endpoint-url https://s3.example.net` пишется
    `aws`.

## Гарантии согласованности {: #consistency }

- **Записанный объект сразу доступен для чтения.** Метаданные читаются с
  [мастера репликасета](../reference/glossary.md#master), и операции с одним
  ключом линеаризуемы.
- **Объект появляется целиком или не появляется вовсе.** Сначала сохраняется
  содержимое, затем создаётся запись об объекте, и только после этого объект
  становится виден. Прерванная запись не оставляет частично записанного
  объекта.
- **При нескольких [площадках](../reference/glossary.md#site) ответ на запись
  подтверждает сохранение на одной площадке.** Копии на остальные площадки
  создаются после ответа. Объект при этом виден на всех площадках сразу: пока
  копии нет, содержимое запрашивается с площадки, где оно есть (см.
  [Репликация между площадками](../concepts/site_replication.md)).

!!! warning "Исключение: чтение с реплик"
    Если администратор задал параметр `read_preference = any`, чтение объектов
    и списки могут вернуть устаревшие данные: только что записанный объект не
    находится, удалённый ещё читается (см.
    [Чтения возвращают устаревшие данные](../admin/troubleshooting.md#ts_outdated_data)).
    По умолчанию параметр равен `leader`.

Что приложение получает при отказах и перегрузке, описано на странице
[Ошибки и повторные запросы](errors.md).

## Поддерживаемые операции {: #supported_operations }

Не поддерживаются шифрование на стороне сервера, настройка репликации через
S3 API, журналирование доступа к бакету, уведомления, восстановление из
архивного класса, S3 Select и каталожные бакеты S3 Express. Остальные операции
перечислены в таблице по алфавиту.

Нереализованная операция, как правило, возвращает `NotImplemented`. Проверка
доступа может выполняться раньше выбора обработчика, поэтому запрос без
требуемых прав получит ответ `AccessDenied`.

| Операция | Есть | Примечание |
| --- | --- | --- |
| `AbortMultipartUpload` | да | |
| `CompleteMultipartUpload` | да | условная публикация `If-Match` и `If-None-Match` |
| `CopyObject` | да | `CopySource` с `versionId`, директивы `COPY` и `REPLACE` |
| `CreateBucket` | да | в том числе `ObjectLockEnabledForBucket` |
| `CreateMultipartUpload` | да | |
| `DeleteBucket` | да | только для пустого бакета; см. [Удаление бакета](buckets.md#bucket_deletion) |
| `DeleteBucketCors` | да | |
| `DeleteBucketEncryption` | нет | шифрование на стороне сервера не реализовано |
| `DeleteBucketLifecycle` | да | |
| `DeleteBucketPolicy` | да | |
| `DeleteBucketReplication` | нет | [репликация между площадками](../reference/glossary.md#site_replication) настраивается не через S3 API |
| `DeleteBucketTagging` | да | |
| `DeleteBucketWebsite` | да | удаляет конфигурацию; сайт не отдаётся, см. [Ограничения](../reference/limits.md) |
| `DeleteObject` | да | в том числе удаление конкретной версии |
| `DeleteObjects` | да | до 1000 ключей, результат по каждому ключу отдельно |
| `DeleteObjectTagging` | да | |
| `DeletePublicAccessBlock` | да | |
| `GetBucketAcl` | да | |
| `GetBucketCors` | да | 404, если правила не задавались |
| `GetBucketEncryption` | нет | |
| `GetBucketLifecycleConfiguration` | да | |
| `GetBucketLocation` | да | регион задаётся параметром `region` |
| `GetBucketLogging` | нет | |
| `GetBucketNotificationConfiguration` | нет | уведомлений в очереди сообщений нет |
| `GetBucketPolicy` | да | документ возвращается без изменений |
| `GetBucketPolicyStatus` | да | |
| `GetBucketReplication` | нет | |
| `GetBucketTagging` | да | |
| `GetBucketVersioning` | да | |
| `GetBucketWebsite` | да | без конфигурации отвечает `NoSuchWebsiteConfiguration` |
| `GetObject` | да | `Range`, `PartNumber`, условные заголовки, `versionId` |
| `GetObjectAcl` | да | |
| `GetObjectAttributes` | да | `Checksum`, `ObjectParts` с размерами и смещениями; `ETag` возвращается в кавычках |
| `GetObjectLegalHold` | да | |
| `GetObjectLockConfiguration` | да | |
| `GetObjectRetention` | да | режимы `GOVERNANCE` и `COMPLIANCE` |
| `GetObjectTagging` | да | |
| `GetPublicAccessBlock` | да | 404, если настройка не задавалась |
| `HeadBucket` | да | |
| `HeadObject` | да | условные заголовки, `PartNumber`, `versionId` |
| `ListBuckets` | да | в том числе `max-buckets` и `continuation-token` |
| `ListDirectoryBuckets` | нет | каталожные бакеты S3 Express не поддерживаются |
| `ListMultipartUploads` | да | |
| `ListObjects` | да | |
| `ListObjectsV2` | да | |
| `ListObjectVersions` | да | |
| `ListParts` | да | |
| `POST Object` | да | загрузка формой из браузера; см. [Ограничения](../reference/limits.md) |
| `PutBucketAcl` | да | |
| `PutBucketCors` | да | до 100 правил, документ проверяется при записи |
| `PutBucketEncryption` | нет | |
| `PutBucketLifecycleConfiguration` | да | |
| `PutBucketLogging` | нет | |
| `PutBucketNotificationConfiguration` | нет | |
| `PutBucketPolicy` | да | документ проверяется при записи |
| `PutBucketReplication` | нет | |
| `PutBucketTagging` | да | |
| `PutBucketVersioning` | да | `Enabled` и `Suspended` |
| `PutBucketWebsite` | да | конфигурация сохраняется целиком; сайт не отдаётся |
| `PutObject` | да | условная запись `If-Match` и `If-None-Match`, контрольные суммы SHA-256 и CRC64/NVME |
| `PutObjectAcl` | да | |
| `PutObjectLegalHold` | да | |
| `PutObjectLockConfiguration` | да | |
| `PutObjectRetention` | да | |
| `PutObjectTagging` | да | |
| `PutPublicAccessBlock` | да | четыре флага, заменяются целиком |
| `RestoreObject` | нет | архивного [класса хранения](../reference/glossary.md#storage_class) нет |
| `SelectObjectContent` | нет | S3 Select не поддерживается |
| `UploadPart` | да | контрольные суммы SHA-256 и CRC64/NVME |
| `UploadPartCopy` | да | в том числе `copy-source-range` |
