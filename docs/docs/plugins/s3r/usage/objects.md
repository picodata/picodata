# Операции с объектами

Страница описывает операции с объектами, которые не сводятся к одному вызову
или ведут себя иначе, чем в Amazon S3: условные запросы, загрузку большими
объёмами, контрольные суммы, чтение части объекта, копирование и массовое
удаление. Она нужна разработчику клиентского приложения. Полный перечень
операций приведён в таблице
[Поддерживаемые операции](operations.md#supported_operations).

Разделы идут в порядке: запись, чтение, копирование, удаление. Каждый раздел
содержит описание, пример и перечень особенностей и ограничений. Примеры
приведены для AWS CLI; тот же порядок действий воспроизводится любым SDK,
работающим по протоколу S3.

## Ключи объектов и логическая структура {: #object_keys }

[Ключ объекта](../reference/glossary.md#object_key) представляет собой произвольную строку.
Символ `/` в ключе образует логическую структуру директорий: перечисление с
разделителем `/` возвращает объекты текущего уровня в `Contents`, а вложенные
уровни в `CommonPrefixes`.

```shell
aws s3api list-objects-v2 --bucket documents --prefix проекты/ --delimiter /
```

Перечисление одного уровня не требует обхода всех ключей бакета: строки
директорий создаются при записи объекта и удаляются фоновым процессом, когда
директория становится пустой.

Префикс служит также единицей разграничения доступа.
[Политика бакета](access.md#bucket_policy) с ресурсом
`arn:aws:s3:::documents/проекты/*` действует на всю вложенную директорию, в том
числе при [массовом удалении](#mass_deletion), где право проверяется
для каждого ключа отдельно.

## Метаданные объекта {: #object_metadata }

Пользовательские метаданные передаются
заголовками `x-amz-meta-*` и возвращаются в ответах `GET` и `HEAD`.
Сохраняются также заголовки `Content-Type`, `Content-Encoding`,
`Content-Disposition`, `Content-Language`, `Cache-Control` и `Expires`.

```shell
aws s3api put-object --bucket documents --key report.pdf --body report.pdf \
    --content-type application/pdf \
    --metadata 'author=alice,department=finance'

aws s3api head-object --bucket documents --key report.pdf
```

При [копировании](#object_copy) метаданные по умолчанию переносятся,
а параметр `--metadata-directive REPLACE` заменяет их значениями из запроса.

## Условная запись объекта {: #object_put }

Условные заголовки позволяют выполнить запись или чтение, только если объект
находится в ожидаемом состоянии. `PutObject` принимает два условных заголовка:

| Заголовок | Запись разрешена, если |
| --- | --- |
| `If-None-Match: *` | ключа ещё нет |
| `If-Match: "<etag>"` | текущее содержимое объекта имеет указанный `ETag` |

На этих двух проверках строятся взаимное исключение и оптимистическое
обновление без внешнего координатора.

```shell
# Создать, только если ключа нет
aws s3api put-object --bucket documents --key lock --body /dev/null \
    --if-none-match '*'
# Второй такой же вызов вернёт PreconditionFailed (412)

# Обновить, только если содержимое не менялось
aws s3api put-object --bucket documents --key state.json --body new.json \
    --if-match '"d41d8cd98f00b204e9800998ecf8427e"'
```

**Особенности и ограничения**

- Запрос с `If-Match` к отсутствующему ключу возвращает `NoSuchKey` (404), а не
  412.
- Те же два заголовка принимает `CompleteMultipartUpload` (см.
  [Составная загрузка](#multipart_upload)).

### Условное чтение {: #conditional_read }

`GetObject` и `HeadObject` поддерживают условные заголовки `If-Match`,
`If-None-Match`, `If-Modified-Since` и `If-Unmodified-Since`.

| Не выполнено условие | Ответ |
| --- | --- |
| `If-Match` или `If-Unmodified-Since` | 412 (`PreconditionFailed`) |
| `If-None-Match` или `If-Modified-Since` | 304 (`Not Modified`) без тела, с заголовком `ETag` объекта |

Условия проверяются в порядке, который задаёт RFC 9110: сначала `If-Match`,
затем `If-Unmodified-Since`, затем `If-None-Match` и `If-Modified-Since`. Из
этого следует:

- запрос, для которого не выполнены условия `If-Unmodified-Since` и
  `If-None-Match`, получает ответ 412;
- условие `If-Unmodified-Since` не проверяется, если задан `If-Match`;
- условие `If-Modified-Since` не проверяется, если задан `If-None-Match`.

## Потоковая загрузка {: #streaming_payload }

Поддерживаются клиенты, которые подписывают тело запроса частями
(`Content-Encoding: aws-chunked`,
`x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD`).

**Особенности и ограничения**

- Тело запроса не накапливается в памяти целиком: данные кодируются и
  передаются на диски по мере поступления. Поэтому размер объекта ограничен
  только свободным местом.

## Составная загрузка {: #multipart_upload }

При [составной загрузке](../reference/glossary.md#multipart_upload) клиент
передаёт объект частями, а хранилище собирает части в единое целое. Как
правило, клиент выполняет составную загрузку автоматически: `aws s3 cp`
переходит на неё для файлов больше порога `multipart_threshold`. Явная
последовательность вызовов:

```shell
# 1. Начать загрузку
aws s3api create-multipart-upload --bucket documents --key big.bin
# { "UploadId": "0197f2c1..."  }

# 2. Передать части
aws s3api upload-part --bucket documents --key big.bin \
    --upload-id 0197f2c1... --part-number 1 --body part1.bin
# { "ETag": "\"9b2c...\""  }

# 3. Собрать объект
aws s3api complete-multipart-upload --bucket documents --key big.bin \
    --upload-id 0197f2c1... \
    --multipart-upload '{"Parts":[{"PartNumber":1,"ETag":"\"9b2c...\""}]}'
```

Часть можно взять из существующего объекта, не передавая её по сети:

```shell
aws s3api upload-part-copy --bucket documents --key merged.bin \
    --upload-id 0197f2c1... --part-number 2 \
    --copy-source documents/source.bin --copy-source-range bytes=0-5242879
```

Незавершённые загрузки можно просмотреть и отменить:

```shell
aws s3api list-multipart-uploads --bucket documents
aws s3api list-parts --bucket documents --key big.bin --upload-id 0197f2c1...
aws s3api abort-multipart-upload --bucket documents --key big.bin \
    --upload-id 0197f2c1...
```

**Особенности и ограничения**

- Номера частей лежат в диапазоне от 1 до 10000.
- Все части, кроме последней, должны иметь размер не менее 5 МиБ, иначе
  завершение загрузки возвращает ошибку `EntityTooSmall`.
- `ETag` собранного объекта имеет вид `"<md5 склеенных md5 частей>-<число
  частей>"` и намеренно не совпадает с MD5 содержимого, как и в Amazon S3.
- `CompleteMultipartUpload`, как и `PutObject`, принимает условные заголовки
  `If-Match` и `If-None-Match` (см. [Условная запись объекта](#object_put)).
  Условие проверяется в момент публикации собранного объекта.
- Незавершённую загрузку, начатую раньше чем `mpu_abort_incomplete_after_s`
  секунд назад, прерывает фоновый процесс `mpugc`, и её части освобождаются.
  Параметр задаёт администратор (см.
  [Сервис s3r_storage](../reference/configuration.md#s3r_storage)). Тот же
  результат даёт [правило жизненного цикла](buckets.md#lifecycle_rules)
  `AbortIncompleteMultipartUpload`.

## Контрольные суммы S3 {: #s3_checksums }

Клиент может передать контрольную сумму содержимого при записи и получить её
при чтении. Поддерживаются CRC64/NVME и SHA-256. Значения `ChecksumSHA256` и
`ChecksumCRC64NVME` передаются в кодировке Base64.

```shell
sha256=$(openssl dgst -sha256 -binary report.pdf | base64 -w0)
aws s3api put-object --bucket documents --key report.pdf \
    --body report.pdf --checksum-sha256 "$sha256"

aws s3api head-object --bucket documents --key report.pdf \
    --checksum-mode ENABLED
aws s3api get-object-attributes --bucket documents --key report.pdf \
    --object-attributes Checksum
```

Если переданное значение не совпадает с принятым содержимым, запрос завершается
ответом `400 BadDigest` и запись не публикуется.

### Когда вычисляется SHA-256 {: #sha256_when }

При записи всегда вычисляется контрольная сумма CRC64/NVME. Вычисление SHA-256
занимает заметную долю процессорного времени записи, поэтому эта сумма
вычисляется, только если для [бакета](../reference/glossary.md#bucket) включена
[дедупликация](../concepts/tenants.md#deduplication) или клиент запросил её
одним из способов:

- передал значение `ChecksumSHA256` в запросе `PutObject` или `UploadPart`, в
  заголовке или в завершающем заголовке (`x-amz-trailer`) после тела запроса;
- указал алгоритм `SHA256` в параметре `ChecksumAlgorithm` запроса `PutObject`,
  `UploadPart`, `CreateMultipartUpload` или `CopyObject`.

### Что возвращается при чтении {: #checksums_read }

- `GetObject` и `HeadObject` возвращают контрольные суммы только при
  `ChecksumMode=ENABLED` и только те, которые были вычислены при записи
  объекта: CRC64/NVME всегда, SHA-256 при условиях из предыдущего раздела.
  Так же отвечает Amazon S3.
- Ответ на чтение диапазона или части не содержит контрольной суммы всего
  объекта, потому что содержит только часть его данных.
- `GetObjectAttributes` с атрибутом `Checksum` возвращает те же суммы без
  чтения содержимого.

### Объекты из нескольких частей и копии {: #checksums_multipart_copy }

- Составная контрольная сумма SHA-256 возвращается для объекта из нескольких
  частей, если загрузка создана запросом `CreateMultipartUpload` с алгоритмом
  `SHA256`. Тогда SHA-256 вычисляется для каждой части, а составная сумма равна
  сумме SHA-256 от последовательности двоичных сумм SHA-256 всех частей в
  кодировке Base64 с суффиксом `-<число частей>`.
- Для загрузки, созданной без алгоритма, контрольная сумма объекта не
  возвращается.
- Составная сумма CRC64/NVME сейчас не возвращается.
- Копия всего объекта, созданная одним запросом `CopyObject` или
  `UploadPartCopy`, получает контрольные суммы источника. Если копии нужна
  сумма SHA-256, которой у источника нет, она вычисляется при копировании.

## Чтение диапазона {: #range_read }

`GetObject` возвращает часть объекта, заданную диапазоном байтов или номером
части.

```shell
aws s3api get-object --bucket documents --key big.bin \
    --range bytes=1048576-2097151 part.bin
```

Для объекта, собранного составной загрузкой, диапазон можно задать номером
части:

```shell
aws s3api get-object --bucket documents --key big.bin --part-number 3 part3.bin
```

**Особенности и ограничения**

- Поддерживаются диапазоны вида `bytes=начало-конец`, `bytes=начало-` и
  `bytes=-длина`.
- Ответ содержит заголовок `Content-Range`, а ответ на запрос по номеру части
  дополнительно содержит заголовок `x-amz-mp-parts-count`.
- С дисков читаются только те части
  [полос](../reference/glossary.md#stripe), в которые попадает диапазон.
- Ответ не содержит контрольной суммы всего объекта (см.
  [Что возвращается при чтении](#checksums_read)).

## Атрибуты объекта {: #object_attributes }

`GetObjectAttributes` возвращает запрошенные сведения об объекте одним ответом,
без чтения содержимого.

```shell
aws s3api get-object-attributes --bucket documents --key big.bin \
    --object-attributes ETag ObjectSize StorageClass ObjectParts
```

**Особенности и ограничения**

- Для объекта, собранного составной загрузкой, атрибут `ObjectParts`
  перечисляет части с номерами и размерами и поддерживает постраничный обход с
  параметрами `--max-parts` и `--part-number-marker`.
- В отличие от Amazon S3, поле `ETag` в ответе этой операции сейчас
  возвращается в кавычках: так значение представляет используемая библиотека
  протокола. Для проверки содержимого используйте атрибут `Checksum`.

## Копирование объекта {: #object_copy }

Копия создаётся на стороне сервера, и содержимое объекта по сети не передаётся.

```shell
aws s3api copy-object --bucket archive --key report-2026.pdf \
    --copy-source documents/report.pdf
```

Источником может служить конкретная версия объекта:

```shell
aws s3api copy-object --bucket archive --key report-old.pdf \
    --copy-source 'documents/report.pdf?versionId=0000019874f3a1c2'
```

**Особенности и ограничения**

- По умолчанию метаданные копируются (`--metadata-directive COPY`). Значение
  `REPLACE` заменяет пользовательские метаданные и тип содержимого значениями
  из запроса. Аналогично работает параметр `--tagging-directive` для тегов.
  Другие значения этих параметров отклоняются с ошибкой `InvalidArgument`.
- Для условного копирования используются заголовок `x-amz-copy-source-if-match`
  и родственные ему заголовки.
- Копирование объекта в самого себя допускается только с параметром
  `--metadata-directive REPLACE`, иначе возвращается ошибка `InvalidRequest`.
- Та же ошибка `InvalidRequest` возвращается, если `versionId` источника в
  `CopyObject` или `UploadPartCopy` указывает на
  [маркер удаления](../reference/glossary.md#delete_marker).
- Контрольные суммы копии описаны в разделе
  [Объекты из нескольких частей и копии](#checksums_multipart_copy).
- Список частей составного объекта читается страницами по 1000 частей. Если
  источник удалён, а прочитаны ещё не все страницы, запрос `CopyObject` или
  `UploadPartCopy` ничего не записывает и возвращает ошибку `503 SlowDown`.
  Клиенты S3 повторяют такой запрос сами. Повторный запрос копирует объект в
  его текущем состоянии, а если источник удалён, возвращает ошибку `NoSuchKey`.
  Amazon S3 в таком случае копирует источник в том состоянии, в котором он был
  в начале запроса.

## Проверка владельца бакета {: #expected_bucket_owner }

Заголовок `x-amz-expected-bucket-owner` защищает операцию над бакетом, который
пересоздан другим владельцем: при несовпадении владельца сервер возвращает
`AccessDenied`. Для бакета-источника в `CopyObject` и `UploadPartCopy`
применяется заголовок `x-amz-source-expected-bucket-owner`. Форма загрузки из
браузера (`POST Object`) передаёт ожидаемого владельца в поле
`x-amz-expected-bucket-owner`, которое проверяется так же, как заголовок.

**Особенности и ограничения**

- В отличие от Amazon S3, ожидаемым значением служит имя пользователя Picodata,
  которому принадлежит бакет, а не идентификатор учётной записи AWS.

## Массовое удаление {: #mass_deletion }

`DeleteObjects` удаляет несколько объектов или версий одним запросом.

```shell
cat > delete.json <<'EOF'
{"Objects": [{"Key": "a.txt"}, {"Key": "b.txt", "VersionId": "0000019874f3a1c2"}],
 "Quiet": false}
EOF

aws s3api delete-objects --bucket documents --delete file://delete.json
```

**Особенности и ограничения**

- Один запрос принимает до 1000 ключей.
- Права проверяются для каждого ключа отдельно, поэтому часть ключей в ответе
  может оказаться в списке `Deleted`, а часть в списке `Errors`.
